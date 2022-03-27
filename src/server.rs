use crate::error::{Error, Result};
use crate::raft;
use crate::sql;
use crate::sql::engine::{Engine as _, Mode};
use crate::sql::execution::ResultSet;
use crate::sql::schema::{Catalog as _, Table};
use crate::sql::types::Row;
use crate::storage::{kv, log};

use ::log::{error, info};
use futures::sink::SinkExt as _;
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::StreamExt as _;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// A toyDB server.
pub struct Server {
    raft: raft::Server,
    raft_listener: Option<TcpListener>,
    sql_listener: Option<TcpListener>,
}

impl Server {
    /// Creates a new toyDB server.
    pub async fn new(
        id: &str,
        peers: HashMap<String, String>,
        raft_store: Box<dyn log::Store>,
        sql_store: Box<dyn kv::Store>,
    ) -> Result<Self> {
        Ok(Server {
            // raft服务器，包含raft节点管理以及raft服务线程、主事件循环线程的创建
            raft: raft::Server::new(
                id,
                peers,
                raft::Log::new(raft_store)?,
                Box::new(sql::engine::Raft::new_state(kv::MVCC::new(sql_store))?),
            )
            .await?,
            raft_listener: None,
            sql_listener: None,
        })
    }

    /// Starts listening on the given ports. Must be called before serve.
    /// 开始监听指定端口
    pub async fn listen(mut self, sql_addr: &str, raft_addr: &str) -> Result<Self> {
        let (sql, raft) =
            tokio::try_join!(TcpListener::bind(sql_addr), TcpListener::bind(raft_addr),)?;
        info!("Listening on {} (SQL) and {} (Raft)", sql.local_addr()?, raft.local_addr()?);
        self.sql_listener = Some(sql);
        self.raft_listener = Some(raft);
        Ok(self)
    }

    /// Serves Raft and SQL requests until the returned future is dropped. Consumes the server.
    /// 开启服务
    pub async fn serve(self) -> Result<()> {
        let sql_listener = self
            .sql_listener
            .ok_or_else(|| Error::Internal("Must listen before serving".into()))?;
        let raft_listener = self
            .raft_listener
            .ok_or_else(|| Error::Internal("Must listen before serving".into()))?;
        // 创建一条管道
        let (raft_tx, raft_rx) = mpsc::unbounded_channel();

        // 发送者位于本地raft服务,而本地raft服务位于sql引擎中。
        // 可见：
        // 客户端连接的是toydb的sql服务地址,sql服务会将raft日志通过管道传输给raft服务
        // raft服务将日志与其他节点进行同步
        // 现在问题是如何接收到其他raft节点的信息,也是通过sql listener还是raft listener
        // 如果是,那么raft服务如何将操作发送给sql引擎。
        // 还有一种可能，raft服务也包括处理本地数据，这样只需要由raft服务与其他节点进行通信即可同步数据
        // sql listener只需要负责接收客户端的消息并生成日志给raft服务即可
        let sql_engine = sql::engine::Raft::new(raft::Client::new(raft_tx));

        tokio::try_join!(
            // raft服务,用于和其他raft节点进行通信
            self.raft.serve(raft_listener, raft_rx),
            Self::serve_sql(sql_listener, sql_engine),
        )?;
        Ok(())
    }

    /// Serves SQL clients.
    async fn serve_sql(listener: TcpListener, engine: sql::engine::Raft) -> Result<()> {
        let mut listener = TcpListenerStream::new(listener);
        // 获取一个连接
        while let Some(socket) = listener.try_next().await? {
            let peer = socket.peer_addr()?;
            let session = Session::new(engine.clone())?;
            tokio::spawn(async move {
                info!("Client {} connected", peer);
                match session.handle(socket).await {
                    Ok(()) => info!("Client {} disconnected", peer),
                    Err(err) => error!("Client {} error: {}", peer, err),
                }
            });
        }
        Ok(())
    }
}

/// A client request.
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Execute(String),
    GetTable(String),
    ListTables,
    Status,
}

/// A server response.
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Execute(ResultSet),
    Row(Option<Row>),
    GetTable(Table),
    ListTables(Vec<String>),
    Status(sql::engine::Status),
}

/// A client session coupled to a SQL session.
/// 与sql会话耦合的客户端会话
/// 
/// TCP Session
pub struct Session {
    engine: sql::engine::Raft,
    // 从raft引擎中获取的session
    // 这里
    sql: sql::engine::Session<sql::engine::Raft>,
}

impl Session {
    /// Creates a new client session.
    /// 创建一个新的session
    fn new(engine: sql::engine::Raft) -> Result<Self> {
        Ok(Self { sql: engine.session()?, engine })
    }

    /// Handles a client connection.
    /// TCP链接会话处理客户端来的消息
    async fn handle(mut self, socket: TcpStream) -> Result<()> {
        let mut stream = tokio_serde::Framed::new(
            Framed::new(socket, LengthDelimitedCodec::new()),
            tokio_serde::formats::Bincode::default(),
        );
        // 1. 接收客户端请求
        while let Some(request) = stream.try_next().await? {
            // 获取响应结果
            let mut response = tokio::task::block_in_place(|| self.request(request));

            // 创建响应结果迭代器
            let mut rows: Box<dyn Iterator<Item = Result<Response>> + Send> =
                Box::new(std::iter::empty());

            // 如果是查询的返回结果
            if let Ok(Response::Execute(ResultSet::Query { rows: ref mut resultrows, .. })) =
                &mut response
            {
                rows = Box::new(
                    // 查询的行数据复制到空迭代器中
                    std::mem::replace(resultrows, Box::new(std::iter::empty()))
                    // 每行数据用Response::Row重新包装
                        .map(|result| result.map(|row| Response::Row(Some(row))))
                        // 在迭代器最后加入一个空行
                        .chain(std::iter::once(Ok(Response::Row(None))))
                        // 过滤异常，如果迭代器中间出现一个异常，那么它后续的所有数据都置为None
                        .scan(false, |err_sent, response| match (&err_sent, &response) {
                            (true, _) => None,
                            (_, Err(error)) => {
                                *err_sent = true;
                                Some(Err(error.clone()))
                            }
                            _ => Some(response),
                        })
                        // 清空None，只保留迭代器最后的一个None
                        .fuse(),
                );
            }
            stream.send(response).await?;
            stream.send_all(&mut tokio_stream::iter(rows.map(Ok))).await?;
        }
        Ok(())
    }

    /// Executes a request.
    /// TCP会话执行请求
    /// 
    /// TCP Session直接访问其中的引擎和数据库会话
    pub fn request(&mut self, request: Request) -> Result<Response> {
        Ok(match request {
            Request::Execute(query) => Response::Execute(self.sql.execute(&query)?),
            Request::GetTable(table) => Response::GetTable(
                self.sql.with_txn(Mode::ReadOnly, |txn| txn.must_read_table(&table))?,
            ),
            Request::ListTables => {
                Response::ListTables(self.sql.with_txn(Mode::ReadOnly, |txn| {
                    Ok(txn.scan_tables()?.map(|t| t.name).collect())
                })?)
            }
            Request::Status => Response::Status(self.engine.status()?),
        })
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        tokio::task::block_in_place(|| self.sql.execute("ROLLBACK").ok());
    }
}
