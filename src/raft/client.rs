use super::{Request, Response, Status};
use crate::error::{Error, Result};

use tokio::sync::{mpsc, oneshot};

/// A client for a local Raft server.
/// 一个本地raft服务端
#[derive(Clone)]
pub struct Client {
    request_tx: mpsc::UnboundedSender<(Request, oneshot::Sender<Result<Response>>)>,
}

impl Client {
    /// Creates a new Raft client.
    pub fn new(
        request_tx: mpsc::UnboundedSender<(Request, oneshot::Sender<Result<Response>>)>,
    ) -> Self {
        Self { request_tx }
    }

    /// Executes a request against the Raft cluster.
    /// 对raft集群执行请求
    async fn request(&self, request: Request) -> Result<Response> {
        let (response_tx, response_rx) = oneshot::channel();
        // 将请求以及返回通道的发送对象打包为元组发送给raft serve
        self.request_tx.send((request, response_tx))?;
        response_rx.await?
    }

    /// Mutates the Raft state machine.
    /// 改变raft状态机
    pub async fn mutate(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        match self.request(Request::Mutate(command)).await? {
            Response::State(response) => Ok(response),
            resp => Err(Error::Internal(format!("Unexpected Raft mutate response {:?}", resp))),
        }
    }

    ///  Queries the Raft state machine. More than this
    /// 在raft客户端执行一次查询
    /// Query a raft command
    pub async fn query(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        match self.request(Request::Query(command)).await? {
            Response::State(response) => Ok(response),
            resp => Err(Error::Internal(format!("Unexpected Raft query response {:?}", resp))),
        }
    }

    /// Fetches Raft node status.
    pub async fn status(&self) -> Result<Status> {
        match self.request(Request::Status).await? {
            Response::Status(status) => Ok(status),
            resp => Err(Error::Internal(format!("Unexpected Raft status response {:?}", resp))),
        }
    }
}
