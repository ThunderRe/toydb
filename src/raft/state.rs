use super::{Address, Entry, Event, Message, Response, Scan, Status};
use crate::error::{Error, Result};

use log::{debug, error};
use std::collections::{BTreeMap, HashMap, HashSet};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt as _;

/// A Raft-managed state machine.
/// raft管理状态机
pub trait State: Send {
    /// Returns the last applied index from the state machine, used when initializing the driver.
    fn applied_index(&self) -> u64;

    /// Mutates the state machine. If the state machine returns Error::Internal, the Raft node
    /// halts. For any other error, the state is applied and the error propagated to the caller.
    fn mutate(&mut self, index: u64, command: Vec<u8>) -> Result<Vec<u8>>;

    /// Queries the state machine. All errors are propagated to the caller.
    /// 查询状态机
    fn query(&self, command: Vec<u8>) -> Result<Vec<u8>>;
}

#[derive(Debug, PartialEq)]
/// A driver instruction.
/// 一条驱动指令,针对的是raft状态机
pub enum Instruction {
    /// Abort all pending operations, e.g. due to leader change.
    /// 终止所有挂起操作, 例如对leader的变更
    Abort,
    /// Apply a log entry.
    /// 应用日志条目
    Apply { entry: Entry },
    /// Notify the given address with the result of applying the entry at the given index.
    /// 在给定索引处应用条目的结果通知给定地址
    Notify { id: Vec<u8>, address: Address, index: u64 },
    /// Query the state machine when the given term and index has been confirmed by vote.
    /// 当给定的term和index都通过投票确认时查询状态机
    Query { id: Vec<u8>, address: Address, command: Vec<u8>, term: u64, index: u64, quorum: u64 },
    /// Extend the given server status and return it to the given address.
    /// 扩展给定的服务器状态并将其返回给给定的地址
    Status { id: Vec<u8>, address: Address, status: Box<Status> },
    /// Votes for queries at the given term and commit index.
    /// 一条查询给定term和已提交索引的投票
    Vote { term: u64, index: u64, address: Address },
}

/// A driver query.
/// 驱动查询对象
struct Query {
    id: Vec<u8>,
    term: u64,
    address: Address,
    command: Vec<u8>,
    quorum: u64,
    votes: HashSet<Address>,
}

/// Drives a state machine, taking operations from state_rx and sending results via node_tx.
/// 驱动状态机，从state_rx获取操作并通过node_tx发送
pub struct Driver {
    state_rx: UnboundedReceiverStream<Instruction>, // 接收来自raft节点的消息
    node_tx: mpsc::UnboundedSender<Message>,        // 发送给主事件循环线程的队列
    applied_index: u64,
    /// Notify clients when their mutation is applied. <index, (client, id)>
    notify: HashMap<u64, (Address, Vec<u8>)>,
    /// Execute client queries when they receive a quorum. <index, <id, query>>
    queries: BTreeMap<u64, BTreeMap<Vec<u8>, Query>>,
}

impl Driver {
    /// Creates a new state machine driver.
    /// 创建一个新的状态机驱动
    pub fn new(
        state_rx: mpsc::UnboundedReceiver<Instruction>,
        node_tx: mpsc::UnboundedSender<Message>,
    ) -> Self {
        Self {
            state_rx: UnboundedReceiverStream::new(state_rx),
            node_tx,
            applied_index: 0,
            notify: HashMap::new(),
            queries: BTreeMap::new(),
        }
    }

    /// Drives a state machine.
    /// 驱动状态机
    pub async fn drive(mut self, mut state: Box<dyn State>) -> Result<()> {
        debug!("Starting state machine driver");
        while let Some(instruction) = self.state_rx.next().await {
            if let Err(error) = self.execute(instruction, &mut *state).await {
                error!("Halting state machine due to error: {}", error);
                return Err(error);
            }
        }
        debug!("Stopping state machine driver");
        Ok(())
    }

    /// Synchronously (re)plays a set of log entries, for initial sync.
    pub fn replay<'a>(&mut self, state: &mut dyn State, mut scan: Scan<'a>) -> Result<()> {
        while let Some(entry) = scan.next().transpose()? {
            debug!("Replaying {:?}", entry);
            if let Some(command) = entry.command {
                match state.mutate(entry.index, command) {
                    Err(error @ Error::Internal(_)) => return Err(error),
                    _ => self.applied_index = entry.index,
                }
            }
        }
        Ok(())
    }

    /// Executes a state machine instruction.
    /// 执行一条状态机指令, 命令来自raft节点
    pub async fn execute(&mut self, i: Instruction, state: &mut dyn State) -> Result<()> {
        debug!("Executing {:?}", i);
        match i {
            Instruction::Abort => {
                self.notify_abort()?;
                self.query_abort()?;
            }

            Instruction::Apply { entry: Entry { index, command, .. } } => {
                if let Some(command) = command {
                    debug!("Applying state machine command {}: {:?}", index, command);
                    match tokio::task::block_in_place(|| state.mutate(index, command)) {
                        Err(error @ Error::Internal(_)) => return Err(error),
                        result => self.notify_applied(index, result)?,
                    };
                }
                // We have to track applied_index here, separately from the state machine, because
                // no-op log entries are significant for whether a query should be executed.
                self.applied_index = index;
                // Try to execute any pending queries, since they may have been submitted for a
                // commit_index which hadn't been applied yet.
                self.query_execute(state)?;
            }

            Instruction::Notify { id, address, index } => {
                if index > state.applied_index() {
                    self.notify.insert(index, (address, id));
                } else {
                    self.send(address, Event::ClientResponse { id, response: Err(Error::Abort) })?;
                }
            }

            // 执行一次查询
            Instruction::Query { id, address, command, index, term, quorum } => {
                self.queries.entry(index).or_default().insert(
                    id.clone(),
                    Query { id, term, address, command, quorum, votes: HashSet::new() },
                );
            }

            Instruction::Status { id, address, mut status } => {
                status.apply_index = state.applied_index();
                self.send(
                    address,
                    Event::ClientResponse { id, response: Ok(Response::Status(*status)) },
                )?;
            }

            Instruction::Vote { term, index, address } => {
                // 将地址添加到给定条件的查询记录中
                self.query_vote(term, index, address);
                // 执行记录
                self.query_execute(state)?;
            }
        }
        Ok(())
    }

    /// Aborts all pending notifications.
    fn notify_abort(&mut self) -> Result<()> {
        for (_, (address, id)) in std::mem::take(&mut self.notify) {
            self.send(address, Event::ClientResponse { id, response: Err(Error::Abort) })?;
        }
        Ok(())
    }

    /// Notifies a client about an applied log entry, if any.
    fn notify_applied(&mut self, index: u64, result: Result<Vec<u8>>) -> Result<()> {
        if let Some((to, id)) = self.notify.remove(&index) {
            self.send(to, Event::ClientResponse { id, response: result.map(Response::State) })?;
        }
        Ok(())
    }

    /// Aborts all pending queries.
    fn query_abort(&mut self) -> Result<()> {
        for (_, queries) in std::mem::take(&mut self.queries) {
            for (id, query) in queries {
                self.send(
                    query.address,
                    Event::ClientResponse { id, response: Err(Error::Abort) },
                )?;
            }
        }
        Ok(())
    }

    /// Executes any queries that are ready.
    /// 执行任何读取到的查询
    fn query_execute(&mut self, state: &mut dyn State) -> Result<()> {
        for query in self.query_ready(self.applied_index) {
            debug!("Executing query {:?}", query.command);
            // 在这里调用raft状态机来执行查询操作，raft状态机拥有一个基于kv的sql引擎可以用来读写本地数据
            let result = state.query(query.command);
            if let Err(error @ Error::Internal(_)) = result {
                return Err(error);
            }
            self.send(
                query.address,
                Event::ClientResponse { id: query.id, response: result.map(Response::State) },
            )?
        }
        Ok(())
    }

    /// Fetches and removes any ready queries, where index <= applied_index.
    fn query_ready(&mut self, applied_index: u64) -> Vec<Query> {
        let mut ready = Vec::new();
        let mut empty = Vec::new();
        for (index, queries) in self.queries.range_mut(..=applied_index) {
            let mut ready_ids = Vec::new();
            for (id, query) in queries.iter_mut() {
                if query.votes.len() as u64 >= query.quorum {
                    ready_ids.push(id.clone());
                }
            }
            for id in ready_ids {
                if let Some(query) = queries.remove(&id) {
                    ready.push(query)
                }
            }
            if queries.is_empty() {
                empty.push(*index)
            }
        }
        for index in empty {
            self.queries.remove(&index);
        }
        ready
    }

    /// Votes for queries up to and including a given commit index for a term by an address.
    /// 查询包括给定的commit索引和term的查询记录，用于投票
    fn query_vote(&mut self, term: u64, commit_index: u64, address: Address) {
        // 获取所有commit index小于给定commit index的记录
        for (_, queries) in self.queries.range_mut(..=commit_index) {
            for (_, query) in queries.iter_mut() {
                if term >= query.term {
                    // 将地址加入到查询中
                    query.votes.insert(address.clone());
                }
            }
        }
    }

    /// Sends a message.
    /// 发送给主事件的消息
    fn send(&self, to: Address, event: Event) -> Result<()> {
        let msg = Message { from: Address::Local, to, term: 0, event };
        debug!("Sending {:?}", msg);
        Ok(self.node_tx.send(msg)?)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Debug)]
    pub struct TestState {
        commands: Arc<Mutex<Vec<Vec<u8>>>>,
        applied_index: Arc<Mutex<u64>>,
    }

    impl TestState {
        pub fn new(applied_index: u64) -> Self {
            Self {
                commands: Arc::new(Mutex::new(Vec::new())),
                applied_index: Arc::new(Mutex::new(applied_index)),
            }
        }

        pub fn list(&self) -> Vec<Vec<u8>> {
            self.commands.lock().unwrap().clone()
        }
    }

    impl State for TestState {
        fn applied_index(&self) -> u64 {
            *self.applied_index.lock().unwrap()
        }

        // Appends the command to the internal commands list.
        fn mutate(&mut self, index: u64, command: Vec<u8>) -> Result<Vec<u8>> {
            self.commands.lock()?.push(command.clone());
            *self.applied_index.lock()? = index;
            Ok(command)
        }

        // Appends the command to the internal commands list.
        fn query(&self, command: Vec<u8>) -> Result<Vec<u8>> {
            self.commands.lock()?.push(command.clone());
            Ok(command)
        }
    }

    async fn setup() -> Result<(
        Box<TestState>,
        mpsc::UnboundedSender<Instruction>,
        mpsc::UnboundedReceiver<Message>,
    )> {
        let state = Box::new(TestState::new(0));
        let (state_tx, state_rx) = mpsc::unbounded_channel();
        let (node_tx, node_rx) = mpsc::unbounded_channel();
        tokio::spawn(Driver::new(state_rx, node_tx).drive(state.clone()));
        Ok((state, state_tx, node_rx))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn driver_abort() -> Result<()> {
        let (state, state_tx, node_rx) = setup().await?;

        state_tx.send(Instruction::Notify {
            id: vec![0x01],
            index: 1,
            address: Address::Peer("a".into()),
        })?;
        state_tx.send(Instruction::Query {
            id: vec![0x02],
            address: Address::Client,
            command: vec![0xf0],
            term: 1,
            index: 1,
            quorum: 2,
        })?;
        state_tx.send(Instruction::Vote { term: 1, index: 1, address: Address::Local })?;
        state_tx.send(Instruction::Abort)?;
        std::mem::drop(state_tx);

        let node_rx = UnboundedReceiverStream::new(node_rx);
        assert_eq!(
            node_rx.collect::<Vec<_>>().await,
            vec![
                Message {
                    from: Address::Local,
                    to: Address::Peer("a".into()),
                    term: 0,
                    event: Event::ClientResponse { id: vec![0x01], response: Err(Error::Abort) }
                },
                Message {
                    from: Address::Local,
                    to: Address::Client,
                    term: 0,
                    event: Event::ClientResponse { id: vec![0x02], response: Err(Error::Abort) }
                }
            ]
        );
        assert_eq!(state.list(), Vec::<Vec<u8>>::new());
        assert_eq!(state.applied_index(), 0);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn driver_apply() -> Result<()> {
        let (state, state_tx, node_rx) = setup().await?;

        state_tx.send(Instruction::Notify {
            id: vec![0x01],
            index: 2,
            address: Address::Client,
        })?;
        state_tx.send(Instruction::Apply { entry: Entry { index: 1, term: 1, command: None } })?;
        state_tx.send(Instruction::Apply {
            entry: Entry { index: 2, term: 1, command: Some(vec![0xaf]) },
        })?;
        std::mem::drop(state_tx);

        let node_rx = UnboundedReceiverStream::new(node_rx);
        assert_eq!(
            node_rx.collect::<Vec<_>>().await,
            vec![Message {
                from: Address::Local,
                to: Address::Client,
                term: 0,
                event: Event::ClientResponse {
                    id: vec![0x01],
                    response: Ok(Response::State(vec![0xaf]))
                }
            }]
        );
        assert_eq!(state.list(), vec![vec![0xaf]]);
        assert_eq!(state.applied_index(), 2);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn driver_query() -> Result<()> {
        let (_, state_tx, node_rx) = setup().await?;

        state_tx.send(Instruction::Query {
            id: vec![0x01],
            address: Address::Client,
            command: vec![0xf0],
            term: 2,
            index: 1,
            quorum: 2,
        })?;
        state_tx.send(Instruction::Apply {
            entry: Entry { index: 1, term: 2, command: Some(vec![0xaf]) },
        })?;
        state_tx.send(Instruction::Vote { term: 2, index: 1, address: Address::Local })?;
        state_tx.send(Instruction::Vote {
            term: 2,
            index: 1,
            address: Address::Peer("a".into()),
        })?;
        std::mem::drop(state_tx);

        let node_rx = UnboundedReceiverStream::new(node_rx);
        assert_eq!(
            node_rx.collect::<Vec<_>>().await,
            vec![Message {
                from: Address::Local,
                to: Address::Client,
                term: 0,
                event: Event::ClientResponse {
                    id: vec![0x01],
                    response: Ok(Response::State(vec![0xf0]))
                }
            }]
        );

        Ok(())
    }

    // A query for an index submitted in a given term cannot be satisfied by votes below that term.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn driver_query_noterm() -> Result<()> {
        let (_, state_tx, node_rx) = setup().await?;

        state_tx.send(Instruction::Query {
            id: vec![0x01],
            address: Address::Client,
            command: vec![0xf0],
            term: 2,
            index: 1,
            quorum: 2,
        })?;
        state_tx.send(Instruction::Apply {
            entry: Entry { index: 1, term: 1, command: Some(vec![0xaf]) },
        })?;
        state_tx.send(Instruction::Vote { term: 2, index: 1, address: Address::Local })?;
        state_tx.send(Instruction::Vote {
            term: 1,
            index: 1,
            address: Address::Peer("a".into()),
        })?;
        std::mem::drop(state_tx);

        let node_rx = UnboundedReceiverStream::new(node_rx);
        assert_eq!(node_rx.collect::<Vec<_>>().await, vec![]);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn driver_query_noquorum() -> Result<()> {
        let (_, state_tx, node_rx) = setup().await?;

        state_tx.send(Instruction::Query {
            id: vec![0x01],
            address: Address::Client,
            command: vec![0xf0],
            term: 1,
            index: 1,
            quorum: 2,
        })?;
        state_tx.send(Instruction::Apply {
            entry: Entry { index: 1, term: 1, command: Some(vec![0xaf]) },
        })?;
        state_tx.send(Instruction::Vote { term: 1, index: 1, address: Address::Local })?;
        std::mem::drop(state_tx);

        let node_rx = UnboundedReceiverStream::new(node_rx);
        assert_eq!(node_rx.collect::<Vec<_>>().await, vec![]);

        Ok(())
    }
}
