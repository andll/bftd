use crate::block::{ValidatorIndex, MAX_BLOCK_SIZE};
use crate::metrics::Metrics;
use crate::network::NoisePublicKey;
use crate::network::{Connection, ConnectionPool, NetworkMessage};
use bytes::Bytes;
use futures::future::{join_all, select_all, Either};
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::task::JoinHandle;
use tokio::time::Instant;

// todo task accounting / spawning to avoid deadlocks
// todo stream buffer control test
pub struct NetworkRpc {
    handle: JoinHandle<()>,
    peer_task_senders: HashMap<NoisePublicKey, mpsc::Sender<PeerRpcTaskCommand>>,
    connection_status: HashMap<NoisePublicKey, Arc<AtomicBool>>,
    connection_counter: Arc<AtomicUsize>,
    connection_status_changed: Arc<Notify>,
    stop: oneshot::Sender<()>,
}

#[derive(Serialize, Deserialize)]
pub struct NetworkRequest(pub Bytes);

#[derive(Serialize, Deserialize)]
pub struct NetworkResponse(pub Bytes);

#[derive(Serialize, Deserialize)]
pub enum RpcMessage {
    RpcRequest(u64, NetworkRequest),
    RpcStreamRequest(u64, NetworkRequest),
    RpcResponse(u64, NetworkResponse),
    RpcStreamItem(u64, NetworkResponse),
    RpcStreamBufferAck(u64 /*rpc id*/, u64 /*position ack*/),
    RpcStreamEOF(u64),
}

const STREAM_BUFFER_SIZE: u64 = (MAX_BLOCK_SIZE as u64) * 10;

pub enum PeerRpcTaskCommand {
    Rpc(NetworkRequest, oneshot::Sender<RpcResult<NetworkResponse>>),
    StreamRpc(NetworkRequest, mpsc::Sender<RpcResult<NetworkResponse>>),
}

pub trait NetworkRpcRouter: Send {
    fn rpc(&mut self, req: NetworkRequest) -> NetworkResponse;
    fn stream_rpc(&mut self, req: NetworkRequest) -> mpsc::Receiver<NetworkResponse>;
    fn connected(&mut self) -> Option<PeerRpcTaskCommand> {
        None
    }
}

impl NetworkRpc {
    pub fn start<Pool: ConnectionPool>(
        pool: Pool,
        peer_routers: HashMap<NoisePublicKey, Box<dyn NetworkRpcRouter>>,
        metrics: Arc<Metrics>,
    ) -> Self {
        let connection_status: HashMap<_, _> = peer_routers
            .keys()
            .map(|k| (k.clone(), Default::default()))
            .collect();
        let (senders, receivers): (Vec<_>, Vec<_>) = peer_routers
            .into_iter()
            .map(|(k, router)| {
                let (s, r) = mpsc::channel::<PeerRpcTaskCommand>(10);
                let data = PeerTaskData {
                    router,
                    receiver: r,
                };
                ((k.clone(), s), (k, data))
            })
            .unzip();
        let peer_task_senders = senders.into_iter().collect();
        let peer_task_data = receivers.into_iter().collect();
        let (stop_sender, stop_receiver) = oneshot::channel();
        let connection_status_changed = Arc::new(Notify::new());
        let connection_counter = Arc::new(AtomicUsize::default());
        let rpc_task = RpcTask {
            pool,
            peer_task_data,
            connection_status: connection_status.clone(),
            connection_counter: connection_counter.clone(),
            connection_status_changed: connection_status_changed.clone(),
            stop: stop_receiver,
            metrics,
        };
        let handle = tokio::spawn(rpc_task.run());
        Self {
            handle,
            peer_task_senders,
            connection_status,
            connection_counter,
            connection_status_changed,
            stop: stop_sender,
        }
    }

    pub async fn rpc(
        &self,
        peer: &NoisePublicKey,
        request: NetworkRequest,
    ) -> RpcResult<NetworkResponse> {
        let Some(peer) = self.peer_task_senders.get(peer) else {
            return Err(RpcError::PeerNotFound);
        };
        let (s, r) = oneshot::channel();
        peer.send(PeerRpcTaskCommand::Rpc(request, s)).await?;
        match r.await {
            Ok(result) => result,
            Err(_) => Err(RpcError::NetworkShutdown),
        }
    }

    pub async fn stream_rpc(
        &self,
        peer: &NoisePublicKey,
        request: NetworkRequest,
    ) -> RpcResult<mpsc::Receiver<RpcResult<NetworkResponse>>> {
        let Some(peer) = self.peer_task_senders.get(peer) else {
            return Err(RpcError::PeerNotFound);
        };
        let (s, r) = mpsc::channel(10);
        peer.send(PeerRpcTaskCommand::StreamRpc(request, s)).await?;
        Ok(r)
    }

    pub fn is_connected(&self, peer: &NoisePublicKey) -> bool {
        let Some(status) = self.connection_status.get(&peer) else {
            return false;
        };
        status.load(Ordering::Relaxed)
    }

    pub async fn wait_connected(&self, timeout: Duration) {
        let started = Instant::now();
        let deadline = started + timeout;
        tracing::debug!("Waiting to connect to all nodes");
        loop {
            let notified = self.connection_status_changed.notified();
            if self.is_all_connected() {
                tracing::debug!("All nodes connected in {:?}", started.elapsed());
                return;
            }
            select! {
                _ = notified => {}
                _ = tokio::time::sleep_until(deadline) => {
                    tracing::debug!("Not every node connected, stopping waiting on timeout");
                    return;
                }
            }
        }
    }

    fn is_all_connected(&self) -> bool {
        self.connection_counter.load(Ordering::Relaxed) == self.connection_status.len() - 1
    }

    pub async fn stop(self) {
        drop(self.peer_task_senders);
        drop(self.stop);
        self.handle.await.ok();
    }
}

struct RpcTask<Pool> {
    pool: Pool,
    peer_task_data: HashMap<NoisePublicKey, PeerTaskData>,
    connection_status: HashMap<NoisePublicKey, Arc<AtomicBool>>,
    connection_counter: Arc<AtomicUsize>,
    connection_status_changed: Arc<Notify>,
    stop: oneshot::Receiver<()>,
    metrics: Arc<Metrics>,
}

struct PeerTaskData {
    router: Box<dyn NetworkRpcRouter>,
    receiver: mpsc::Receiver<PeerRpcTaskCommand>,
}

impl<Pool: ConnectionPool> RpcTask<Pool> {
    async fn run(mut self) {
        let mut peer_tasks: HashMap<
            NoisePublicKey,
            (oneshot::Sender<()>, JoinHandle<Option<PeerTaskData>>),
        > = HashMap::new();
        loop {
            let task_wait = if peer_tasks.is_empty() {
                Either::Left(futures::future::pending())
            } else {
                Either::Right(select_all(
                    peer_tasks
                        .iter_mut()
                        .map(|(k, (_, j))| j.map(|r| (k.clone(), r))),
                ))
            };
            let connection = select! {
                connection = self.pool.connections().recv() => {
                    connection
                }
                ((key, peer_task_data), _, _) = task_wait => {
                    let peer_task_data = peer_task_data.unwrap();
                    self.connection_status.get(&key).expect("Unexpected validator connection public key").store(false, Ordering::Relaxed);
                    self.connection_counter.fetch_sub(1, Ordering::Relaxed);
                    self.metrics.rpc_connected_peers.sub(1);
                    self.connection_status_changed.notify_waiters();
                    peer_tasks.remove(&key).unwrap();
                    if let Some(peer_task_data) = peer_task_data {
                        self.peer_task_data.insert(key, peer_task_data);
                    }
                    continue;
                }
                _ = &mut self.stop => {
                    break;
                }
            };
            let Some(connection) = connection else {
                tracing::warn!("Network shut down while RPC was running");
                break;
            };
            let peer_task_data = if let Some((stop, previous_task)) =
                peer_tasks.remove(&connection.peer.public_key)
            {
                drop(stop);
                previous_task.await.unwrap()
            } else {
                self.connection_status
                    .get(&connection.peer.public_key)
                    .expect("Unexpected validator connection public key")
                    .store(true, Ordering::Relaxed);
                self.connection_counter.fetch_add(1, Ordering::Relaxed);
                self.metrics.rpc_connected_peers.add(1);
                self.connection_status_changed.notify_waiters();
                self.peer_task_data.remove(&connection.peer.public_key)
            };
            let Some(peer_task_data) = peer_task_data else {
                tracing::debug!(
                    "Rejecting connection to {} because rpc for this node has shut down",
                    connection.peer.index
                );
                continue;
            };
            let (stop_send, stop_rcv) = oneshot::channel();
            tracing::debug!("Peer {} connected", connection.peer.index);

            let peer_public_key = connection.peer.public_key.clone();
            let peer_task = PeerTask {
                connection,
                peer_task_data,
                stop: Some(stop_rcv),
                rpc_requests: Default::default(),
                inbound_stream_requests: Default::default(),
                tag: Default::default(),
            };
            let peer_task = tokio::spawn(peer_task.run());
            peer_tasks.insert(peer_public_key, (stop_send, peer_task));
        }
        if !peer_tasks.is_empty() {
            join_all(
                peer_tasks
                    .into_iter()
                    .map(|(_, (_stop /*drop stop here*/, j))| j),
            )
            .await;
        }
        self.pool.shutdown().await;
    }
}

struct PeerTask {
    connection: Connection,
    peer_task_data: PeerTaskData,
    stop: Option<oneshot::Receiver<()>>,

    rpc_requests: HashMap<u64, oneshot::Sender<RpcResult<NetworkResponse>>>,
    inbound_stream_requests: HashMap<u64, (u64, mpsc::Sender<RpcResult<NetworkResponse>>)>,
    tag: u64,
}

impl PeerTask {
    async fn run(mut self) -> Option<PeerTaskData> {
        let proceed = match self.run_until_stopped().await {
            Err(err) => {
                tracing::warn!(
                    "Rpc connection to peer {} terminated with error: {:?}",
                    self.connection.peer.public_key,
                    err
                );
                // Current connection ended with error, waiting for new connection
                true
            }
            Ok(proceed) => proceed,
        };
        tracing::debug!("Peer {} disconnected", self.connection.peer.index);
        if proceed {
            Some(self.peer_task_data)
        } else {
            None
        }
    }

    async fn run_until_stopped(&mut self) -> RpcResult<bool> {
        // todo - wait / clean tasks
        let initial_command = self.peer_task_data.router.connected();
        if let Some(command) = initial_command {
            self.process_command(command).await?;
        }
        let mut stop = self.stop.take().unwrap();
        let r = select! {
            r = self.run_inner() => {
                r
            }
            _ = &mut stop => {
                // stop signaled. connection is being replaced - return true to switch to next connection
                Ok(true)
            }
        };
        self.stop = Some(stop);
        r
    }

    async fn run_inner(&mut self) -> RpcResult<bool> {
        let mut outbound_streams = HashMap::new();
        loop {
            select! {
                message = self.connection.receiver.recv() => {
                    // network connection dropped, return true to wait for next connection
                    let Some(message) = message else {return Ok(true)};
                    let message = bincode::deserialize::<RpcMessage>(&message.data)?;
                    match message {
                        RpcMessage::RpcRequest(tag, request) => {
                            let response = self.peer_task_data.router.rpc(request);
                            Self::send_message(
                                &mut self.connection.sender,
                                &RpcMessage::RpcResponse(tag, response),
                            )
                            .await?;
                        }
                        RpcMessage::RpcStreamRequest(tag, request) => {
                            let stream = self.peer_task_data.router.stream_rpc(request);
                            let (ack_sender, ack_receiver) = mpsc::channel(10);
                            let stream_task = StreamRpcResponseTask {
                                stream,
                                ack_receiver,
                                tag,
                                network_sender: self.connection.sender.clone(),
                                peer: self.connection.peer.index,
                            };
                            // todo track task
                            let stream_task = tokio::spawn(stream_task.run());
                            outbound_streams.insert(tag, (ack_sender, stream_task));
                        }
                        RpcMessage::RpcResponse(tag, resp) => {
                                let ch = self.rpc_requests.remove(&tag);
                                let Some(ch) = ch else {return Err(RpcError::UnmatchedResponse)};
                                ch.send(Ok(resp)).ok();
                            }
                        RpcMessage::RpcStreamItem(tag, item) => {
                                let ch = self.inbound_stream_requests.get_mut(&tag);
                                let Some((bytes, ch)) = ch else {return Err(RpcError::UnmatchedResponse)};
                                *bytes = if let Some(bytes) = bytes.checked_add(item.0.len() as u64) {
                                    bytes
                                } else {
                                    tracing::warn!("Terminating connection to {} after receiving absurd amount of bytes in a stream", self.connection.peer.public_key);
                                    // terminating the current network connection, but allowing to start next peer task with new connection
                                    return Ok(true);
                                };
                                Self::send_message(&mut self.connection.sender, &RpcMessage::RpcStreamBufferAck(tag, *bytes)).await?;
                                ch.send(Ok(item)).await.ok(); // Continue to receive stream(receiver disconnected)?
                            }
                        RpcMessage::RpcStreamBufferAck(tag, bytes) => {
                            let Some((ack_sender, _)) = outbound_streams.get_mut(&tag) else {
                                // todo cleanup tasks
                                return Err(RpcError::UnmatchedAck)
                            };
                            ack_sender.send(bytes).await.ok(); // Continue to receive stream(receiver disconnected)?
                        }
                        RpcMessage::RpcStreamEOF(tag) => {
                                self.inbound_stream_requests.remove(&tag);
                            }
                    }
                }
                command = self.peer_task_data.receiver.recv() => {
                    // command received dropped, return false to stop peer processing
                    let Some(command): Option<PeerRpcTaskCommand> = command else {return Ok(false)};
                    self.process_command(command).await?;
                }
            }
        }
    }

    async fn process_command(&mut self, command: PeerRpcTaskCommand) -> RpcResult<()> {
        match command {
            PeerRpcTaskCommand::Rpc(request, ch) => {
                self.tag += 1;
                self.rpc_requests.insert(self.tag, ch);
                Self::send_message(
                    &mut self.connection.sender,
                    &RpcMessage::RpcRequest(self.tag, request),
                )
                .await?;
            }
            PeerRpcTaskCommand::StreamRpc(request, ch) => {
                self.tag += 1;
                self.inbound_stream_requests.insert(self.tag, (0, ch));
                Self::send_message(
                    &mut self.connection.sender,
                    &RpcMessage::RpcStreamRequest(self.tag, request),
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn send_message(
        sender: &mut mpsc::Sender<NetworkMessage>,
        message: &RpcMessage,
    ) -> RpcResult<()> {
        let message = bincode::serialize(&message)?;
        sender
            .send(NetworkMessage {
                data: message.into(),
            })
            .await?;
        Ok(())
    }
}

struct StreamRpcResponseTask {
    stream: mpsc::Receiver<NetworkResponse>,
    ack_receiver: mpsc::Receiver<u64>,
    network_sender: mpsc::Sender<NetworkMessage>,
    tag: u64,
    peer: ValidatorIndex,
}

impl StreamRpcResponseTask {
    async fn run(mut self) -> RpcResult<()> {
        let mut sent = 0u64;
        let mut acked = 0u64;
        loop {
            let not_acked = sent.saturating_sub(acked);
            let recv_enabled = not_acked < STREAM_BUFFER_SIZE;
            if !recv_enabled {
                tracing::debug!("Streaming task to {} is blocked, waiting for ack. Sent {sent}, acked {acked}, not acked {not_acked}", self.peer);
            }
            select! {
                item = self.stream.recv(), if recv_enabled  =>{
                    if let Some(item) = item {
                        sent += item.0.len() as u64; // todo check overflow
                        PeerTask::send_message(
                            &mut self.network_sender,
                            &RpcMessage::RpcStreamItem(self.tag, item),
                        )
                        .await?;
                    } else {
                        PeerTask::send_message(
                            &mut self.network_sender,
                            &RpcMessage::RpcStreamEOF(self.tag),
                        )
                        .await?;
                        break;
                    }
                }
                ack = self.ack_receiver.recv() => {
                    if let Some(ack) = ack {
                        acked = ack;
                    } else {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

pub type RpcResult<T> = Result<T, RpcError>;

#[derive(thiserror::Error, Debug)]
pub enum RpcError {
    #[error("serialization error")]
    SerializationError(#[from] bincode::Error),
    #[error("broken pipe")]
    BrokenPipe,
    #[error("peer not found")]
    PeerNotFound,
    #[error("network shutdown")]
    NetworkShutdown,
    #[error("unmatched response")]
    UnmatchedResponse,
    #[error("unmatched ack")]
    UnmatchedAck,
}

impl From<mpsc::error::SendError<NetworkMessage>> for RpcError {
    fn from(_value: mpsc::error::SendError<NetworkMessage>) -> Self {
        Self::BrokenPipe
    }
}

impl From<mpsc::error::SendError<PeerRpcTaskCommand>> for RpcError {
    fn from(_value: mpsc::error::SendError<PeerRpcTaskCommand>) -> Self {
        Self::BrokenPipe
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::log::enable_test_logging;
    use crate::network::TestConnectionPool;
    use tokio::sync::mpsc::Receiver;

    #[tokio::test]
    pub async fn rpc_test() {
        enable_test_logging();
        let test_pool = TestConnectionPool::new(2, 8180).await;
        let metrics = Metrics::new_test();

        let ([pool1, pool2], [kpb1, kpb2], runtimes) = test_pool.into_parts();
        let rpc1 = NetworkRpc::start(
            pool1,
            [(kpb2.clone(), TestRpcRouter::new())].into_iter().collect(),
            metrics.clone(),
        );
        let rpc2 = NetworkRpc::start(
            pool2,
            [(kpb1.clone(), TestRpcRouter::new())].into_iter().collect(),
            metrics.clone(),
        );

        let bytes = rpc1
            .rpc(&kpb2, NetworkRequest(Bytes::new()))
            .await
            .unwrap()
            .0;
        assert_eq!(bytes.as_ref(), &[3, 5, 7]);
        let bytes = rpc2
            .rpc(&kpb1, NetworkRequest(Bytes::new()))
            .await
            .unwrap()
            .0;
        assert_eq!(bytes.as_ref(), &[3, 5, 7]);

        let mut r = rpc1
            .stream_rpc(&kpb2, NetworkRequest(Bytes::new()))
            .await
            .unwrap();
        let bytes = r.recv().await.unwrap().unwrap().0;
        assert_eq!(bytes.as_ref(), &[3, 4]);
        let bytes = r.recv().await.unwrap().unwrap().0;
        assert_eq!(bytes.as_ref(), &[5, 6]);
        assert!(r.recv().await.is_none());

        TestConnectionPool::drop_runtimes(runtimes).await;
    }

    struct TestRpcRouter;

    impl NetworkRpcRouter for TestRpcRouter {
        fn rpc(&mut self, _req: NetworkRequest) -> NetworkResponse {
            NetworkResponse(vec![3, 5, 7].into())
        }

        fn stream_rpc(&mut self, _req: NetworkRequest) -> Receiver<NetworkResponse> {
            let (s, r) = mpsc::channel(10);
            tokio::spawn(async move {
                s.send(NetworkResponse(vec![3, 4].into())).await.ok();
                s.send(NetworkResponse(vec![5, 6].into())).await.ok();
            });
            r
        }
    }

    impl TestRpcRouter {
        fn new() -> Box<dyn NetworkRpcRouter> {
            Box::new(TestRpcRouter)
        }
    }
}
