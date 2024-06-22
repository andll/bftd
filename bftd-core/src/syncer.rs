use crate::block::{AuthorRound, Block, BlockReference, Round, ValidatorIndex};
use crate::block_manager::BlockStore;
use crate::committee::Committee;
use crate::consensus::{Commit, CommitDecision, UniversalCommitter, UniversalCommitterBuilder};
use crate::core::Core;
use crate::crypto::Signer;
use crate::metrics::Metrics;
use crate::network::ConnectionPool;
use crate::rpc::{
    NetworkRequest, NetworkResponse, NetworkRpc, NetworkRpcRouter, PeerRpcTaskCommand, RpcResult,
};
use crate::store::{CommitInterpreter, CommitStore};
use bytes::Bytes;
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::ops::Add;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

pub struct Syncer {
    handle: JoinHandle<()>,
    stop: oneshot::Sender<()>,
}

struct SyncerTask<S, B, C> {
    core: Core<S, B>,
    committer: UniversalCommitter<B>,
    block_store: B,
    rpc: NetworkRpc,
    last_proposed_round_sender: tokio::sync::watch::Sender<Round>,
    blocks_receiver: mpsc::Receiver<Arc<Block>>,
    last_decided: AuthorRound,
    last_commit: Option<Commit>,
    stop: oneshot::Receiver<()>,
    clock: C,
    metrics: Arc<Metrics>,
}

struct SyncerInner<B> {
    block_store: B,
    last_proposed_round_receiver: tokio::sync::watch::Receiver<Round>,
    validator_index: ValidatorIndex,
    committee: Arc<Committee>,
    blocks_sender: mpsc::Sender<Arc<Block>>,
}

pub trait Clock: Send + 'static {
    fn time_ns(&self) -> u64;
}

#[derive(Clone)]
pub struct SystemTimeClock {
    start: std::time::Instant,
    start_timestamp: u64,
}

impl Syncer {
    pub fn start<S: Signer, B: BlockStore + CommitStore + Clone, C: Clock + Clone>(
        core: Core<S, B>,
        block_store: B,
        pool: ConnectionPool,
        clock: C,
    ) -> Self {
        let committee = core.committee().clone();
        let metrics = core.metrics().clone();
        let committer = UniversalCommitterBuilder::new(committee.clone(), block_store.clone())
            .with_pipeline(true)
            .with_number_of_leaders(1)
            .build();
        let validator_index = core.validator_index();
        let (last_proposed_round_sender, last_proposed_round_receiver) =
            tokio::sync::watch::channel(core.last_proposed_round());
        let (blocks_sender, blocks_receiver) = mpsc::channel(10);
        let inner = Arc::new(SyncerInner {
            block_store: block_store.clone(),
            last_proposed_round_receiver,
            validator_index,
            blocks_sender,
            committee: committee.clone(),
        });
        let peer_routers = committee
            .enumerate_validators()
            .map(|(index, vi)| {
                let key = vi.network_key.clone();
                let peer_router = PeerRouter {
                    inner: inner.clone(),
                    peer_index: index,
                    clock: clock.clone(),
                };
                (key, Box::new(peer_router) as Box<dyn NetworkRpcRouter>)
            })
            .collect();
        let rpc = NetworkRpc::start(pool, peer_routers);
        let (stop_sender, stop_receiver) = oneshot::channel();

        let last_commit = block_store.last_commit();
        let last_decided = last_commit
            .as_ref()
            .map(Commit::author_round)
            .unwrap_or_default();

        let syncer = SyncerTask {
            core,
            committer,
            block_store,
            rpc,
            last_proposed_round_sender,
            blocks_receiver,
            last_decided,
            last_commit,
            stop: stop_receiver,
            clock,
            metrics,
        };
        let handle = tokio::spawn(syncer.run());
        Syncer {
            handle,
            stop: stop_sender,
        }
    }

    pub async fn stop(self) {
        drop(self.stop);
        self.handle.await.ok();
    }
}

impl<S: Signer, B: BlockStore + CommitStore + Clone, C: Clock> SyncerTask<S, B, C> {
    pub async fn run(mut self) {
        self.try_make_proposal();
        let (mut committed, mut skipped) = (0usize, 0usize);
        let mut proposal_deadline: Pin<Box<dyn Future<Output = ()> + Send>> =
            futures::future::pending().boxed();
        let mut proposal_deadline_set = false;
        let mut waiting_leaders: Option<Vec<ValidatorIndex>> = None;
        self.rpc.wait_connected(Duration::from_secs(2)).await;
        loop {
            select! {
                block = self.blocks_receiver.recv() => {
                    let Some(block) = block else {return;};
                    let reference = *block.reference();
                    // todo need more block verification
                    let age_ms = self.clock.time_ns().saturating_sub(block.time_ns()) / 1000 / 1000;
                    self.metrics.syncer_received_block_age_ms.observe(age_ms as f64);
                    let add_block_result = self.core.add_block(block);
                    let added_this = add_block_result.added.iter().any(|b|*b.reference() == reference);
                    tracing::debug!("Received {reference} {} age {age_ms} ms", if added_this {
                        let added_blocks: Vec<_> = add_block_result.added.iter().map(|b|b.reference()).collect();
                        format!("(block accepted, added {added_blocks:?})")
                    } else {
                        format!("(missing parents new {:?}, old {:?})", add_block_result.new_missing, add_block_result.previously_missing)
                    });
                    // todo handle missing blocks
                    if let Some(next_proposal_round) = self.core.vector_clock_round() {
                        let check_round = next_proposal_round.previous();
                        let mut ready = true;
                        let leaders = self.committer.get_leaders(check_round);
                        for leader in &leaders {
                            // todo - exists method instead of get
                            if self.block_store.get_blocks_at_author_round(*leader, check_round).is_empty() {
                                if !self.rpc.is_connected(self.committee().network_key(*leader)) {
                                    tracing::debug!("Missing leader {}{}, not waiting because there is no connection", leader, check_round);
                                    continue;
                                }
                                tracing::debug!("Not ready to make proposal, missing {}{}", leader, check_round);
                                ready = false;
                                break;
                            }
                        }
                        if ready {
                            self.try_make_proposal();
                            proposal_deadline = futures::future::pending().boxed();
                            proposal_deadline_set = false;
                            waiting_leaders = None;
                        } else {
                            if !proposal_deadline_set {
                                proposal_deadline = tokio::time::sleep_until(Instant::now().add(Duration::from_secs(1))).boxed();
                                proposal_deadline_set = true;
                                waiting_leaders = Some(leaders);
                            }
                        }
                    } else {
                        let missing = self.core.missing_validators_for_proposal();
                        let round = self.core.last_proposed_round();
                        tracing::debug!("Still waiting for validators {missing:?} at round {round}");
                    }
                    let commits = self.committer.try_commit(self.last_decided, *self.last_proposed_round_sender.borrow());
                    for c in commits {
                        self.last_decided = c.author_round();
                        self.put_commit(&c);
                        match c  {
                            CommitDecision::Commit(_) => {
                                committed += 1;
                            },
                            CommitDecision::Skip(_) => {
                                skipped += 1;
                            }
                        }
                    }
                    if (committed % 10 == 0 && committed > 0) || (skipped % 10 == 0 && skipped > 0) {
                        println!("stat: committed {}, skipped {}", committed, skipped);
                    }
                }
                _ = &mut proposal_deadline => {
                    let waiting_round = self.core.vector_clock_round().unwrap_or_default().previous();
                    let timeouts: Vec<_> = waiting_leaders.as_ref().unwrap().iter().map(|l|AuthorRound::new(*l, waiting_round)).collect();
                    tracing::warn!("Leader timeout {timeouts:?}");
                    self.try_make_proposal();
                    proposal_deadline = futures::future::pending().boxed();
                    proposal_deadline_set = false;
                    waiting_leaders = None;
                }
                _ = &mut self.stop => {
                    break;
                }
            }
        }
        tracing::debug!("Syncer stopped, waiting for rpc to stop");
        self.rpc.stop().await;
    }

    fn put_commit(&mut self, decision: &CommitDecision) {
        let leader = match decision {
            CommitDecision::Commit(leader) => leader,
            CommitDecision::Skip(author_round) => {
                // todo - might want to store this too
                tracing::debug!("Skipping commit at {}", author_round);
                return;
            }
        };
        let index = self
            .last_commit
            .as_ref()
            .map(|c| c.index() + 1)
            .unwrap_or_default();
        let interpreter = CommitInterpreter::new(&self.block_store);
        let all_blocks = interpreter.interpret_commit(index, leader.clone());
        let commit = Commit::new(
            self.last_commit.as_ref(),
            index,
            *leader.reference(),
            all_blocks,
        );
        self.block_store.store_commit(&commit);
        tracing::debug!("Committed {}", commit);
        self.last_commit = Some(commit);
    }

    fn try_make_proposal(&mut self) {
        let round = self.core.vector_clock_round();
        let Some(round) = round else {
            return;
        };
        let previous = round.previous();
        if previous > self.core.last_proposed_round() {
            if self
                .committer
                .is_leader(previous, self.core.validator_index())
            {
                self.make_proposal_for_round(previous);
            }
        }
        self.make_proposal_for_round(round);
    }

    fn make_proposal_for_round(&mut self, round: Round) {
        let proposal = self
            .core
            .make_proposal(&mut (), round, self.clock.time_ns());
        tracing::debug!("Generated proposal {}", proposal);
        self.last_proposed_round_sender
            .send(proposal.reference().round)
            .ok();
    }

    fn committee(&self) -> &Arc<Committee> {
        self.core.committee()
    }
}

struct PeerRouter<B, C> {
    inner: Arc<SyncerInner<B>>,
    peer_index: ValidatorIndex,
    clock: C,
}

impl<B: BlockStore, C: Clock + Clone> PeerRouter<B, C> {
    fn stream_rpc(
        &mut self,
        req: NetworkRequest,
    ) -> anyhow::Result<mpsc::Receiver<NetworkResponse>> {
        let r = bincode::deserialize::<StreamRpcRequest>(&req.0)?;
        match r {
            StreamRpcRequest::Subscribe(round) => {
                // todo track task
                // todo limit one subscription per peer
                let (sender, receiver) = mpsc::channel(10);
                tokio::spawn(Self::stream_task(
                    self.inner.clone(),
                    sender,
                    round,
                    self.peer_index,
                    self.clock.clone(),
                ));
                Ok(receiver)
            }
        }
    }

    fn rpc(&mut self, req: NetworkRequest) -> anyhow::Result<NetworkResponse> {
        let r = bincode::deserialize::<RpcRequest>(&req.0)?;
        match r {
            RpcRequest::GetBlock(reference) => {
                let block = self.inner.block_store.get(&reference);
                let response = RpcResponse::GetBlockResponse(block.map(|b| b.data().clone()));
                let response = bincode::serialize(&response)?;
                Ok(NetworkResponse(response.into()))
            }
        }
    }

    async fn stream_task(
        inner: Arc<SyncerInner<B>>,
        sender: mpsc::Sender<NetworkResponse>,
        mut last_sent: Round,
        peer_index: ValidatorIndex,
        clock: impl Clock,
    ) {
        let mut round_receiver = inner.last_proposed_round_receiver.clone();
        // todo - check initial condition is ok
        tracing::debug!("Starting subscription from {peer_index}");
        loop {
            let round = *round_receiver.borrow_and_update();
            while round > last_sent {
                // todo batch read range w/ chunks
                last_sent = last_sent.next();
                let Some(own_block) = inner.block_store.get_own(inner.validator_index, last_sent)
                else {
                    continue; // todo - more efficient, iterating through each round right now
                };
                let age_ms = clock.time_ns().saturating_sub(own_block.time_ns()) / 1000 / 1000;
                tracing::debug!(
                    "Sending {} to {peer_index} age {age_ms} ms",
                    own_block.reference()
                );
                let response = StreamRpcResponse::Block(own_block.data().clone());
                let response = bincode::serialize(&response).expect("Serialization failed");
                if sender.send(NetworkResponse(response.into())).await.is_err() {
                    tracing::debug!("Subscription from {peer_index} ended");
                    return;
                }
            }
            if round_receiver.changed().await.is_err() {
                break;
            }
        }
    }

    async fn receive_subscription_inner(
        peer: ValidatorIndex,
        mut receiver: mpsc::Receiver<RpcResult<NetworkResponse>>,
        block_sender: mpsc::Sender<Arc<Block>>,
        committee: Arc<Committee>,
    ) -> anyhow::Result<()> {
        while let Some(block) = receiver.recv().await {
            let block = block?;
            let block = bincode::deserialize::<StreamRpcResponse>(&block.0)?;
            let StreamRpcResponse::Block(block) = block;
            let block = committee.verify_block(block, Some(peer))?;
            let block = Arc::new(block);
            if block_sender.send(block).await.is_err() {
                break;
            }
        }
        Ok(())
    }

    async fn receive_subscription(
        peer: ValidatorIndex,
        receiver: mpsc::Receiver<RpcResult<NetworkResponse>>,
        block_sender: mpsc::Sender<Arc<Block>>,
        committee: Arc<Committee>,
    ) {
        if let Err(err) =
            Self::receive_subscription_inner(peer, receiver, block_sender, committee).await
        {
            tracing::warn!("Error receiving stream from {peer}: {err}");
        }
    }
}

impl<B: BlockStore, C: Clock + Clone> NetworkRpcRouter for PeerRouter<B, C> {
    fn rpc(&mut self, req: NetworkRequest) -> NetworkResponse {
        self.rpc(req).unwrap() // todo handle error
    }

    fn stream_rpc(&mut self, req: NetworkRequest) -> mpsc::Receiver<NetworkResponse> {
        self.stream_rpc(req).unwrap() // todo handle error
    }

    fn connected(&mut self) -> Option<PeerRpcTaskCommand> {
        let round = self.inner.block_store.last_known_round(self.peer_index);
        let request = StreamRpcRequest::Subscribe(round);
        let request = bincode::serialize(&request).expect("Serialization failed");
        let (sender, receiver) = mpsc::channel(10);
        // todo track task
        tokio::spawn(Self::receive_subscription(
            self.peer_index,
            receiver,
            self.inner.blocks_sender.clone(),
            self.inner.committee.clone(),
        ));
        Some(PeerRpcTaskCommand::StreamRpc(
            NetworkRequest(request.into()),
            sender,
        ))
    }
}

#[derive(Serialize, Deserialize)]
enum StreamRpcRequest {
    Subscribe(Round),
}

#[derive(Serialize, Deserialize)]
enum RpcRequest {
    GetBlock(BlockReference),
}

#[derive(Serialize, Deserialize)]
enum RpcResponse {
    GetBlockResponse(Option<Bytes>),
}

#[derive(Serialize, Deserialize)]
enum StreamRpcResponse {
    Block(Bytes),
}

impl SystemTimeClock {
    pub fn new() -> Self {
        let start = std::time::Instant::now();
        let start_timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        Self {
            start,
            start_timestamp,
        }
    }
}

impl Clock for SystemTimeClock {
    fn time_ns(&self) -> u64 {
        // little trick to avoid syscall every time we want to get timestamp
        // todo - synchronize with actual time sometimes?
        self.start_timestamp + self.start.elapsed().as_nanos() as u64
    }
}
