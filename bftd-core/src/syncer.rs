use crate::block::{AuthorRound, Block, BlockReference, Round, ValidatorIndex};
use crate::block_manager::BlockManager;
use crate::committee::{BlockMatch, BlockVerifiedByCommittee, Committee};
use crate::consensus::{Commit, CommitDecision, UniversalCommitter, UniversalCommitterBuilder};
use crate::core::{Core, ProposalMaker};
use crate::crypto::Signer;
use crate::fetcher::BlockFetcher;
use crate::log_byzantine;
use crate::metrics::{Metrics, UtilizationTimerExt};
use crate::network::ConnectionPool;
use crate::rpc::{
    NetworkRequest, NetworkResponse, NetworkRpc, NetworkRpcRouter, PeerRpcTaskCommand, RpcResult,
};
use crate::store::BlockStore;
use crate::store::{CommitInterpreter, CommitStore};
use anyhow::bail;
use bytes::Bytes;
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use std::cmp;
use std::future::Future;
use std::ops::Add;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::select;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

pub struct Syncer {
    handle: JoinHandle<()>,
    stop: oneshot::Sender<()>,
    last_commit_receiver: watch::Receiver<Option<u64>>,
    verification_task: JoinHandle<()>,
}

struct SyncerTask<S, B, C, P> {
    core: Core<S, B>,
    committer: UniversalCommitter<B>,
    block_store: B,
    rpc: Arc<NetworkRpc>,
    // note fetcher contains ref to sender part of SyncerTask::blocks_receiver
    fetcher: BlockFetcher,
    last_proposed_round_sender: watch::Sender<Round>,
    blocks_receiver: mpsc::Receiver<Arc<Block>>,
    proposer: P,
    last_decided: AuthorRound,
    last_commit: Option<Commit>,
    stop: oneshot::Receiver<()>,
    clock: C,
    metrics: Arc<Metrics>,
    last_commit_sender: watch::Sender<Option<u64>>,
    last_known_round: Round,
    block_manager: BlockManager<B>,
}

struct SyncerInner<B, F> {
    block_store: B,
    last_proposed_round_receiver: watch::Receiver<Round>,
    validator_index: ValidatorIndex,
    committee: Arc<Committee>,
    blocks_sender: mpsc::Sender<Arc<Block>>,
    block_filter: F,
    metrics: Arc<Metrics>,
}

pub trait Clock: Send + 'static {
    fn time_ns(&self) -> u64;
}

pub trait BlockFilter: Send + Sync + 'static {
    fn check_block(&self, block: &Block) -> anyhow::Result<()>;
}

#[derive(Clone)]
pub struct SystemTimeClock {
    start: std::time::Instant,
    start_timestamp: u64,
}

impl Syncer {
    pub fn start<
        S: Signer,
        B: BlockStore + CommitStore + Clone,
        C: Clock + Clone,
        P: ProposalMaker,
        F: BlockFilter,
    >(
        core: Core<S, B>,
        block_store: B,
        pool: ConnectionPool,
        clock: C,
        proposer: P,
        block_filter: F,
    ) -> Self {
        let committee = core.committee().clone();
        let metrics = core.metrics().clone();
        let committer = UniversalCommitterBuilder::new(committee.clone(), block_store.clone())
            .with_pipeline(true)
            .with_number_of_leaders(1)
            .build();
        let validator_index = core.validator_index();
        let last_proposed_round = core.last_proposed_round();
        let (last_proposed_round_sender, last_proposed_round_receiver) =
            watch::channel(last_proposed_round);
        let last_known_round = last_proposed_round;
        let (blocks_sender, blocks_receiver) = mpsc::channel(10);
        let inner = Arc::new(SyncerInner {
            block_store: block_store.clone(),
            last_proposed_round_receiver,
            validator_index,
            blocks_sender,
            committee: committee.clone(),
            block_filter,
            metrics: metrics.clone(),
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
        let rpc = NetworkRpc::start(pool, peer_routers, metrics.clone());
        let rpc = Arc::new(rpc);
        let (stop_sender, stop_receiver) = oneshot::channel();

        let last_commit = block_store.last_commit();
        let last_decided = last_commit
            .as_ref()
            .map(Commit::author_round)
            .unwrap_or_default();

        let (last_commit_sender, last_commit_receiver) =
            watch::channel(last_commit.as_ref().map(Commit::index));

        let (unverified_block_sender, unverified_block_receiver) = mpsc::channel(32);

        let fetcher = BlockFetcher::new(
            unverified_block_sender,
            rpc.clone(),
            committee.clone(),
            metrics.clone(),
            validator_index,
        );

        let verification_task =
            tokio::spawn(inner.run_verification_task(unverified_block_receiver));

        let block_manager = BlockManager::new(block_store.clone(), metrics.clone());

        let syncer = SyncerTask {
            core,
            committer,
            block_store,
            rpc,
            fetcher,
            last_proposed_round_sender,
            blocks_receiver,
            proposer,
            last_decided,
            last_commit,
            stop: stop_receiver,
            clock,
            metrics,
            last_commit_sender,
            last_known_round,
            block_manager,
        };
        let handle = tokio::spawn(syncer.run());
        Syncer {
            handle,
            stop: stop_sender,
            last_commit_receiver,
            verification_task,
        }
    }

    pub async fn stop(self) {
        self.verification_task.abort();
        drop(self.stop);
        self.verification_task.await.ok();
        self.handle.await.ok();
    }

    pub fn last_commit_receiver(&self) -> &watch::Receiver<Option<u64>> {
        &self.last_commit_receiver
    }
}

impl<S: Signer, B: BlockStore + CommitStore + Clone, C: Clock, P: ProposalMaker>
    SyncerTask<S, B, C, P>
{
    const STALL_TIMEOUT: Duration = Duration::from_secs(15);

    pub async fn run(mut self) {
        self.try_make_proposal();
        let (mut committed, mut skipped) = (0usize, 0usize);
        let mut proposal_deadline: Pin<Box<dyn Future<Output = ()> + Send>> =
            futures::future::pending().boxed();
        let mut proposal_deadline_set = false;
        let mut stall_deadline = Instant::now() + Self::STALL_TIMEOUT;
        let mut waiting_leaders: Option<Vec<ValidatorIndex>> = None;
        self.rpc.wait_connected(Duration::from_secs(2)).await;
        const MAX_BLOCK_BATCH: usize = 1024;
        let mut blocks = Vec::with_capacity(MAX_BLOCK_BATCH);
        loop {
            select! {
                blocks_received = self.blocks_receiver.recv_many(&mut blocks, MAX_BLOCK_BATCH) => {
                    // main purpose for recv_many is not to propose too often during catch up
                    if blocks_received == 0 {
                        return;
                    }
                    stall_deadline = Instant::now() + Self::STALL_TIMEOUT;
                    let _timer = self.metrics.syncer_main_loop_util_ns.utilization_timer();
                    self.metrics.syncer_main_loop_calls.inc();
                    for block in blocks.drain(..) {
                        let reference = *block.reference();
                        // todo need more block verification
                        let age_ms = ns_to_ms(self.clock.time_ns().saturating_sub(block.time_ns()));
                        self.metrics.syncer_received_block_age_ms.with_label_values(&[self.metrics.validator_label(block.author())]).observe(age_ms as f64);
                        // todo - block manager can benefit from receiving all blocks in one call
                        // But for the fetcher we need to accurately track which blocks depends on which
                        let add_block_result = self.block_manager.add_block(block);
                        self.core.add_blocks(&add_block_result.added);
                        self.fetcher.handle_add_block_result(&reference, &add_block_result);
                        let added_this = add_block_result.added.iter().any(|b|*b.reference() == reference);
                        tracing::debug!("Received {reference} {} age {age_ms} ms", if added_this {
                            let added_blocks: Vec<_> = add_block_result.added.iter().map(|b|b.reference()).collect();
                            format!("(block accepted, added {added_blocks:?})")
                        } else {
                            format!("(missing parents new {:?}, old {:?})", add_block_result.new_missing, add_block_result.previously_missing)
                        });
                        // todo add max_round as a field to AddBlockResult, avoid iteration
                        let max_round = add_block_result.added.iter().map(|b|b.round()).max();
                        if let Some(max_round) = max_round {
                            self.last_known_round = cmp::max(self.last_known_round, max_round);
                        }
                    }
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
                    }
                    let commits = self.committer.try_commit(self.last_decided, self.last_known_round);
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
                    if (committed % 100 == 0 && committed > 0) || (skipped % 100 == 0 && skipped > 0) {
                        log::info!("stat: committed {}, skipped {}", committed, skipped);
                    }
                }
                _ = &mut proposal_deadline => {
                    let _timer = self.metrics.syncer_main_loop_util_ns.utilization_timer();
                    self.metrics.syncer_main_loop_calls.inc();
                    let waiting_round = self.core.vector_clock_round().unwrap_or_default().previous();
                    let timeouts: Vec<_> = waiting_leaders.as_ref().unwrap().iter().map(|l|AuthorRound::new(*l, waiting_round)).collect();
                    self.metrics.syncer_leader_timeouts.inc();
                    tracing::warn!("Leader timeout {timeouts:?}");
                    self.try_make_proposal();
                    proposal_deadline = futures::future::pending().boxed();
                    proposal_deadline_set = false;
                    waiting_leaders = None;
                }
                _ = tokio::time::sleep_until(stall_deadline) => {
                    stall_deadline = Instant::now() + Self::STALL_TIMEOUT;
                    let missing = self.core.missing_validators_for_proposal();
                    let round = self.core.last_proposed_round();
                    tracing::warn!("No activity for {} seconds. Still waiting for validators {missing:?} at round {round}", Self::STALL_TIMEOUT.as_secs());
                }
                _ = &mut self.stop => {
                    break;
                }
            }
        }
        tracing::info!("Syncer stopped, waiting for rpc to stop");
        self.fetcher.stop().await;
        let Ok(rpc) = Arc::try_unwrap(self.rpc) else {
            panic!("Can't unwrap rpc, fetcher did not stop properly")
        };
        rpc.stop().await;
        tracing::info!("Rpc stopped");
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
        for block in &all_blocks {
            if block.author() == self.core.validator_index() {
                let age_ns = self.clock.time_ns().saturating_sub(block.time_ns());
                self.metrics
                    .syncer_own_block_commit_age_ms
                    .observe(ns_to_ms(age_ns) as f64);
            }
        }
        let all_blocks = all_blocks.into_iter().map(|b| *b.reference()).collect();
        let previous_timestamp_ns = self
            .last_commit
            .as_ref()
            .map(Commit::commit_timestamp_ns)
            .unwrap_or_default();
        let commit_timestamp_ns = cmp::max(leader.time_ns(), previous_timestamp_ns);
        let commit = Commit::new(
            self.last_commit.as_ref(),
            index,
            *leader.reference(),
            commit_timestamp_ns,
            all_blocks,
        );
        self.block_store.store_commit(&commit);
        if commit.index() % 1000 == 0 {
            tracing::info!("Committed {}", commit);
        }
        self.last_commit_sender.send(Some(commit.index())).ok();
        self.metrics
            .syncer_last_committed_round
            .set(commit.leader().round().0 as i64);
        self.metrics
            .syncer_last_commit_index
            .set(commit.index() as i64);
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
            .make_proposal(&mut self.proposer, round, self.clock.time_ns());
        tracing::debug!("Generated proposal {}", proposal);
        self.last_proposed_round_sender
            .send(proposal.reference().round)
            .ok();
    }

    fn committee(&self) -> &Arc<Committee> {
        self.core.committee()
    }
}

struct PeerRouter<B, C, F> {
    inner: Arc<SyncerInner<B, F>>,
    peer_index: ValidatorIndex,
    clock: C,
}

impl<B: BlockStore, C: Clock + Clone, F: BlockFilter> PeerRouter<B, C, F> {
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
        inner: Arc<SyncerInner<B, F>>,
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
                let age_ms = ns_to_ms(clock.time_ns().saturating_sub(own_block.time_ns()));
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
        inner: Arc<SyncerInner<B, F>>,
    ) -> anyhow::Result<()> {
        tracing::debug!("Starting receiving subscription from {peer}");
        while let Some(response) = receiver.recv().await {
            let response = response?;
            let response = bincode::deserialize::<StreamRpcResponse>(&response.0)?;
            let StreamRpcResponse::Block(block) = response;
            let block = inner.parse_verify_block(block, BlockMatch::Author(peer))?;
            if inner.blocks_sender.send(block).await.is_err() {
                break;
            }
        }
        tracing::debug!("Receiving subscription from {peer} ended");
        Ok(())
    }

    async fn receive_subscription(
        peer: ValidatorIndex,
        receiver: mpsc::Receiver<RpcResult<NetworkResponse>>,
        inner: Arc<SyncerInner<B, F>>,
    ) {
        if let Err(err) = Self::receive_subscription_inner(peer, receiver, inner).await {
            tracing::warn!("Error receiving stream from {peer}: {err}");
        }
    }
}

impl<B, F: BlockFilter> SyncerInner<B, F> {
    fn parse_verify_block(&self, data: Bytes, matcher: BlockMatch) -> anyhow::Result<Arc<Block>> {
        let block = self
            .committee
            .verify_block(data, matcher, self.metrics.clone())?;
        self.complete_block_verification(block)
    }

    fn complete_block_verification(
        &self,
        block: BlockVerifiedByCommittee,
    ) -> anyhow::Result<Arc<Block>> {
        let block = block.extract_for_further_verification();
        if let Err(err) = self.block_filter.check_block(&block) {
            bail!(
                "Block filter verification failed for block {}: {err}",
                block.reference()
            );
        }
        let block = Arc::new(block);
        Ok(block)
    }

    async fn run_verification_task(
        self: Arc<Self>,
        mut receiver: mpsc::Receiver<BlockVerifiedByCommittee>,
    ) {
        // todo this task might become bottleneck at some point
        while let Some(block) = receiver.recv().await {
            let reference = *block.reference();
            let block = self.complete_block_verification(block);
            let block = match block {
                Err(err) => {
                    log_byzantine!(
                        "[byzantine] Block {reference} has failed secondary verification: {err}"
                    );
                    // todo - need to clean BlockManager
                    continue;
                }
                Ok(block) => block,
            };
            if self.blocks_sender.send(block).await.is_err() {
                return;
            }
        }
    }
}

impl<B: BlockStore, C: Clock + Clone, F: BlockFilter> NetworkRpcRouter for PeerRouter<B, C, F> {
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
            self.inner.clone(),
        ));
        Some(PeerRpcTaskCommand::StreamRpc(
            NetworkRequest(request.into()),
            sender,
        ))
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) enum StreamRpcRequest {
    Subscribe(Round),
}

#[derive(Serialize, Deserialize)]
pub(crate) enum RpcRequest {
    GetBlock(BlockReference),
}

#[derive(Serialize, Deserialize)]
pub(crate) enum RpcResponse {
    GetBlockResponse(Option<Bytes>),
}

#[derive(Serialize, Deserialize)]
pub(crate) enum StreamRpcResponse {
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

impl BlockFilter for () {
    fn check_block(&self, _block: &Block) -> anyhow::Result<()> {
        Ok(())
    }
}

fn ns_to_ms(ns: u64) -> u64 {
    ns / 1000 / 1000
}
