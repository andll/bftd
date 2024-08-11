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
use crate::protocol_config::ProtocolConfig;
use crate::rpc::{
    NetworkRequest, NetworkResponse, NetworkRpc, NetworkRpcRouter, PeerRpcTaskCommand, RpcResult,
};
use crate::store::{BlockReader, BlockStore, BlockViewStore, DagExt};
use crate::store::{CommitInterpreter, CommitStore};
use anyhow::bail;
use bytes::Bytes;
use futures::future::OptionFuture;
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::Add;
use std::pin::Pin;
use std::sync::Arc;
#[cfg(feature = "syncer_thread")]
use std::thread;
use std::time::{Duration, SystemTime};
use std::{cmp, fmt};
use tokio::select;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

pub struct Syncer {
    #[cfg(feature = "syncer_thread")]
    handle: thread::JoinHandle<()>,
    #[cfg(not(feature = "syncer_thread"))]
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
    blocks_receiver: mpsc::Receiver<(Arc<Block>, BlockSource)>,
    last_decided: AuthorRound,
    last_commit: Option<Commit>,
    stop: oneshot::Receiver<()>,
    clock: C,
    metrics: Arc<Metrics>,
    last_commit_sender: watch::Sender<Option<u64>>,
    last_known_round: Round,
    block_manager: BlockManager<B>,
    uncommitted_counter: UncommittedCounter,
    protocol_config: ProtocolConfig,
    _proposer: PhantomData<P>,
}

struct SyncerInner<B, F, C> {
    block_store: B,
    last_proposed_round_receiver: watch::Receiver<Round>,
    validator_index: ValidatorIndex,
    committee: Arc<Committee>,
    blocks_sender: mpsc::Sender<(Arc<Block>, BlockSource)>,
    block_filter: F,
    clock: C,
    metrics: Arc<Metrics>,
}

enum BlockSource {
    Subscription,
    Rpc,
}

/// This is the maximum clock difference between correct validators that is acceptable.
/// If a validator clock diverges more than this duration, it's blocks are going to be rejected.
const SLEEP_UP_TO_NS: u64 = Duration::from_secs(2).as_nanos() as u64;
const BLOCKS_CHANNEL_CAPACITY: usize = 2048;

/// Provides proposer with timestamps.
/// This is used for reporting block age as well,
/// so time_ns is called frequently and should be optimized.
/// Time returned by this clock should always be monotonic.
/// Implementation of the Clock should account for possible reverse of a local clock.
pub trait Clock: Send + Sync + 'static {
    /// Current timestamp in nanoseconds
    fn time_ns(&self) -> u64;

    /// Current timestamp as duration
    fn time(&self) -> Duration {
        Duration::from_nanos(self.time_ns())
    }
}

/// Application-specific filtering of block payload.
pub trait BlockFilter: Send + Sync + 'static {
    /// Checks whether the block payload is valid.
    ///
    /// This function should return the same result on all correct validators at any time.
    /// It should rely on any mutable state other than provided when a blockchain is initialized.
    ///
    /// Corresponding application-specific ProposalMaker should always return correct payloads
    /// that are correct from the perspective of a block filter.
    ///
    /// This function is executed concurrently from peer tasks.
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
        B: BlockStore + BlockViewStore + CommitStore + Clone,
        C: Clock + Clone,
        P: ProposalMaker,
        F: BlockFilter,
        CP: ConnectionPool,
    >(
        core: Core<S, B>,
        block_store: B,
        pool: CP,
        clock: C,
        proposer: P,
        block_filter: F,
        protocol_config: ProtocolConfig,
    ) -> Self {
        let committee = core.committee().clone();
        let metrics = core.metrics().clone();
        let committer =
            UniversalCommitterBuilder::new(committee.clone(), block_store.clone(), metrics.clone())
                .with_pipeline(true)
                .with_leader_election(protocol_config.leader_election)
                .build();
        let validator_index = core.validator_index();
        let last_proposed_round = core.last_proposed_round();
        let (last_proposed_round_sender, last_proposed_round_receiver) =
            watch::channel(last_proposed_round);
        let last_known_round = last_proposed_round;
        let (blocks_sender, blocks_receiver) = mpsc::channel(BLOCKS_CHANNEL_CAPACITY);
        let inner = Arc::new(SyncerInner {
            block_store: block_store.clone(),
            last_proposed_round_receiver,
            validator_index,
            blocks_sender,
            committee: committee.clone(),
            block_filter,
            metrics: metrics.clone(),
            clock: clock.clone(),
        });
        let peer_routers = committee
            .enumerate_validators()
            .map(|(index, vi)| {
                let key = vi.network_key.clone();
                let peer_router = PeerRouter {
                    inner: inner.clone(),
                    peer_index: index,
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

        let block_manager = BlockManager::new(
            block_store.clone(),
            metrics.clone(),
            committee.clone(),
            protocol_config.clone(),
        );

        let syncer = SyncerTask {
            core,
            committer,
            block_store,
            rpc,
            fetcher,
            last_proposed_round_sender,
            blocks_receiver,
            last_decided,
            last_commit,
            stop: stop_receiver,
            clock,
            last_commit_sender,
            last_known_round,
            block_manager,
            uncommitted_counter: UncommittedCounter::new(metrics.clone()),
            metrics,
            protocol_config,
            _proposer: PhantomData,
        };
        #[cfg(feature = "syncer_thread")]
        let handle = thread::Builder::new()
            .name("bftd-server.syncer".to_string())
            .spawn(move || syncer.run_thread(proposer))
            .unwrap();
        #[cfg(not(feature = "syncer_thread"))]
        let handle = tokio::spawn(syncer.run(proposer));
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
        #[cfg(feature = "syncer_thread")]
        self.handle.join().ok();
        #[cfg(not(feature = "syncer_thread"))]
        self.handle.await.ok();
    }

    pub fn last_commit_receiver(&self) -> &watch::Receiver<Option<u64>> {
        &self.last_commit_receiver
    }
}

impl<
        S: Signer,
        B: BlockStore + CommitStore + BlockViewStore + Clone,
        C: Clock,
        P: ProposalMaker,
    > SyncerTask<S, B, C, P>
{
    const STALL_TIMEOUT: Duration = Duration::from_secs(15);

    #[cfg(feature = "syncer_thread")]
    pub fn run_thread(self, proposer: P) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(self.run(proposer));
    }

    pub async fn run(mut self, mut proposer: P) {
        self.try_make_proposal(&mut proposer);
        let (mut committed, mut skipped) = (0usize, 0usize);
        let mut proposal_deadline = ProposalDeadline::new();
        let mut stall_deadline = Instant::now() + Self::STALL_TIMEOUT;
        self.rpc.wait_connected(Duration::from_secs(2)).await;
        let mut blocks = Vec::with_capacity(BLOCKS_CHANNEL_CAPACITY);
        loop {
            select! {
                blocks_received = self.blocks_receiver.recv_many(&mut blocks, BLOCKS_CHANNEL_CAPACITY) => {
                    // main purpose for recv_many is not to propose too often during catch up
                    if blocks_received == 0 {
                        return;
                    }
                    stall_deadline = Instant::now() + Self::STALL_TIMEOUT;
                    let _timer = self.metrics.syncer_main_loop_util_ns.utilization_timer();
                    self.metrics.syncer_main_loop_calls.inc();
                    for (block, source) in blocks.drain(..) {
                        let reference = *block.reference();
                        // todo need more block verification
                        let age_ms = ns_to_ms(self.clock.time_ns().saturating_sub(block.time_ns()));
                        self.metrics.syncer_received_block_age_ms.with_label_values(&[self.metrics.validator_label(block.author())]).observe(age_ms as f64);
                        // todo - block manager can benefit from receiving all blocks in one call
                        // But for the fetcher we need to accurately track which blocks depends on which
                        let add_block_result = self.block_manager.add_block(block);
                        self.uncommitted_counter.add(&add_block_result.added);
                        self.core.add_blocks(&add_block_result.added);
                        self.fetcher.handle_add_block_result(&reference, &add_block_result);
                        let added_this = add_block_result.added.iter().any(|b|*b.reference() == reference);
                        tracing::debug!("Received {reference}({source}) {} age {age_ms} ms", if added_this {
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
                    if let Some(next_proposal_round) = self.core.threshold_clock_round() {
                        let check_round = next_proposal_round.previous();
                        // todo use ValidatorSet instead of Vec
                        let mut wait_leaders = self.committer.get_leaders(check_round);
                        wait_leaders.retain(|leader| {
                            if *leader == self.core.validator_index() {
                                return false; // not waiting for self
                            }
                            if self.block_store.last_known_round(*leader) < check_round {
                                if self.rpc.is_connected(self.committee().network_key(*leader)) {
                                    tracing::debug!("Not ready to make proposal, missing {}{}", leader, check_round);
                                    true
                                } else {
                                    tracing::debug!("Missing leader {}{}, not waiting because there is no connection", leader, check_round);
                                    false
                                }
                            } else {
                                // Leader block is present
                                false
                            }
                        });
                        if wait_leaders.is_empty() {
                            if !self.protocol_config.empty_commit_timeout_set() || self.uncommitted_counter.has_uncommitted_non_empty() {
                                self.try_make_proposal(&mut proposer);
                                proposal_deadline.reset();
                            } else if !proposal_deadline.is_set_for_payload() {
                                // todo set deadline based on round start time?
                                let deadline = Instant::now().add(self.protocol_config.empty_commit_timeout);
                                proposal_deadline.set_payload_deadline(deadline)
                            }
                        } else {
                            let previous = next_proposal_round.previous();
                            if previous > self.core.last_proposed_round() {
                                if self.committer.is_leader(previous, self.core.validator_index()) {
                                    self.try_make_proposal_for_round(previous, &mut proposer);
                                }
                            }
                            if !proposal_deadline.is_set() {
                                let deadline = Instant::now().add(self.protocol_config.leader_timeout);
                                proposal_deadline.set_leaders_deadline(deadline, wait_leaders.into_iter().collect());
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
                _ = OptionFuture::from(proposer.proposal_waiter()), if proposal_deadline.is_set_for_payload() => {
                    let _timer = self.metrics.syncer_main_loop_util_ns.utilization_timer();
                    self.metrics.syncer_main_loop_calls.inc();
                    self.try_make_proposal(&mut proposer);
                    proposal_deadline.reset();
                }
                _ = proposal_deadline.future() => {
                    let _timer = self.metrics.syncer_main_loop_util_ns.utilization_timer();
                    self.metrics.syncer_main_loop_calls.inc();
                    let waiting_round = self.core.threshold_clock_round().unwrap_or_default().previous();
                    if !proposal_deadline.is_set_for_payload() {
                        let timeouts: Vec<_> = proposal_deadline.waiting_validators.as_ref().unwrap().iter().map(|l|AuthorRound::new(*l, waiting_round)).collect();
                        self.metrics.syncer_leader_timeouts.inc();
                        tracing::warn!("Leader timeout {timeouts:?}");
                        tracing::trace!("Dag: {}", self.block_store.print_dag(&self.block_store.get(self.core.last_proposed_reference()).unwrap(), 6));
                    }
                    self.try_make_proposal(&mut proposer);
                    proposal_deadline.reset();
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
        self.uncommitted_counter.committed(&all_blocks);
        for block in &all_blocks {
            if !block.is_genesis() && block.author() == self.core.validator_index() {
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
        self.block_store.round_committed(leader.round());
        self.last_commit_sender.send(Some(commit.index())).ok();
        self.metrics
            .syncer_last_committed_round
            .set(commit.leader().round().0 as i64);
        self.metrics
            .syncer_last_commit_index
            .set(commit.index() as i64);
        self.last_commit = Some(commit);
    }

    fn try_make_proposal(&mut self, proposer: &mut P) {
        let round = self.core.threshold_clock_round();
        let Some(round) = round else {
            return;
        };

        let previous = round.previous();
        if previous > self.core.last_proposed_round() {
            if self
                .committer
                .is_leader(previous, self.core.validator_index())
            {
                self.try_make_proposal_for_round(previous, proposer);
            }
        }
        self.try_make_proposal_for_round(round, proposer);
    }

    fn try_make_proposal_for_round(&mut self, round: Round, proposer: &mut P) {
        if !self.core.critical_block_supported(round) {
            return;
        }
        let proposal = self
            .core
            .make_proposal(proposer, round, self.clock.time_ns());
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
    inner: Arc<SyncerInner<B, F, C>>,
    peer_index: ValidatorIndex,
}

impl<B: BlockReader + Send + Sync + 'static, C: Clock, F: BlockFilter> PeerRouter<B, C, F> {
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
        inner: Arc<SyncerInner<B, F, C>>,
        sender: mpsc::Sender<NetworkResponse>,
        mut last_sent: Round,
        peer_index: ValidatorIndex,
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
                let age_ms = ns_to_ms(inner.clock.time_ns().saturating_sub(own_block.time_ns()));
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
        inner: Arc<SyncerInner<B, F, C>>,
    ) -> anyhow::Result<()> {
        tracing::debug!("Starting receiving subscription from {peer}");
        while let Some(response) = receiver.recv().await {
            let response = response?;
            let response = bincode::deserialize::<StreamRpcResponse>(&response.0)?;
            let StreamRpcResponse::Block(block) = response;
            let block = inner
                .parse_verify_block(block, BlockMatch::Author(peer))
                .await?;
            if inner
                .blocks_sender
                .send((block, BlockSource::Subscription))
                .await
                .is_err()
            {
                break;
            }
        }
        tracing::debug!("Receiving subscription from {peer} ended");
        Ok(())
    }

    async fn receive_subscription(
        peer: ValidatorIndex,
        receiver: mpsc::Receiver<RpcResult<NetworkResponse>>,
        inner: Arc<SyncerInner<B, F, C>>,
    ) {
        if let Err(err) = Self::receive_subscription_inner(peer, receiver, inner).await {
            tracing::warn!("Error receiving stream from {peer}: {err}");
        }
    }
}

impl<B, F: BlockFilter, C: Clock> SyncerInner<B, F, C> {
    async fn parse_verify_block(
        &self,
        data: Bytes,
        matcher: BlockMatch,
    ) -> anyhow::Result<Arc<Block>> {
        let block = self
            .committee
            .verify_block(data, matcher, self.metrics.clone())?;
        self.complete_block_verification(block).await
    }

    async fn complete_block_verification(
        &self,
        block: BlockVerifiedByCommittee,
    ) -> anyhow::Result<Arc<Block>> {
        let block = block.extract_for_further_verification();
        let current_time_ns = self.clock.time_ns();
        let block_in_the_future_ns = block.time_ns().saturating_sub(current_time_ns);
        if block_in_the_future_ns != 0 {
            if block_in_the_future_ns < SLEEP_UP_TO_NS {
                tokio::time::sleep(Duration::from_nanos(block_in_the_future_ns)).await;
            } else {
                // todo - this is the only block check that not all correct validator will agree on.
                bail!(
                    "Rejecting block {} as it's timestamp is too far in the future: {} ms",
                    block.reference(),
                    ns_to_ms(block_in_the_future_ns)
                );
            }
        }
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
        // todo run one task per validator since other validators can starve complete_block_verification
        while let Some(block) = receiver.recv().await {
            let reference = *block.reference();
            let block = self.complete_block_verification(block).await;
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
            if self
                .blocks_sender
                .send((block, BlockSource::Rpc))
                .await
                .is_err()
            {
                return;
            }
        }
    }
}

impl<B: BlockReader + Send + Sync + 'static, C: Clock + Clone, F: BlockFilter> NetworkRpcRouter
    for PeerRouter<B, C, F>
{
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

// Test implementation returning constant value
impl Clock for u64 {
    fn time_ns(&self) -> u64 {
        *self
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

impl fmt::Display for BlockSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BlockSource::Subscription => write!(f, "sub"),
            BlockSource::Rpc => write!(f, "rpc"),
        }
    }
}

struct UncommittedCounter {
    uncommitted_non_empty_blocks: HashSet<BlockReference>,
    metrics: Arc<Metrics>,
}

impl UncommittedCounter {
    pub fn new(metrics: Arc<Metrics>) -> Self {
        // It's ok to have this clean on startup
        Self {
            uncommitted_non_empty_blocks: Default::default(),
            metrics,
        }
    }
    pub fn add(&mut self, blocks: &[Arc<Block>]) {
        for added in blocks {
            if !added.payload().is_empty() {
                assert!(self.uncommitted_non_empty_blocks.insert(*added.reference()));
            }
        }
        self.metrics
            .syncer_uncommitted_non_empty_blocks
            .set(self.uncommitted_non_empty_blocks.len() as i64);
    }

    pub fn committed(&mut self, blocks: &[Arc<Block>]) {
        for block in blocks {
            if !block.payload().is_empty() {
                // block might not be in the set since we don't report uncommitted blocks on restart
                self.uncommitted_non_empty_blocks.remove(block.reference());
            }
        }
        self.metrics
            .syncer_uncommitted_non_empty_blocks
            .set(self.uncommitted_non_empty_blocks.len() as i64);
    }

    pub fn has_uncommitted_non_empty(&self) -> bool {
        !self.uncommitted_non_empty_blocks.is_empty()
    }
}

struct ProposalDeadline {
    future: Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
    // None means deadline is not set
    // Some(none-empty-vec) means deadline is set for leaders
    // Some(empty-vec) means deadline is set for payload
    waiting_validators: Option<Vec<ValidatorIndex>>,
}

impl ProposalDeadline {
    pub fn new() -> Self {
        Self {
            future: futures::future::pending().boxed(),
            waiting_validators: None,
        }
    }
    pub fn set_leaders_deadline(&mut self, deadline: Instant, waiting: Vec<ValidatorIndex>) {
        self.future = tokio::time::sleep_until(deadline).boxed();
        self.waiting_validators = Some(waiting);
    }

    pub fn set_payload_deadline(&mut self, deadline: Instant) {
        self.future = tokio::time::sleep_until(deadline).boxed();
        self.waiting_validators = Some(vec![]);
    }

    pub fn reset(&mut self) {
        self.future = futures::future::pending().boxed();
        self.waiting_validators = None;
    }

    // Should be combined with future_set() check
    pub fn future(&mut self) -> &mut Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        &mut self.future
    }

    pub fn is_set(&self) -> bool {
        self.waiting_validators.is_some()
    }

    pub fn is_set_for_payload(&self) -> bool {
        if let Some(w) = &self.waiting_validators {
            w.is_empty()
        } else {
            false
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::block::tests::blk_p;

    #[test]
    fn uncommitted_counter_test() {
        let mut uc = UncommittedCounter::new(Metrics::new_test());
        let b1 = blk_p(0, 0, vec![], &[1]);
        let b2 = blk_p(1, 0, vec![], &[]);
        let b3 = blk_p(2, 0, vec![], &[2]);
        uc.add(&[b2.clone(), b3.clone()]);
        assert_eq!(uc.uncommitted_non_empty_blocks.len(), 1);
        uc.committed(&[b1]); // non empty but was not reported previously
        assert_eq!(uc.uncommitted_non_empty_blocks.len(), 1);
        uc.committed(&[b2]); // reported but empty
        assert_eq!(uc.uncommitted_non_empty_blocks.len(), 1);
        assert!(uc.has_uncommitted_non_empty());
        uc.committed(&[b3]);
        assert_eq!(uc.uncommitted_non_empty_blocks.len(), 0);
        assert!(!uc.has_uncommitted_non_empty());
    }
}
