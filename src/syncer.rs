use crate::block::{AuthorRound, Block, BlockReference, Round, ValidatorIndex};
use crate::block_manager::BlockStore;
use crate::committee::Committee;
use crate::consensus::{DecidedCommit, UniversalCommitter, UniversalCommitterBuilder};
use crate::core::Core;
use crate::crypto::Signer;
use crate::rpc::{
    NetworkRequest, NetworkResponse, NetworkRpc, NetworkRpcRouter, PeerRpcTaskCommand, RpcResult,
};
use crate::ConnectionPool;
use bytes::Bytes;
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::ops::Add;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

pub struct Syncer {
    handle: JoinHandle<()>,
    stop: oneshot::Sender<()>,
}

struct SyncerTask<S, B> {
    core: Core<S, B>,
    committer: UniversalCommitter<B>,
    block_store: B,
    rpc: NetworkRpc,
    last_proposed_round_sender: tokio::sync::watch::Sender<Round>,
    blocks_receiver: mpsc::Receiver<Arc<Block>>,
    last_decided: AuthorRound,
    stop: oneshot::Receiver<()>,
}

struct SyncerInner<B> {
    block_store: B,
    last_proposed_round_receiver: tokio::sync::watch::Receiver<Round>,
    validator_index: ValidatorIndex,
    committee: Arc<Committee>,
    blocks_sender: mpsc::Sender<Arc<Block>>,
}

impl Syncer {
    pub fn start<S: Signer, B: BlockStore + Clone>(
        core: Core<S, B>,
        block_store: B,
        pool: ConnectionPool,
    ) -> Self {
        let committee = core.committee().clone();
        let committer = UniversalCommitterBuilder::new(committee.clone(), block_store.clone())
            .with_pipeline(true)
            .with_number_of_leaders(1)
            .build();
        let validator_index = core.validator_index();
        let (last_proposed_round_sender, last_proposed_round_receiver) =
            tokio::sync::watch::channel(Round::ZERO);
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
                };
                (key, Box::new(peer_router) as Box<dyn NetworkRpcRouter>)
            })
            .collect();
        let rpc = NetworkRpc::start(pool, peer_routers);
        let (stop_sender, stop_receiver) = oneshot::channel();
        let syncer = SyncerTask {
            core,
            committer,
            block_store,
            rpc,
            last_proposed_round_sender,
            blocks_receiver,
            last_decided: AuthorRound::default(), // todo load
            stop: stop_receiver,
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

impl<S: Signer, B: BlockStore + Clone> SyncerTask<S, B> {
    pub async fn run(mut self) {
        for block in self.core.committee().genesis_blocks() {
            let block = Arc::new(block);
            self.core.add_block(block);
        }
        let proposed = self.make_proposal();
        assert!(proposed, "must generate proposal after genesis");
        let (mut committed, mut skipped) = (0usize, 0usize);
        let mut proposal_deadline: Pin<Box<dyn Future<Output = ()> + Send>> =
            futures::future::pending().boxed();
        let mut proposal_deadline_set = false;
        loop {
            select! {
                block = self.blocks_receiver.recv() => {
                    // use rand::Rng;
                    // use rand::rngs::ThreadRng;
                    // let d = ThreadRng::default().gen_range(0..1000);
                    // tokio::time::sleep(std::time::Duration::from_micros(d)).await;
                    let Some(block) = block else {return;};
                    log::debug!("[{}] Received block {}", self.core.validator_index(), block.reference());
                    // todo need more block verification
                    let _new_missing = self.core.add_block(block);
                    // todo handle missing blocks
                    if let Some(next_proposal_round) = self.core.next_proposal_round() {
                        let check_round = next_proposal_round.previous();
                        let mut ready = true;
                        let leaders = self.committer.get_leaders(check_round);
                        for leader in leaders {
                            // todo - exists method instead of get
                            if self.block_store.get_blocks_at_author_round(leader, check_round).is_empty() {
                                log::debug!("[{}] Not ready to make proposal, missing {}{}", self.core.validator_index(), leader, check_round);
                                // todo check network connection to the leader
                                ready = false;
                                break;
                            }
                        }
                        if ready {
                            self.make_proposal();
                            proposal_deadline = futures::future::pending().boxed();
                            proposal_deadline_set = false;
                        } else {
                            if !proposal_deadline_set {
                                proposal_deadline = tokio::time::sleep_until(Instant::now().add(Duration::from_secs(1))).boxed();
                                proposal_deadline_set = true;
                            }
                        }
                    }
                    let commits = self.committer.try_commit(self.last_decided, *self.last_proposed_round_sender.borrow());
                    for c in commits {
                        self.last_decided = c.author_round();
                        match c  {
                            DecidedCommit::Commit(c) => {
                                log::debug!("[{}] Committed {}", self.core.validator_index(), c.reference());
                                committed += 1;
                            },
                            DecidedCommit::Skip(author_round) => {
                                log::debug!("[{}] Skipping commit at {}", self.core.validator_index(), author_round);
                                skipped += 1;
                            }
                        }
                    }
                    if (committed % 10 == 0 && committed > 0) || (skipped % 10 == 0 && skipped > 0) {
                        println!("[{}] stat: committed {}, skipped {}", self.core.validator_index(), committed, skipped);
                    }
                }
                _ = &mut proposal_deadline => {
                    self.make_proposal();
                    proposal_deadline = futures::future::pending().boxed();
                    proposal_deadline_set = false;
                }
                _ = &mut self.stop => {
                    break;
                }
            }
        }
        log::debug!("Syncer stopped, waiting for rpc to stop");
        self.rpc.stop().await;
    }

    fn make_proposal(&mut self) -> bool {
        let proposal = self.core.try_make_proposal(&mut ());
        if let Some(proposal) = proposal {
            log::debug!(
                "[{}] Generated proposal {}",
                self.core.validator_index(),
                proposal
            );
            self.last_proposed_round_sender
                .send(proposal.reference().round)
                .ok();
            true
        } else {
            false
        }
    }
}

struct PeerRouter<B> {
    inner: Arc<SyncerInner<B>>,
    peer_index: ValidatorIndex,
}

impl<B: BlockStore> PeerRouter<B> {
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
                tokio::spawn(Self::stream_task(self.inner.clone(), sender, round));
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
    ) {
        let mut round_receiver = inner.last_proposed_round_receiver.clone();
        // todo - check initial condition is ok
        while let Ok(()) = round_receiver.changed().await {
            let round = *round_receiver.borrow();
            while round > last_sent {
                // todo batch read range w/ chunks
                last_sent = last_sent.next();
                let Some(own_block) = inner.block_store.get_own(inner.validator_index, last_sent)
                else {
                    continue; // todo - more efficient, iterating through each round right now
                };
                let response = StreamRpcResponse::Block(own_block.data().clone());
                let response = bincode::serialize(&response).expect("Serialization failed");
                if sender.send(NetworkResponse(response.into())).await.is_err() {
                    return;
                }
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
            log::warn!("Error receiving stream from {peer}: {err}");
        }
    }
}

impl<B: BlockStore> NetworkRpcRouter for PeerRouter<B> {
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
