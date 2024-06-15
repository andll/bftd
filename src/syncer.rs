use crate::block::{Block, BlockReference, Round, ValidatorIndex};
use crate::block_manager::BlockStore;
use crate::core::Core;
use crate::crypto::Signer;
use crate::rpc::{NetworkRequest, NetworkResponse, NetworkRpc, NetworkRpcRouter};
use crate::ConnectionPool;
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct Syncer<S, B> {
    inner: Arc<SyncerInner<S, B>>,
    rpc: NetworkRpc,
}

struct SyncerInner<S, B> {
    core: Mutex<Core<S, B>>,
    block_store: B,
    last_proposed_round: tokio::sync::watch::Receiver<Round>,
    validator_index: ValidatorIndex,
}

impl<S: Signer, B: BlockStore> Syncer<S, B> {
    pub fn start(core: Core<S, B>, block_store: B, pool: ConnectionPool) -> Self {
        let committee = core.committee().clone();
        let validator_index = core.validator_index();
        let core = Mutex::new(core);
        let (last_proposed_round_s, last_proposed_round_r) =
            tokio::sync::watch::channel(Round::ZERO);
        let inner = Arc::new(SyncerInner {
            core,
            block_store,
            last_proposed_round: last_proposed_round_r,
            validator_index,
        });
        let peer_routers = committee
            .enumerate_validators()
            .map(|(index, vi)| {
                let key = vi.network_key.clone();
                let peer_router = PeerRouter {
                    inner: inner.clone(),
                    index,
                };
                (key, Box::new(peer_router) as Box<dyn NetworkRpcRouter>)
            })
            .collect();
        let rpc = NetworkRpc::start(pool, peer_routers);
        Self { inner, rpc }
    }
}

struct PeerRouter<S, B> {
    inner: Arc<SyncerInner<S, B>>,
    index: ValidatorIndex,
}

impl<S: Signer, B: BlockStore> PeerRouter<S, B> {
    fn stream_rpc(
        &mut self,
        req: NetworkRequest,
    ) -> anyhow::Result<mpsc::Receiver<NetworkResponse>> {
        let r = bincode::deserialize::<StreamRpcRequest>(&req.0)?;
        match r {
            StreamRpcRequest::Subscribe(round) => {
                // todo track task
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
        inner: Arc<SyncerInner<S, B>>,
        sender: mpsc::Sender<NetworkResponse>,
        mut last_sent: Round,
    ) {
        let mut round_receiver = inner.last_proposed_round.clone();
        // todo - check initial condition is ok
        while let Ok(()) = round_receiver.changed().await {
            let round = *round_receiver.borrow();
            loop {
                if round > last_sent {
                    // todo batch read range w/ chunks
                    let last_sent = last_sent.next();
                    let own_block = inner
                        .block_store
                        .get_own(inner.validator_index, last_sent)
                        .expect("Missing own block which was signaled as ready");
                    let response = RpcResponse::Block(own_block.data().clone());
                    let response = bincode::serialize(&response).expect("Serialization failed");
                    if sender.send(NetworkResponse(response.into())).await.is_err() {
                        return;
                    }
                }
            }
        }
    }
}

impl<S: Signer, B: BlockStore> NetworkRpcRouter for PeerRouter<S, B> {
    fn rpc(&mut self, req: NetworkRequest) -> NetworkResponse {
        self.rpc(req).unwrap() // todo handle error
    }

    fn stream_rpc(&mut self, req: NetworkRequest) -> mpsc::Receiver<NetworkResponse> {
        self.stream_rpc(req).unwrap() // todo handle error
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
    Block(Bytes),
    GetBlockResponse(Option<Bytes>),
}
