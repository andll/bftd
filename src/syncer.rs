use crate::block::{BlockReference, Round, ValidatorIndex};
use crate::block_manager::BlockStore;
use crate::core::Core;
use crate::crypto::Signer;
use crate::rpc::{NetworkRequest, NetworkResponse, NetworkRpc, NetworkRpcRouter};
use crate::ConnectionPool;
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;

pub struct Syncer<S, B> {
    inner: Arc<SyncerInner<S, B>>,
    rpc: NetworkRpc,
}

struct SyncerInner<S, B> {
    core: Mutex<Core<S, B>>,
}

impl<S: Signer, B: BlockStore> Syncer<S, B> {
    pub fn start(core: Core<S, B>, pool: ConnectionPool) -> Self {
        let committee = core.committee().clone();
        let core = Mutex::new(core);
        let inner = Arc::new(SyncerInner { core });
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
    fn stream_rpc(&mut self, req: NetworkRequest) -> anyhow::Result<Receiver<NetworkResponse>> {
        let r = bincode::deserialize::<StreamRpcRequest>(&req.0)?;
        match r {
            StreamRpcRequest::Subscribe(round) => {
                unimplemented!()
            }
        }
    }

    fn rpc(&mut self, req: NetworkRequest) -> anyhow::Result<NetworkResponse> {
        let r = bincode::deserialize::<RpcRequest>(&req.0)?;
        match r {
            RpcRequest::GetBlock(reference) => {
                unimplemented!()
            }
        }
    }
}

impl<S: Signer, B: BlockStore> NetworkRpcRouter for PeerRouter<S, B> {
    fn rpc(&mut self, req: NetworkRequest) -> NetworkResponse {
        self.rpc(req).unwrap() // todo handle error
    }

    fn stream_rpc(&mut self, req: NetworkRequest) -> Receiver<NetworkResponse> {
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
}
