use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use bftd_core::block::{BlockHash, BlockReference, Round, ValidatorIndex};
use bftd_core::block_manager::BlockStore;
use bftd_core::consensus::Commit;
use bftd_core::mempool::{BasicMempoolClient, MAX_TRANSACTION};
use bftd_core::store::CommitStore;
use bytes::Bytes;
use std::future::IntoFuture;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

pub struct BftdServer {
    join_handle: JoinHandle<io::Result<()>>,
}

impl BftdServer {
    pub async fn start<B: BlockStore + CommitStore>(
        address: SocketAddr,
        mempool_client: BasicMempoolClient,
        block_store: B,
    ) -> anyhow::Result<Self> {
        let state = BftdServerState {
            mempool_client,
            block_store,
        };
        let state = Arc::new(state);
        let app = Router::new()
            .route("/send", post(BftdServerState::send))
            .route(
                "/block/:round/:author/:hash",
                get(BftdServerState::get_block),
            )
            .route("/commit/:index", get(BftdServerState::get_commit))
            .with_state(state)
            // todo - check if MAX_TRANSACTION is allowed or need +1
            .layer(DefaultBodyLimit::max(MAX_TRANSACTION));

        tracing::info!("Bftd server started on {address}");
        let listener = TcpListener::bind(&address).await?;
        let join_handle = tokio::spawn(axum::serve(listener, app).into_future());
        Ok(Self { join_handle })
    }

    pub async fn stop(self) {
        self.join_handle.abort();
        self.join_handle.await.ok();
    }
}

struct BftdServerState<B> {
    mempool_client: BasicMempoolClient,
    block_store: B,
}

type HttpResult<T> = Result<T, (StatusCode, &'static str)>;

impl<B: BlockStore + CommitStore> BftdServerState<B> {
    async fn get_block(
        State(state): State<Arc<Self>>,
        Path(round): Path<u64>,
        Path(author): Path<u64>,
        Path(hash): Path<String>,
    ) -> HttpResult<Bytes> {
        let block_reference = BlockReference {
            round: Round(round),
            author: ValidatorIndex(author),
            hash: parse_block_hash(&hash)?,
        };
        let Some(block) = state.block_store.get(&block_reference) else {
            return Err((StatusCode::NOT_FOUND, "Block not found"));
        };
        Ok(block.data().clone())
    }

    async fn get_commit(
        State(state): State<Arc<Self>>,
        Path(index): Path<u64>,
    ) -> HttpResult<Json<Commit>> {
        let Some(commit) = state.block_store.get_commit(index) else {
            return Err((StatusCode::NOT_FOUND, "Commit not found"));
        };
        Ok(Json(commit))
    }

    async fn send(State(state): State<Arc<Self>>, body: Bytes) -> StatusCode {
        if body.len() > MAX_TRANSACTION {
            return StatusCode::BAD_REQUEST;
        }
        // todo - avoid memory copy
        if state
            .mempool_client
            .send_transaction(body.into())
            .await
            .is_ok()
        {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

fn parse_block_hash(h: &str) -> HttpResult<BlockHash> {
    match hex::decode(&h) {
        Ok(hash) => match hash.try_into() {
            Ok(hash) => Ok(BlockHash(hash)),
            Err(_) => Err((StatusCode::BAD_REQUEST, "Block hash length incorrect")),
        },
        Err(_) => Err((
            StatusCode::BAD_REQUEST,
            "Failed to decode block hash from hex",
        )),
    }
}
