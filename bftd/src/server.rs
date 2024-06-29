use crate::mempool::{BasicMempoolClient, TransactionsPayloadReader, MAX_TRANSACTION};
use async_stream::stream;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::header::ACCEPT;
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive};
use axum::response::{IntoResponse, Response, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use bftd_core::block::{Block, BlockHash, BlockReference, Round, ValidatorIndex};
use bftd_core::consensus::Commit;
use bftd_core::store::BlockReader;
use bftd_core::store::CommitStore;
use bftd_core::syncer::Syncer;
use bytes::Bytes;
use futures::Stream;
use serde::{Deserialize, Serialize, Serializer};
use std::future::IntoFuture;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::Instant;

pub struct BftdServer {
    join_handle: JoinHandle<io::Result<()>>,
}

impl BftdServer {
    pub async fn start<B: BlockReader + CommitStore>(
        address: SocketAddr,
        mempool_client: BasicMempoolClient,
        block_store: B,
        syncer: Arc<Syncer>,
    ) -> anyhow::Result<Self> {
        let state = BftdServerState {
            mempool_client,
            block_store,
            syncer,
        };
        let state = Arc::new(state);
        let app = Router::new()
            .route("/send", post(BftdServerState::send))
            .route("/tail", get(BftdServerState::tail))
            .route(
                "/blocks/:round/:author/:hash",
                get(BftdServerState::get_block),
            )
            .route("/commits/:index", get(BftdServerState::get_commit))
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
    syncer: Arc<Syncer>,
}

type HttpResult<T> = Result<T, (StatusCode, &'static str)>;

impl<B: BlockReader + CommitStore> BftdServerState<B> {
    async fn get_block(
        State(state): State<Arc<Self>>,
        headers: HeaderMap,
        Path((round, author, hash)): Path<(u64, u64, String)>,
    ) -> HttpResult<Response> {
        let block_reference = BlockReference {
            round: Round(round),
            author: ValidatorIndex(author),
            hash: parse_block_hash(&hash)?,
        };
        let Some(accept) = headers.get(ACCEPT) else {
            return Err((
                StatusCode::BAD_REQUEST,
                "Use Accept header(application/json or application/octet-stream)",
            ));
        };
        let Some(block) = state.block_store.get(&block_reference) else {
            return Err((StatusCode::NOT_FOUND, "Block not found"));
        };
        let accept = accept.to_str().map_err(|_| ());
        if accept.eq(&Ok(mime::APPLICATION_OCTET_STREAM.as_ref())) {
            Ok(block.data().clone().into_response())
        } else if accept.eq(&Ok(mime::APPLICATION_JSON.as_ref())) {
            // panic here is prevented by using TransactionsPayloadBlockFilter when running cluster
            let payload_reader = TransactionsPayloadReader::new_verify(block.payload_bytes())
                .expect("Local block has invalid payload");
            let block = JsonBlock::from_block(&block, &payload_reader);
            Ok(Json(block).into_response())
        } else {
            return Err((StatusCode::BAD_REQUEST, "Unsupported Accept value"));
        }
    }

    async fn get_commit(
        State(state): State<Arc<Self>>,
        Path(index): Path<u64>,
    ) -> HttpResult<Json<JsonCommit>> {
        let lag = index.saturating_sub(
            state
                .syncer
                .last_commit_receiver()
                .borrow()
                .unwrap_or_default(),
        );
        if lag > 10 {
            return Err((StatusCode::NOT_FOUND, "Commit not found"));
        } else if lag > 0 {
            // We can block request for short period if commit is not yet available but lag is not too large
            // This allows to use get_commit endpoint to long poll for next commit
            // (nit) there is off-by-one error here as we do not wait for 0 commit if it's not available. Seem not important though
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut receiver = state.syncer.last_commit_receiver().clone();
            while receiver.borrow_and_update().unwrap_or_default() < index {
                let fut = tokio::time::timeout_at(deadline, receiver.changed());
                match fut.await {
                    Ok(Ok(())) => {}                                                   // resume loop
                    Ok(Err(_)) => return Err((StatusCode::INTERNAL_SERVER_ERROR, "")), // syncer has stopped
                    Err(_) => return Err((StatusCode::NOT_FOUND, "Commit not found")), // timeout
                }
            }
        }
        let Some(commit) = state.block_store.get_commit(index) else {
            return Err((StatusCode::NOT_FOUND, "Commit not found"));
        };
        Ok(Json(JsonCommit::from_commit(&commit)))
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

    async fn tail(
        State(state): State<Arc<Self>>,
        Query(tail): Query<Tail>,
    ) -> Result<Sse<impl Stream<Item = anyhow::Result<Event>>>, StatusCode> {
        let last_commit = state
            .syncer
            .last_commit_receiver()
            .borrow()
            .unwrap_or_default();
        if tail.from > last_commit {
            return Err(StatusCode::NOT_FOUND);
        }
        let mut from = tail.from;
        let mut commit_receiver = state.syncer.last_commit_receiver().clone();
        let stream = stream! {
            loop {
                let last = commit_receiver.borrow_and_update().map(|c|c+1).unwrap_or_default();
                for commit in from..last {
                    let commit = state.block_store.get_commit(commit).expect("Commit not found");
                    // todo cache
                    let blocks = UnrolledCommit::unroll_commit(&commit, &state.block_store);
                    // todo quite a bit of overhead with empty commits
                    if blocks.is_empty() {
                        continue;
                    }
                    let commit = UnrolledCommit::from(&commit, &blocks);
                    let commit = serde_json::to_string_pretty(&commit).expect("Serialization failed");
                    let event = Event::default().data(&commit);
                    yield Ok(event);
                }
                from = last;
                if let Err(_) = commit_receiver.changed().await {
                    break;
                }
            }
        };
        let sse = Sse::new(stream).keep_alive(KeepAlive::default());
        Ok(sse)
    }
}

#[derive(Deserialize)]
struct Tail {
    #[serde(default)]
    from: u64,
    #[serde(default)]
    skip_empty_commits: bool,
    #[serde(default)]
    pretty: bool,
}

#[derive(Serialize)]
struct UnrolledCommit<'a> {
    commit: JsonCommit,
    blocks: Vec<JsonBlock<'a>>,
}

impl<'a> UnrolledCommit<'a> {
    pub fn from(commit: &Commit, blocks: &'a [(Arc<Block>, TransactionsPayloadReader)]) -> Self {
        let info = JsonCommit::from_commit(commit);
        let blocks = blocks
            .iter()
            .map(|(b, t)| JsonBlock::from_block(b, t))
            .collect();
        Self {
            commit: info,
            blocks,
        }
    }

    /// Load all blocks in the commit, skipping blocks with empty payload
    fn unroll_commit(
        commit: &Commit,
        block_store: &impl BlockReader,
    ) -> Vec<(Arc<Block>, TransactionsPayloadReader)> {
        let blocks = block_store.get_multi(commit.all_blocks());
        blocks
            .into_iter()
            .filter_map(|b| {
                let b = b.expect("Block referenced by commit not found in store");
                let reader = TransactionsPayloadReader::new_verify(b.payload_bytes())
                    .expect("Locally stored block has incorrect payload");
                if reader.is_empty() {
                    None
                } else {
                    Some((b, reader))
                }
            })
            .collect()
    }
}

#[derive(Serialize)]
struct JsonBlock<'a> {
    reference: String,
    signature: String,
    chain_id: String,
    time_ns: u64,
    parents: Vec<String>,
    transactions: Vec<TransactionSer<'a>>,
}
#[derive(Serialize)]
struct JsonCommit {
    index: u64,
    leader: String,
    commit_timestamp_ns: u64,
    /// All blocks in commit, leader block is the last block in this list
    all_blocks: Vec<String>,
    previous_commit_hash: String,
    commit_hash: String,
}

struct TransactionSer<'a>(&'a [u8]);

impl<'a> Serialize for TransactionSer<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&hex::encode(self.0))
    }
}

impl<'a> JsonBlock<'a> {
    pub fn from_block(block: &'a Block, payload_reader: &'a TransactionsPayloadReader) -> Self {
        Self {
            reference: format_block_reference(block.reference()),
            signature: hex::encode(&block.signature().0),
            chain_id: format_hash(&block.chain_id().0),
            time_ns: block.time_ns(),
            parents: block.parents().iter().map(format_block_reference).collect(),
            transactions: payload_reader.iter_slices().map(TransactionSer).collect(),
        }
    }
}

impl JsonCommit {
    pub fn from_commit(commit: &Commit) -> Self {
        Self {
            index: commit.index(),
            leader: format_block_reference(commit.leader()),
            commit_timestamp_ns: commit.commit_timestamp_ns(),
            all_blocks: commit
                .all_blocks()
                .iter()
                .map(format_block_reference)
                .collect(),
            previous_commit_hash: format_hash(&commit.previous_commit_hash().unwrap_or_default()),
            commit_hash: format_hash(commit.commit_hash()),
        }
    }
}

fn format_block_reference(r: &BlockReference) -> String {
    format!(
        "{:0>6}/{:0>6}/{}",
        r.round.0,
        r.author.0,
        format_hash(&r.hash.0)
    )
}

fn format_hash(hash: &[u8]) -> String {
    hex::encode(hash)
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
