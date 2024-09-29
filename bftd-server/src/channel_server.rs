use crate::channel_payload::ChannelPayload;
use crate::channel_store::{ChannelElementKey, ChannelId, ChannelStore};
use crate::commit_reader::CommitReader;
use crate::mempool::BasicMempoolClient;
use async_stream::stream;
use axum::extract::{DefaultBodyLimit, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive};
use axum::response::Sse;
use axum::routing::{get, post};
use axum::Router;
use bftd_core::store::BlockReader;
use bftd_core::store::CommitStore;
use bftd_core::syncer::Syncer;
use bytes::Bytes;
use futures::Stream;
use serde::{Deserialize, Serialize};
use serde_with::formats::CommaSeparator;
use serde_with::serde_as;
use serde_with::StringWithSeparator;
use std::future::IntoFuture;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

pub struct BftdChannelServer {
    join_handle: JoinHandle<io::Result<()>>,
    store_sync_handler: JoinHandle<()>,
}

pub const MAX_CHUNK: usize = 64 * 1024 * 1024;

impl BftdChannelServer {
    pub async fn start<B: BlockReader + CommitStore + Clone>(
        address: SocketAddr,
        mempool_client: BasicMempoolClient,
        block_store: B,
        syncer: Arc<Syncer>,
        path: impl AsRef<std::path::Path>,
    ) -> anyhow::Result<Self> {
        let channel_store = ChannelStore::open(path).expect("Failed to open channel store");
        let channel_store = Arc::new(channel_store);
        let store_sync_handler = tokio::spawn(Self::sync_channel_store(
            block_store,
            syncer,
            channel_store.clone(),
        ));
        let state = BftdChannelServerState {
            mempool_client,
            channel_store,
        };
        let state = Arc::new(state);
        let app = Router::new()
            .route("/submit", post(BftdChannelServerState::submit))
            .route("/tail", get(BftdChannelServerState::tail))
            .with_state(state)
            // todo - check if MAX_CHUNK is allowed or need +1
            .layer(DefaultBodyLimit::max(MAX_CHUNK));

        tracing::info!("Bftd channel server started on {address}");
        let listener = TcpListener::bind(&address).await?;
        let join_handle = tokio::spawn(axum::serve(listener, app).into_future());
        Ok(Self {
            join_handle,
            store_sync_handler,
        })
    }

    pub async fn stop(self) {
        self.join_handle.abort();
        self.store_sync_handler.abort();
        self.join_handle.await.ok();
        self.store_sync_handler.await.ok();
    }

    async fn sync_channel_store<B: BlockReader + CommitStore>(
        store: B,
        syncer: Arc<Syncer>,
        channel_store: Arc<ChannelStore>,
    ) {
        let last_processed = channel_store.last_processed_commit_index();
        let read_commits_from = if let Some(last_processed) = last_processed {
            last_processed + 1
        } else {
            Default::default()
        };
        let mut commit_reader = CommitReader::start(
            store,
            syncer.last_commit_receiver().clone(),
            read_commits_from,
        );
        while let Some(commit) = commit_reader.recv_commit().await {
            let mut commit_channel_updates = vec![];
            let commit_index = commit.info.index();
            for (_, reader) in commit.blocks {
                for payload in reader.iter_bytes() {
                    let payload = ChannelPayload::from_bytes(&payload);
                    let payload = payload.unwrap(); // todo block filter
                    for channel in payload.channels() {
                        commit_channel_updates.push((*channel, payload.payload().clone()));
                    }
                }
            }
            channel_store.update_channels(commit_index, commit_channel_updates.into_iter());
        }
    }
}

struct BftdChannelServerState {
    mempool_client: BasicMempoolClient,
    channel_store: Arc<ChannelStore>,
}

impl BftdChannelServerState {
    async fn submit(
        State(state): State<Arc<Self>>,
        Query(query): Query<SubmitQuery>,
        body: Bytes,
    ) -> StatusCode {
        if body.len() > MAX_CHUNK {
            return StatusCode::BAD_REQUEST;
        }
        let body = ChannelPayload::new(query.channels, body);
        let body = body.into_byte_vec();
        if state.mempool_client.send_transaction(body).await.is_ok() {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        }
    }

    async fn tail(
        State(state): State<Arc<Self>>,
        Query(tail): Query<Tail>,
    ) -> Result<Sse<impl Stream<Item = Result<Event, axum::Error>>>, StatusCode> {
        let mut receiver = state.channel_store.subscribe_channel(tail.from);
        let stream = stream! {
            while let Some((key, data)) = receiver.recv().await {
                yield Event::default()
                .id(key.to_string())
                .json_data(AsString(&data));
            }
        };
        Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
    }
}

#[serde_as]
#[derive(Deserialize)]
struct SubmitQuery {
    #[serde_as(as = "StringWithSeparator::<CommaSeparator, ChannelId>")]
    channels: Vec<ChannelId>,
}
#[serde_as]
#[derive(Serialize)]
struct AsString<'a>(#[serde_as(as = "serde_with::Bytes")] &'a [u8]);

#[derive(Deserialize)]
struct Tail {
    from: ChannelElementKey,
}
