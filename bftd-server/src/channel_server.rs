use crate::channel_payload::ChannelPayload;
use crate::channel_store::ChannelStore;
use crate::commit_reader::CommitReader;
use crate::mempool::{BasicMempoolClient, TransactionsPayloadReader};
use async_stream::stream;
use axum::extract::{DefaultBodyLimit, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive};
use axum::response::Sse;
use axum::routing::{get, post};
use axum::{Json, Router};
use bftd_core::committee::Committee;
use bftd_core::store::BlockReader;
use bftd_core::store::CommitStore;
use bftd_core::syncer::Syncer;
use bftd_types::http_types::{
    ChannelElementMetadata, ChannelElementWithMetadata, Metadata, NetworkInfo, SubmitQuery,
    TailQuery,
};
use bftd_types::types::ChannelElementKey;
use bytes::Bytes;
use futures::Stream;
use std::collections::HashSet;
use std::future::IntoFuture;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::{io, mem};
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
        let committee = syncer.committee().clone();
        let store_sync_handler = tokio::spawn(Self::sync_channel_store(
            block_store.clone(),
            syncer,
            channel_store.clone(),
        ));
        let state = BftdChannelServerState {
            mempool_client,
            channel_store,
            block_store,
            committee,
            http_port: address.port(), // assuming all peers have the same port
        };
        let state = Arc::new(state);
        let app = Router::new()
            .route("/info", get(BftdChannelServerState::info))
            .route("/ping", get(BftdChannelServerState::ping))
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
        let mut commit_reader = CommitReader::start(store, &syncer, read_commits_from);
        while let Some(commit) = commit_reader.recv_commit().await {
            let mut channels = HashSet::new();
            let commit_index = commit.info.index();
            for block in commit.blocks.iter() {
                let reader = TransactionsPayloadReader::new_verify(block.payload_bytes())
                    .expect("Deserializing block payload should not fail, is block filter installed correctly?");
                for payload in reader.iter_bytes() {
                    let payload = ChannelPayload::from_bytes(&payload);
                    let payload = payload.unwrap(); // todo block filter
                    for channel in payload.channels() {
                        channels.insert(*channel);
                    }
                }
            }
            channel_store.update_channels(commit_index, channels);
        }
    }
}

fn encode_element(block: usize, element: usize) -> u32 {
    // todo - enforce max number of elements in block
    (element + (block << 16)) as u32
}

struct BftdChannelServerState<B> {
    mempool_client: BasicMempoolClient,
    channel_store: Arc<ChannelStore>,
    block_store: B,
    committee: Arc<Committee>,
    http_port: u16,
}

impl<B: CommitStore + BlockReader> BftdChannelServerState<B> {
    async fn info(State(state): State<Arc<Self>>) -> Json<NetworkInfo> {
        let chain_id = state.committee.chain_id().to_string();
        let committee = state
            .committee
            .enumerate_validators()
            .map(|(_, v)| {
                let address = v
                    .network_address
                    .to_socket_addrs()
                    .expect("Failed to parse committee member address")
                    .next()
                    .expect("Committee member address is empty");
                format!("http://{}:{}", address.ip(), state.http_port)
            })
            .collect();
        Json(NetworkInfo {
            chain_id,
            committee,
        })
    }

    async fn ping(_: State<Arc<Self>>) -> StatusCode {
        StatusCode::OK
    }

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
        Query(tail): Query<TailQuery>,
    ) -> Result<Sse<impl Stream<Item = Result<Event, axum::Error>>>, StatusCode> {
        let mut receiver = state.channel_store.subscribe_channel(tail.from);
        let meta_timestamps = tail.metadata.contains(&Metadata::Timestamp);
        let meta_block = tail.metadata.contains(&Metadata::Block);
        let meta_commit = tail.metadata.contains(&Metadata::Commit);
        let channel = tail.from.channel;

        let stream = stream! {
            const CAPACITY: usize = 32;
            let mut commits = Vec::with_capacity(CAPACITY);
            while receiver.recv_many(&mut commits, CAPACITY).await > 0 {
                let mut elements = vec![];
                for commit_index in mem::take(&mut commits) {
                    let commit = state.block_store.get_commit(commit_index).expect("Commit not found");
                    for (block_index, block) in commit.all_blocks().iter().enumerate() {
                        let block = state.block_store.get(block).expect("Committed block not found");
                        let payload = TransactionsPayloadReader::new_verify(block.payload_bytes())
                            .expect("Failed to parse payload");
                        for (index, payload) in payload.iter_bytes().enumerate() {
                            // todo ensure in block filter
                            let payload = ChannelPayload::from_bytes(&payload).expect("Failed to parse channel payload");
                            if !payload.channels().contains(&channel) {
                                continue;
                            }

                            let element = encode_element(block_index, index);

                            let key = ChannelElementKey {
                                channel,
                                commit_index,
                                element,
                            };

                            if key <= tail.from {
                                continue;
                            }

                            let mut metadata = ChannelElementMetadata::default();
                            if meta_timestamps {
                                metadata.timestamp_ns = Some(commit.commit_timestamp_ns());
                            }
                            if meta_commit {
                                metadata.commit = format!("{:08x}#{}", commit.index(), hex::encode(commit.commit_hash()));
                            }
                            if meta_block {
                                metadata.block  = format!("{}", block.reference());
                            }
                            let element = ChannelElementWithMetadata {
                                key,
                                data: payload.payload().to_vec(),
                                metadata,
                            };
                            elements.push(element);
                        }
                    }
                }
                yield Event::default()
                .json_data(&elements);
            }
        };
        Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
    }
}
