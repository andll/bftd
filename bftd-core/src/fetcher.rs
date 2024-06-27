use crate::block::{BlockReference, ValidatorIndex};
use crate::block_manager::AddBlockResult;
use crate::committee::{BlockMatch, BlockVerifiedByCommittee, Committee};
use crate::log_byzantine;
use crate::metrics::Metrics;
use crate::rpc::{NetworkRequest, NetworkResponse, NetworkRpc, RpcResult};
use crate::syncer::{RpcRequest, RpcResponse};
use crate::threshold_clock::ValidatorSet;
use bytes::Bytes;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc;
use tokio::task::{AbortHandle, JoinSet};

pub struct BlockFetcher {
    fetch_requests: HashMap<BlockReference, FetchTaskControl>,
    inner: Arc<BlockFetcherInner>,
    tasks: JoinSet<()>,
}

struct BlockFetcherInner {
    blocks_sender: mpsc::Sender<BlockVerifiedByCommittee>,
    rpc: Arc<NetworkRpc>,
    committee: Arc<Committee>,
    metrics: Arc<Metrics>,
    validator_index: ValidatorIndex,
    inflight_peer_rpc: Vec<AtomicUsize>,
}

struct FetchTaskControl {
    abort_handle: AbortHandle,
    source: Arc<Mutex<ValidatorSet>>,
}

struct FetchTask {
    inner: Arc<BlockFetcherInner>,
    block_reference: BlockReference,
    source: Arc<Mutex<ValidatorSet>>,
    not_sent: Vec<ValidatorIndex>,
}

// todo - limit on the receiving side as well
const MAX_RPC_PER_PEER: usize = 50;

impl BlockFetcher {
    pub fn new(
        blocks_sender: mpsc::Sender<BlockVerifiedByCommittee>,
        rpc: Arc<NetworkRpc>,
        committee: Arc<Committee>,
        metrics: Arc<Metrics>,
        validator_index: ValidatorIndex,
    ) -> Self {
        let inflight_peer_rpc = committee
            .enumerate_indexes()
            .map(|_| Default::default())
            .collect();
        let inner = BlockFetcherInner {
            blocks_sender,
            rpc,
            committee,
            metrics,
            validator_index,
            inflight_peer_rpc,
        };
        let inner = Arc::new(inner);
        Self {
            fetch_requests: Default::default(),
            tasks: JoinSet::new(),
            inner,
        }
    }

    pub fn handle_add_block_result(&mut self, source_ref: &BlockReference, r: &AddBlockResult) {
        for new_missing in &r.new_missing {
            let source = ValidatorSet::from_iter([source_ref.author]);
            let source = Arc::new(Mutex::new(source));
            let not_sent = self
                .inner
                .committee
                .all_indexes_shuffled_excluding(self.inner.validator_index);
            let fetch_task = FetchTask {
                inner: self.inner.clone(),
                block_reference: *new_missing,
                source: source.clone(),
                not_sent,
            };
            assert!(!self.fetch_requests.contains_key(new_missing));
            let abort_handle = self.tasks.spawn(fetch_task.run());
            self.inner.metrics.fetcher_missing_block_tasks.inc();
            let fetch_request = FetchTaskControl {
                abort_handle,
                source,
            };
            self.fetch_requests.insert(*new_missing, fetch_request);
        }
        for previously_missing in &r.previously_missing {
            if let Some(control) = self.fetch_requests.get(previously_missing) {
                // This can be none because previously_missing can be suspended block which won't have fetch task running
                control.source.lock().insert(source_ref.author);
            }
        }
        for added in &r.added {
            self.block_received(added.reference());
        }
        for suspended in &r.suspended {
            self.block_received(suspended);
        }
        // Cleanup join set from completed tasks
        while let Some(next) = self.tasks.try_join_next() {
            // Entry from self.fetch_requests will be removed when block arrives to syncer
            next.ok(); // don't care about JoinError
            self.inner.metrics.fetcher_missing_block_tasks.dec();
        }
    }

    pub async fn stop(mut self) {
        self.tasks.abort_all();
        while let Some(next) = self.tasks.join_next().await {
            next.ok(); // don't care about JoinError
            self.inner.metrics.fetcher_missing_block_tasks.dec();
        }
    }

    fn block_received(&mut self, reference: &BlockReference) {
        if let Some(fetch_request) = self.fetch_requests.remove(reference) {
            fetch_request.abort_handle.abort();
        }
    }
}

impl FetchTask {
    pub async fn run(mut self) {
        let connected = self.inner.rpc.is_connected(
            self.inner
                .committee
                .network_key(self.block_reference.author),
        );
        if connected {
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        let request = RpcRequest::GetBlock(self.block_reference);
        let request = bincode::serialize(&request).expect("Request serialization failed");
        let request = Bytes::from(request);
        let inner = self.inner.clone();
        let mut futures = FuturesUnordered::new();
        loop {
            let priority_peer = self.find_priority();
            let was_source;
            // todo - need to have some limit on number of outbound RPCs to peer
            let peer = if let Some(peer) = priority_peer {
                was_source = true;
                Some(peer)
            } else {
                was_source = false;
                self.find_any()
            };
            if let Some(peer) = peer {
                peer.slice_get(&self.inner.inflight_peer_rpc)
                    .fetch_add(1, Ordering::Relaxed);
                futures.push(Self::fetch_block_from_peer_task(
                    &inner,
                    request.clone(),
                    peer,
                    was_source,
                    self.block_reference,
                ));
            } else {
                // No more peers(either created fetch task for each peer, or no connection to some peers)
            }
            // futures and self.not_sent can't be empty at the same time
            // This can change if we allow to remove peer from the list
            select! {
                resp = &mut futures.next(), if !futures.is_empty() => {
                    match resp.unwrap() {
                        Ok(block) => {
                            self.inner.blocks_sender.send(block).await.ok();
                            return;
                        }
                        Err(peer) => {
                            self.not_sent.push(peer);
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(500)), if !self.not_sent.is_empty() => {

                }
            }
        }
    }

    fn find_priority(&mut self) -> Option<ValidatorIndex> {
        let lock = self.source.lock();
        vec_take_if(&mut self.not_sent, |v| {
            let inflight = v
                .slice_get(&self.inner.inflight_peer_rpc)
                .load(Ordering::Relaxed);
            inflight < MAX_RPC_PER_PEER
                && lock.contains(*v)
                && self
                    .inner
                    .rpc
                    .is_connected(self.inner.committee.network_key(*v))
        })
    }

    fn find_any(&mut self) -> Option<ValidatorIndex> {
        vec_take_if(&mut self.not_sent, |v| {
            let inflight = v
                .slice_get(&self.inner.inflight_peer_rpc)
                .load(Ordering::Relaxed);
            inflight < MAX_RPC_PER_PEER
                && self
                    .inner
                    .rpc
                    .is_connected(self.inner.committee.network_key(*v))
        })
    }

    async fn fetch_block_from_peer_task(
        inner: &Arc<BlockFetcherInner>,
        request: Bytes,
        peer: ValidatorIndex,
        source: bool,
        reference: BlockReference,
    ) -> Result<BlockVerifiedByCommittee, ValidatorIndex> {
        tracing::debug!("Sending get_block RPC to {peer} to get block {reference}");
        let response = inner
            .rpc
            .rpc(inner.committee.network_key(peer), NetworkRequest(request))
            .await;
        peer.slice_get(&inner.inflight_peer_rpc)
            .fetch_sub(1, Ordering::Relaxed);
        let response = Self::parse_get_block_response(response);
        match response {
            Ok(Some(bytes)) => {
                // todo - use other checks such as BlockFilter
                match inner.committee.verify_block(
                    bytes,
                    BlockMatch::Reference(reference),
                    inner.metrics.clone(),
                ) {
                    Ok(block) => Ok(block),
                    Err(err) => {
                        log_byzantine!("[byzantine] Peer {peer} send incorrect block: {err}");
                        Err(peer)
                    }
                }
            }
            Ok(None) => {
                if source {
                    // todo can we remove suspended source block from BlockManager?
                    log_byzantine!("[byzantine] Peer {peer} sent block referencing {reference} but does respond to get_block request");
                }
                Err(peer)
            }
            Err(err) => {
                tracing::warn!("Rpc failed to {peer}: {err}");
                Err(peer)
            }
        }
    }

    fn parse_get_block_response(response: RpcResult<NetworkResponse>) -> RpcResult<Option<Bytes>> {
        let response = response?;
        let response = bincode::deserialize::<RpcResponse>(&response.0)?;
        let RpcResponse::GetBlockResponse(block) = response;
        Ok(block)
    }
}

fn vec_take_if<T, F: Fn(&T) -> bool>(v: &mut Vec<T>, f: F) -> Option<T> {
    let matched = v
        .iter()
        .enumerate()
        .find_map(|(i, t)| if f(t) { Some(i) } else { None });
    matched.map(|m| v.remove(m))
}
