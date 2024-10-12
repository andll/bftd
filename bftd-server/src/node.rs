use crate::builder::BftdBuilder;
use crate::channel_server::BftdChannelServer;
use crate::commit_reader::CommitReader;
use crate::load_gen::{LoadGen, LoadGenConfig, LoadGenMetrics};
use crate::mempool::{BasicMempool, BasicMempoolClient, TransactionsPayloadBlockFilter};
use crate::prometheus::{start_prometheus_server, PrometheusJoinHandle};
use crate::server::BftdServer;
use bftd_core::block_cache::BlockCache;
use bftd_core::genesis::Genesis;
use bftd_core::store::rocks_store::RocksStore;
use bftd_core::syncer::Syncer;
use futures::future::join_all;
use futures::FutureExt;
use prometheus::Registry;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use tokio::runtime;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

pub struct Node {
    builder: BftdBuilder,
}

pub struct NodeHandle {
    syncer: Arc<Syncer>,
    prometheus_handle: Option<PrometheusJoinHandle>,
    server_handle: Option<Box<dyn Stoppable>>,
    runtime: Arc<Runtime>,
    load_gen_handle: Option<JoinHandle<()>>,
    store: Arc<BlockCache<RocksStore>>,
    mempool_client: BasicMempoolClient,
}

impl Node {
    pub fn load(path: &PathBuf, genesis: Arc<Genesis>) -> anyhow::Result<Self> {
        let builder = BftdBuilder::from_path(genesis, path)?;
        Ok(Self { builder })
    }

    pub fn start(self) -> anyhow::Result<NodeHandle> {
        let runtime = runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name(format!("{}", self.builder.config().validator_index))
            .build()
            .unwrap();
        let runtime = Arc::new(runtime);
        runtime.block_on(self.start_inner(runtime.clone()))
    }
    async fn start_inner(self, runtime: Arc<Runtime>) -> anyhow::Result<NodeHandle> {
        let builder = self.builder;
        let config = builder.config().clone();
        let channels_storage_path = builder.storage_path().join("channels");
        let load_gen = config.load_gen.map(|c| LoadGenConfig::parse(&c));
        let load_gen = load_gen.transpose()?;
        let (proposer, mempool_client) = BasicMempool::new();
        let registry = Registry::new();
        let (syncer, block_store) = builder
            .with_block_filter(TransactionsPayloadBlockFilter)
            .with_proposal_maker(proposer)
            .with_registry(registry.clone())
            .start()
            .await?;
        let prometheus_handle = if let Some(prometheus_bind) = config.prometheus_bind {
            Some(start_prometheus_server(prometheus_bind, &registry).await?)
        } else {
            None
        };

        let syncer = Arc::new(syncer);
        let server_handle = if let Some(http_server_bind) = config.http_server_bind {
            let handle = if config.use_channel_server.unwrap_or(true) {
                Box::new(
                    BftdChannelServer::start(
                        http_server_bind,
                        mempool_client.clone(),
                        block_store.clone(),
                        syncer.clone(),
                        channels_storage_path,
                    )
                    .await?,
                ) as Box<dyn Stoppable>
            } else {
                Box::new(
                    BftdServer::start(
                        http_server_bind,
                        mempool_client.clone(),
                        block_store.clone(),
                        syncer.clone(),
                    )
                    .await?,
                ) as Box<dyn Stoppable>
            };
            Some(handle)
        } else {
            None
        };
        let load_gen_handle = if let Some(load_gen) = load_gen {
            let load_gen_metrics = LoadGenMetrics::new_in_registry(&registry);
            Some(LoadGen::start(
                load_gen,
                mempool_client.clone(),
                load_gen_metrics,
            ))
        } else {
            None
        };
        Ok(NodeHandle {
            syncer,
            prometheus_handle,
            runtime,
            server_handle,
            load_gen_handle,
            store: block_store,
            mempool_client,
        })
    }
}

impl NodeHandle {
    pub fn stop_all(v: Vec<NodeHandle>) {
        let mut rh = Vec::with_capacity(v.len());
        for node in v {
            let r = node.runtime.clone();
            let h = r.spawn(node.stop_inner());
            rh.push((r, h));
        }
        for (r, h) in rh {
            r.block_on(h).ok();
            drop(r);
        }
    }

    // Caller needs to clone self.runtime before this call and then drop it outside async context
    async fn stop_inner(self) {
        let mut futures = Vec::new();
        if let Some(load_gen) = self.load_gen_handle {
            load_gen.abort();
            futures.push(load_gen.map(|_| ()).boxed());
        }
        if let Some(server) = self.server_handle {
            futures.push(server.stop());
        }
        if let Some(prometheus) = self.prometheus_handle {
            prometheus.abort();
            futures.push(prometheus.map(|_| ()).boxed());
        }
        join_all(futures).await;
        let Ok(syncer) = Arc::try_unwrap(self.syncer) else {
            panic!("Can't unwrap syncer - bftd-server server did not stop properly?")
        };
        syncer.stop().await;
    }

    pub fn stop(self) {
        // This clone is needed so that runtime is dropped here, rather than in the context of stop_inner
        let runtime = self.runtime.clone();
        runtime.block_on(self.stop_inner());
    }

    pub fn store(&self) -> &Arc<BlockCache<RocksStore>> {
        &self.store
    }

    pub fn commit_receiver(&self) -> &tokio::sync::watch::Receiver<Option<u64>> {
        self.syncer.last_commit_receiver()
    }

    pub fn mempool_client(&self) -> &BasicMempoolClient {
        &self.mempool_client
    }

    pub fn runtime(&self) -> &Arc<Runtime> {
        &self.runtime
    }

    pub async fn send_transaction(&self, transaction: Vec<u8>) {
        self.mempool_client
            .send_transaction(transaction)
            .await
            .expect("Mempool has stopped")
    }

    pub fn read_commits_from(&self, from_index_included: u64) -> CommitReader {
        CommitReader::start(self.store.clone(), &self.syncer, from_index_included)
    }
}

trait Stoppable: Send + Sync + 'static {
    fn stop(self: Box<Self>) -> Pin<Box<dyn Future<Output = ()> + Send>>;
}

impl Stoppable for BftdServer {
    fn stop(self: Box<Self>) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        BftdServer::stop(*self).boxed()
    }
}

impl Stoppable for BftdChannelServer {
    fn stop(self: Box<Self>) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        BftdChannelServer::stop(*self).boxed()
    }
}
