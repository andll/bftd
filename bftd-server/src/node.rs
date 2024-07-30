use crate::config::BftdConfig;
use crate::load_gen::{LoadGen, LoadGenConfig, LoadGenMetrics};
use crate::mempool::{BasicMempool, TransactionsPayloadBlockFilter};
use crate::prometheus::{start_prometheus_server, PrometheusJoinHandle};
use crate::server::BftdServer;
use bftd_core::block_cache::BlockCache;
use bftd_core::committee::resolve_one;
use bftd_core::core::Core;
use bftd_core::crypto::Ed25519Signer;
use bftd_core::genesis::Genesis;
use bftd_core::metrics::Metrics;
use bftd_core::network::{NoisePrivateKey, TcpConnectionPool};
use bftd_core::store::rocks_store::RocksStore;
use bftd_core::store::{BlockReader, BlockStore};
use bftd_core::syncer::{Syncer, SystemTimeClock};
use futures::future::join_all;
use futures::FutureExt;
use prometheus::Registry;
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tokio::runtime;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

pub struct Node {
    config: BftdConfig,
    noise_private_key: NoisePrivateKey,
    protocol_private_key: Ed25519Signer,
    genesis: Arc<Genesis>,
    storage_path: PathBuf,
}

pub struct NodeHandle {
    syncer: Arc<Syncer>,
    prometheus_handle: Option<PrometheusJoinHandle>,
    server_handle: Option<BftdServer>,
    runtime: Arc<Runtime>,
    load_gen_handle: Option<JoinHandle<()>>,
    #[allow(dead_code)]
    store: Arc<BlockCache<RocksStore>>,
}

impl Node {
    pub fn load(path: &PathBuf, genesis: Arc<Genesis>) -> anyhow::Result<Self> {
        let config = fs::read_to_string(path.join("config"))?;
        let storage_path = path.join("storage");
        fs::create_dir_all(&storage_path)?;
        let config = toml::from_str(&config)?;
        let noise_private_key = fs::read(path.join("noise_key"))?.into();
        let protocol_private_key = fs::read(path.join("protocol_key"))?.as_slice().try_into()?;
        // todo check private keys consistency with committee
        Ok(Self {
            config,
            noise_private_key,
            protocol_private_key,
            genesis,
            storage_path,
        })
    }

    pub fn start(self) -> anyhow::Result<NodeHandle> {
        let runtime = runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name(format!("{}", self.config.validator_index))
            .build()
            .unwrap();
        let runtime = Arc::new(runtime);
        runtime.block_on(self.start_inner(runtime.clone()))
    }
    async fn start_inner(self, runtime: Arc<Runtime>) -> anyhow::Result<NodeHandle> {
        let load_gen = self.config.load_gen.map(|c| LoadGenConfig::parse(&c));
        let load_gen = load_gen.transpose()?;
        let committee = self.genesis.make_committee();
        let peers = committee.make_peers_info();
        let bind = if let Some(bind) = self.config.bind.as_ref() {
            // todo - better detection between SocketAddr and IpAddr
            if bind.contains(":") && bind != "::" {
                SocketAddr::from_str(bind).expect("Failed to parse bind address as socket addr")
            } else {
                let port =
                    resolve_one(committee.network_address(self.config.validator_index)).port();
                let addr = IpAddr::from_str(bind).expect("Failed to parse bind address as ip addr");
                SocketAddr::new(addr, port)
            }
        } else {
            resolve_one(committee.network_address(self.config.validator_index))
        };

        let pool = TcpConnectionPool::start(
            bind,
            self.noise_private_key.clone(),
            peers,
            self.config.validator_index.0 as usize,
        )
        .await?;
        let registry = Registry::new();
        let metrics = Metrics::new_in_registry(&registry, &committee);
        let block_store = RocksStore::open(&self.storage_path, &committee, metrics.clone())?;
        for genesis_block in committee.genesis_blocks() {
            if !block_store.exists(genesis_block.reference()) {
                block_store.put(Arc::new(genesis_block));
            }
        }
        let block_store = BlockCache::new(block_store, &committee);
        let block_store = Arc::new(block_store);
        let prometheus_handle = if let Some(prometheus_bind) = self.config.prometheus_bind {
            Some(start_prometheus_server(prometheus_bind, &registry).await?)
        } else {
            None
        };
        let core = Core::new(
            self.protocol_private_key,
            block_store.clone(),
            committee,
            self.config.validator_index,
            metrics,
            self.genesis.protocol_config().clone(),
        );
        let clock = SystemTimeClock::new();
        let (proposer, mempool_client) = BasicMempool::new();
        let syncer = Syncer::start(
            core,
            block_store.clone(),
            pool,
            clock,
            proposer,
            TransactionsPayloadBlockFilter,
            self.genesis.protocol_config().clone(),
        );

        let syncer = Arc::new(syncer);
        let server_handle = if let Some(http_server_bind) = self.config.http_server_bind {
            Some(
                BftdServer::start(
                    http_server_bind,
                    mempool_client.clone(),
                    block_store.clone(),
                    syncer.clone(),
                )
                .await?,
            )
        } else {
            None
        };
        let load_gen_handle = if let Some(load_gen) = load_gen {
            let load_gen_metrics = LoadGenMetrics::new_in_registry(&registry);
            Some(LoadGen::start(load_gen, mempool_client, load_gen_metrics))
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
            futures.push(server.stop().boxed());
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

    #[allow(dead_code)]
    pub fn stop(self) {
        // This clone is needed so that runtime is dropped here, rather than in the context of stop_inner
        let runtime = self.runtime.clone();
        runtime.block_on(self.stop_inner());
    }

    #[cfg(test)]
    pub fn store(&self) -> &Arc<BlockCache<RocksStore>> {
        &self.store
    }

    #[cfg(test)]
    pub fn commit_receiver(&self) -> &tokio::sync::watch::Receiver<Option<u64>> {
        self.syncer.last_commit_receiver()
    }
}