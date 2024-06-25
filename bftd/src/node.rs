use crate::load_gen::{LoadGen, LoadGenConfig, LoadGenMetrics};
use crate::mempool::{BasicMempool, TransactionsPayloadBlockFilter};
use crate::prometheus::{start_prometheus_server, PrometheusJoinHandle};
use crate::server::BftdServer;
use bftd_core::config::BftdConfig;
use bftd_core::core::Core;
use bftd_core::crypto::Ed25519Signer;
use bftd_core::genesis::Genesis;
use bftd_core::metrics::Metrics;
use bftd_core::network::{ConnectionPool, NoisePrivateKey};
use bftd_core::store::rocks_store::RocksStore;
use bftd_core::syncer::{Syncer, SystemTimeClock};
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

#[allow(dead_code)]
pub struct NodeHandle {
    syncer: Arc<Syncer>,
    prometheus_handle: Option<PrometheusJoinHandle>,
    server_handle: Option<BftdServer>,
    runtime: Arc<Runtime>,
    load_gen_handle: Option<JoinHandle<()>>,
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

    pub fn start(self, load_gen: Option<LoadGenConfig>) -> anyhow::Result<NodeHandle> {
        let runtime = runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name(format!("{}", self.config.validator_index))
            .build()
            .unwrap();
        let runtime = Arc::new(runtime);
        runtime.block_on(self.start_inner(runtime.clone(), load_gen))
    }
    async fn start_inner(
        self,
        runtime: Arc<Runtime>,
        load_gen: Option<LoadGenConfig>,
    ) -> anyhow::Result<NodeHandle> {
        let committee = self.genesis.make_committee();
        let peers = committee.make_peers_info();
        let bind = if let Some(bind) = self.config.bind.as_ref() {
            if bind.contains(":") {
                SocketAddr::from_str(bind).expect("Failed to parse bind address as socket addr")
            } else {
                let port = committee
                    .network_address(self.config.validator_index)
                    .port();
                let addr = IpAddr::from_str(bind).expect("Failed to parse bind address as ip addr");
                SocketAddr::new(addr, port)
            }
        } else {
            *committee.network_address(self.config.validator_index)
        };

        let pool = ConnectionPool::start(
            bind,
            self.noise_private_key.clone(),
            peers,
            self.config.validator_index.0 as usize,
        )
        .await?;
        let registry = Registry::new();
        let metrics = Metrics::new_in_registry(&registry, &committee);
        let block_store = RocksStore::open(&self.storage_path, metrics.clone())?;
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
        );

        let syncer = Arc::new(syncer);
        let server_handle = if let Some(http_server_bind) = self.config.http_server_bind {
            Some(
                BftdServer::start(
                    http_server_bind,
                    mempool_client.clone(),
                    block_store,
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
        })
    }
}
