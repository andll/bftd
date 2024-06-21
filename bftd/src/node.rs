use crate::prometheus::{start_prometheus_server, PrometheusJoinHandle};
use bftd_core::config::BftdConfig;
use bftd_core::core::Core;
use bftd_core::crypto::Ed25519Signer;
use bftd_core::genesis::Genesis;
use bftd_core::metrics::Metrics;
use bftd_core::network::{ConnectionPool, NoisePrivateKey};
use bftd_core::store::SledStore;
use bftd_core::syncer::{Syncer, SystemTimeClock};
use prometheus::Registry;
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tokio::runtime;
use tokio::runtime::Runtime;

pub struct Node {
    config: BftdConfig,
    noise_private_key: NoisePrivateKey,
    protocol_private_key: Ed25519Signer,
    genesis: Arc<Genesis>,
    storage_path: PathBuf,
}

#[allow(dead_code)]
pub struct NodeHandle {
    syncer: Syncer,
    prometheus_handle: Option<PrometheusJoinHandle>,
    runtime: Runtime,
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
        let runtime = runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name(format!("{}", self.config.validator_index))
            .build()
            .unwrap();
        let _enter = runtime.enter(); // rest of the function in the context of node's runtime

        let pool = runtime.block_on(ConnectionPool::start(
            bind,
            self.noise_private_key.clone(),
            peers,
            self.config.validator_index.0 as usize,
        ))?;
        let block_store = SledStore::open(&self.storage_path)?;
        let block_store = Arc::new(block_store);
        let registry = Registry::new();
        let metrics = Metrics::new_in_registry(&registry);
        let prometheus_handle = if let Some(prometheus_bind) = self.config.prometheus_bind {
            Some(runtime.block_on(start_prometheus_server(prometheus_bind, &registry))?)
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
        let syncer = Syncer::start(core, block_store, pool, clock);

        Ok(NodeHandle {
            syncer,
            prometheus_handle,
            runtime,
        })
    }
}
