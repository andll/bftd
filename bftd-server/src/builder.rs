use crate::config::BftdConfig;
use bftd_core::block_cache::BlockCache;
use bftd_core::committee::resolve_one;
use bftd_core::core::{Core, ProposalMaker};
use bftd_core::crypto::Ed25519Signer;
use bftd_core::genesis::Genesis;
use bftd_core::metrics::Metrics;
use bftd_core::network::{NoisePrivateKey, TcpConnectionPool};
use bftd_core::store::rocks_store::RocksStore;
use bftd_core::store::{BlockReader, BlockStore};
use bftd_core::syncer::{BlockFilter, Clock, Syncer, SystemTimeClock};
use prometheus::Registry;
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

pub struct BftdBuilder<P = (), F = (), C = SystemTimeClock> {
    config: BftdConfig,
    noise_private_key: NoisePrivateKey,
    protocol_private_key: Ed25519Signer,
    genesis: Arc<Genesis>,
    storage_path: PathBuf,

    proposal_maker: P,
    block_filter: F,
    clock: C,

    registry: Registry,
}

pub type BftdServerBlockStore = Arc<BlockCache<RocksStore>>;

impl BftdBuilder {
    pub fn from_path(genesis: Arc<Genesis>, path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
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

            proposal_maker: (),
            block_filter: (),
            clock: SystemTimeClock::new(),

            registry: Registry::new(),
        })
    }
}

impl<P, F, C> BftdBuilder<P, F, C> {
    pub fn with_proposal_maker<PP: ProposalMaker>(
        self,
        proposal_maker: PP,
    ) -> BftdBuilder<PP, F, C> {
        BftdBuilder {
            config: self.config,
            noise_private_key: self.noise_private_key,
            protocol_private_key: self.protocol_private_key,
            genesis: self.genesis,
            storage_path: self.storage_path,

            proposal_maker,
            block_filter: self.block_filter,
            clock: self.clock,

            registry: self.registry,
        }
    }

    pub fn with_block_filter<FF: BlockFilter>(self, block_filter: FF) -> BftdBuilder<P, FF, C> {
        BftdBuilder {
            config: self.config,
            noise_private_key: self.noise_private_key,
            protocol_private_key: self.protocol_private_key,
            genesis: self.genesis,
            storage_path: self.storage_path,

            proposal_maker: self.proposal_maker,
            block_filter,
            clock: self.clock,

            registry: self.registry,
        }
    }

    pub fn with_clock<CC: Clock>(self, clock: CC) -> BftdBuilder<P, F, CC> {
        BftdBuilder {
            config: self.config,
            noise_private_key: self.noise_private_key,
            protocol_private_key: self.protocol_private_key,
            genesis: self.genesis,
            storage_path: self.storage_path,

            proposal_maker: self.proposal_maker,
            block_filter: self.block_filter,
            clock,

            registry: self.registry,
        }
    }

    pub fn with_registry(mut self, registry: Registry) -> Self {
        self.registry = registry;
        self
    }

    pub fn config(&self) -> &BftdConfig {
        &self.config
    }

    pub fn storage_path(&self) -> &Path {
        &self.storage_path
    }
}

impl<P: ProposalMaker, F: BlockFilter, C: Clock + Clone> BftdBuilder<P, F, C> {
    pub async fn start(self) -> anyhow::Result<(Syncer, BftdServerBlockStore)> {
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
        let metrics = Metrics::new_in_registry(&self.registry, &committee);
        let block_store =
            RocksStore::open(&self.storage_path.join("bftd"), &committee, metrics.clone())?;
        for genesis_block in committee.genesis_blocks() {
            if !block_store.exists(genesis_block.reference()) {
                block_store.put(Arc::new(genesis_block));
            }
        }
        let block_store = BlockCache::new(block_store, &committee);
        let block_store = Arc::new(block_store);
        let core = Core::new(
            self.protocol_private_key,
            block_store.clone(),
            committee,
            self.config.validator_index,
            metrics,
            self.genesis.protocol_config().clone(),
        );
        let syncer = Syncer::start(
            core,
            block_store.clone(),
            pool,
            self.clock,
            self.proposal_maker,
            self.block_filter,
            self.genesis.protocol_config().clone(),
        );

        Ok((syncer, block_store))
    }
}
