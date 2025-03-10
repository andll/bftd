use crate::config::BftdConfig;
use crate::node::{Node, NodeHandle};
use bftd_core::block::ValidatorIndex;
use bftd_core::committee::{Stake, ValidatorInfo};
use bftd_core::crypto::{blake2_hash, generate_validator_key_pair, Ed25519Signer};
use bftd_core::genesis::Genesis;
use bftd_core::network::{generate_network_keypair, NoisePrivateKey};
use bftd_core::protocol_config::ProtocolConfig;
use handlebars::Handlebars;
use rand::rngs::ThreadRng;
use serde::Serialize;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

pub struct TestCluster {
    genesis: Genesis,
    protocol_private_keys: Vec<Ed25519Signer>,
    network_private_keys: Vec<NoisePrivateKey>,
    configs: Vec<BftdConfig>,
}

impl TestCluster {
    pub fn generate(
        name: &str,
        peer_addresses: Vec<String>,
        bind: Option<String>,
        prometheus_bind: Option<SocketAddr>,
        http_server_bind: impl Fn(ValidatorIndex) -> Option<SocketAddr>,
        protocol_config: ProtocolConfig,
        load_gen: Option<String>,
    ) -> Self {
        let mut rng = ThreadRng::default();
        let (protocol_private_keys, protocol_public_keys): (Vec<_>, Vec<_>) = peer_addresses
            .iter()
            .map(|_| generate_validator_key_pair(&mut rng))
            .unzip();
        let (network_private_keys, network_public_keys): (Vec<_>, Vec<_>) = peer_addresses
            .iter()
            .map(|_| generate_network_keypair())
            .unzip();
        let mut configs = Vec::with_capacity(peer_addresses.len());
        for validator_index in 0..peer_addresses.len() {
            let validator_index = ValidatorIndex(validator_index as u64);
            let config = BftdConfig {
                bind: bind.clone(),
                validator_index,
                prometheus_bind,
                http_server_bind: http_server_bind(validator_index),
                load_gen: load_gen.clone(),
                use_channel_server: None,
            };
            configs.push(config);
        }
        let generation = blake2_hash(name.as_bytes());
        let validator_info = protocol_public_keys
            .into_iter()
            .zip(network_public_keys.into_iter())
            .zip(peer_addresses.into_iter())
            .map(
                |((consensus_key, network_key), network_address)| ValidatorInfo {
                    consensus_key,
                    network_key,
                    network_address,
                    stake: Stake(1),
                },
            )
            .collect();
        let genesis = Genesis::new(generation, validator_info, protocol_config);
        Self {
            genesis,
            protocol_private_keys,
            network_private_keys,
            configs,
        }
    }

    pub fn store_into(
        &self,
        path: &PathBuf,
        prometheus_template: Option<PathBuf>,
    ) -> anyhow::Result<()> {
        let prometheus_template = prometheus_template
            .map(fs::read_to_string)
            .map(Result::unwrap);
        let genesis_path = Self::genesis_path(&path);
        fs::write(&genesis_path, self.genesis.data())?;
        for (v, ((noise_pk, protocol_pk), config)) in self
            .network_private_keys
            .iter()
            .zip(self.protocol_private_keys.iter())
            .zip(self.configs.iter())
            .enumerate()
        {
            let peer_dir = Self::peer_path(&path, v);
            fs::create_dir(&peer_dir)?;
            fs::write(peer_dir.join("noise_key"), noise_pk)?;
            fs::write(peer_dir.join("protocol_key"), protocol_pk)?;
            let config_yml = toml::to_string_pretty(&config).unwrap();
            fs::write(peer_dir.join("config"), &config_yml)?;
            if let Some(prometheus_template) = prometheus_template.as_ref() {
                let prometheus_bind = config
                    .prometheus_bind
                    .expect("prometheus_bind is required with prometheus_template");
                let mut hb = Handlebars::new();
                hb.set_strict_mode(true);
                let args = PrometheusTemplateArgs {
                    validator: format!("{}", config.validator_index),
                    prometheus_bind: prometheus_bind.to_string(),
                };
                let prometheus_config = hb.render_template(&prometheus_template, &args)?;
                fs::write(peer_dir.join("prometheus.yml"), &prometheus_config)?;
                fs::hard_link(&genesis_path, peer_dir.join("genesis"))?;
            }
        }
        Ok(())
    }

    pub fn start_test_cluster(path: PathBuf) -> anyhow::Result<Vec<NodeHandle>> {
        Self::start_test_cluster_partially(path, None)
    }

    pub fn start_test_cluster_partially(
        path: PathBuf,
        nodes_to_start: Option<Vec<usize>>,
    ) -> anyhow::Result<Vec<NodeHandle>> {
        let genesis = Genesis::load(Self::genesis_path(&path))?;
        let genesis = Arc::new(genesis);
        let nodes_to_start =
            nodes_to_start.unwrap_or_else(|| (0..genesis.validators().len()).collect());
        let mut nodes = Vec::with_capacity(nodes_to_start.len());
        for v in nodes_to_start {
            let peer_dir = Self::peer_path(&path, v);
            let node = Node::load(&peer_dir, genesis.clone())?;
            nodes.push(node);
        }
        let mut handles = Vec::with_capacity(nodes.len());
        for node in nodes {
            let handle = node.start()?;
            handles.push(handle);
        }
        Ok(handles)
    }

    fn genesis_path(path: &PathBuf) -> PathBuf {
        path.join("genesis")
    }

    fn peer_path(path: &PathBuf, v: usize) -> PathBuf {
        path.join(format!("{v:0>3}"))
    }
}

pub fn start_node(path: PathBuf) -> anyhow::Result<NodeHandle> {
    let genesis = Genesis::load(TestCluster::genesis_path(&path))?;
    let genesis = Arc::new(genesis);
    let node = Node::load(&path, genesis)?;
    let handle = node.start()?;
    Ok(handle)
}

#[derive(Serialize)]
struct PrometheusTemplateArgs {
    validator: String,
    prometheus_bind: String,
}
