use bftd_core::block::ValidatorIndex;
use bftd_core::committee::{Stake, ValidatorInfo};
use bftd_core::config::BftdConfig;
use bftd_core::crypto::{blake2_hash, generate_validator_key_pair, Ed25519Signer};
use bftd_core::genesis::Genesis;
use bftd_core::network::{generate_network_keypair, NoisePrivateKey};
use rand::rngs::ThreadRng;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::{fs, io};

pub struct TestCluster {
    genesis: Genesis,
    protocol_private_keys: Vec<Ed25519Signer>,
    network_private_keys: Vec<NoisePrivateKey>,
    configs: Vec<BftdConfig>,
}

impl TestCluster {
    pub fn generate(name: &str, peer_addresses: Vec<SocketAddr>, bind: Option<String>) -> Self {
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
        let genesis = Genesis::new(generation, validator_info);
        Self {
            genesis,
            protocol_private_keys,
            network_private_keys,
            configs,
        }
    }

    pub fn store_into(&self, path: &PathBuf) -> io::Result<()> {
        for (v, ((noise_pk, protocol_pk), config)) in self
            .network_private_keys
            .iter()
            .zip(self.protocol_private_keys.iter())
            .zip(self.configs.iter())
            .enumerate()
        {
            let peer_dir = path.join(format!("{v:0>3}"));
            fs::create_dir(&peer_dir)?;
            fs::write(peer_dir.join("noise_key"), noise_pk)?;
            fs::write(peer_dir.join("protocol_key"), protocol_pk)?;
            let config = toml::to_string_pretty(&config).unwrap();
            fs::write(peer_dir.join("config"), &config)?;
        }
        fs::write(path.join("genesis"), self.genesis.data())?;
        Ok(())
    }
}
