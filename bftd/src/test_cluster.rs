use std::net::SocketAddr;
use rand::rngs::ThreadRng;
use bftd_core::committee::{Stake, ValidatorInfo};
use bftd_core::crypto::{blake2_hash, Ed25519Signer, generate_validator_key_pair};
use bftd_core::genesis::Genesis;
use bftd_core::network::{generate_network_keypair, NoisePrivateKey};

pub struct TestCluster {
    genesis: Genesis,
    protocol_private_keys: Vec<Ed25519Signer>,
    network_private_keys: Vec<NoisePrivateKey>,
}

impl TestCluster {
    pub fn generate(name: &str, peer_addresses: Vec<SocketAddr>) -> Self {
        let mut rng = ThreadRng::default();
        let (protocol_private_keys, protocol_public_keys): (Vec<_>, Vec<_>) = peer_addresses.iter().map(|_| generate_validator_key_pair(&mut rng)).unzip();
        let (network_private_keys, network_public_keys): (Vec<_>, Vec<_>) = peer_addresses.iter().map(|_| generate_network_keypair()).unzip();
        let generation = blake2_hash(name.as_bytes());
        let validator_info = protocol_public_keys.into_iter().zip(network_public_keys.into_iter()).zip(peer_addresses.into_iter()).map(|((consensus_key, network_key), network_address)|{
            ValidatorInfo {
                consensus_key,
                network_key,
                network_address,
                stake: Stake(1),
            }
        }).collect();
        let genesis = Genesis::new(generation, validator_info);
        Self {
            genesis,
            protocol_private_keys,
            network_private_keys,
        }
    }
}
