use crate::block::ValidatorIndex;
use crate::block_manager::MemoryBlockStore;
use crate::committee::{Committee, Stake, ValidatorInfo};
use crate::core::Core;
use crate::crypto::Ed25519Signer;
use crate::network::{ConnectionPool, NoisePublicKey, PeerInfo};
use crate::syncer::Syncer;
use rand::rngs::ThreadRng;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::runtime::Runtime;

mod block;
mod block_manager;
mod committee;
mod core;
mod crypto;
mod network;
mod rpc;
mod syncer;
mod threshold_clock;

fn main() {
    env_logger::init();
    let builder = snow::Builder::new("Noise_NN_25519_ChaChaPoly_BLAKE2s".parse().unwrap());
    let num_validators = 3usize;
    let mut peers = Vec::with_capacity(num_validators);
    let mut noise_private_keys = Vec::with_capacity(num_validators);
    let mut protocol_private_keys = Vec::with_capacity(num_validators);
    let mut validators = Vec::with_capacity(num_validators);
    let mut rng = ThreadRng::default();
    for i in 0..num_validators {
        let kp = builder.generate_keypair().unwrap();
        let port = 8080 + i;
        let kpb = NoisePublicKey(kp.public.clone());
        let peer_info = PeerInfo {
            address: format!("127.0.0.1:{port}").parse().unwrap(),
            public_key: kpb.clone(),
        };
        noise_private_keys.push(kp.private);
        let protocol_private_key = ed25519_consensus::SigningKey::new(&mut rng);
        let protocol_public_key = protocol_private_key.verification_key();
        protocol_private_keys.push(protocol_private_key);
        validators.push(ValidatorInfo {
            consensus_key: protocol_public_key.into(),
            network_key: kpb,
            stake: Stake(1),
            network_address: peer_info.address,
        });
        peers.push(peer_info);
    }
    let committee = Arc::new(Committee::new(validators));
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    let mut syncers = Vec::with_capacity(num_validators);
    for (i, (noise_private_key, protocol_private_key)) in noise_private_keys
        .into_iter()
        .zip(protocol_private_keys.into_iter())
        .enumerate()
    {
        let validator_index = ValidatorIndex(i as u64);
        let address = committee.validator(validator_index).network_address;
        let pool = runtime.block_on(ConnectionPool::start(address, noise_private_key.into(), peers.clone(), i))
            .unwrap();
        let block_store = Arc::new(MemoryBlockStore::default());
        let core = Core::new(
            Ed25519Signer::from(protocol_private_key),
            block_store.clone(),
            committee.clone(),
            validator_index,
        );
        {
            let _enter = runtime.enter();
            let syncer = Syncer::start(core, block_store, pool);
            syncers.push(syncer);
        }
    }
    thread::sleep(Duration::from_secs(3));
    println!("OK");
}
