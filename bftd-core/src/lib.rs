// use crate::block::ValidatorIndex;
// use crate::block_manager::MemoryBlockStore;
// use crate::committee::{Committee, Stake, ValidatorInfo};
// use crate::core::Core;
// use crate::crypto::Ed25519Signer;
// use crate::network::{ConnectionPool, NoisePublicKey, PeerInfo};
// use crate::syncer::{Clock, Syncer};
// use futures::future::join_all;
// use rand::rngs::ThreadRng;
// use std::sync::Arc;
// use std::thread;
// use std::time::{Duration, Instant};
// use tracing_subscriber::EnvFilter;

pub mod block;
pub mod block_manager;
pub mod committee;
pub mod config;
pub mod consensus;
pub mod core;
pub mod crypto;
pub mod genesis;
mod log;
pub mod mempool;
pub mod network;
pub mod rpc;
#[cfg(feature = "server")]
pub mod server;
pub mod store;
pub mod syncer;
mod threshold_clock;

// fn main() {
//     tracing_subscriber::fmt()
//         .with_env_filter(EnvFilter::from_default_env())
//         .with_thread_names(true)
//         .init();
//     let builder = snow::Builder::new("Noise_NN_25519_ChaChaPoly_BLAKE2s".parse().unwrap());
//     let num_validators = 10usize;
//     let mut peers = Vec::with_capacity(num_validators);
//     let mut noise_private_keys = Vec::with_capacity(num_validators);
//     let mut protocol_private_keys = Vec::with_capacity(num_validators);
//     let mut validators = Vec::with_capacity(num_validators);
//     let mut rng = ThreadRng::default();
//     for i in 0..num_validators {
//         let kp = builder.generate_keypair().unwrap();
//         let port = 8080 + i;
//         let kpb = NoisePublicKey(kp.public.clone());
//         let peer_info = PeerInfo {
//             address: format!("127.0.0.1:{port}").parse().unwrap(),
//             public_key: kpb.clone(),
//             index: ValidatorIndex(i as u64),
//         };
//         noise_private_keys.push(kp.private);
//         let protocol_private_key = ed25519_consensus::SigningKey::new(&mut rng);
//         let protocol_public_key = protocol_private_key.verification_key();
//         protocol_private_keys.push(protocol_private_key);
//         validators.push(ValidatorInfo {
//             consensus_key: protocol_public_key.into(),
//             network_key: kpb,
//             stake: Stake(1),
//             network_address: peer_info.address,
//         });
//         peers.push(peer_info);
//     }
//     let committee = Arc::new(Committee::new(validators));
//     let mut syncers = Vec::with_capacity(num_validators);
//     let mut validator_runtimes = vec![];
//     let clock = Instant::now();
//     for (i, (noise_private_key, protocol_private_key)) in noise_private_keys
//         .into_iter()
//         .zip(protocol_private_keys.into_iter())
//         .enumerate()
//     {
//         let validator_index = ValidatorIndex(i as u64);
//         let runtime = tokio::runtime::Builder::new_multi_thread()
//             .thread_name(format!("[{validator_index}]"))
//             .enable_all()
//             .build()
//             .unwrap();
//         let address = committee.validator(validator_index).network_address;
//         let pool = runtime
//             .block_on(ConnectionPool::start(
//                 address,
//                 noise_private_key.into(),
//                 peers.clone(),
//                 i,
//             ))
//             .unwrap();
//         let block_store = Arc::new(MemoryBlockStore::default());
//         let core = Core::new(
//             Ed25519Signer::from(protocol_private_key),
//             block_store.clone(),
//             committee.clone(),
//             validator_index,
//         );
//         {
//             let _enter = runtime.enter();
//             let syncer = Syncer::start(core, block_store, pool, clock);
//             syncers.push(syncer);
//         }
//         validator_runtimes.push(runtime);
//     }
//     thread::sleep(Duration::from_secs(10));
//     println!("Stopping");
//     let runtime = tokio::runtime::Builder::new_multi_thread()
//         .enable_all()
//         .build()
//         .unwrap();
//     runtime.block_on(join_all(syncers.into_iter().map(Syncer::stop)));
//     println!("OK");
// }
//
// impl Clock for Instant {
//     fn time_ns(&self) -> u64 {
//         self.elapsed().as_nanos() as u64
//     }
// }
