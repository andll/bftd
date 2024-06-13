use crate::network::{ConnectionPool, NoisePublicKey, PeerInfo};
use std::time::Duration;

mod block;
mod block_manager;
mod committee;
mod core;
mod crypto;
mod network;
mod rpc;
mod threshold_clock;

#[tokio::main]
async fn main() {
    env_logger::init();
    let builder = snow::Builder::new("Noise_NN_25519_ChaChaPoly_BLAKE2s".parse().unwrap());
    let kp1 = builder.generate_keypair().unwrap();
    let kp2 = builder.generate_keypair().unwrap();

    let kpb1 = NoisePublicKey(kp1.public.clone());
    let kpb2 = NoisePublicKey(kp2.public.clone());
    let peers = vec![
        PeerInfo {
            address: "127.0.0.1:8080".parse().unwrap(),
            public_key: kpb1.clone(),
        },
        PeerInfo {
            address: "127.0.0.1:8081".parse().unwrap(),
            public_key: kpb2.clone(),
        },
    ];
    println!("K1: {kpb1}");
    println!("K2: {kpb2}");

    let _pool1 = ConnectionPool::start("127.0.0.1:8080", kp1.private.into(), peers.clone(), 0)
        .await
        .unwrap();
    let _pool2 = ConnectionPool::start("127.0.0.1:8081", kp2.private.into(), peers, 1)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    println!("OK");
}
