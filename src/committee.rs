use crate::block::Block;
use crate::crypto::{Blake2Hasher, Ed25519Verifier};
use crate::NoisePublicKey;
use anyhow::{bail, ensure};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Committee {
    validators: Vec<ValidatorInfo>,
    total_stake: Stake,
    f_threshold: Stake,  // The minimum stake required for validity(f+1)
    f2_threshold: Stake, // The minimum stake required for quorum(2f+1)
}

const MAX_COMMITTEE: u64 = 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, Copy)]
pub struct Stake(u64);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidatorInfo {
    consensus_key: Ed25519Verifier,
    network_key: NoisePublicKey,
    network_address: SocketAddr,
    stake: Stake,
}

impl Committee {
    pub fn new(validators: Vec<ValidatorInfo>) -> Self {
        assert!(validators.len() < MAX_COMMITTEE as usize);
        let total_stake = validators.iter().map(|v| v.stake.0).sum();
        let f_threshold = total_stake / 3;
        let f2_threshold = 2 * total_stake / 3;
        let total_stake = Stake(total_stake);
        let f_threshold = Stake(f_threshold);
        let f2_threshold = Stake(f2_threshold);
        Self {
            validators,
            total_stake,
            f_threshold,
            f2_threshold,
        }
    }

    pub fn verify_block(&self, data: Bytes) -> anyhow::Result<Block> {
        let author = Block::author_from_bytes(&data)?;
        ensure!(author.0 < MAX_COMMITTEE, "Validator not found");
        let Some(author) = self.validators.get(author.0 as usize) else {
            bail!("Validator not found")
        };
        // todo - other validity (thr clock etc)
        Block::from_bytes(data, &Blake2Hasher, &author.consensus_key)
    }
}
