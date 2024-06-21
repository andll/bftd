use crate::block::{Block, ChainId, Round, ValidatorIndex};
use crate::crypto::{Blake2Hasher, Ed25519Verifier};
use crate::network::NoisePublicKey;
use anyhow::{bail, ensure};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
#[cfg(test)]
use std::net::{IpAddr, Ipv4Addr};
use std::ops::AddAssign;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Committee {
    validators: Vec<ValidatorInfo>,
    total_stake: Stake,
    pub f_threshold: Stake,  // The minimum stake required for validity(f+1)
    pub f2_threshold: Stake, // The minimum stake required for quorum(2f+1)
}

const MAX_COMMITTEE: u64 = 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, Copy, PartialOrd, PartialEq)]
pub struct Stake(pub u64);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidatorInfo {
    pub consensus_key: Ed25519Verifier,
    pub network_key: NoisePublicKey,
    pub network_address: SocketAddr,
    pub stake: Stake,
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

    pub fn verify_block(
        &self,
        data: Bytes,
        expected_author: Option<ValidatorIndex>,
    ) -> anyhow::Result<Block> {
        let author = Block::author_from_bytes(&data)?;
        if let Some(expected_author) = expected_author {
            ensure!(
                expected_author == author,
                "Received block authored by {author} validator, expected {expected_author}"
            );
        }
        ensure!(author.0 < MAX_COMMITTEE, "Validator not found");
        let Some(author) = self.validators.get(author.0 as usize) else {
            bail!("Validator not found")
        };
        // todo - other validity (thr clock etc)
        Block::from_bytes(data, &Blake2Hasher, &author.consensus_key)
    }

    pub fn get_stake(&self, index: ValidatorIndex) -> Stake {
        self.validators
            .get(index.0 as usize)
            .expect("Authority not found")
            .stake
    }

    pub fn validator(&self, index: ValidatorIndex) -> &ValidatorInfo {
        self.validators
            .get(index.0 as usize)
            .expect("Validator not found")
    }

    pub fn network_key(&self, index: ValidatorIndex) -> &NoisePublicKey {
        &self.validator(index).network_key
    }

    pub fn enumerate_validators(&self) -> impl Iterator<Item = (ValidatorIndex, &ValidatorInfo)> {
        self.validators
            .iter()
            .enumerate()
            .map(|(i, vi)| (ValidatorIndex(i as u64), vi))
    }

    pub fn enumerate_indexes(&self) -> impl Iterator<Item = ValidatorIndex> + '_ {
        self.validators
            .iter()
            .enumerate()
            .map(|(i, _)| ValidatorIndex(i as u64))
    }

    pub fn genesis_blocks(&self, chain_id: ChainId) -> Vec<Block> {
        self.enumerate_validators()
            .map(|(i, _)| Block::genesis(chain_id, i))
            .collect()
    }

    pub fn elect_leader(&self, round: Round, offset: u64) -> ValidatorIndex {
        let mut h = seahash::SeaHasher::new();
        round.hash(&mut h);
        offset.hash(&mut h);
        let h = h.finish();
        let index = h % (self.validators.len() as u64);
        ValidatorIndex(index)
    }
}

impl Stake {
    pub const ZERO: Stake = Stake(0);
}

impl AddAssign for Stake {
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0;
    }
}

#[cfg(test)]
impl Committee {
    pub fn new_test(v: Vec<u64>) -> Self {
        Self::new(
            v.into_iter()
                .map(|stake| ValidatorInfo {
                    consensus_key: Default::default(),
                    network_key: Default::default(),
                    network_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
                    stake: Stake(stake),
                })
                .collect(),
        )
    }
}

impl fmt::Display for Stake {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
