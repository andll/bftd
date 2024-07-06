use crate::block::{Block, BlockReference, ChainId, Round, ValidatorIndex, MAX_PARENTS};
use crate::crypto::{Blake2Hasher, Ed25519Verifier};
use crate::metrics::Metrics;
use crate::network::{NoisePublicKey, PeerInfo};
use anyhow::{bail, ensure};
use bytes::Bytes;
use rand::prelude::SliceRandom;
use rand::rngs::ThreadRng;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::net::{SocketAddr, ToSocketAddrs};
use std::ops::AddAssign;
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Committee {
    chain_id: ChainId,
    validators: Vec<ValidatorInfo>,
    total_stake: Stake,
    pub f_threshold: Stake,  // The minimum stake required for validity(f+1)
    pub f2_threshold: Stake, // The minimum stake required for quorum(2f+1)
}

// todo - need to make sure this is ok
const MAX_COMMITTEE: u64 = MAX_PARENTS as u64;

#[derive(Clone, Debug, Serialize, Deserialize, Copy, PartialOrd, PartialEq)]
pub struct Stake(pub u64);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidatorInfo {
    pub consensus_key: Ed25519Verifier,
    pub network_key: NoisePublicKey,
    pub network_address: String,
    pub stake: Stake,
}

impl Committee {
    pub fn new(chain_id: ChainId, validators: Vec<ValidatorInfo>) -> Self {
        assert!(validators.len() < MAX_COMMITTEE as usize);
        let total_stake = validators.iter().map(|v| v.stake.0).sum();
        let f_threshold = total_stake / 3;
        let f2_threshold = 2 * total_stake / 3;
        let total_stake = Stake(total_stake);
        let f_threshold = Stake(f_threshold);
        let f2_threshold = Stake(f2_threshold);
        Self {
            chain_id,
            validators,
            total_stake,
            f_threshold,
            f2_threshold,
        }
    }

    pub fn verify_block(
        &self,
        data: Bytes,
        matcher: BlockMatch,
        metrics: Arc<Metrics>,
    ) -> anyhow::Result<BlockVerifiedByCommittee> {
        let reference = Block::reference_from_block_bytes(&data)?;
        let author = reference.author;
        match matcher {
            BlockMatch::Author(expected_author) => {
                ensure!(
                    expected_author == author,
                    "Received block authored by {author} validator, expected {expected_author}"
                );
            }
            BlockMatch::Reference(expected_reference) => {
                ensure!(
                    expected_reference == reference,
                    "Received block reference {reference} validator, expected {expected_reference}"
                );
            }
        }
        ensure!(author.0 < MAX_COMMITTEE, "Validator not found");
        let Some(author) = self.validators.get(author.0 as usize) else {
            bail!("Validator not found")
        };
        // todo - other validity (thr clock etc)
        // todo put chain id into network handshake
        let block = Block::from_bytes(data, &Blake2Hasher, &author.consensus_key, Some(metrics))?;
        ensure!(
            &self.chain_id == block.chain_id(),
            "Received block from a different chain"
        );
        Ok(BlockVerifiedByCommittee(block))
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

    pub fn network_address(&self, index: ValidatorIndex) -> &String {
        &self.validator(index).network_address
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

    /// Returns vec![T::default(); committee::len()]
    pub fn committee_vec_default<T: Default>(&self) -> Vec<T> {
        self.validators.iter().map(|_| Default::default()).collect()
    }

    /// Returns block view of genesis blocks
    pub fn genesis_view(&self) -> Vec<Option<BlockReference>> {
        self.genesis_blocks()
            .into_iter()
            .map(|b| Some(*b.reference()))
            .collect()
    }

    pub fn all_indexes_shuffled_excluding(&self, exclude: ValidatorIndex) -> Vec<ValidatorIndex> {
        let mut all_indexes: Vec<_> = self.enumerate_indexes().filter(|v| v != &exclude).collect();
        all_indexes.shuffle(&mut ThreadRng::default());
        all_indexes
    }

    pub fn genesis_blocks(&self) -> Vec<Block> {
        self.enumerate_validators()
            .map(|(i, _)| Block::genesis(self.chain_id, i))
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

    pub fn make_peers_info(&self) -> Vec<PeerInfo> {
        self.validators
            .iter()
            .enumerate()
            .map(|(index, info)| PeerInfo {
                address: resolve_one(&info.network_address),
                public_key: info.network_key.clone(),
                index: ValidatorIndex(index as u64),
            })
            .collect()
    }

    pub fn len(&self) -> usize {
        self.validators.len()
    }

    pub fn chain_id(&self) -> &ChainId {
        &self.chain_id
    }
}

pub fn resolve_one(s: &str) -> SocketAddr {
    let addrs: Vec<_> = s
        .to_socket_addrs()
        .expect("Failed to resolve socket address")
        .collect();
    if addrs.is_empty() {
        panic!("{s} did not resolve into any addresses");
    } else if addrs.len() > 1 {
        tracing::warn!(
            "Validator address {s} resolved into multiple addresses. Only one address will be used"
        );
    }
    addrs.into_iter().next().unwrap()
}

pub enum BlockMatch {
    Author(ValidatorIndex),
    Reference(BlockReference),
}

/// Block that passed some verifications:
/// - Static checks (block header parsed, hash matches)
/// - Committee checks(signature is correct, chain_id matches)
pub struct BlockVerifiedByCommittee(Block);

impl BlockVerifiedByCommittee {
    pub fn extract_for_further_verification(self) -> Block {
        self.0
    }

    pub fn reference(&self) -> &BlockReference {
        self.0.reference()
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
            ChainId::CHAIN_ID_TEST,
            v.into_iter()
                .map(|stake| ValidatorInfo {
                    consensus_key: Default::default(),
                    network_key: Default::default(),
                    network_address: "0.0.0.0:0".to_string(),
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
