// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::block::{
    format_author_round, AuthorRound, Block, BlockReference, Round, BLOCK_HASH_LENGTH,
};
use blake2::{Blake2b, Digest};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

mod base_committer;
mod universal_committer;

/// Default wave length for all committers. A longer wave_length increases the chance of committing the leader
/// under asynchrony at the cost of latency in the common case.
pub const DEFAULT_WAVE_LENGTH: u64 = MINIMUM_WAVE_LENGTH;

/// We need at least one leader round, one voting round, and one decision round.
pub const MINIMUM_WAVE_LENGTH: u64 = 3;

pub use universal_committer::{UniversalCommitter, UniversalCommitterBuilder};

/// The status of every leader output by the committers. While the core only cares about committed
/// leaders, providing a richer status allows for easier debugging, testing, and composition with
/// advanced commit strategies.
// #[derive(Debug, Clone, Eq, PartialEq, Hash)]
enum LeaderStatus {
    Commit(Arc<Block>),
    Skip(AuthorRound),
    Undecided(AuthorRound),
}

#[derive(Clone)]
pub enum CommitDecision {
    Commit(Arc<Block>),
    Skip(AuthorRound),
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct Commit {
    index: u64,
    leader: BlockReference,
    commit_timestamp_ns: u64,
    /// All blocks in commit, leader block is the last block in this list
    all_blocks: Vec<BlockReference>,
    previous_commit_hash: Option<[u8; BLOCK_HASH_LENGTH]>,
    commit_hash: [u8; BLOCK_HASH_LENGTH],
}

impl Commit {
    #[cfg(test)]
    pub fn new_test(index: u64, leader: BlockReference, all_blocks: Vec<BlockReference>) -> Self {
        Self {
            index,
            leader,
            commit_timestamp_ns: 0,
            all_blocks,
            previous_commit_hash: Default::default(),
            commit_hash: Default::default(),
        }
    }
    pub fn new(
        previous: Option<&Commit>,
        index: u64,
        leader: BlockReference,
        commit_timestamp_ns: u64,
        all_blocks: Vec<BlockReference>,
    ) -> Self {
        let previous_commit_hash;
        if let Some(previous) = previous {
            previous_commit_hash = Some(previous.commit_hash);
            assert_eq!(index, previous.index + 1);
            assert!(commit_timestamp_ns >= previous.commit_timestamp_ns);
        } else {
            previous_commit_hash = None;
            assert_eq!(index, 0);
        }
        assert!(!all_blocks.is_empty());
        assert_eq!(all_blocks.last().unwrap(), &leader);
        let mut commit_hash = Blake2b::new();
        commit_hash.update(&previous_commit_hash.unwrap_or_default());
        commit_hash.update(&index.to_be_bytes());
        // do not hash commit.leader (included as part of all_blocks)
        commit_hash.update(&commit_timestamp_ns.to_be_bytes());
        commit_hash.update(&(all_blocks.len() as u64).to_be_bytes());
        for block in all_blocks.iter() {
            commit_hash.update(&block.round_author_hash_encoding());
        }
        let commit_hash = commit_hash.finalize().into();
        Self {
            index,
            leader,
            commit_timestamp_ns,
            all_blocks,
            previous_commit_hash,
            commit_hash,
        }
    }

    pub fn author_round(&self) -> AuthorRound {
        self.leader.author_round()
    }

    pub fn index(&self) -> u64 {
        self.index
    }

    pub fn leader(&self) -> &BlockReference {
        &self.leader
    }

    pub fn commit_timestamp_ns(&self) -> u64 {
        self.commit_timestamp_ns
    }

    pub fn all_blocks(&self) -> &[BlockReference] {
        &self.all_blocks
    }

    pub fn commit_hash(&self) -> &[u8; BLOCK_HASH_LENGTH] {
        &self.commit_hash
    }

    pub fn round(&self) -> Round {
        self.leader.round()
    }

    pub fn previous_commit_hash(&self) -> &Option<[u8; BLOCK_HASH_LENGTH]> {
        &self.previous_commit_hash
    }
}

impl Hash for Commit {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.commit_hash.hash(state)
    }
}

impl CommitDecision {
    pub(crate) fn author_round(&self) -> AuthorRound {
        match self {
            CommitDecision::Commit(block) => block.author_round(),
            CommitDecision::Skip(author_round) => *author_round,
        }
    }
}

impl LeaderStatus {
    pub fn round(&self) -> Round {
        match self {
            Self::Commit(block) => block.round(),
            Self::Skip(leader) => leader.round,
            Self::Undecided(leader) => leader.round,
        }
    }

    pub fn into_decided(self) -> Option<CommitDecision> {
        match self {
            LeaderStatus::Commit(block) => Some(CommitDecision::Commit(block)),
            LeaderStatus::Skip(skip) => Some(CommitDecision::Skip(skip)),
            LeaderStatus::Undecided(_) => None,
        }
    }

    pub fn is_decided(&self) -> bool {
        match self {
            Self::Commit(_) => true,
            Self::Skip(_) => true,
            Self::Undecided(_) => false,
        }
    }
}

impl fmt::Display for LeaderStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Commit(block) => write!(f, "Commit({})", block.reference()),
            Self::Skip(leader) => write!(
                f,
                "Skip({})",
                format_author_round(leader.author, leader.round)
            ),
            Self::Undecided(leader) => write!(
                f,
                "Undecided({})",
                format_author_round(leader.author, leader.round)
            ),
        }
    }
}

impl fmt::Debug for Commit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl fmt::Display for Commit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "<Commit#{:0>6}#{} {}=>{:?}>",
            self.index,
            hex::encode(&self.commit_hash[..4]),
            self.leader,
            self.all_blocks
        )
    }
}
