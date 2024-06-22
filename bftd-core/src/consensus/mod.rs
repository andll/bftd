// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::block::{format_author_round, AuthorRound, Block, BlockReference, Round};
use serde::{Deserialize, Serialize};
use std::fmt;
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
    pub index: u64,
    pub leader: BlockReference,
}

impl Commit {
    pub fn author_round(&self) -> AuthorRound {
        self.leader.author_round()
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
        write!(f, "<Commit @{} {}>", self.index, self.leader)
    }
}
