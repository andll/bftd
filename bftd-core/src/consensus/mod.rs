// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::block::{format_author_round, AuthorRound, Block, Round};
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
pub enum DecidedCommit {
    Commit(Arc<Block>),
    Skip(AuthorRound),
}

impl DecidedCommit {
    pub(crate) fn author_round(&self) -> AuthorRound {
        match self {
            DecidedCommit::Commit(block) => block.author_round(),
            DecidedCommit::Skip(author_round) => *author_round,
        }
    }
}

pub trait CommitStore {
    fn commit_leader(&self, leader_status: DecidedCommit);
}

impl LeaderStatus {
    pub fn round(&self) -> Round {
        match self {
            Self::Commit(block) => block.round(),
            Self::Skip(leader) => leader.round,
            Self::Undecided(leader) => leader.round,
        }
    }

    pub fn into_decided(self) -> Option<DecidedCommit> {
        match self {
            LeaderStatus::Commit(block) => Some(DecidedCommit::Commit(block)),
            LeaderStatus::Skip(skip) => Some(DecidedCommit::Skip(skip)),
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
