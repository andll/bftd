// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::block::{format_author_round, AuthorRound, Block, Round, ValidatorIndex};
use std::fmt;
use std::sync::Arc;

mod base_committer;
mod universal_committer;

/// Default wave length for all committers. A longer wave_length increases the chance of committing the leader
/// under asynchrony at the cost of latency in the common case.
pub const DEFAULT_WAVE_LENGTH: u64 = MINIMUM_WAVE_LENGTH;

/// We need at least one leader round, one voting round, and one decision round.
pub const MINIMUM_WAVE_LENGTH: u64 = 3;

/// The status of every leader output by the committers. While the core only cares about committed
/// leaders, providing a richer status allows for easier debugging, testing, and composition with
/// advanced commit strategies.
// #[derive(Debug, Clone, Eq, PartialEq, Hash)]
#[derive(Clone)]
pub enum LeaderStatus {
    Commit(Arc<Block>),
    Skip(AuthorRound),
    Undecided(AuthorRound),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum Decision {
    Direct,
    Indirect,
}

impl LeaderStatus {
    pub fn round(&self) -> Round {
        match self {
            Self::Commit(block) => block.round(),
            Self::Skip(leader) => leader.round,
            Self::Undecided(leader) => leader.round,
        }
    }

    pub fn authority(&self) -> ValidatorIndex {
        match self {
            Self::Commit(block) => block.author(),
            Self::Skip(leader) => leader.author,
            Self::Undecided(leader) => leader.author,
        }
    }

    pub fn is_decided(&self) -> bool {
        match self {
            Self::Commit(_) => true,
            Self::Skip(_) => true,
            Self::Undecided(_) => false,
        }
    }

    pub fn into_decided_author_round(self) -> AuthorRound {
        match self {
            Self::Commit(block) => AuthorRound::new(block.author(), block.round()),
            Self::Skip(leader) => leader,
            Self::Undecided(..) => panic!("Decided block is either Commit or Skip"),
        }
    }

    pub fn into_committed_block(self) -> Option<Arc<Block>> {
        match self {
            Self::Commit(block) => Some(block),
            Self::Skip(..) => None,
            Self::Undecided(..) => panic!("Decided block is either Commit or Skip"),
        }
    }
}
//
// impl PartialOrd for LeaderStatus {
//     fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
//         Some(self.cmp(other))
//     }
// }
//
// impl Ord for LeaderStatus {
//     fn cmp(&self, other: &Self) -> std::cmp::Ordering {
//         (self.round(), self.authority()).cmp(&(other.round(), other.authority()))
//     }
// }

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
