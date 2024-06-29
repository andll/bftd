// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::block::{
    format_author_round, AuthorRound, Block, BlockReference, Round, ValidatorIndex,
};
use crate::committee::Committee;
use crate::committee::Stake;
use crate::store::BlockReader;
use crate::threshold_clock::{QuorumThreshold, StakeAggregator};
use std::collections::HashMap;
use std::iter::Sum;
use std::{fmt::Display, sync::Arc};

use super::{LeaderStatus, DEFAULT_WAVE_LENGTH, MINIMUM_WAVE_LENGTH};

/// The consensus protocol operates in 'waves'. Each wave is composed of a leader round, at least one
/// voting round, and one decision round.
type WaveNumber = u64;

pub struct BaseCommitterOptions {
    /// The length of a wave (minimum 3)
    pub wave_length: u64,
    /// The offset used in the leader-election protocol. THis is used by the multi-committer to ensure
    /// that each [`BaseCommitter`] instance elects a different leader.
    pub leader_offset: u64,
    /// The offset of the first wave. This is used by the pipelined committer to ensure that each
    /// [`BaseCommitter`] instances operates on a different view of the dag.
    pub round_offset: u64,
}

impl Default for BaseCommitterOptions {
    fn default() -> Self {
        Self {
            wave_length: DEFAULT_WAVE_LENGTH,
            leader_offset: 0,
            round_offset: 0,
        }
    }
}

/// The [`BaseCommitter`] contains the bare bone commit logic. Once instantiated, the method `try_direct_decide`
/// and `try_indirect_decide` can be called at any time and any number of times (it is idempotent) to determine
/// whether a leader can be committed or skipped.
pub struct BaseCommitter<B> {
    /// The committee information
    committee: Arc<Committee>,
    /// Keep all block data
    block_store: B,
    /// The options used by this committer
    options: BaseCommitterOptions,
}

impl<B: BlockReader> BaseCommitter<B> {
    pub fn new(committee: Arc<Committee>, block_store: B) -> Self {
        Self {
            committee,
            block_store,
            options: BaseCommitterOptions::default(),
        }
    }

    pub fn with_options(mut self, options: BaseCommitterOptions) -> Self {
        assert!(options.wave_length >= MINIMUM_WAVE_LENGTH);
        self.options = options;
        self
    }

    /// Return the wave in which the specified round belongs.
    fn wave_number(&self, round: Round) -> WaveNumber {
        round.0.saturating_sub(self.options.round_offset) / self.options.wave_length
    }

    /// Return the leader round of the specified wave number. The leader round is always the first
    /// round of the wave.
    fn leader_round(&self, wave: WaveNumber) -> Round {
        Round(wave * self.options.wave_length + self.options.round_offset)
    }

    /// Return the decision round of the specified wave. The decision round is always the last
    /// round of the wave.
    fn decision_round(&self, wave: WaveNumber) -> Round {
        let wave_length = self.options.wave_length;
        Round(wave * wave_length + wave_length - 1 + self.options.round_offset)
    }

    /// The leader-elect protocol is offset by `leader_offset` to ensure that different committers
    /// with different leader offsets elect different leaders for the same round number. This function
    /// returns `None` if there are no leaders for the specified round.
    pub fn elect_leader(&self, round: Round) -> Option<AuthorRound> {
        let wave = self.wave_number(round);
        if self.leader_round(wave) != round {
            return None;
        }

        let offset = self.options.leader_offset;
        Some(AuthorRound::new(
            self.committee.elect_leader(round, offset),
            round,
        ))
    }

    /// Find which block is supported at (author, round) by the given block.
    /// Blocks can indirectly reference multiple other blocks at (author, round), but only one block at
    /// (author, round)  will be supported by the given block. If block A supports B at (author, round),
    /// it is guaranteed that any processed block by the same author that directly or indirectly parents
    /// A will also support B at (author, round).
    fn find_support(&self, author_round: AuthorRound, from: &Arc<Block>) -> Option<BlockReference> {
        if from.round() < author_round.round {
            return None;
        }
        for parent in from.parents() {
            if parent.author_round() == author_round {
                return Some(*parent);
            }
            // Weak links may point to blocks with lower round numbers than strong links.
            if parent.round() <= author_round.round {
                continue;
            }
            let parent = self
                .block_store
                .get(parent)
                .expect("We should have the whole sub-dag by now");
            if let Some(support) = self.find_support(author_round, &parent) {
                return Some(support);
            }
        }
        None
    }

    /// Check whether the specified block (`potential_certificate`) is a vote for
    /// the specified leader (`leader_block`).
    fn is_vote(&self, potential_vote: &Arc<Block>, leader_block: &Arc<Block>) -> bool {
        let author_round = leader_block.author_round();
        self.find_support(author_round, potential_vote) == Some(*leader_block.reference())
    }

    /// Check whether the specified block (`potential_certificate`) is a certificate for
    /// the specified leader (`leader_block`). An `all_votes` map can be provided as a cache to quickly
    /// skip checking against the block store on whether a reference is a vote. This is done for efficiency.
    /// Bear in mind that the `all_votes` should refer to votes considered to the same `leader_block` and it can't
    /// be reused for different leaders.
    fn is_certificate(
        &self,
        potential_certificate: &Arc<Block>,
        leader_block: &Arc<Block>,
        all_votes: &mut HashMap<BlockReference, bool>,
    ) -> bool {
        let mut votes_stake_aggregator = StakeAggregator::<QuorumThreshold>::new();
        for reference in potential_certificate.parents() {
            let is_vote = if let Some(is_vote) = all_votes.get(reference) {
                *is_vote
            } else {
                let potential_vote = self
                    .block_store
                    .get(reference)
                    .expect("We should have the whole sub-dag by now");
                let is_vote = self.is_vote(&potential_vote, leader_block);
                all_votes.insert(*reference, is_vote);
                is_vote
            };

            if is_vote {
                tracing::trace!(
                    "[{self}] {reference} is a vote for {}",
                    leader_block.reference()
                );
                if votes_stake_aggregator.add(reference.author, &self.committee) {
                    return true;
                }
            }
        }
        false
    }

    /// Decide the status of a target leader from the specified anchor. We commit the target leader
    /// if it has a certified link to the anchor. Otherwise, we skip the target leader.
    fn decide_leader_from_anchor(&self, anchor: &Arc<Block>, leader: AuthorRound) -> LeaderStatus {
        // Get the block(s) proposed by the leader. There could be more than one leader block
        // per round (produced by a Byzantine leader).
        let leader_blocks = self
            .block_store
            .get_blocks_at_author_round(leader.author, leader.round);

        // Get all blocks that could be potential certificates for the target leader. These blocks
        // are in the decision round of the target leader and are linked to the anchor.
        let wave = self.wave_number(leader.round);
        let decision_round = self.decision_round(wave);
        let potential_certificates = self.block_store.linked_to_round(anchor, decision_round);

        // Use those potential certificates to determine which (if any) of the target leader
        // blocks can be committed.
        let mut certified_leader_blocks: Vec<_> = leader_blocks
            .into_iter()
            .filter(|leader_block| {
                let mut all_votes = HashMap::new();
                potential_certificates.iter().any(|potential_certificate| {
                    self.is_certificate(potential_certificate, leader_block, &mut all_votes)
                })
            })
            .collect();

        // There can be at most one certified leader, otherwise it means the BFT assumption is broken.
        if certified_leader_blocks.len() > 1 {
            panic!("More than one certified block at wave {wave} from leader {leader}")
        }

        // We commit the target leader if it has a certificate that is an ancestor of the anchor.
        // Otherwise skip it.
        match certified_leader_blocks.pop() {
            Some(certified_leader_block) => LeaderStatus::Commit(certified_leader_block.clone()),
            None => LeaderStatus::Skip(leader),
        }
    }

    /// Check whether the specified leader has enough blames (that is, 2f+1 non-votes) to be
    /// directly skipped.
    fn enough_leader_blame(&self, voting_round: Round, leader: ValidatorIndex) -> bool {
        let voting_blocks = self.block_store.get_blocks_by_round(voting_round);

        let mut blame_stake_aggregator = StakeAggregator::<QuorumThreshold>::new();
        for voting_block in &voting_blocks {
            let voter = voting_block.reference().author;
            if voting_block
                .parents()
                .iter()
                .all(|parent| parent.author != leader)
            {
                tracing::trace!(
                    "[{self}] {} is a blame for leader {}",
                    voting_block.reference(),
                    format_author_round(leader, voting_round.previous())
                );
                if blame_stake_aggregator.add(voter, &self.committee) {
                    return true;
                }
            }
        }
        false
    }

    /// Check whether the specified leader has enough support (that is, 2f+1 certificates)
    /// to be directly committed.
    fn enough_leader_support(&self, decision_round: Round, leader_block: &Arc<Block>) -> bool {
        let decision_blocks = self.block_store.get_blocks_by_round(decision_round);

        // quickly reject if there isn't enough stake to support the leader from the potential certificates
        let total_stake: Stake = decision_blocks
            .iter()
            .map(|b| self.committee.get_stake(b.author()))
            .sum();
        if total_stake < self.committee.f2_threshold {
            tracing::debug!(
                "Not enough support for: {}. Stake not enough: {} < {}",
                leader_block.round(),
                total_stake,
                self.committee.f2_threshold
            );
            return false;
        }

        let mut certificate_stake_aggregator = StakeAggregator::<QuorumThreshold>::new();
        let mut all_votes = HashMap::new();
        for decision_block in &decision_blocks {
            let authority = decision_block.reference().author;
            if self.is_certificate(decision_block, leader_block, &mut all_votes) {
                tracing::trace!(
                    "[{self}] {} is a certificate for leader {}",
                    decision_block.reference(),
                    leader_block.reference()
                );
                if certificate_stake_aggregator.add(authority, &self.committee) {
                    return true;
                }
            }
        }
        false
    }

    /// Apply the indirect decision rule to the specified leader to see whether we can indirect-commit
    /// or indirect-skip it.
    // #[tracing::instrument(skip_all, fields(leader = %format_author_round(leader.author, leader.round)))]
    pub fn try_indirect_decide<'a>(
        &self,
        leader: AuthorRound,
        leaders: impl Iterator<Item = &'a LeaderStatus>,
    ) -> LeaderStatus {
        // The anchor is the first committed leader with round higher than the decision round of the
        // target leader. We must stop the iteration upon encountering an undecided leader.
        let anchors = leaders.filter(|x| leader.round + self.options.wave_length <= x.round());

        for anchor in anchors {
            tracing::trace!(
                "[{self}] Trying to indirect-decide {} using anchor {anchor}",
                format_author_round(leader.author, leader.round),
            );
            match anchor {
                LeaderStatus::Commit(anchor) => {
                    return self.decide_leader_from_anchor(anchor, leader);
                }
                LeaderStatus::Skip(..) => (),
                LeaderStatus::Undecided(..) => break,
            }
        }

        LeaderStatus::Undecided(leader)
    }

    /// Apply the direct decision rule to the specified leader to see whether we can direct-commit or
    /// direct-skip it.
    // #[tracing::instrument(skip_all, fields(leader = %format_author_round(leader.author, leader.round)))]
    pub fn try_direct_decide(&self, leader: AuthorRound) -> LeaderStatus {
        // Check whether the leader has enough blame. That is, whether there are 2f+1 non-votes
        // for that leader (which ensure there will never be a certificate for that leader).
        let voting_round = leader.round + 1;
        if self.enough_leader_blame(voting_round, leader.author) {
            return LeaderStatus::Skip(leader);
        }

        // Check whether the leader(s) has enough support. That is, whether there are 2f+1
        // certificates over the leader. Note that there could be more than one leader block
        // (created by Byzantine leaders).
        let wave = self.wave_number(leader.round);
        let decision_round = self.decision_round(wave);
        let leader_blocks = self
            .block_store
            .get_blocks_at_author_round(leader.author, leader.round);
        let mut leaders_with_enough_support: Vec<_> = leader_blocks
            .into_iter()
            .filter(|l| self.enough_leader_support(decision_round, l))
            .map(LeaderStatus::Commit)
            .collect();

        // There can be at most one leader with enough support for each round, otherwise it means
        // the BFT assumption is broken.
        if leaders_with_enough_support.len() > 1 {
            panic!(
                "[{self}] More than one certified block for {}",
                format_author_round(leader.author, leader.round)
            )
        }

        leaders_with_enough_support
            .pop()
            .unwrap_or_else(|| LeaderStatus::Undecided(leader))
    }
}

impl<B> Display for BaseCommitter<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Committer-L{}-R{}",
            self.options.leader_offset, self.options.round_offset
        )
    }
}

impl Sum for Stake {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        Self(iter.map(|s| s.0).sum())
    }
}
