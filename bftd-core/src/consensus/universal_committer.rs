// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::block::{AuthorRound, Round, ValidatorIndex};
use crate::committee::Committee;
use crate::consensus::base_committer::BaseCommitterOptions;
use crate::store::BlockStore;
use std::{collections::VecDeque, sync::Arc};

use super::{base_committer::BaseCommitter, CommitDecision, LeaderStatus, DEFAULT_WAVE_LENGTH};

/// A universal committer uses a collection of committers to commit a sequence of leaders.
/// It can be configured to use a combination of different commit strategies, including
/// multi-leaders, backup leaders, and pipelines.
pub struct UniversalCommitter<B> {
    committers: Vec<BaseCommitter<B>>,
    // metrics: Arc<Metrics>,
}

impl<B: BlockStore> UniversalCommitter<B> {
    /// Try to commit part of the dag. This function is idempotent and returns a list of
    /// ordered decided leaders.
    // #[tracing::instrument(skip_all, fields(last_decided = %last_decided))]
    pub fn try_commit(
        &self,
        last_decided: AuthorRound,
        highest_known_round: Round,
    ) -> Vec<CommitDecision> {
        // Try to decide as many leaders as possible, starting with the highest round.
        let mut leaders = VecDeque::new();
        // try to commit a leader up to the highest_known_round - 2. There is no reason to try and
        // iterate on higher rounds as in order to make a direct decision for a leader at round R we
        // need blocks from round R+2 to figure out that enough certificates and support exist to commit a leader.
        'outer: for round in (last_decided.round.0..=highest_known_round.0.saturating_sub(2)).rev()
        {
            let round = Round(round);
            for committer in self.committers.iter().rev() {
                // Skip committers that don't have a leader for this round.
                let Some(leader) = committer.elect_leader(round) else {
                    continue;
                };

                // now that we reached the last committed leader we can stop the commit rule
                if leader == last_decided {
                    tracing::debug!("Leader {leader} - reached last committed, now exit",);
                    break 'outer;
                }

                tracing::debug!("Trying to decide {leader} with {committer}",);

                // Try to directly decide the leader.
                let status = committer.try_direct_decide(leader);
                tracing::debug!("Outcome of direct rule: {status}");

                // If we can't directly decide the leader, try to indirectly decide it.
                if status.is_decided() {
                    leaders.push_front(status);
                } else {
                    let status = committer.try_indirect_decide(leader, leaders.iter());
                    tracing::debug!("Outcome of indirect rule: {status}");
                    leaders.push_front(status);
                }
            }
        }

        // The decided sequence is the longest prefix of decided leaders.
        leaders
            .into_iter()
            // Filter out all the genesis.
            .filter(|s| s.round() > Round::ZERO)
            // Stop the sequence upon encountering an undecided leader.
            .map_while(LeaderStatus::into_decided)
            .collect()
    }

    /// Return list of leaders for the round. Syncer may give those leaders some extra time.
    /// To preserve (theoretical) liveness, we should wait `Delta` time for at least the first leader.
    /// Can return empty vec if round does not have a designated leader.
    pub fn get_leaders(&self, round: Round) -> Vec<ValidatorIndex> {
        self.committers
            .iter()
            .filter_map(|committer| committer.elect_leader(round))
            .map(|l| l.author)
            .collect()
    }

    pub fn is_leader(&self, round: Round, author: ValidatorIndex) -> bool {
        let ar = AuthorRound::new(author, round);
        self.committers
            .iter()
            .any(|committer| committer.elect_leader(round) == Some(ar))
    }
}

/// A builder for a universal committer. By default, the builder creates a single base committer,
/// that is, a single leader and no pipeline.
pub struct UniversalCommitterBuilder<B> {
    committee: Arc<Committee>,
    block_store: B,
    // metrics: Arc<Metrics>,
    wave_length: u64,
    number_of_leaders: u64,
    pipeline: bool,
}

impl<B: BlockStore + Clone> UniversalCommitterBuilder<B> {
    pub fn new(committee: Arc<Committee>, block_store: B) -> Self {
        Self {
            committee,
            block_store,
            // metrics,
            wave_length: DEFAULT_WAVE_LENGTH,
            number_of_leaders: 1,
            pipeline: false,
        }
    }

    #[allow(dead_code)]
    pub fn with_wave_length(mut self, wave_length: u64) -> Self {
        self.wave_length = wave_length;
        self
    }

    pub fn with_number_of_leaders(mut self, number_of_leaders: u64) -> Self {
        self.number_of_leaders = number_of_leaders;
        self
    }

    pub fn with_pipeline(mut self, pipeline: bool) -> Self {
        self.pipeline = pipeline;
        self
    }

    pub fn build(self) -> UniversalCommitter<B> {
        let mut committers = Vec::new();
        let pipeline_stages = if self.pipeline { self.wave_length } else { 1 };
        for round_offset in 0..pipeline_stages {
            for leader_offset in 0..self.number_of_leaders {
                let options = BaseCommitterOptions {
                    wave_length: self.wave_length,
                    round_offset,
                    leader_offset,
                };
                let committer =
                    BaseCommitter::new(self.committee.clone(), self.block_store.clone())
                        .with_options(options);
                committers.push(committer);
            }
        }

        UniversalCommitter {
            committers,
            // metrics: self.metrics,
        }
    }
}
