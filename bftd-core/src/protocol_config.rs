use crate::consensus::LeaderElection;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ProtocolConfig {
    /// How much time to wait for blocks from round leaders
    pub(crate) leader_timeout: Duration,
    /// How many leaders we have per round
    pub(crate) leader_election: LeaderElection,
    /// How much time to wait before proposing on round if all uncommitted blocks are empty.
    /// Limits the number of empty blocks generated under the low load.
    ///
    /// It can be zero if we want to produce blocks as fast as possible.
    ///
    /// This parameter only makes sense if ProposalMaker::proposal_waiter is implemented for the proposer.
    ///
    /// This timeout plays no role if ProposalMaker::proposal_waiter always returns None.
    pub(crate) empty_commit_timeout: Duration,
    pub(crate) critical_block_check: bool,
}

impl Default for ProtocolConfig {
    fn default() -> Self {
        Self {
            leader_timeout: Duration::from_secs(2),
            leader_election: LeaderElection::All,
            empty_commit_timeout: Duration::ZERO,
            critical_block_check: true,
        }
    }
}

impl ProtocolConfig {
    pub fn empty_commit_timeout_set(&self) -> bool {
        self.empty_commit_timeout != Duration::ZERO
    }
}

#[derive(Default)]
pub struct ProtocolConfigBuilder(ProtocolConfig);

impl ProtocolConfigBuilder {
    pub fn with_leader_timeout(&mut self, leader_timeout: Duration) {
        self.0.leader_timeout = leader_timeout;
    }
    pub fn leader_timeout(&self) -> Duration {
        self.0.leader_timeout
    }

    pub fn with_leader_election(&mut self, leader_election: LeaderElection) {
        self.0.leader_election = leader_election;
    }

    pub fn with_empty_commit_timeout(&mut self, empty_commit_timeout: Duration) {
        self.0.empty_commit_timeout = empty_commit_timeout;
    }

    pub fn with_recommended_empty_commit_timeout(&mut self) {
        self.0.empty_commit_timeout = self.0.leader_timeout / 2;
    }

    pub fn with_critical_block_check(&mut self, b: bool) {
        self.0.critical_block_check = b;
    }

    pub fn empty_commit_timeout(&self) -> Duration {
        self.0.empty_commit_timeout
    }

    pub fn build(self) -> ProtocolConfig {
        if self.0.leader_timeout <= self.0.empty_commit_timeout {
            panic!("leader_timeout should be larger then empty_commit_timeout");
        }
        if self.0.leader_timeout < self.0.empty_commit_timeout * 2 {
            tracing::error!("Protocol might not work well if leader_timeout({:?}) is less double of empty_commit_timeout({:?})", self.0.leader_timeout, self.0.empty_commit_timeout);
        }
        if self.0.critical_block_check && self.0.leader_election != LeaderElection::All {
            tracing::error!("When critical block check is set, performance can be impacted if leader election is not set to All(current selection {:?}).", self.0.leader_election);
        }
        self.0
    }
}
