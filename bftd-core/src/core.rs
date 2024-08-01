use crate::block::{Block, BlockReference, Round, ValidatorIndex};
use crate::committee::Committee;
use crate::crypto::{Blake2Hasher, Signer};
use crate::metrics::Metrics;
use crate::protocol_config::ProtocolConfig;
use crate::store::{BlockStore, BlockViewStore, DagExt};
use crate::threshold_clock::{StakeAggregator, ThresholdClockAggregator, ValidityThreshold};
use bytes::Bytes;
use smallvec::SmallVec;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;

pub struct Core<S, B> {
    signer: S,
    block_store: B,
    proposer_clock: ThresholdClockAggregator,
    committee: Arc<Committee>,
    critical_blocks: BTreeMap<Round, (BlockReference, StakeAggregator<ValidityThreshold>)>,
    last_proposed_reference: BlockReference,
    index: ValidatorIndex,
    parents_accumulator: ParentsAccumulator,
    metrics: Arc<Metrics>,
    protocol_config: ProtocolConfig,
}

/// Application-specific trait to generate block payload.
pub trait ProposalMaker: Send + 'static {
    /// This function is called when core is ready to make a new proposal.
    /// The result of this function is used as a block payload.
    ///
    /// This function should not block,
    /// and should rather return empty payload when there is nothing to propose.
    ///
    /// Returned payload size should be less or equal to MAX_BLOCK_PAYLOAD.
    /// Returned payload should pass verification by the corresponding BlockFilter.
    fn make_proposal(&mut self) -> Bytes;

    /// If a proposal currently is not ready, this can return an optional future
    /// that resolves when the proposal is ready.
    ///
    /// If None is returned, a proposal is created without waiting for non-empty payload.
    fn proposal_waiter<'a>(&'a mut self) -> Option<Pin<Box<dyn Future<Output = ()> + 'a + Send>>> {
        None
    }
}

impl<S: Signer, B: BlockStore + BlockViewStore> Core<S, B> {
    pub fn new(
        signer: S,
        block_store: B,
        committee: Arc<Committee>,
        index: ValidatorIndex,
        metrics: Arc<Metrics>,
        protocol_config: ProtocolConfig,
    ) -> Self {
        let mut latest_blocks = Vec::with_capacity(committee.len());
        let mut last_own_block = None;
        for validator in committee.enumerate_indexes() {
            let last_block = block_store.last_known_block(validator);
            if validator == index {
                last_own_block = Some(last_block.clone());
            }
            latest_blocks.push(last_block);
        }
        let last_own_block = last_own_block.expect("Must find last own bock");
        let last_proposed_reference = *last_own_block.reference();
        let last_proposed_round = last_proposed_reference.round();
        let proposer_clock = ThresholdClockAggregator::new(last_proposed_round);
        // todo recover other nodes for parents accumulator?
        let parents_accumulator = ParentsAccumulator::new(index, &committee);
        let mut this = Self {
            signer,
            block_store,
            proposer_clock,
            committee,
            critical_blocks: Default::default(),
            last_proposed_reference,
            index,
            parents_accumulator,
            metrics,
            protocol_config,
        };
        this.update_critical_blocks();
        this.add_blocks(&latest_blocks);
        this
    }

    /// Handles added blocks.
    /// Blocks added here must be already stored in BlockStore.
    pub fn add_blocks(&mut self, blocks: &[Arc<Block>]) {
        for b in blocks {
            self.proposer_clock
                .add_block(*b.reference(), &self.committee);
            self.add_block_to_critical_block_aggregators(b.reference());
        }
        self.parents_accumulator.add_blocks(blocks);
        self.update_critical_blocks();
    }

    fn add_block_to_critical_block_aggregators(&mut self, reference: &BlockReference) {
        if !self.protocol_config.critical_block_check {
            return;
        }
        for (proposal_round, (critical_block, aggregator)) in self.critical_blocks.iter_mut() {
            Self::update_critical_block_aggregator(
                *proposal_round,
                aggregator,
                critical_block,
                reference,
                &self.block_store,
                self.index,
                &self.committee,
            );
        }
    }

    fn update_critical_block_aggregator(
        proposal_round: Round,
        aggregator: &mut StakeAggregator<ValidityThreshold>,
        critical_block: &BlockReference,
        reference: &BlockReference,
        block_store: &B,
        index: ValidatorIndex,
        committee: &Committee,
    ) {
        if aggregator.contains(reference.author()) {
            return;
        }
        if reference.round >= proposal_round {
            return;
        }
        let block_view = block_store.get_block_view(reference);
        let block_view_round = index
            .slice_get(&block_view)
            .expect("Own block view can not be None")
            .round();
        if block_view_round >= critical_block.round() {
            log::debug!("Added support from {reference} for critical block {critical_block} for proposal round {proposal_round}");
            aggregator.add(reference.author(), committee);
        }
    }

    fn update_critical_blocks(&mut self) {
        if !self.protocol_config.critical_block_check {
            return;
        }
        let proposal_rounds = self.possible_proposal_rounds();
        self.critical_blocks
            .retain(|r, _| proposal_rounds.contains(r));
        for round in proposal_rounds {
            let Entry::Vacant(va) = self.critical_blocks.entry(round) else {
                continue;
            };
            let Some(critical_block) = self
                .block_store
                .critical_block_for_round(round, self.last_proposed_reference)
            else {
                continue;
            };
            log::debug!(
                "Init critical block aggregator for round {round}, critical block {critical_block}"
            );
            let mut aggregator = StakeAggregator::new();
            for validator in self.committee.enumerate_indexes() {
                // todo BlockCache support for last_block and last_known_block_view
                let last_block = self.block_store.last_known_block(validator);
                Self::update_critical_block_aggregator(
                    round,
                    &mut aggregator,
                    &critical_block,
                    last_block.reference(),
                    &self.block_store,
                    self.index,
                    &self.committee,
                );
                if let Some(preceding) = last_block.preceding() {
                    // todo check round of preceding and compare it to round
                    // todo - check if we actually need this block
                    Self::update_critical_block_aggregator(
                        round,
                        &mut aggregator,
                        &critical_block,
                        preceding,
                        &self.block_store,
                        self.index,
                        &self.committee,
                    );
                }
            }
            va.insert((critical_block, aggregator));
        }
    }

    /// returns threshold clock round for new proposal
    /// returns none if the threshold clock round is below or equal to the last proposed round
    pub fn threshold_clock_round(&self) -> Option<Round> {
        let round = self.proposer_clock.get_round();
        if round > self.last_proposed_round() {
            Some(round)
        } else {
            None
        }
    }

    fn possible_proposal_rounds(&self) -> SmallVec<[Round; 2]> {
        let round = self.threshold_clock_round();
        let mut rounds = SmallVec::new();
        let Some(round) = round else {
            return rounds;
        };

        let previous = round.previous();
        if previous > self.last_proposed_round() {
            // todo - consult with commit rule if we need previous round
            rounds.push(previous);
        }
        rounds.push(round);

        rounds
    }

    pub fn missing_validators_for_proposal(&self) -> Vec<ValidatorIndex> {
        // todo - rewrite. Use ValidatorSet instead of BTreeSet
        // todo fix ValidatorSet::present returning more indexes then validators in committee
        let mut s = BTreeSet::from_iter(self.committee.enumerate_indexes());
        for v in self.proposer_clock.validator_set().present() {
            s.remove(&v);
        }
        s.into_iter().collect()
    }

    pub fn last_proposed_round(&self) -> Round {
        self.last_proposed_reference.round
    }

    pub fn last_proposed_reference(&self) -> &BlockReference {
        &self.last_proposed_reference
    }

    pub fn critical_block_supported(&self, round: Round) -> bool {
        if !self.protocol_config.critical_block_check {
            return true;
        }
        let possible_proposal_rounds = self.possible_proposal_rounds();
        if !possible_proposal_rounds.contains(&round) {
            panic!("critical_block_supported invariant violation on round {round}: possible_proposal_rounds: {possible_proposal_rounds:?}, round: {round}");
        }
        let critical_block_from_store = self
            .block_store
            .critical_block_for_round(round, self.last_proposed_reference);
        let critical_block = self.critical_blocks.get(&round);
        let Some((critical_block, aggregator)) = critical_block else {
            if let Some(critical_block_from_store) = critical_block_from_store {
                panic!("critical_block_supported invariant violation on round {round} - critical_block_from_store({critical_block_from_store}), no critical block aggregator");
            }
            return true;
        };
        assert_eq!(
            Some(*critical_block),
            critical_block_from_store,
            "Critical block invariant violation"
        );
        let satisfied = aggregator.satisfied(&self.committee);
        if !satisfied {
            tracing::debug!("Not creating proposal - critical block {critical_block} for round {round} does not have enough support, missing validators {:?}", aggregator.missing(&self.committee));
        }
        satisfied
    }

    pub fn make_proposal(
        &mut self,
        proposal_maker: &mut impl ProposalMaker,
        round: Round,
        time_ns: u64,
    ) -> Arc<Block> {
        assert!(round <= self.proposer_clock.get_round());
        assert!(round > self.last_proposed_round());
        let payload = proposal_maker.make_proposal();
        let parents = self.parents_accumulator.take_parents(round);
        let block = Block::new(
            round,
            self.index,
            *self.committee.chain_id(),
            time_ns,
            &payload,
            parents,
            &self.signer,
            &Blake2Hasher,
            Some(self.metrics.clone()),
        );
        let block = Arc::new(block);
        self.block_store.put(block.clone());
        self.block_store.flush();
        self.add_blocks(&[block.clone()]);
        self.last_proposed_reference = *block.reference();
        self.update_critical_blocks();
        self.metrics.core_last_proposed_round.set(round.0 as i64);
        self.metrics
            .core_last_proposed_block_size
            .set(block.data().len() as i64);
        block
    }

    pub fn committee(&self) -> &Arc<Committee> {
        &self.committee
    }

    pub fn validator_index(&self) -> ValidatorIndex {
        self.index
    }
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }
}

struct ParentsAccumulator {
    this_validator: ValidatorIndex,
    // Last seen parents by validator, up to 3 rounds
    // Can probably use more efficient data structure than BTreeSet for this...
    parents: Vec<BTreeSet<BlockReference>>,
    previous_own_block: Option<BlockReference>,
}

impl ParentsAccumulator {
    pub fn new(this_validator: ValidatorIndex, committee: &Committee) -> Self {
        let parents = committee.committee_vec_default();
        Self {
            this_validator,
            parents,
            previous_own_block: None,
        }
    }

    pub fn add_blocks(&mut self, blocks: &[Arc<Block>]) {
        for block in blocks {
            self.add_block(block);
        }
    }

    fn add_block(&mut self, block: &Block) {
        if block.author() == self.this_validator {
            if let Some(prev) = self.previous_own_block {
                panic!(
                    "Adding new own block {}, but did not use previous own block {prev} in parents",
                    block.reference()
                );
            }
            self.previous_own_block = Some(*block.reference());
            return;
        }
        let btree = block.author().slice_get_mut(&mut self.parents);
        let mut range = btree.range(BlockReference::range_for_round(block.round()));
        if let Some(current) = range.next() {
            // todo - right now this panic can actually get triggered!
            panic!(
                "Inserting parent for {} while already have parent {current}",
                block.reference()
            );
        }
        btree.insert(*block.reference());
        if btree.len() > 3 {
            // We only care about up to 3 highest rounds parents per validator
            btree.pop_first();
        }
    }

    pub fn take_parents(&mut self, proposal_round: Round) -> Vec<BlockReference> {
        let mut parents = Vec::with_capacity(self.parents.len());
        parents.push(
            self.previous_own_block
                .take()
                .expect("Must have own block parent"),
        );
        for btree in self.parents.iter_mut() {
            let mut p = btree.split_off(&BlockReference::first_for_round(proposal_round));
            // p is a portion of btree >= proposal_round(what we need to keep), swapping it into *btree
            mem::swap(btree, &mut p);
            // now p is a portion of btree < proposal_round, which is where we take parent
            if let Some(parent) = p.pop_last() {
                assert!(
                    parent.round() < proposal_round,
                    "Taking parent {parent}, proposal round {proposal_round}"
                );
                parents.push(parent);
            }
        }
        parents
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.parents.iter().all(BTreeSet::is_empty) && self.previous_own_block.is_none()
    }
}

// todo delete
impl ProposalMaker for () {
    fn make_proposal(&mut self) -> Bytes {
        Bytes::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::tests::{blk, br};

    #[test]
    pub fn parents_accumulator_test() {
        let committee = Committee::new_test(vec![1, 2]);
        let mut pa = ParentsAccumulator::new(ValidatorIndex(0), &committee);
        pa.add_blocks(&[
            blk(0, 0, vec![]),
            blk(1, 1, vec![br(1, 0)]),
            blk(1, 2, vec![br(1, 1)]),
        ]);
        assert_eq!(pa.take_parents(Round(3)), vec![br(0, 0), br(1, 2)]);
        assert!(pa.is_empty());
        let mut pa = ParentsAccumulator::new(ValidatorIndex(0), &committee);
        pa.add_blocks(&[
            blk(1, 1, vec![br(1, 0)]),
            blk(1, 2, vec![br(1, 1)]),
            blk(0, 0, vec![]),
        ]);
        assert_eq!(pa.take_parents(Round(2)), vec![br(0, 0), br(1, 1)]);
        assert!(!pa.is_empty());
        pa.add_blocks(&[blk(0, 1, vec![])]);
        assert_eq!(pa.take_parents(Round(3)), vec![br(0, 1), br(1, 2)]);
        assert!(pa.is_empty());
    }
}
