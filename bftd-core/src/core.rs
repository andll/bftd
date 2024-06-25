use crate::block::{Block, BlockReference, ChainId, Round, ValidatorIndex};
use crate::block_manager::{AddBlockResult, BlockManager, BlockStore};
use crate::committee::Committee;
use crate::crypto::{Blake2Hasher, Signer};
use crate::metrics::Metrics;
use crate::threshold_clock::ThresholdClockAggregator;
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

pub struct Core<S, B> {
    signer: S,
    block_manager: BlockManager<B>,
    proposer_clock: ThresholdClockAggregator,
    committee: Arc<Committee>,
    last_proposed_round: Round,
    index: ValidatorIndex,
    parents_accumulator: ParentsAccumulator,
    metrics: Arc<Metrics>,
}

pub trait ProposalMaker: Send + 'static {
    fn make_proposal(&mut self) -> Bytes;
}

impl<S: Signer, B: BlockStore> Core<S, B> {
    pub fn new(
        signer: S,
        block_store: B,
        committee: Arc<Committee>,
        index: ValidatorIndex,
        metrics: Arc<Metrics>,
    ) -> Self {
        for block in committee.genesis_blocks() {
            let block = Arc::new(block);
            block_store.put(block);
        }
        let mut latest_blocks = Vec::with_capacity(committee.len());
        for validator in committee.enumerate_indexes() {
            let last_block = block_store.last_known_block(validator);
            latest_blocks.push(last_block);
        }
        let last_proposed_round = block_store.last_known_round(index);
        let block_manager = BlockManager::new(block_store, metrics.clone());
        let proposer_clock = ThresholdClockAggregator::new(last_proposed_round);
        // todo recover other nodes for parents accumulator?
        let parents_accumulator = ParentsAccumulator::new();
        let mut this = Self {
            signer,
            block_manager,
            proposer_clock,
            committee,
            last_proposed_round,
            index,
            parents_accumulator,
            metrics,
        };
        this.blocks_inserted(&latest_blocks);
        this
    }

    /// returns new missing blocks
    pub fn add_block(&mut self, block: Arc<Block>) -> AddBlockResult {
        let result = self.block_manager.add_block(block);
        self.blocks_inserted(&result.added);
        result
    }

    /// returns vector clock round for new proposal
    /// returns none if vector clock round is below or equal last proposed round
    pub fn vector_clock_round(&self) -> Option<Round> {
        let round = self.proposer_clock.get_round();
        if round > self.last_proposed_round {
            Some(round)
        } else {
            None
        }
    }

    pub fn missing_validators_for_proposal(&self) -> Vec<ValidatorIndex> {
        let mut s = BTreeSet::from_iter(self.committee.enumerate_indexes());
        for v in self.proposer_clock.validator_set().present() {
            s.remove(&v);
        }
        s.into_iter().collect()
    }

    pub fn last_proposed_round(&self) -> Round {
        self.last_proposed_round
    }

    pub fn make_proposal(
        &mut self,
        proposal_maker: &mut impl ProposalMaker,
        round: Round,
        time_ns: u64,
    ) -> Arc<Block> {
        assert!(round <= self.proposer_clock.get_round());
        assert!(round > self.last_proposed_round);
        let payload = proposal_maker.make_proposal();
        let parents = self.parents_accumulator.take_parents(round);
        let block = Block::new(
            round,
            self.index,
            ChainId::CHAIN_ID_TEST,
            time_ns,
            &payload,
            parents,
            &self.signer,
            &Blake2Hasher,
            Some(self.metrics.clone()),
        );
        let block = Arc::new(block);
        let result = self.block_manager.add_block(block.clone());
        self.block_manager.flush();
        result.assert_added(block.reference());
        self.blocks_inserted(&result.added);
        self.last_proposed_round = round;
        self.metrics.core_last_proposed_round.set(round.0 as i64);
        self.metrics.core_last_proposed_block_size.set(block.data().len() as i64);
        block
    }

    fn blocks_inserted(&mut self, blocks: &[Arc<Block>]) {
        for b in blocks {
            self.proposer_clock
                .add_block(*b.reference(), &self.committee);
        }
        self.parents_accumulator.add_blocks(blocks);
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
    parents: BTreeMap<BlockReference, Arc<Block>>,
}

impl ParentsAccumulator {
    pub fn new() -> Self {
        Self {
            parents: Default::default(),
        }
    }

    pub fn add_blocks(&mut self, blocks: &[Arc<Block>]) {
        for block in blocks {
            self.parents.insert(*block.reference(), block.clone());
        }
    }

    pub fn take_parents(&mut self, before_round_excluded: Round) -> Vec<BlockReference> {
        let mut remove_parents = HashSet::<BlockReference>::new();
        let mut parents = HashSet::<BlockReference>::new();
        let all_parents = self
            .parents
            .range(..BlockReference::first_block_reference_for_round(before_round_excluded));
        let all_parents: Vec<_> = all_parents.map(|(r, _)| *r).collect();
        for parent in all_parents {
            let parent = self.parents.remove(&parent).unwrap();
            remove_parents.extend(parent.parents());
            // todo - keep own previous proposal block?
            parents.insert(*parent.reference());
        }
        parents.retain(|k| !remove_parents.contains(k));
        parents.into_iter().collect()
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
        let mut pa = ParentsAccumulator::new();
        pa.add_blocks(&[blk(1, 1, vec![br(1, 0)]), blk(1, 2, vec![br(1, 1)])]);
        assert_eq!(pa.take_parents(Round(3)), vec![br(1, 2)]);
        assert!(pa.parents.is_empty());
        pa.add_blocks(&[blk(1, 1, vec![br(1, 0)]), blk(1, 2, vec![br(1, 1)])]);
        assert_eq!(pa.take_parents(Round(2)), vec![br(1, 1)]);
        assert_eq!(pa.parents.len(), 1);
        assert_eq!(pa.take_parents(Round(3)), vec![br(1, 2)]);
        assert!(pa.parents.is_empty());
    }
}
