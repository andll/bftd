use crate::block::{Block, BlockReference, Round, ValidatorIndex};
use crate::block_manager::{BlockManager, BlockStore};
use crate::committee::Committee;
use crate::crypto::{Blake2Hasher, Signer};
use crate::threshold_clock::ThresholdClockAggregator;
use bytes::Bytes;
use std::collections::HashSet;
use std::sync::Arc;

pub struct Core<S, B> {
    signer: S,
    block_manager: BlockManager<B>,
    proposer_clock: ThresholdClockAggregator,
    committee: Arc<Committee>,
    last_proposed_round: Round,
    index: ValidatorIndex,
    parents_accumulator: ParentsAccumulator,
}

pub trait ProposalMaker {
    fn make_proposal(&mut self) -> Bytes;
}

impl<S: Signer, B: BlockStore> Core<S, B> {
    pub fn new(
        signer: S,
        block_store: B,
        committee: Arc<Committee>,
        index: ValidatorIndex,
    ) -> Self {
        let block_manager = BlockManager::new(block_store);
        // todo recover items below
        let last_proposed_round = Round::ZERO;
        let proposer_clock = ThresholdClockAggregator::new(last_proposed_round);
        let parents_accumulator = ParentsAccumulator::new();
        Self {
            signer,
            block_manager,
            proposer_clock,
            committee,
            last_proposed_round,
            index,
            parents_accumulator,
        }
    }

    /// returns new missing blocks
    pub fn add_block(&mut self, block: Arc<Block>) -> Vec<BlockReference> {
        let result = self.block_manager.add_block(block);
        self.blocks_inserted(&result.added);
        result.new_missing
    }

    pub fn next_proposal_round(&self) -> Option<Round> {
        let round = self.proposer_clock.get_round();
        if round > self.last_proposed_round {
            Some(round)
        } else {
            None
        }
    }

    pub fn try_make_proposal(
        &mut self,
        proposal_maker: &mut impl ProposalMaker,
    ) -> Option<Arc<Block>> {
        let round = self.proposer_clock.get_round();
        if round <= self.last_proposed_round {
            return None;
        }
        let payload = proposal_maker.make_proposal();
        let parents = self.parents_accumulator.take_parents();
        let block = Block::new(
            round,
            self.index,
            &payload,
            parents,
            &self.signer,
            &Blake2Hasher,
        );
        let block = Arc::new(block);
        let result = self.block_manager.add_block(block.clone());
        result.assert_added(block.reference());
        self.blocks_inserted(&result.added);
        self.last_proposed_round = round;
        Some(block)
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
}

struct ParentsAccumulator {
    parents: HashSet<BlockReference>,
}

impl ParentsAccumulator {
    pub fn new() -> Self {
        Self {
            parents: Default::default(),
        }
    }

    pub fn add_blocks(&mut self, blocks: &[Arc<Block>]) {
        let mut remove_parents = HashSet::<BlockReference>::new();
        for block in blocks {
            self.parents.insert(*block.reference());
            // todo - keep own previous proposal block?
            remove_parents.extend(block.parents());
        }
        self.parents.retain(|r| !remove_parents.contains(r));
    }

    pub fn take_parents(&mut self) -> Vec<BlockReference> {
        self.parents.drain().collect()
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
        pa.add_blocks(vec![blk(1, 1, vec![br(1, 0)]), blk(1, 2, vec![br(1, 1)])]);
        assert_eq!(pa.take_parents(), vec![br(1, 2)]);
        assert!(pa.parents.is_empty());
    }
}
