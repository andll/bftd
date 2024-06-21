use crate::block::{Block, BlockReference, Round, ValidatorIndex};
use crate::committee::{Committee, Stake};
use std::cmp::Ordering;
use std::marker::PhantomData;

// A block is threshold clock valid if:
// - all included blocks have a round number lower than the block round number.
// - the set of authorities with blocks included has a quorum in the current committee.
// todo - tests and usage
#[allow(dead_code)]
pub fn threshold_clock_valid_non_genesis(block: &Block, committee: &Committee) -> bool {
    // get a committee from the creator of the block
    let round_number = block.reference().round;
    let previous_round = round_number.previous();

    // Ensure all includes have a round number smaller than the block round number
    for parent in block.parents() {
        if parent.round >= block.reference().round {
            return false;
        }
    }

    let mut aggregator = StakeAggregator::<QuorumThreshold>::new();
    let mut is_quorum = false;
    // Collect the validators with included blocks at round_number  - 1
    for parent in block.parents() {
        if parent.round == previous_round {
            is_quorum = aggregator.add(parent.author, committee);
        }
    }

    // Ensure the set of validators with includes has a quorum in the current committee
    is_quorum
}

pub struct ThresholdClockAggregator {
    aggregator: StakeAggregator<QuorumThreshold>,
    round: Round,
}

impl ThresholdClockAggregator {
    pub fn new(round: Round) -> Self {
        Self {
            aggregator: StakeAggregator::new(),
            round,
        }
    }

    pub fn add_block(&mut self, block: BlockReference, committee: &Committee) {
        match block.round.cmp(&self.round) {
            // Blocks with round less then what we currently build are irrelevant here
            Ordering::Less => {}
            // If we processed block for round r, we also have stored 2f+1 blocks from r-1
            Ordering::Greater => {
                self.aggregator.clear();
                self.aggregator.add(block.author, committee);
                self.round = block.round;
            }
            Ordering::Equal => {
                if self.aggregator.add(block.author, committee) {
                    self.aggregator.clear();
                    // We have seen 2f+1 blocks for current round, advance
                    self.round = block.round.next();
                }
            }
        }
    }

    pub fn get_round(&self) -> Round {
        self.round
    }

    pub fn validator_set(&self) -> &ValidatorSet {
        &self.aggregator.votes
    }
}

pub struct QuorumThreshold;
pub struct ValidityThreshold;

pub trait CommitteeThreshold {
    fn is_threshold(committee: &Committee, amount: Stake) -> bool;
}

impl CommitteeThreshold for QuorumThreshold {
    fn is_threshold(committee: &Committee, amount: Stake) -> bool {
        amount.0 > committee.f2_threshold.0
    }
}

impl CommitteeThreshold for ValidityThreshold {
    fn is_threshold(committee: &Committee, amount: Stake) -> bool {
        amount.0 > committee.f_threshold.0
    }
}

pub struct StakeAggregator<TH> {
    votes: ValidatorSet,
    stake: Stake,
    _phantom: PhantomData<TH>,
}

impl<TH: CommitteeThreshold> StakeAggregator<TH> {
    pub fn new() -> Self {
        Self {
            votes: Default::default(),
            stake: Stake(0),
            _phantom: Default::default(),
        }
    }

    pub fn add(&mut self, vote: ValidatorIndex, committee: &Committee) -> bool {
        let stake = committee.get_stake(vote);
        if self.votes.insert(vote) {
            self.stake += stake;
        }
        TH::is_threshold(committee, self.stake)
    }

    pub fn clear(&mut self) {
        self.votes.clear();
        self.stake = Stake::ZERO;
    }
}

type ValidatorSetElementType = u64;
const ELEMENT_BITS: usize = ValidatorSetElementType::BITS as usize;
const VALIDATOR_SET_ELEMENT_COUNT: usize = 8;
#[derive(Default)]
// todo tests
pub struct ValidatorSet([ValidatorSetElementType; VALIDATOR_SET_ELEMENT_COUNT]);

impl ValidatorSet {
    #[allow(dead_code)]
    pub const MAX_SIZE: usize = ELEMENT_BITS * VALIDATOR_SET_ELEMENT_COUNT;

    #[inline]
    pub fn insert(&mut self, index: ValidatorIndex) -> bool {
        let index = index.0 as usize;
        let byte_index = index / ELEMENT_BITS;
        let bit_index = index % ELEMENT_BITS;

        let bit = 1 << bit_index;
        if (self.0[byte_index] & bit) != 0 {
            false
        } else {
            self.0[byte_index] |= bit;
            true
        }
    }

    #[inline]
    #[allow(dead_code)]
    pub fn remove(&mut self, index: ValidatorIndex) -> bool {
        let index = index.0 as usize;
        let byte_index = index / ELEMENT_BITS;
        let bit_index = index % ELEMENT_BITS;

        let bit = 1 << bit_index;
        if (self.0[byte_index] & bit) != 0 {
            self.0[byte_index] ^= bit;
            true
        } else {
            false
        }
    }

    #[inline]
    #[allow(dead_code)]
    pub fn contains(&self, index: ValidatorIndex) -> bool {
        let index = index.0 as usize;
        let byte_index = index / ELEMENT_BITS;
        let bit_index = index % ELEMENT_BITS;

        let bit = 1 << bit_index;
        (self.0[byte_index] & bit) != 0
    }

    pub fn present(&self) -> impl Iterator<Item = ValidatorIndex> + '_ {
        self.0.iter().enumerate().flat_map(|(byte_index, byte)| {
            (0..ELEMENT_BITS)
                .filter(move |bit_index| (byte & (1 << bit_index)) != 0)
                .map(move |bit_index| {
                    ValidatorIndex((byte_index * ELEMENT_BITS + bit_index) as u64)
                })
        })
    }

    #[inline]
    pub fn clear(&mut self) {
        self.0.fill(0);
    }

    #[allow(dead_code)]
    pub fn all_set() -> Self {
        Self([u64::MAX; VALIDATOR_SET_ELEMENT_COUNT])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::tests::br;

    #[test]
    fn test_threshold_clock_aggregator() {
        let committee = Committee::new_test(vec![1, 1, 1, 1]);
        let mut aggregator = ThresholdClockAggregator::new(Round::ZERO);
        aggregator.add_block(br(0, 0), &committee);
        assert_eq!(aggregator.get_round().0, 0);
        aggregator.add_block(br(0, 1), &committee);
        assert_eq!(aggregator.get_round().0, 1);
        aggregator.add_block(br(1, 0), &committee);
        assert_eq!(aggregator.get_round().0, 1);
        aggregator.add_block(br(1, 1), &committee);
        assert_eq!(aggregator.get_round().0, 1);
        aggregator.add_block(br(2, 1), &committee);
        assert_eq!(aggregator.get_round().0, 2);
        aggregator.add_block(br(3, 1), &committee);
        assert_eq!(aggregator.get_round().0, 2);
    }
}
