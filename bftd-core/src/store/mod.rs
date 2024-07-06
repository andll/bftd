use crate::block::{Block, BlockReference, Round, ValidatorIndex};
use crate::committee::{Committee, Stake};
use crate::consensus::Commit;
use std::collections::HashSet;
use std::ops::Deref;
use std::sync::Arc;

pub mod memory_store;
pub mod rocks_store;
#[cfg(feature = "sled_store")]
pub mod sled_store;

pub trait CommitStore: Send + Sync + 'static {
    /// Store commit
    fn store_commit(&self, commit: &Commit);

    /// Load commit by index
    /// This is not used directly by core but useful for other services
    fn get_commit(&self, index: u64) -> Option<Commit>;

    /// Load commit with the highest index
    fn last_commit(&self) -> Option<Commit>;

    /// Associate block with commit index
    fn set_block_commit(&self, r: &BlockReference, index: u64);
    /// Get commit index associated with the block
    fn get_block_commit(&self, r: &BlockReference) -> Option<u64>;
}

pub struct CommitInterpreter<'a, S> {
    store: &'a S,
}

impl<'a, S: CommitStore + BlockStore> CommitInterpreter<'a, S> {
    pub fn new(store: &'a S) -> Self {
        Self { store }
    }

    pub fn interpret_commit(&self, index: u64, leader: Arc<Block>) -> Vec<Arc<Block>> {
        let mut blocks = Vec::new();
        self.store.set_block_commit(leader.reference(), index);
        let mut to_inspect = Vec::new();
        blocks.push(leader.clone());
        to_inspect.push(leader);
        // todo - keep this between interpreting?
        let mut seen_parents = HashSet::new();
        while let Some(block) = to_inspect.pop() {
            for parent in block.parents() {
                if !seen_parents.insert(*parent) {
                    continue;
                }
                if let Some(associated_index) = self.store.get_block_commit(parent) {
                    if associated_index < index {
                        // skip previously committed block
                        continue;
                    } else if associated_index == index {
                        // use this block
                    } else {
                        panic!("Inspecting commit with associated index {associated_index} while building commit {index}");
                    }
                }
                self.store.set_block_commit(parent, index);
                let parent = self.store.get(parent).expect("Parent block not found");
                blocks.push(parent.clone());
                to_inspect.push(parent);
            }
        }
        // todo - sort by round?
        blocks.reverse();
        blocks
    }
}

impl<T: CommitStore> CommitStore for Arc<T> {
    fn store_commit(&self, commit: &Commit) {
        self.deref().store_commit(commit)
    }

    fn get_commit(&self, index: u64) -> Option<Commit> {
        self.deref().get_commit(index)
    }

    fn last_commit(&self) -> Option<Commit> {
        self.deref().last_commit()
    }

    fn set_block_commit(&self, r: &BlockReference, index: u64) {
        self.deref().set_block_commit(r, index)
    }

    fn get_block_commit(&self, r: &BlockReference) -> Option<u64> {
        self.deref().get_block_commit(r)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::tests::{blk, br};
    use crate::block::{Block, BlockReference};
    use crate::metrics::Metrics;
    use crate::store::rocks_store::RocksStore;
    use std::sync::Arc;
    use tempdir::TempDir;

    #[test]
    fn commit_interpreter_test() {
        let dir = TempDir::new("commit_interpreter_test").unwrap();
        let store = RocksStore::open(dir, Metrics::new_test()).unwrap();
        store.put(blk(0, 0, vec![]));
        let b0 = blk(1, 0, vec![]);
        store.put(b0.clone());
        store.put(blk(2, 0, vec![]));

        let b1 = blk(1, 1, vec![br(1, 0), br(0, 0)]);
        store.put(b1.clone());
        store.put(blk(2, 1, vec![br(2, 0), br(0, 0)]));
        let c2 = blk(3, 2, vec![br(1, 0), br(2, 1)]);
        store.put(c2.clone());

        let interpreter = CommitInterpreter::new(&store);
        let c = interpreter.interpret_commit(0, b0);
        assert_eq!(to_refs(c), vec![br(1, 0)]);

        let c = interpreter.interpret_commit(1, b1);
        assert_eq!(to_refs(c), vec![br(0, 0), br(1, 1)]);

        let c = interpreter.interpret_commit(1, c2);
        assert_eq!(to_refs(c), vec![br(0, 0), br(2, 0), br(2, 1), br(3, 2)]);
    }

    fn to_refs(v: Vec<Arc<Block>>) -> Vec<BlockReference> {
        v.into_iter().map(|b| *b.reference()).collect()
    }
}

pub trait BlockStore: BlockReader + Send + Sync + 'static {
    fn put(&self, block: Arc<Block>);
    fn flush(&self);

    /// Informs store that **some** leader at round was committed; it can be used to optimize caching
    fn round_committed(&self, round: Round);
}

pub trait BlockViewStore {
    fn get_block_view(&self, r: &BlockReference) -> Vec<Option<BlockReference>>;
}

pub trait BlockReader: Sync + 'static {
    fn get(&self, key: &BlockReference) -> Option<Arc<Block>>;
    fn get_multi<'a>(
        &self,
        keys: impl IntoIterator<
            Item = &'a BlockReference,
            IntoIter = impl Iterator<Item = &'a BlockReference>,
        >,
    ) -> Vec<Option<Arc<Block>>> {
        keys.into_iter().map(|k| self.get(k)).collect()
    }
    fn get_own(&self, validator: ValidatorIndex, round: Round) -> Option<Arc<Block>>;
    fn last_known_round(&self, validator: ValidatorIndex) -> Round;
    fn last_known_block(&self, validator: ValidatorIndex) -> Arc<Block>;
    fn exists(&self, key: &BlockReference) -> bool;

    fn get_blocks_by_round(&self, round: Round) -> Vec<Arc<Block>>;
    fn get_blocks_at_author_round(&self, author: ValidatorIndex, round: Round) -> Vec<Arc<Block>>;

    // Returns all the ancestors of the later_block to the `earlier_round`, assuming that there is
    // a path to them.
    fn linked_to_round(&self, block: &Arc<Block>, round: Round) -> Vec<Arc<Block>> {
        // todo tests
        // todo optimize / index
        let mut parents = vec![block.clone()];
        for r in (round.0..block.round().0).rev() {
            let blocks_for_round = self.get_blocks_by_round(Round(r));
            parents = blocks_for_round
                .iter()
                .filter_map(|block| {
                    if parents
                        .iter()
                        .any(|x| x.parents().contains(block.reference()))
                    {
                        Some(block.clone())
                    } else {
                        None
                    }
                })
                .collect();
            if parents.is_empty() {
                break;
            }
        }
        parents
    }
}

impl<T: BlockStore> BlockStore for Arc<T> {
    fn put(&self, block: Arc<Block>) {
        self.deref().put(block)
    }

    fn flush(&self) {
        self.deref().flush()
    }

    fn round_committed(&self, round: Round) {
        self.deref().round_committed(round)
    }
}

impl<T: BlockReader + Send> BlockReader for Arc<T> {
    fn get(&self, key: &BlockReference) -> Option<Arc<Block>> {
        self.deref().get(key)
    }

    fn get_own(&self, validator: ValidatorIndex, round: Round) -> Option<Arc<Block>> {
        self.deref().get_own(validator, round)
    }

    fn last_known_round(&self, validator: ValidatorIndex) -> Round {
        self.deref().last_known_round(validator)
    }

    fn last_known_block(&self, validator: ValidatorIndex) -> Arc<Block> {
        self.deref().last_known_block(validator)
    }

    fn exists(&self, key: &BlockReference) -> bool {
        self.deref().exists(key)
    }

    fn get_blocks_by_round(&self, round: Round) -> Vec<Arc<Block>> {
        self.deref().get_blocks_by_round(round)
    }

    fn get_blocks_at_author_round(&self, author: ValidatorIndex, round: Round) -> Vec<Arc<Block>> {
        self.deref().get_blocks_at_author_round(author, round)
    }

    fn linked_to_round(&self, block: &Arc<Block>, round: Round) -> Vec<Arc<Block>> {
        self.deref().linked_to_round(block, round)
    }
}

/*
   Block relationship.

   * Block A is a **parent** of block B if block B lists reference(A) in the list B.parents
   * Block A is **referenced** by block B if block A is in the sub-tree of parents of block B.
   * Block A is **preceding** block B if A is a parent of B and author(A) = author(B).
        In other words, the preceding block is a parent block authored by the same validator.
        Every block (except for genesis blocks) has a single preceding block.
   * The critical block for block B is defined as following:
      * If block A is the preceding block for B and A.round < B.round - 1, then A is critical block for B
      * If block A is a preceding block for B and A.round = B.round - 1,
         then block preceding to A is a critical block for B.
   * If block B has a critical block A, then B is only valid if minimum of f+1 of B's parents reference block A.
     In a way, this critical block rule is essentially a "mirror" rule to a vector clock rule of mysticeti protocol.
     Vector clock rule states that a validator should hear from 2f+1 of other nodes before it can produce a next block.
     The critical block rule states that a validator should broadcast its blocks and get them included by other validators, before it can create more blocks.
     Vector clock rule prevents validator from generating blocks if they can't receive blocks from other validators.
     Critical block rule prevents validator from generating blocks if other validators have not received earlier blocks from this validator.

     * When inserting block A into local store, we determine whether to persist a taint flag along with block A.
       * If block preceding to A has a taint flag set, then block A also has a taint flag set
       * If block preceding to A is not equal to the last received block from author(A) then block A also has taint flag set.
     Unlike the other properties above, a taint flag is a **local** property of a block - correct validators do not always come to the same conclusion on whether some block is tainted or not.
     Lemma - blocks produced by correct validators will never be perceived as tainted by other correct validators.


*/
pub trait DagExt: BlockReader + BlockViewStore {
    fn merge_block_ref_into(
        &self,
        left: &mut Vec<Option<BlockReference>>,
        reference: &BlockReference,
    ) {
        let left = reference.author.slice_get_mut(left);
        self.merge_one_block_ref(left, &Some(*reference))
    }

    fn merge_block_view_into(
        &self,
        left: &mut Vec<Option<BlockReference>>,
        right: &Vec<Option<BlockReference>>,
    ) {
        for (left, right) in left.iter_mut().zip(right.iter()) {
            self.merge_one_block_ref(left, right)
        }
    }

    fn merge_one_block_ref(
        &self,
        left: &mut Option<BlockReference>,
        right: &Option<BlockReference>,
    ) {
        let Some(left_ref) = left else {
            // Left is already 'None', nothing to be done
            return;
        };
        let Some(right_ref) = right else {
            // Right is 'None', left needs to be set to 'None'
            *left = None;
            return;
        };
        if left_ref.round == right_ref.round {
            if left_ref != right_ref {
                // Round is the same, references are different, setting left to None
                *left = None;
            } // else: Round is the same, references are the same (no need to change left)
        } else if left_ref.round > right_ref.round {
            // Left is higher round then right
            if !self.is_connected(*left_ref, right_ref) {
                // Left does not contain right in a tree
                println!("left({left_ref}) is not connected to right({right_ref})");
                *left = None;
            } // else: Left is higher round and contains right (no need to change left)
        } else
        /* right_ref.round > left_ref.round  */
        {
            // Right is higher round then left
            if self.is_connected(*right_ref, left_ref) {
                // Right is higher round and contains left, update left to right
                *left = Some(*right_ref);
            } else {
                println!("right({right_ref}) is not connected to left({left_ref})");
                // Left and right are conflicting, set left to None
                *left = None;
            }
        }
    }

    /// Checks if parent block is connected to child via the link of the preceding blocks.
    /// This does not check if blocks are connected via links other than a preceding link.
    fn is_connected(&self, mut block: BlockReference, root: &BlockReference) -> bool {
        loop {
            if block == *root {
                return true;
            }
            if block.is_genesis() {
                return false;
            }
            block = *self
                .get(&block)
                .expect("Parent block must be stored locally")
                .preceding()
                .expect("Non-genesis block must have preceding block");
        }
    }

    /// Evaluates critical block for the given block
    fn critical_block(&self, block: &Block) -> Option<BlockReference> {
        let preceding = block.preceding()?;
        self.critical_block_for_round(block.round(), preceding)
    }

    /// Evaluate what would be a critical block for a block
    /// (which might not yet exist) with a specified round and preceding block.
    fn critical_block_for_round(
        &self,
        round: Round,
        preceding: &BlockReference,
    ) -> Option<BlockReference> {
        if preceding.round() == round.previous() {
            let preceding = self.get(preceding).expect("Parent block not found");
            Some(*preceding.preceding()?)
        } else {
            Some(*preceding)
        }
    }

    /// Evaluates total stoke that links from given block to its critical block.
    /// Returns None if block does not have a critical block (block is among very early blocks in epoch).
    ///
    /// Provided block does not need to be inserted in block store,
    /// but all its parents should already be in the block store.
    fn critical_block_links(&self, block: &Block, committee: &Committee) -> Option<Stake> {
        let critical_block = self.critical_block(block)?;
        let author = block.author();
        let mut stake = Stake::ZERO;
        for parent in block.parents() {
            if parent.is_genesis() {
                continue;
            }
            let parent_view = self.get_block_view(parent);
            if let Some(view) = author.slice_get(&parent_view) {
                // todo - also check critical block is a mainline block?
                if view.round() >= critical_block.round() {
                    stake += committee.get_stake(parent.author);
                }
            }
        }
        Some(stake)
    }

    fn fill_block_view(&self, block: &Block, block_view: &mut Vec<Option<BlockReference>>) {
        for parent in block.parents() {
            if parent.is_genesis() {
                continue;
            }
            let parent_block_view = self.get_block_view(parent);
            self.merge_block_view_into(block_view, &parent_block_view);
            self.merge_block_ref_into(block_view, parent);
        }
    }
}

impl<T: BlockReader + BlockViewStore> DagExt for T {}
