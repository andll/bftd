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
        let committee = Committee::new_test(vec![1; 3]);
        let store = RocksStore::open(dir, &committee, Metrics::new_test()).unwrap();
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
    /// Inserts block into block store.
    /// If block store also implements BlockViewStore,
    /// it should also evaluate and store block view for a given block.
    fn put(&self, block: Arc<Block>);

    /// Inserts block into block store along with pre-computed block_view.
    fn put_with_block_view(&self, block: Arc<Block>, block_view: Vec<Option<BlockReference>>);
    /// Flushes block store. This is called after the proposal is generated.
    fn flush(&self);

    /// Informs store that **some** leader at round was committed; it can be used to optimize caching
    fn round_committed(&self, round: Round);
}

/// Store provides block view information
pub trait BlockViewStore {
    /// Returns block view for a given block.
    /// Block view should exist for every non-genesis block in the store.
    fn get_block_view(&self, r: &BlockReference) -> Vec<Option<BlockReference>>;
}

pub trait BlockReader {
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

    fn put_with_block_view(&self, block: Arc<Block>, block_view: Vec<Option<BlockReference>>) {
        self.deref().put_with_block_view(block, block_view)
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

impl<T: BlockViewStore> BlockViewStore for Arc<T> {
    fn get_block_view(&self, r: &BlockReference) -> Vec<Option<BlockReference>> {
        self.deref().get_block_view(r)
    }
}

/*
   Block relationship.

   * Block A is a **parent** of block B if block B lists reference(A) in the list B.parents
   * Block A is **referenced** by block B if block A is in the sub-tree of parents of block B.
   * Block A is **preceding** block B if A is a parent of B and author(A) = author(B).
        In other words, the preceding block is a parent block authored by the same validator.
        Every block (except for genesis blocks) has a single preceding block.
   * The mainline of block A is set of blocks formed as transitive closure of the preceding operator.
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

    * BlockView(A) where A is a block and V is a given validator defined as the following:
      * For each genesis block G and validator V, BlockView(G)[V] := GenesisBlock(V)
      * For each non-genesis block A,
        def BlockView(A) :=
          block_view = BlockView(A.preceding)
          for parent in A.parents:
             block_view = merge_block_view(block_view, BlockView(parent))
             block_view[parent.author] = merge_view_element(block_view[parent.author], parent)
          return block_view
    * merge_block_view operator is defined as the following:
      merge_block_view(A, B) = {for each validator V, V => merge_view_element(block_view(A)[V], block_view(B)[V]}
      def merge_view_element(LeftView, RightView):
         if LeftView is None || RightView is None:
            return None
         if !mainline_connected(LeftView, RightView):
            return None
         if LeftView.round > RightView.round:
            return LeftView
         else
            return RightView
    * Because BlockView(A) is only a function of A and its sub-dag, all correct validators will evaluate same BlockView(A) for any block A.
    * BlockView(A) can be cached when A is inserted into block store, to avoid expensive computations.
    * Lemma. For any given block A, BlockView(A)[V] is None if and only if sub-dag of A contains two blocks produced by V that are not connected to each other via mainline.
      If sub-dag of A only contains blocks produced by V that mainline connects, merge_view_element will never return None, and therefore BlockView(A)[V] will not be None.
      Let's say sub-dag of A contains two blocks B and B` that are both produced by V, but are not part of same mainline.
      Because sub-dag of A contains B and B`, it also contains some blocks X with parents T and T`,
      such as sub-dag of T only contains B and sub-dag of T` only contains B`.
      BlockView(T)[V] will point to some block C, produced by validator V, and
      BlockView(T`)[V] will point to some block C`, also produced by V.
      Block B will be on mainline of C, and B` on mainline of C`.
      Because X references both T and T` as parents, merging BlockView(T) and BlockView(T`) on validator V will produce None, and BlockView(X)[V] will be set to None.
      Because BlockView(X)[V] is None, all blocks that contain X in the sub-dag will have BlockView[V] set to None.
      Because sub-dag of A contains X, BlockView(A)[V] is also None.
    * In other words, BlockView for block A maps validator V to its last produced block (as seen by A's sub-dag) if V did not equivocate in A's sub-dag, and None otherwise.
    * Lemma - for any block A and any correct validator V, BlockView(A)[V] is not None:
      Because any two blocks produced by a correct validator are connected via mainline, BlockView(A)[V] is not None.

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
            if left_ref.round.0 - right_ref.round.0 > 20 {
                tracing::warn!("left({left_ref}) far from right({right_ref})");
            }
            // Left is higher round then right
            if !self.is_connected(*left_ref, right_ref) {
                // Left does not contain right in a tree
                tracing::warn!("left({left_ref}) is not connected to right({right_ref})");
                *left = None;
            } // else: Left is higher round and contains right (no need to change left)
        } else
        /* right_ref.round > left_ref.round  */
        {
            if right_ref.round.0 - left_ref.round.0 > 20 {
                tracing::warn!("right({right_ref}) far from left({left_ref})");
            }
            // Right is higher round then left
            if self.is_connected(*right_ref, left_ref) {
                // Right is higher round and contains left, update left to right
                *left = Some(*right_ref);
            } else {
                tracing::warn!("right({right_ref}) is not connected to left({left_ref})");
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
        self.critical_block_for_round(block.round(), *preceding)
    }

    /// Evaluate what would be a critical block for a block
    /// (which might not yet exist) with a specified round and preceding block.
    fn critical_block_for_round(
        &self,
        round: Round,
        mut preceding: BlockReference,
    ) -> Option<BlockReference> {
        assert!(round >= preceding.round());
        const CRITICAL_BLOCK_GAP: u64 = 2;
        while round.0 - preceding.round().0 < CRITICAL_BLOCK_GAP {
            preceding = *self
                .get(&preceding)
                .expect("Preceding block not found")
                .preceding()?;
        }
        Some(preceding)
    }

    /// Evaluates total stake that links from given block to its critical block.
    /// Returns None if block does not have a critical block (block is among very early blocks in epoch).
    ///
    /// Provided block does not need to be inserted in block store,
    /// but all its parents should already be in the block store.
    fn critical_block_support(
        &self,
        block: &Block,
        committee: &Committee,
    ) -> Option<(BlockReference, Stake)> {
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
        Some((critical_block, stake))
    }

    fn print_dag(&self, block: &Block, limit: usize) -> String {
        let mut seen = HashSet::with_capacity(128);
        let mut out = String::with_capacity(4 * 1024);
        self.print_dag_inner(block, limit, &mut seen, &mut out);
        out
    }

    fn print_dag_inner(
        &self,
        block: &Block,
        limit: usize,
        seen: &mut HashSet<BlockReference>,
        out: &mut String,
    ) {
        use std::fmt::Write;
        writeln!(out, "{block}").unwrap();
        if limit == 0 {
            return;
        }
        for parent in block.parents() {
            if seen.insert(*parent) {
                let parent = self.get(parent).expect("Parent block not found");
                self.print_dag_inner(&parent, limit - 1, seen, out);
            }
        }
    }

    fn fill_block_view(
        &self,
        block: &Block,
        genesis_view: &Vec<Option<BlockReference>>,
    ) -> Vec<Option<BlockReference>> {
        let span = tracing::span!(
            tracing::Level::WARN,
            "fill_block_view",
            "{}",
            block.reference()
        );
        let _enter = span.enter();
        let mut block_view: Option<Vec<Option<BlockReference>>> = None;
        for parent in block.parents() {
            if parent.is_genesis() {
                if block_view.is_none() {
                    block_view = Some(genesis_view.clone());
                }
                continue;
            }
            let parent_block_view = self.get_block_view(parent);
            if let Some(block_view) = &mut block_view {
                let span = tracing::span!(
                    tracing::Level::WARN,
                    "parent_block_view",
                    "{parent}: {parent_block_view:?}"
                );
                let _enter = span.enter();
                self.merge_block_view_into(block_view, &parent_block_view);
            } else {
                block_view = Some(parent_block_view);
            }
            self.merge_block_ref_into(block_view.as_mut().unwrap(), parent);
        }
        // todo unwrap_or_else needed because we insert genesis blocks via block_store.put and
        // execute this code path for them (which we should not)
        block_view.unwrap_or_else(|| genesis_view.clone())
    }
}

impl<T: BlockReader + BlockViewStore> DagExt for T {}
