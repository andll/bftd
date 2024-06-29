use crate::block::{Block, BlockHash, BlockReference, Round, ValidatorIndex};
use crate::consensus::Commit;
use std::collections::{BTreeMap, HashSet};
use std::ops::Deref;
use std::sync::Arc;

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

impl BlockStore for MemoryBlockStore {
    fn put(&self, block: Arc<Block>) {
        self.write()
            .entry(block.round())
            .or_default()
            .insert((block.author(), *block.block_hash()), block);
    }

    fn flush(&self) {}
    fn round_committed(&self, _round: Round) {}
}
impl BlockReader for MemoryBlockStore {
    fn get(&self, key: &BlockReference) -> Option<Arc<Block>> {
        self.read()
            .get(&key.round)?
            .get(&(key.author, key.hash))
            .cloned()
    }

    fn get_own(&self, validator: ValidatorIndex, round: Round) -> Option<Arc<Block>> {
        Some(
            self.read()
                .get(&round)?
                .range((validator, BlockHash::MIN)..(validator, BlockHash::MAX))
                .next()?
                .1
                .clone(),
        )
    }

    fn last_known_round(&self, validator: ValidatorIndex) -> Round {
        self.last_known_block(validator).round()
    }

    fn last_known_block(&self, validator: ValidatorIndex) -> Arc<Block> {
        // todo performance
        let lock = self.read();
        for (_round, map) in lock.iter().rev() {
            if let Some((_, block)) = map
                .range((validator, BlockHash::MIN)..(validator, BlockHash::MAX))
                .next()
            {
                return block.clone();
            }
        }
        panic!("Should have at least one block for each validator");
    }

    fn exists(&self, key: &BlockReference) -> bool {
        let lock = self.read();
        let m = lock.get(&key.round);
        match m {
            Some(map) => map.contains_key(&(key.author, key.hash)),
            None => false,
        }
    }

    fn get_blocks_by_round(&self, round: Round) -> Vec<Arc<Block>> {
        let lock = self.read();
        if let Some(map) = lock.get(&round) {
            // todo - is cloning a good idea here?
            map.values().cloned().collect()
        } else {
            vec![]
        }
    }

    fn get_blocks_at_author_round(&self, author: ValidatorIndex, round: Round) -> Vec<Arc<Block>> {
        let lock = self.read();
        if let Some(map) = lock.get(&round) {
            // todo - is cloning a good idea here?
            map.range((author, BlockHash::MIN)..(author, BlockHash::MAX))
                .map(|(_, b)| b.clone())
                .collect()
        } else {
            vec![]
        }
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

pub type MemoryBlockStore =
    parking_lot::RwLock<BTreeMap<Round, BTreeMap<(ValidatorIndex, BlockHash), Arc<Block>>>>;
