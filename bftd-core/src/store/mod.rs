use crate::block::{Block, BlockReference};
use crate::block_manager::BlockStore;
use crate::consensus::Commit;
use std::collections::HashSet;
use std::ops::Deref;
use std::sync::Arc;

mod rocks_store;
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
    use crate::store::sled_store::SledStore;
    use std::sync::Arc;
    use tempdir::TempDir;

    #[test]
    fn commit_interpreter_test() {
        let dir = TempDir::new("commit_interpreter_test").unwrap();
        let store = SledStore::open(dir, Metrics::new_test()).unwrap();
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