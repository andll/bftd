use crate::block::{Block, BlockReference, Round, ValidatorIndex};
use crate::block_manager::BlockStore;
use crate::consensus::Commit;
use crate::metrics::Metrics;
use sled::{IVec, Tree};
use std::collections::HashSet;
use std::io;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

pub struct SledStore {
    blocks: Tree,
    index: Tree,
    commits: Tree,
    block_commits: Tree,
    metrics: Arc<Metrics>,
}

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

impl SledStore {
    pub fn open(path: impl AsRef<Path>, metrics: Arc<Metrics>) -> io::Result<Self> {
        let db = sled::Config::new().path(path).open()?;
        let blocks = db.open_tree("blocks")?;
        let index = db.open_tree("index")?;
        let commits = db.open_tree("commits")?;
        let block_commits = db.open_tree("block_commits")?;
        Ok(Self {
            blocks,
            index,
            commits,
            block_commits,
            metrics,
        })
    }

    fn decode(&self, v: IVec) -> Arc<Block> {
        Arc::new(
            Block::from_bytes_unchecked(v.to_vec().into(), Some(self.metrics.clone()))
                .expect("Failed to load block from store"),
        )
    }
}

impl BlockStore for SledStore {
    fn put(&self, block: Arc<Block>) {
        let index_key = block.reference().author_round_hash_encoding();
        let block_key = block.reference().round_author_hash_encoding();
        self.index
            .insert(&index_key, block_key.to_vec())
            .expect("Storage operation failed");
        // todo Bytes <-> IVec avoid data copy
        self.blocks
            .insert(block_key, block.data().to_vec())
            .expect("Storage operation failed");
    }

    fn flush(&self) {
        // trees share pagecache so single flush should be enough for blocks and index
        self.blocks.flush().expect("Flush failed");
    }

    fn get(&self, key: &BlockReference) -> Option<Arc<Block>> {
        let block_key = key.round_author_hash_encoding();
        let block = self
            .blocks
            .get(&block_key)
            .expect("Storage operation failed")?;
        Some(self.decode(block))
    }

    fn get_own(&self, validator: ValidatorIndex, round: Round) -> Option<Arc<Block>> {
        let from = BlockReference::first_block_reference_for_round_author(round, validator)
            .round_author_hash_encoding();
        let to = BlockReference::last_block_reference_for_round_author(round, validator)
            .round_author_hash_encoding();

        let (_, block) = self
            .blocks
            .range(from..to)
            .next()?
            .expect("Storage operation failed");
        Some(self.decode(block))
    }

    fn last_known_round(&self, validator: ValidatorIndex) -> Round {
        self.last_known_block(validator).round()
    }

    fn last_known_block(&self, validator: ValidatorIndex) -> Arc<Block> {
        let from = BlockReference::first_block_reference_for_round_author(Round::ZERO, validator)
            .author_round_hash_encoding();
        let to = BlockReference::first_block_reference_for_round_author(Round::MAX, validator)
            .author_round_hash_encoding();

        for key in self.index.range(from..to).rev() {
            let key = key.expect("Storage operation failed").1;
            let block = self.blocks.get(&key).expect("Storage operation failed");
            // index can be dirty, skip to next entry in that case
            let Some(block) = block else {
                continue;
            };
            return self.decode(block);
        }
        panic!("No blocks found for validator in the storage(should have at least genesis block)");
    }

    fn exists(&self, key: &BlockReference) -> bool {
        self.blocks
            .contains_key(&key.round_author_hash_encoding())
            .expect("Storage operation failed")
    }

    fn get_blocks_by_round(&self, round: Round) -> Vec<Arc<Block>> {
        let from =
            BlockReference::first_block_reference_for_round(round).round_author_hash_encoding();
        let to = BlockReference::first_block_reference_for_round(round.next())
            .round_author_hash_encoding();
        self.blocks
            .range(from..to)
            .map(|data| self.decode(data.expect("Storage operation failed").1))
            .collect()
    }

    fn get_blocks_at_author_round(&self, author: ValidatorIndex, round: Round) -> Vec<Arc<Block>> {
        let from = BlockReference::first_block_reference_for_round_author(round, author)
            .round_author_hash_encoding();
        let to = BlockReference::last_block_reference_for_round_author(round, author)
            .round_author_hash_encoding();
        self.blocks
            .range(from..to)
            .filter_map(|data| {
                let block = data.expect("Storage operation failed").1;
                Some(self.decode(block))
            })
            .collect()
    }

    fn linked_to_round(&self, block: &Arc<Block>, round: Round) -> Vec<Arc<Block>> {
        // todo tests
        // todo code dedup with MemoryStore
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

impl CommitStore for SledStore {
    fn store_commit(&self, commit: &Commit) {
        let key = commit.index().to_be_bytes();
        let commit = bincode::serialize(commit).expect("Serialization failed");
        self.commits
            .insert(&key, commit)
            .expect("Storage operation failed");
    }

    fn get_commit(&self, index: u64) -> Option<Commit> {
        let key = index.to_be_bytes();
        let commit = self.commits.get(&key).expect("Storage operation failed")?;
        let commit = bincode::deserialize(commit.as_ref())
            .expect("Deserializing commit from storage failed");
        Some(commit)
    }

    fn last_commit(&self) -> Option<Commit> {
        let (_, commit) = self.commits.last().expect("Storage operation failed")?;
        let commit = bincode::deserialize(commit.as_ref())
            .expect("Deserializing commit from storage failed");
        Some(commit)
    }

    fn set_block_commit(&self, r: &BlockReference, index: u64) {
        let prev = self
            .block_commits
            .insert(r.round_author_hash_encoding(), &index.to_be_bytes())
            .expect("Storage operation failed");
        if let Some(prev) = prev {
            let prev = u64::from_be_bytes(prev.as_ref().try_into().unwrap());
            if prev != index {
                panic!("Overwriting commit index for {r} from {prev} to {index}");
            }
        }
    }

    fn get_block_commit(&self, r: &BlockReference) -> Option<u64> {
        let v = self
            .block_commits
            .get(r.round_author_hash_encoding())
            .expect("Storage operation failed");
        v.map(|v| u64::from_be_bytes(v.as_ref().try_into().unwrap()))
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
    use tempdir::TempDir;

    #[test]
    fn sled_store_test() {
        let dir = TempDir::new("sled_store_test").unwrap();
        let store = SledStore::open(dir, Metrics::new_test()).unwrap();
        for author in 0..3 {
            for round in 0..(author + 2) {
                store.put(blk(author, round, vec![]));
            }
        }
        // todo - do not use same hash for all blk/br blocks
        assert_eq!(
            store.get(&br(0, 1)).unwrap().data(),
            blk(0, 1, vec![]).data()
        );
        assert!(store.exists(&br(0, 1)));
        assert_eq!(
            store.get(&br(2, 3)).unwrap().data(),
            blk(2, 3, vec![]).data()
        );
        assert!(store.exists(&br(2, 3)));

        assert_eq!(
            store.get_own(ValidatorIndex(2), Round(1)).unwrap().data(),
            blk(2, 1, vec![]).data()
        );

        assert_eq!(store.last_known_round(ValidatorIndex(2)), Round(3));
        assert_eq!(store.last_known_round(ValidatorIndex(1)), Round(2));

        let r = store.get_blocks_by_round(Round(2));
        assert_eq!(r.len(), 2);
        let mut r = r.into_iter();
        assert_eq!(r.next().unwrap().reference(), &br(1, 2));
        assert_eq!(r.next().unwrap().reference(), &br(2, 2));
        let r = store.get_blocks_by_round(Round(1));
        assert_eq!(r.len(), 3);
        let mut r = r.into_iter();
        assert_eq!(r.next().unwrap().reference(), &br(0, 1));
        assert_eq!(r.next().unwrap().reference(), &br(1, 1));
        assert_eq!(r.next().unwrap().reference(), &br(2, 1));

        let r = store.get_blocks_at_author_round(ValidatorIndex(1), Round(2));
        assert_eq!(r.len(), 1);
        assert_eq!(r.into_iter().next().unwrap().reference(), &br(1, 2));

        let r = store.get_blocks_at_author_round(ValidatorIndex(1), Round(5));
        assert_eq!(r.len(), 0);
    }

    #[test]
    fn sled_commit_store_test() {
        let dir = TempDir::new("sled_commit_store_test").unwrap();
        let store = SledStore::open(dir, Metrics::new_test()).unwrap();
        store.store_commit(&c(0, br(1, 1)));
        let last = c(1, br(2, 1));
        store.store_commit(&last);
        assert_eq!(store.last_commit(), Some(last));

        store.set_block_commit(&br(2, 3), 5);
        assert_eq!(store.get_block_commit(&br(2, 3)), Some(5));
        assert_eq!(store.get_block_commit(&br(3, 3)), None);
    }

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

    fn c(i: u64, r: BlockReference) -> Commit {
        Commit::new_test(i, r, vec![r])
    }

    fn to_refs(v: Vec<Arc<Block>>) -> Vec<BlockReference> {
        v.into_iter().map(|b| *b.reference()).collect()
    }
}
