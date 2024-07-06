use crate::block::{Block, BlockReference, Round, ValidatorIndex};
use crate::committee::Committee;
use crate::consensus::Commit;
use crate::metrics::Metrics;
use crate::store::{BlockReader, BlockStore};
use crate::store::{BlockViewStore, CommitStore, DagExt};
use bytes::Bytes;
use rocksdb::{ColumnFamily, Direction, IteratorMode, Options, DB};
use std::path::Path;
use std::sync::Arc;

pub struct RocksStore {
    db: Arc<DB>,
    genesis_view: Vec<Option<BlockReference>>,
    metrics: Arc<Metrics>,
}

impl RocksStore {
    pub fn from_db(
        db: Arc<DB>,
        committee: &Committee,
        metrics: Arc<Metrics>,
    ) -> Result<Self, rocksdb::Error> {
        let genesis_view = committee.genesis_view();
        Ok(Self {
            db,
            genesis_view,
            metrics,
        })
    }

    pub fn open(
        path: impl AsRef<Path>,
        committee: &Committee,
        metrics: Arc<Metrics>,
    ) -> Result<Self, rocksdb::Error> {
        let db = DB::open_cf(&Self::options(), path, Self::cfs())?;
        let db = Arc::new(db);
        Self::from_db(db, committee, metrics)
    }

    pub fn cfs() -> &'static [&'static str] {
        &["blocks", "index", "commits", "block_commits", "block_view"]
    }

    pub fn options() -> Options {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options
    }

    fn decode<T: Into<Bytes>>(&self, v: T) -> Arc<Block> {
        Arc::new(
            Block::from_bytes_unchecked(v.into(), Some(self.metrics.clone()))
                .expect("Failed to load block from store"),
        )
    }

    fn blocks(&self) -> &ColumnFamily {
        self.db.cf_handle("blocks").unwrap()
    }

    fn index(&self) -> &ColumnFamily {
        self.db.cf_handle("index").unwrap()
    }

    fn commits(&self) -> &ColumnFamily {
        self.db.cf_handle("commits").unwrap()
    }

    fn block_commits(&self) -> &ColumnFamily {
        self.db.cf_handle("block_commits").unwrap()
    }

    fn block_view(&self) -> &ColumnFamily {
        self.db.cf_handle("block_view").unwrap()
    }
}

impl BlockStore for RocksStore {
    fn put(&self, block: Arc<Block>) {
        let block_view = self.fill_block_view(&block, &self.genesis_view);
        self.put_with_block_view(block, block_view)
    }

    fn put_with_block_view(&self, block: Arc<Block>, block_view: Vec<Option<BlockReference>>) {
        let index_key = block.reference().author_round_hash_encoding();
        let block_key = block.reference().round_author_hash_encoding();
        let block_view = bincode::serialize(&block_view).expect("Failed to serialize block view");
        self.db
            .put_cf(self.index(), &index_key, &block_key)
            .expect("Storage operation failed");
        self.db
            .put_cf(self.block_view(), &block_key, &block_view)
            .expect("Storage operation failed");
        self.db
            .put_cf(self.blocks(), &block_key, block.data())
            .expect("Storage operation failed");
    }

    fn flush(&self) {
        // self.db.flush().expect("Flush failed");
    }

    fn round_committed(&self, _round: Round) {}
}
impl BlockReader for RocksStore {
    fn get(&self, key: &BlockReference) -> Option<Arc<Block>> {
        let block_key = key.round_author_hash_encoding();
        let block = self
            .db
            .get_cf(self.blocks(), &block_key)
            .expect("Storage operation failed")?;
        Some(self.decode(block))
    }

    fn get_own(&self, validator: ValidatorIndex, round: Round) -> Option<Arc<Block>> {
        let from =
            BlockReference::first_for_round_author(round, validator).round_author_hash_encoding();
        let to =
            BlockReference::last_for_round_author(round, validator).round_author_hash_encoding();

        let iter = self
            .db
            .iterator_cf(self.blocks(), IteratorMode::From(&from, Direction::Forward));
        let mut iter = bound_iter(iter, &to);
        let block = iter.next()?;
        Some(self.decode(block))
    }

    fn last_known_round(&self, validator: ValidatorIndex) -> Round {
        self.last_known_block(validator).round()
    }

    fn last_known_block(&self, validator: ValidatorIndex) -> Arc<Block> {
        let from = BlockReference::first_for_round_author(Round::ZERO, validator)
            .author_round_hash_encoding();
        let to = BlockReference::first_for_round_author(Round::MAX, validator)
            .author_round_hash_encoding();

        let iter = self
            .db
            .iterator_cf(self.index(), IteratorMode::From(&to, Direction::Reverse));
        let iter = bound_iter_upper(iter, &from);
        for block_ref in iter {
            let key = BlockReference::from_bytes(
                block_ref.as_ref().try_into().expect("Invalid index value"),
            );
            // index can be dirty, skip to next entry in that case
            let Some(block) = self.get(&key) else {
                continue;
            };
            return block;
        }
        panic!("No blocks found for validator in the storage(should have at least genesis block)");
    }

    fn exists(&self, key: &BlockReference) -> bool {
        // todo skip deser
        self.get(key).is_some()
    }

    fn get_blocks_by_round(&self, round: Round) -> Vec<Arc<Block>> {
        let from = BlockReference::first_for_round(round).round_author_hash_encoding();
        let to = BlockReference::first_for_round(round.next()).round_author_hash_encoding();
        let iter = self
            .db
            .iterator_cf(self.blocks(), IteratorMode::From(&from, Direction::Forward));
        let iter = bound_iter(iter, &to);

        iter.map(|data| self.decode(data)).collect()
    }

    fn get_blocks_at_author_round(&self, author: ValidatorIndex, round: Round) -> Vec<Arc<Block>> {
        let from =
            BlockReference::first_for_round_author(round, author).round_author_hash_encoding();
        let to = BlockReference::last_for_round_author(round, author).round_author_hash_encoding();
        let iter = self
            .db
            .iterator_cf(self.blocks(), IteratorMode::From(&from, Direction::Forward));
        let iter = bound_iter(iter, &to);

        iter.map(|data| self.decode(data)).collect()
    }
}

impl CommitStore for RocksStore {
    fn store_commit(&self, commit: &Commit) {
        let key = commit.index().to_be_bytes();
        let commit = bincode::serialize(commit).expect("Serialization failed");
        self.db
            .put_cf(self.commits(), &key, &commit)
            .expect("Storage operation failed");
    }

    fn get_commit(&self, index: u64) -> Option<Commit> {
        let key = index.to_be_bytes();
        let commit = self
            .db
            .get_cf(self.commits(), &key)
            .expect("Storage operation failed")?;
        let commit = bincode::deserialize(commit.as_ref())
            .expect("Deserializing commit from storage failed");
        Some(commit)
    }

    fn last_commit(&self) -> Option<Commit> {
        let (_, commit) = self
            .db
            .iterator_cf(
                self.commits(),
                IteratorMode::From(&u64::MAX.to_be_bytes(), Direction::Reverse),
            )
            .next()?
            .expect("Storage operation failed");
        let commit = bincode::deserialize(commit.as_ref())
            .expect("Deserializing commit from storage failed");
        Some(commit)
    }

    fn set_block_commit(&self, r: &BlockReference, index: u64) {
        let k = r.round_author_hash_encoding();
        if let Some(prev) = self
            .db
            .get_cf(self.block_commits(), &k)
            .expect("Storage operation failed")
        {
            let prev = u64::from_be_bytes(prev.as_slice().try_into().unwrap());
            if prev != index {
                panic!("Overwriting commit index for {r} from {prev} to {index}");
            }
        }
        let v = index.to_be_bytes();
        self.db
            .put_cf(self.block_commits(), &k, &v)
            .expect("Storage operation failed");
    }

    fn get_block_commit(&self, r: &BlockReference) -> Option<u64> {
        let k = r.round_author_hash_encoding();
        let v = self
            .db
            .get_cf(self.block_commits(), &k)
            .expect("Storage operation failed");
        v.map(|v| u64::from_be_bytes(v.as_slice().try_into().unwrap()))
    }
}

impl BlockViewStore for RocksStore {
    fn get_block_view(&self, r: &BlockReference) -> Vec<Option<BlockReference>> {
        let block_view = self
            .db
            .get_cf(self.block_view(), r.round_author_hash_encoding())
            .expect("Storage operation failed");
        let Some(block_view) = block_view else {
            panic!("Block view for block {r} not found");
        };
        bincode::deserialize(&block_view).expect("Failed to deserialize block view")
    }
}

fn bound_iter<'a, const N: usize>(
    it: impl Iterator<Item = Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>> + 'a,
    upper_bound_included: &'a [u8; N],
) -> impl Iterator<Item = Box<[u8]>> + 'a {
    it.map_while(|r| {
        let (k, v) = r.expect("Storage operation failed");
        let k: &[u8; N] = k.as_ref().try_into().expect("Key has an incorrect length");
        if k > upper_bound_included {
            None
        } else {
            Some(v)
        }
    })
}

fn bound_iter_upper<'a, const N: usize>(
    it: impl Iterator<Item = Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>> + 'a,
    lower_bound_included: &'a [u8; N],
) -> impl Iterator<Item = Box<[u8]>> + 'a {
    it.map_while(|r| {
        let (k, v) = r.expect("Storage operation failed");
        let k: &[u8; N] = k.as_ref().try_into().expect("Key has an incorrect length");
        if k < lower_bound_included {
            None
        } else {
            Some(v)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::tests::{blk, br};
    use tempdir::TempDir;

    #[test]
    fn rocks_store_test() {
        let dir = TempDir::new("sled_store_test").unwrap();
        let committee = Committee::new_test(vec![1; 3]);
        let store = RocksStore::open(dir, &committee, Metrics::new_test()).unwrap();
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
    fn rocks_commit_store_test() {
        let dir = TempDir::new("sled_commit_store_test").unwrap();
        let committee = Committee::new_test(vec![1; 3]);
        let store = RocksStore::open(dir, &committee, Metrics::new_test()).unwrap();
        store.store_commit(&c(0, br(1, 1)));
        let last = c(1, br(2, 1));
        store.store_commit(&last);
        assert_eq!(store.last_commit(), Some(last));

        store.set_block_commit(&br(2, 3), 5);
        assert_eq!(store.get_block_commit(&br(2, 3)), Some(5));
        assert_eq!(store.get_block_commit(&br(3, 3)), None);
    }

    fn c(i: u64, r: BlockReference) -> Commit {
        Commit::new_test(i, r, vec![r])
    }
}
