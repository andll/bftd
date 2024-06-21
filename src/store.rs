use crate::block::{Block, BlockReference, Round, ValidatorIndex};
use crate::block_manager::BlockStore;
use sled::{Db, IVec, Tree};
use std::io;
use std::path::Path;
use std::sync::Arc;

pub struct SledStore {
    blocks: Tree,
    index: Tree,
}

impl SledStore {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let db = sled::open(path)?;
        let blocks = db.open_tree("blocks")?;
        let index = db.open_tree("index")?;
        Ok(Self { blocks, index })
    }

    fn decode(v: IVec) -> Arc<Block> {
        Arc::new(
            Block::from_bytes_unchecked(v.to_vec().into())
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

    fn get(&self, key: &BlockReference) -> Option<Arc<Block>> {
        let block_key = key.round_author_hash_encoding();
        let block = self
            .blocks
            .get(&block_key)
            .expect("Storage operation failed")?;
        Some(Self::decode(block))
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
        Some(Self::decode(block))
    }

    fn last_known_round(&self, validator: ValidatorIndex) -> Round {
        let from = BlockReference::first_block_reference_for_round_author(Round::ZERO, validator)
            .author_round_hash_encoding();
        let to = BlockReference::first_block_reference_for_round_author(Round::MAX, validator)
            .author_round_hash_encoding();

        // todo - need to verify entry in .blocks exists since index can be dirty
        if let Some(v) = self.index.range(from..to).last() {
            // todo implement as BlockReference method
            let round = u64::from_be_bytes(
                v.expect("Storage operation failed").0[8..16]
                    .try_into()
                    .unwrap(),
            );
            Round(round)
        } else {
            panic!(
                "No blocks found for validator in the storage(should have at least genesis block"
            );
        }
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
            .map(|data| Self::decode(data.expect("Storage operation failed").1))
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
                Some(Self::decode(block))
            })
            .collect()
    }

    fn linked_to_round(&self, block: &Arc<Block>, round: Round) -> Vec<Arc<Block>> {
        todo!()
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
        let store = SledStore::open(dir).unwrap();
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
}
