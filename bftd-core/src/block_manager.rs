use crate::block::{Block, BlockReference};
use crate::metrics::Metrics;
use crate::store::BlockStore;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

pub struct BlockManager<S> {
    missing_inverse: HashMap<BlockReference, HashMap<BlockReference, Arc<Block>>>,
    store: S,
    metrics: Arc<Metrics>,
}

pub struct AddBlockResult {
    pub added: Vec<Arc<Block>>,
    pub suspended: Vec<BlockReference>,
    pub new_missing: Vec<BlockReference>,
    pub previously_missing: Vec<BlockReference>,
}

impl<S: BlockStore> BlockManager<S> {
    pub fn new(store: S, metrics: Arc<Metrics>) -> Self {
        Self {
            store,
            missing_inverse: Default::default(),
            metrics,
        }
    }

    pub fn add_block(&mut self, block: Arc<Block>) -> AddBlockResult {
        let mut added = Vec::new();
        let mut new_missing = Vec::new();
        let mut previously_missing = Vec::new();
        let mut blocks = HashMap::new();
        let mut suspended = Vec::new();
        blocks.insert(*block.reference(), block);
        while let Some(block) = blocks.keys().next().copied() {
            let block = blocks.remove(&block).unwrap();
            if self.store.exists(block.reference()) {
                continue;
            }
            let missing: Vec<BlockReference> = block
                .parents()
                .iter()
                .filter_map(|p| if self.store.exists(p) { None } else { Some(*p) })
                .collect();
            if missing.is_empty() {
                let missing_inverse = self.missing_inverse.remove(block.reference());
                self.metrics
                    .block_manager_missing_inverse_len
                    .set(self.missing_inverse.len() as i64);
                if let Some(missing_inverse) = missing_inverse {
                    // todo - need to be a bit smart about it to skip blocks that aren't ready yet
                    for (k, v) in missing_inverse {
                        blocks.insert(k, v);
                    }
                }
                added.push(block.clone());
                self.store.put(block);
            } else {
                // todo tests
                // todo only newly suspended blocks
                suspended.push(*block.reference());
                for parent in missing {
                    let entry = self.missing_inverse.entry(parent);
                    if matches!(entry, Entry::Vacant(_)) {
                        new_missing.push(parent);
                    } else {
                        previously_missing.push(parent);
                    }
                    let map = entry.or_default();
                    map.insert(*block.reference(), block.clone());
                    self.metrics
                        .block_manager_missing_inverse_len
                        .set(self.missing_inverse.len() as i64);
                }
            }
        }
        AddBlockResult {
            added,
            suspended,
            new_missing,
            previously_missing,
        }
    }

    pub fn flush(&self) {
        self.store.flush()
    }
}

impl AddBlockResult {
    pub fn assert_added(&self, v: &BlockReference) {
        assert!(self.added.iter().any(|b| b.reference() == v));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::tests::{blk, br};
    use crate::block::{Round, ValidatorIndex};
    use parking_lot::Mutex;
    use std::collections::HashSet;

    #[test]
    pub fn block_store_test() {
        let store = Mutex::new(HashMap::new());
        let mut block_store = BlockManager::new(store, Metrics::new_test());
        let r = block_store.add_block(blk(1, 1, vec![br(0, 0), br(1, 0)]));
        r.assert_added_empty();
        r.assert_new_missing(vec![br(0, 0), br(1, 0)]);
        assert_eq!(block_store.store.lock().len(), 0);
        let r = block_store.add_block(blk(2, 2, vec![br(0, 0), br(1, 1)]));
        r.assert_added_empty();
        r.assert_new_missing(vec![br(1, 1)]);
        assert_eq!(block_store.store.lock().len(), 0);
        let r = block_store.add_block(blk(1, 0, vec![]));
        r.assert_added_multi(vec![br(1, 0)]);
        r.assert_no_new_missing();
        assert_eq!(block_store.store.lock().len(), 1);
        assert!(block_store.store.lock().contains_key(&br(1, 0)));
        let r = block_store.add_block(blk(0, 0, vec![]));
        r.assert_added_multi(vec![br(0, 0), br(1, 1), br(2, 2)]);
        r.assert_no_new_missing();
        assert_eq!(block_store.missing_inverse.len(), 0);
        assert_eq!(
            block_store.metrics.block_manager_missing_inverse_len.get(),
            0
        );
        assert_eq!(block_store.store.lock().len(), 4);
        assert!(block_store.store.lock().contains_key(&br(0, 0)));
        assert!(block_store.store.lock().contains_key(&br(1, 1)));
        assert!(block_store.store.lock().contains_key(&br(2, 2)));
    }

    impl BlockStore for Mutex<HashMap<BlockReference, Arc<Block>>> {
        fn put(&self, block: Arc<Block>) {
            self.lock().insert(*block.reference(), block);
        }

        fn flush(&self) {}

        fn get(&self, key: &BlockReference) -> Option<Arc<Block>> {
            self.lock().get(key).cloned()
        }

        fn get_own(&self, _validator: ValidatorIndex, _round: Round) -> Option<Arc<Block>> {
            unimplemented!()
        }

        fn last_known_round(&self, _validator: ValidatorIndex) -> Round {
            unimplemented!()
        }

        fn last_known_block(&self, _validator: ValidatorIndex) -> Arc<Block> {
            unimplemented!()
        }

        fn exists(&self, key: &BlockReference) -> bool {
            self.lock().contains_key(key)
        }

        fn get_blocks_by_round(&self, _round: Round) -> Vec<Arc<Block>> {
            unimplemented!()
        }

        fn get_blocks_at_author_round(
            &self,
            _author: ValidatorIndex,
            _round: Round,
        ) -> Vec<Arc<Block>> {
            unimplemented!()
        }

        fn linked_to_round(&self, _block: &Arc<Block>, _round: Round) -> Vec<Arc<Block>> {
            unimplemented!()
        }
    }

    impl AddBlockResult {
        #[track_caller]
        pub fn assert_added_empty(&self) {
            assert!(self.added.is_empty())
        }

        #[track_caller]
        pub fn assert_no_new_missing(&self) {
            assert!(self.new_missing.is_empty())
        }

        #[track_caller]
        pub fn assert_added_multi(&self, v: Vec<BlockReference>) {
            assert_eq!(
                HashSet::<BlockReference>::from_iter(self.added.iter().map(|b| *b.reference())),
                HashSet::from_iter(v.into_iter())
            )
        }

        #[track_caller]
        pub fn assert_new_missing(&self, v: Vec<BlockReference>) {
            assert_eq!(
                HashSet::<BlockReference>::from_iter(self.new_missing.iter().cloned()),
                HashSet::from_iter(v.into_iter())
            )
        }
    }
}
