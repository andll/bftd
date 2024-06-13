use crate::block::{Block, BlockReference};
use std::collections::HashMap;
use std::sync::Arc;

pub struct BlockStore<S> {
    missing_inverse: HashMap<BlockReference, HashMap<BlockReference, Arc<Block>>>,
    store: S,
}

pub trait Store {
    fn put(&self, block: Arc<Block>);
    fn get(&self, key: &BlockReference) -> Option<Arc<Block>>;
    fn exists(&self, key: &BlockReference) -> bool {
        self.get(key).is_some()
    }
}

impl<S: Store> BlockStore<S> {
    pub fn new(store: S) -> Self {
        Self {
            store,
            missing_inverse: Default::default(),
        }
    }

    pub fn try_add_block(&mut self, block: Arc<Block>) {
        let mut blocks = HashMap::new();
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
                if let Some(missing_inverse) = missing_inverse {
                    for (k, v) in missing_inverse {
                        blocks.insert(k, v);
                    }
                }
                self.store.put(block);
            } else {
                for parent in missing {
                    let map = self.missing_inverse.entry(parent).or_default();
                    map.insert(*block.reference(), block.clone());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::tests::{blk, br};
    use std::cell::RefCell;

    #[test]
    pub fn block_store_test() {
        let store = RefCell::new(HashMap::new());
        let mut block_store = BlockStore::new(store);
        block_store.try_add_block(blk(1, 1, vec![br(0, 0), br(1, 0)]));
        assert_eq!(block_store.store.borrow().len(), 0);
        block_store.try_add_block(blk(2, 2, vec![br(0, 0), br(1, 1)]));
        assert_eq!(block_store.store.borrow().len(), 0);
        block_store.try_add_block(blk(1, 0, vec![]));
        assert_eq!(block_store.store.borrow().len(), 1);
        assert!(block_store.store.borrow().contains_key(&br(1, 0)));
        block_store.try_add_block(blk(0, 0, vec![]));
        assert_eq!(block_store.store.borrow().len(), 4);
        assert!(block_store.store.borrow().contains_key(&br(0, 0)));
        assert!(block_store.store.borrow().contains_key(&br(1, 1)));
        assert!(block_store.store.borrow().contains_key(&br(2, 2)));
    }

    impl Store for RefCell<HashMap<BlockReference, Arc<Block>>> {
        fn put(&self, block: Arc<Block>) {
            self.borrow_mut().insert(*block.reference(), block);
        }

        fn get(&self, key: &BlockReference) -> Option<Arc<Block>> {
            self.borrow().get(key).cloned()
        }

        fn exists(&self, key: &BlockReference) -> bool {
            self.borrow().contains_key(key)
        }
    }
}
