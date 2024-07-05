use crate::block::{Block, BlockHash, BlockReference, Round, ValidatorIndex};
use crate::committee::Committee;
use crate::store::{BlockReader, BlockStore, BlockViewStore};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

pub struct MemoryBlockStore {
    genesis_view: Vec<Option<BlockReference>>,
    inner: parking_lot::RwLock<MemoryBlockStoreInner>,
}

#[derive(Default)]
pub struct MemoryBlockStoreInner {
    blocks: BTreeMap<Round, BTreeMap<(ValidatorIndex, BlockHash), Arc<Block>>>,
    block_view: HashMap<BlockReference, Vec<Option<BlockReference>>>,
}

impl BlockStore for MemoryBlockStore {
    fn put(&self, block: Arc<Block>) {
        let mut lock = self.inner.write();
        let block_view = self.genesis_view.clone();
        for parent in block.parents() {
            if parent.is_genesis() {
                continue;
            }
        }
        lock.block_view.insert(*block.reference(), block_view);
        let prev = lock
            .blocks
            .entry(block.round())
            .or_default()
            .insert((block.author(), *block.block_hash()), block);
        if prev.is_some() {
            panic!("Re-inserting block that was already inserted");
        }
    }

    fn flush(&self) {}
    fn round_committed(&self, _round: Round) {}
}
impl BlockReader for MemoryBlockStoreInner {
    fn get(&self, key: &BlockReference) -> Option<Arc<Block>> {
        self.blocks
            .get(&key.round)?
            .get(&(key.author, key.hash))
            .cloned()
    }

    fn get_own(&self, validator: ValidatorIndex, round: Round) -> Option<Arc<Block>> {
        Some(
            self.blocks
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
        for (_round, map) in self.blocks.iter().rev() {
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
        let m = self.blocks.get(&key.round);
        match m {
            Some(map) => map.contains_key(&(key.author, key.hash)),
            None => false,
        }
    }

    fn get_blocks_by_round(&self, round: Round) -> Vec<Arc<Block>> {
        if let Some(map) = self.blocks.get(&round) {
            // todo - is cloning a good idea here?
            map.values().cloned().collect()
        } else {
            vec![]
        }
    }

    fn get_blocks_at_author_round(&self, author: ValidatorIndex, round: Round) -> Vec<Arc<Block>> {
        if let Some(map) = self.blocks.get(&round) {
            // todo - is cloning a good idea here?
            map.range((author, BlockHash::MIN)..(author, BlockHash::MAX))
                .map(|(_, b)| b.clone())
                .collect()
        } else {
            vec![]
        }
    }
}

impl BlockViewStore for MemoryBlockStore {
    fn get_block_view(&self, r: &BlockReference) -> Vec<Option<BlockReference>> {
        let lock = self.inner.read();
        lock.block_view
            .get(r)
            .expect("Block view not found")
            .clone()
    }
}
impl MemoryBlockStore {
    pub fn from_committee(committee: &Committee) -> Self {
        let genesis_blocks = committee.genesis_blocks();
        let genesis_view = genesis_blocks
            .into_iter()
            .map(|b| Some(*b.reference()))
            .collect();
        Self {
            genesis_view,
            inner: Default::default(),
        }
    }
}

impl BlockReader for MemoryBlockStore {
    fn get(&self, key: &BlockReference) -> Option<Arc<Block>> {
        self.inner.read().get(key)
    }

    fn get_own(&self, validator: ValidatorIndex, round: Round) -> Option<Arc<Block>> {
        self.inner.read().get_own(validator, round)
    }

    fn last_known_round(&self, validator: ValidatorIndex) -> Round {
        self.inner.read().last_known_round(validator)
    }

    fn last_known_block(&self, validator: ValidatorIndex) -> Arc<Block> {
        self.inner.read().last_known_block(validator)
    }

    fn exists(&self, key: &BlockReference) -> bool {
        self.inner.read().exists(key)
    }

    fn get_blocks_by_round(&self, round: Round) -> Vec<Arc<Block>> {
        self.inner.read().get_blocks_by_round(round)
    }

    fn get_blocks_at_author_round(&self, author: ValidatorIndex, round: Round) -> Vec<Arc<Block>> {
        self.inner.read().get_blocks_at_author_round(author, round)
    }
}
