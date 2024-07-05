use crate::block::{Block, BlockReference, Round, ValidatorIndex};
use crate::consensus::Commit;
use crate::store::{BlockReader, BlockStore, CommitStore};
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct BlockCache<B> {
    cache: RwLock<BlockCacheInner>,
    store: B,
}

struct BlockCacheInner {
    cache: BTreeMap<Round, BTreeMap<BlockReference, Arc<Block>>>,
    low_watermark_included: Round,
}

impl<B: BlockStore + CommitStore> BlockCache<B> {
    pub fn new(store: B) -> Self {
        let partially_committed_round = store.last_commit().map(|c| c.round()).unwrap_or_default();
        let low_watermark_included = Round(
            partially_committed_round
                .0
                .saturating_sub(Self::PRESERVE_ROUNDS_AFTER_COMMIT),
        );
        let cache = Self::load_cache(&store, low_watermark_included);
        let cache = BlockCacheInner {
            cache,
            low_watermark_included,
        };
        let cache = RwLock::new(cache);
        Self { cache, store }
    }
}

impl<B: BlockReader> BlockCache<B> {
    // Helps with synchronization and linearizer
    const PRESERVE_ROUNDS_AFTER_COMMIT: u64 = 10;

    pub fn update_watermark(&self, new_low_watermark_included: Round) {
        self.cache
            .write()
            .update_watermark(new_low_watermark_included);
    }

    fn load_cache(
        store: &B,
        mut round: Round,
    ) -> BTreeMap<Round, BTreeMap<BlockReference, Arc<Block>>> {
        let mut map = BTreeMap::new();
        loop {
            let blocks = store.get_blocks_by_round(round);
            if blocks.is_empty() {
                break;
            }
            let blocks = blocks.into_iter().map(|b| (*b.reference(), b)).collect();
            map.insert(round, blocks);
            round = round.next();
        }
        map
    }
}

impl BlockCacheInner {
    #[inline]
    fn cached_round(&self, round: Round) -> bool {
        round >= self.low_watermark_included
    }

    fn update_watermark(&mut self, new_low_watermark_included: Round) {
        self.cache.retain(|r, _| *r >= new_low_watermark_included);
        self.low_watermark_included = new_low_watermark_included;
    }
}

impl<B: BlockStore> BlockStore for BlockCache<B> {
    fn put(&self, block: Arc<Block>) {
        let mut cache = self.cache.write();
        if cache.cached_round(block.round()) {
            cache
                .cache
                .entry(block.round())
                .or_default()
                .insert(*block.reference(), block.clone());
        }
        drop(cache);
        self.store.put(block)
    }

    fn flush(&self) {
        self.store.flush()
    }

    fn round_committed(&self, round: Round) {
        self.update_watermark(Round(
            round.0.saturating_sub(Self::PRESERVE_ROUNDS_AFTER_COMMIT),
        ))
    }
}

impl<B: BlockReader> BlockReader for BlockCache<B> {
    fn get(&self, key: &BlockReference) -> Option<Arc<Block>> {
        let cache = self.cache.read();
        if cache.cached_round(key.round()) {
            return cache.get(key);
        }
        drop(cache);
        self.store.get(key)
    }

    fn get_own(&self, validator: ValidatorIndex, round: Round) -> Option<Arc<Block>> {
        let cache = self.cache.read();
        if cache.cached_round(round) {
            return cache.get_own(validator, round);
        }
        drop(cache);
        self.store.get_own(validator, round)
    }

    fn last_known_round(&self, validator: ValidatorIndex) -> Round {
        self.store.last_known_round(validator)
    }

    fn last_known_block(&self, validator: ValidatorIndex) -> Arc<Block> {
        self.store.last_known_block(validator)
    }

    fn exists(&self, key: &BlockReference) -> bool {
        let cache = self.cache.read();
        if cache.cached_round(key.round()) {
            return cache.exists(key);
        }
        drop(cache);
        self.store.exists(key)
    }

    fn get_blocks_by_round(&self, round: Round) -> Vec<Arc<Block>> {
        let cache = self.cache.read();
        if cache.cached_round(round) {
            return cache.get_blocks_by_round(round);
        }
        drop(cache);
        self.store.get_blocks_by_round(round)
    }

    fn get_blocks_at_author_round(&self, author: ValidatorIndex, round: Round) -> Vec<Arc<Block>> {
        let cache = self.cache.read();
        if cache.cached_round(round) {
            return cache.get_blocks_at_author_round(author, round);
        }
        drop(cache);
        self.store.get_blocks_at_author_round(author, round)
    }

    fn linked_to_round(&self, block: &Arc<Block>, round: Round) -> Vec<Arc<Block>> {
        let cache = self.cache.read();
        if cache.cached_round(round) {
            // Perform entire linked_to_round within single lock
            return cache.linked_to_round(block, round);
        }
        drop(cache);
        self.store.linked_to_round(block, round)
    }
}

impl BlockReader for BlockCacheInner {
    fn get(&self, key: &BlockReference) -> Option<Arc<Block>> {
        self.cache.get(&key.round())?.get(key).cloned()
    }

    fn get_own(&self, validator: ValidatorIndex, round: Round) -> Option<Arc<Block>> {
        let round_cache = self.cache.get(&round)?;
        let mut range = round_cache.range(
            BlockReference::first_block_reference_for_round_author(round, validator)
                ..=BlockReference::last_block_reference_for_round_author(round, validator),
        );
        return range.next().map(|(_, v)| v.clone());
    }

    fn last_known_round(&self, _validator: ValidatorIndex) -> Round {
        unimplemented!()
    }

    fn last_known_block(&self, _validator: ValidatorIndex) -> Arc<Block> {
        unimplemented!()
    }

    fn exists(&self, key: &BlockReference) -> bool {
        let Some(round_cache) = self.cache.get(&key.round) else {
            return false;
        };
        return round_cache.contains_key(key);
    }

    fn get_blocks_by_round(&self, round: Round) -> Vec<Arc<Block>> {
        let Some(round_cache) = self.cache.get(&round) else {
            return vec![];
        };
        return round_cache.values().cloned().collect();
    }

    fn get_blocks_at_author_round(&self, author: ValidatorIndex, round: Round) -> Vec<Arc<Block>> {
        let Some(round_cache) = self.cache.get(&round) else {
            return vec![];
        };
        let range = round_cache.range(
            BlockReference::first_block_reference_for_round_author(round, author)
                ..=BlockReference::last_block_reference_for_round_author(round, author),
        );
        return range.map(|(_, b)| b.clone()).collect();
    }
}

impl<B: CommitStore> CommitStore for BlockCache<B> {
    fn store_commit(&self, commit: &Commit) {
        self.store.store_commit(commit)
    }

    fn get_commit(&self, index: u64) -> Option<Commit> {
        self.store.get_commit(index)
    }

    fn last_commit(&self) -> Option<Commit> {
        self.store.last_commit()
    }

    fn set_block_commit(&self, r: &BlockReference, index: u64) {
        self.store.set_block_commit(r, index)
    }

    fn get_block_commit(&self, r: &BlockReference) -> Option<u64> {
        self.store.get_block_commit(r)
    }
}
