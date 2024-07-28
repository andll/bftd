use crate::block::{Block, BlockReference, Round, ValidatorIndex};
use crate::committee::Committee;
use crate::consensus::Commit;
use crate::store::{BlockReader, BlockStore, BlockViewStore, CommitStore, DagExt};
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct BlockCache<B> {
    cache: RwLock<BlockCacheInner>,
    store: B,
    genesis_view: Vec<Option<BlockReference>>,
}

struct BlockCacheInner {
    cache: BTreeMap<Round, BTreeMap<BlockReference, CacheEntry>>,
    last_known_blocks: Vec<Arc<Block>>,
    low_watermark_included: Round,
}

struct CacheEntry {
    block: Arc<Block>,
    block_view: Vec<Option<BlockReference>>,
}

impl<B: BlockStore + CommitStore + BlockViewStore> BlockCache<B> {
    pub fn new(store: B, committee: &Committee) -> Self {
        let partially_committed_round = store.last_commit().map(|c| c.round()).unwrap_or_default();
        let low_watermark_included = Round(
            partially_committed_round
                .0
                .saturating_sub(Self::PRESERVE_ROUNDS_AFTER_COMMIT),
        );
        let cache = Self::load_cache(&store, low_watermark_included);
        let last_known_blocks = Self::load_last_known(&store, committee);
        let cache = BlockCacheInner {
            cache,
            last_known_blocks,
            low_watermark_included,
        };
        let cache = RwLock::new(cache);
        let genesis_view = committee.genesis_view();
        Self {
            cache,
            store,
            genesis_view,
        }
    }
}

impl<B: BlockReader + BlockViewStore> BlockCache<B> {
    // Helps with synchronization and linearizer
    const PRESERVE_ROUNDS_AFTER_COMMIT: u64 = 10;

    pub fn update_watermark(&self, new_low_watermark_included: Round) {
        self.cache
            .write()
            .update_watermark(new_low_watermark_included);
    }

    fn load_last_known(store: &B, committee: &Committee) -> Vec<Arc<Block>> {
        committee
            .enumerate_indexes()
            .map(|vi| store.last_known_block(vi))
            .collect()
    }

    fn load_cache(
        store: &B,
        mut round: Round,
    ) -> BTreeMap<Round, BTreeMap<BlockReference, CacheEntry>> {
        let mut map = BTreeMap::new();
        loop {
            let blocks = store.get_blocks_by_round(round);
            if blocks.is_empty() {
                break;
            }
            let blocks = blocks
                .into_iter()
                .map(|block| {
                    let reference = *block.reference();
                    let block_view = store.get_block_view(&reference);
                    let entry = CacheEntry { block, block_view };
                    (reference, entry)
                })
                .collect();
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

    fn get_block_view_cached(&self, r: &BlockReference) -> Vec<Option<BlockReference>> {
        self.cache
            .get(&r.round())
            .expect("Cache line not found for round")
            .get(r)
            .expect("Block view not found")
            .block_view
            .clone()
    }
}

impl<B: BlockStore + BlockViewStore> BlockStore for BlockCache<B> {
    fn put(&self, block: Arc<Block>) {
        let cache = self.cache.upgradable_read();
        let block_view = (&*cache, &self.store).fill_block_view(&block, &self.genesis_view);
        // todo - we want to check this only in smoke tests...
        debug_assert!(block_view.iter().all(Option::is_some));
        let entry = CacheEntry {
            block_view: block_view.clone(),
            block: block.clone(),
        };
        let mut cache = RwLockUpgradableReadGuard::upgrade(cache);
        if cache.cached_round(block.round()) {
            cache
                .cache
                .entry(block.round())
                .or_default()
                .insert(*block.reference(), entry);
        }
        // still holding cache lock here
        self.store.put_with_block_view(block.clone(), block_view);
        let validator = block.author();
        let last_known = cache.last_known_block(validator);
        if block.round() > last_known.round() {
            *validator.slice_get_mut(&mut cache.last_known_blocks) = block;
        }
    }

    fn put_with_block_view(&self, _block: Arc<Block>, _block_view: Vec<Option<BlockReference>>) {
        unimplemented!()
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
        self.cache.read().last_known_round(validator)
    }

    fn last_known_block(&self, validator: ValidatorIndex) -> Arc<Block> {
        self.cache.read().last_known_block(validator)
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

impl<B: BlockViewStore> BlockViewStore for BlockCache<B> {
    fn get_block_view(&self, r: &BlockReference) -> Vec<Option<BlockReference>> {
        let cache = self.cache.read();
        if cache.cached_round(r.round()) {
            return cache.get_block_view_cached(r);
        }
        drop(cache);
        self.store.get_block_view(r)
    }
}

impl BlockReader for BlockCacheInner {
    fn get(&self, key: &BlockReference) -> Option<Arc<Block>> {
        self.cache
            .get(&key.round())?
            .get(key)
            .map(CacheEntry::block)
    }

    fn get_own(&self, validator: ValidatorIndex, round: Round) -> Option<Arc<Block>> {
        let round_cache = self.cache.get(&round)?;
        let mut range = round_cache.range(
            BlockReference::first_for_round_author(round, validator)
                ..=BlockReference::last_for_round_author(round, validator),
        );
        return range.next().map(|(_, v)| v.block());
    }

    fn last_known_round(&self, validator: ValidatorIndex) -> Round {
        self.last_known_block(validator).round()
    }

    fn last_known_block(&self, validator: ValidatorIndex) -> Arc<Block> {
        validator.slice_get(&self.last_known_blocks).clone()
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
        return round_cache.values().map(CacheEntry::block).collect();
    }

    fn get_blocks_at_author_round(&self, author: ValidatorIndex, round: Round) -> Vec<Arc<Block>> {
        let Some(round_cache) = self.cache.get(&round) else {
            return vec![];
        };
        let range = round_cache.range(
            BlockReference::first_for_round_author(round, author)
                ..=BlockReference::last_for_round_author(round, author),
        );
        return range.map(|(_, b)| b.block()).collect();
    }
}

// Implementing BlockViewStore and BlockReader for (&BlockCacheInner, &B)
// This allows running DagExt::fill_block_view to leverage locked cache but fallback to storage if needed
impl<B: BlockViewStore> BlockViewStore for (&BlockCacheInner, &B) {
    fn get_block_view(&self, r: &BlockReference) -> Vec<Option<BlockReference>> {
        if self.0.cached_round(r.round) {
            // self.0.get_block_view_cached.fetch_add(1, Ordering::Relaxed);
            return self.0.get_block_view_cached(r);
        }
        // self.0.get_block_view_uncached.fetch_add(1, Ordering::Relaxed);
        self.1.get_block_view(r)
    }
}

impl<B: BlockReader> BlockReader for (&BlockCacheInner, &B) {
    fn get(&self, key: &BlockReference) -> Option<Arc<Block>> {
        if self.0.cached_round(key.round) {
            // self.0.get_block_cached.fetch_add(1, Ordering::Relaxed);
            self.0.get(key)
        } else {
            // self.0.get_block_uncached.fetch_add(1, Ordering::Relaxed);
            self.1.get(key)
        }
    }

    fn get_own(&self, validator: ValidatorIndex, round: Round) -> Option<Arc<Block>> {
        if self.0.cached_round(round) {
            self.0.get_own(validator, round)
        } else {
            self.1.get_own(validator, round)
        }
    }

    fn last_known_round(&self, _validator: ValidatorIndex) -> Round {
        unimplemented!()
    }

    fn last_known_block(&self, _validator: ValidatorIndex) -> Arc<Block> {
        unimplemented!()
    }

    fn exists(&self, key: &BlockReference) -> bool {
        if self.0.cached_round(key.round()) {
            self.0.exists(key)
        } else {
            self.1.exists(key)
        }
    }

    fn get_blocks_by_round(&self, round: Round) -> Vec<Arc<Block>> {
        if self.0.cached_round(round) {
            self.0.get_blocks_by_round(round)
        } else {
            self.1.get_blocks_by_round(round)
        }
    }

    fn get_blocks_at_author_round(&self, author: ValidatorIndex, round: Round) -> Vec<Arc<Block>> {
        if self.0.cached_round(round) {
            self.0.get_blocks_at_author_round(author, round)
        } else {
            self.1.get_blocks_at_author_round(author, round)
        }
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

impl CacheEntry {
    fn block(&self) -> Arc<Block> {
        self.block.clone()
    }
}
