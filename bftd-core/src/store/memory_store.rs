use crate::block::{Block, BlockHash, BlockReference, Round, ValidatorIndex};
use crate::committee::Committee;
use crate::store::{BlockReader, BlockStore, BlockViewStore, DagExt};
use parking_lot::RwLockUpgradableReadGuard;
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
        let lock = self.inner.upgradable_read();
        let block_view = self.fill_block_view(&block, &self.genesis_view);

        let mut lock = RwLockUpgradableReadGuard::upgrade(lock);
        if !block.is_genesis() {
            lock.block_view.insert(*block.reference(), block_view);
        }
        let prev = lock
            .blocks
            .entry(block.round())
            .or_default()
            .insert((block.author(), *block.block_hash()), block);
        if prev.is_some() {
            panic!("Re-inserting block that was already inserted");
        }
    }

    fn put_with_block_view(&self, block: Arc<Block>, block_view: Vec<Option<BlockReference>>) {
        let mut lock = self.inner.write();
        if !block.is_genesis() {
            lock.block_view.insert(*block.reference(), block_view);
        }
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

impl BlockViewStore for MemoryBlockStoreInner {
    fn get_block_view(&self, r: &BlockReference) -> Vec<Option<BlockReference>> {
        let Some(bv) = self.block_view.get(r) else {
            panic!("Block view for {r} not found")
        };
        bv.clone()
    }
}

impl BlockViewStore for MemoryBlockStore {
    fn get_block_view(&self, r: &BlockReference) -> Vec<Option<BlockReference>> {
        self.inner.read().get_block_view(r)
    }
}
impl MemoryBlockStore {
    pub fn from_committee(committee: &Committee) -> Self {
        let genesis_view = committee.genesis_view();
        Self {
            genesis_view,
            inner: Default::default(),
        }
    }

    #[cfg(test)]
    pub fn from_genesis_blocks(genesis_blocks: Vec<Arc<Block>>) -> Self {
        let genesis_view = genesis_blocks
            .iter()
            .map(|b| Some(*b.reference()))
            .collect();
        let this = Self {
            genesis_view,
            inner: Default::default(),
        };
        for block in genesis_blocks {
            this.put(block);
        }
        this
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::tests::blk;
    use crate::committee::Stake;
    use crate::{b, br};

    macro_rules! parse_br_option (
        ($p:expr) => (
            if $p == "_" {
                None
            } else {
                Some(br!($p))
            }
        );
    );

    macro_rules! bw (
        ($p:expr) => (
            $p.into_iter().map(|v|parse_br_option!(v)).collect::<Vec<_>>()
        );
    );

    #[test]
    fn test_block_view() {
        let committee = Committee::new_test(vec![1; 5]);
        let genesis_blocks = committee
            .enumerate_indexes()
            .map(|vi| blk(vi.0, 0, vec![]))
            .collect();
        let store = MemoryBlockStore::from_genesis_blocks(genesis_blocks);
        store.put(b!("A1", ["A0", "B0", "C0"]));
        assert_eq!(
            store.get_block_view(&br!("A1")),
            bw!(["A0", "B0", "C0", "D0", "E0"])
        );
        store.put(b!("B1", ["B0", "C0"]));
        assert_eq!(
            store.get_block_view(&br!("B1")),
            bw!(["A0", "B0", "C0", "D0", "E0"])
        );
        store.put(b!("B2", ["B1", "C0"]));
        assert_eq!(
            store.get_block_view(&br!("B2")),
            bw!(["A0", "B1", "C0", "D0", "E0"])
        );
        store.put(b!("C2", ["C0", "B0"]));
        assert_eq!(
            store.get_block_view(&br!("C2")),
            bw!(["A0", "B0", "C0", "D0", "E0"])
        );
        store.put(b!("C4", ["C0", "B0"]));
        assert_eq!(
            store.get_block_view(&br!("C4")),
            bw!(["A0", "B0", "C0", "D0", "E0"])
        );
        store.put(b!("B3", ["B2", "C2"]));
        assert_eq!(
            store.get_block_view(&br!("B3")),
            bw!(["A0", "B2", "C2", "D0", "E0"])
        );
        store.put(b!("A5", ["A0", "B2", "C4"]));
        assert_eq!(
            store.get_block_view(&br!("A5")),
            bw!(["A0", "B2", "C4", "D0", "E0"])
        );
        store.put(b!("B6", ["B3", "A5"]));
        // B3 sees C2 and A5 sees C4 => C has block view set to None
        assert_eq!(
            store.get_block_view(&br!("B6")),
            bw!(["A5", "B3", "_", "D0", "E0"])
        );
    }

    #[test]
    fn critical_block_test() {
        let committee = Committee::new_test(vec![1; 3]);
        let genesis_blocks = committee
            .enumerate_indexes()
            .map(|vi| blk(vi.0, 0, vec![]))
            .collect();
        let store = MemoryBlockStore::from_genesis_blocks(genesis_blocks);

        let a1 = b!("A1", ["A0", "B0", "C0"]);
        store.put(a1.clone());
        assert_eq!(store.critical_block(&a1), None);
        let a2 = b!("A2", ["A1", "B0", "C0"]);
        store.put(a2.clone());
        store.put(b!("B2", ["B0", "A1", "C0"]));
        assert_eq!(store.critical_block(&a2), Some(br!("A0")));
        assert_eq!(
            store.critical_block_support(&a2, &committee),
            Some(Stake(1))
        );
        let a3 = b!("A3", ["A2", "B2", "C0"]);
        store.put(a3.clone());
        assert_eq!(store.critical_block(&a3), Some(br!("A1")));
        assert_eq!(
            store.critical_block_support(&a3, &committee),
            Some(Stake(2))
        );
    }
}
