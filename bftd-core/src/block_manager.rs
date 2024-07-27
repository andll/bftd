use crate::block::{Block, BlockReference};
use crate::committee::Committee;
use crate::log_byzantine;
use crate::metrics::Metrics;
use crate::protocol_config::ProtocolConfig;
use crate::store::{BlockStore, BlockViewStore, DagExt};
use anyhow::bail;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub struct BlockManager<S> {
    missing_inverse: HashMap<BlockReference, HashSet<BlockReference>>,
    suspended: HashMap<BlockReference, SuspendedInfo>,
    store: S,
    metrics: Arc<Metrics>,
    committee: Arc<Committee>,
    protocol_config: ProtocolConfig,
}

struct SuspendedInfo {
    block: Arc<Block>,
    missing_parents: HashSet<BlockReference>,
}

pub struct AddBlockResult {
    /// Blocks that have all parents stored locally and are now stored in BlockStore
    pub added: Vec<Arc<Block>>,
    /// Blocks that are received but suspended (do not have all parents)
    pub suspended: Vec<BlockReference>,
    /// Blocks that are rejected due to violations
    pub rejected: Vec<BlockReference>,
    /// Missing parents references (was not seen before)
    pub new_missing: Vec<BlockReference>,
    /// Missing parents references (either already suspended or referenced by some previously suspended block)
    pub previously_missing: Vec<BlockReference>,
}

impl<S: BlockStore + BlockViewStore> BlockManager<S> {
    pub fn new(
        store: S,
        metrics: Arc<Metrics>,
        committee: Arc<Committee>,
        protocol_config: ProtocolConfig,
    ) -> Self {
        Self {
            store,
            missing_inverse: Default::default(),
            suspended: Default::default(),
            metrics,
            committee,
            protocol_config,
        }
    }

    pub fn add_block(&mut self, block: Arc<Block>) -> AddBlockResult {
        let mut added = Vec::new();
        let mut new_missing = Vec::new();
        let mut previously_missing = Vec::new();
        let mut blocks = HashMap::new();
        let mut suspended = Vec::new();
        let mut rejected = Vec::new();
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
            let reference = *block.reference();
            if missing.is_empty() {
                // Add block
                let missing_inverse = self.missing_inverse.remove(block.reference());
                self.metrics
                    .block_manager_missing_inverse_len
                    .set(self.missing_inverse.len() as i64);
                if let Some(missing_inverse) = missing_inverse {
                    for k in missing_inverse {
                        if let Entry::Occupied(mut oc) = self.suspended.entry(k) {
                            let info = oc.get_mut();
                            assert!(info.missing_parents.remove(block.reference()));
                            if info.missing_parents.is_empty() {
                                let block = oc.remove().block;
                                assert_eq!(&k, block.reference());
                                blocks.insert(k, block);
                            }
                        } else {
                            panic!(
                                "Block reference {k} is in missing_inverse, but not in suspended"
                            );
                        }
                    }
                }
                if let Err(err) = self.check_block_before_adding(&block) {
                    log_byzantine!(
                        "[byzantine] Block {} rejected by block manager: {err}",
                        block.reference()
                    );
                    // todo reject all blocks referenced by the rejected block
                    self.metrics.block_manager_rejected.inc();
                    rejected.push(*block.reference());
                } else {
                    self.metrics.block_manager_added.inc();
                    added.push(block.clone());
                    self.store.put(block);
                }
            } else {
                // Suspend block
                match self.suspended.entry(reference) {
                    Entry::Occupied(_oc) => {
                        // nothing to do, already suspended this block
                        continue;
                    }
                    Entry::Vacant(va) => {
                        va.insert(SuspendedInfo {
                            block,
                            missing_parents: missing.iter().cloned().collect(),
                        });
                    }
                }
                suspended.push(reference);
                for parent in missing {
                    let entry = self.missing_inverse.entry(parent);
                    if matches!(entry, Entry::Vacant(_)) {
                        if self.suspended.contains_key(&parent) {
                            previously_missing.push(parent);
                        } else {
                            new_missing.push(parent);
                        }
                    } else {
                        previously_missing.push(parent);
                    }
                    let map = entry.or_default();
                    assert!(map.insert(reference));
                    self.metrics
                        .block_manager_missing_inverse_len
                        .set(self.missing_inverse.len() as i64);
                }
            }
        }
        AddBlockResult {
            added,
            suspended,
            rejected,
            new_missing,
            previously_missing,
        }
    }

    /// Final verifications when we have all block parents present
    /// - Check block timestamp is equal or higher than parents timestamps
    /// - Check if critical block for a block has enough support
    fn check_block_before_adding(&self, block: &Arc<Block>) -> anyhow::Result<()> {
        for parent in block.parents() {
            let parent = self.store.get(parent).expect("All parents must be present");
            if parent.time_ns() > block.time_ns() {
                bail!("Block {} failed DAG validation: parent {} has timestamp {}, block timestamp {}",
                    block.reference(), parent.reference(), parent.time_ns(), block.time_ns()
                );
            }
        }
        if self.protocol_config.critical_block_check {
            self.check_critical_block(block)?;
        }
        Ok(())
    }

    fn check_critical_block(&self, block: &Arc<Block>) -> anyhow::Result<()> {
        let critical_block_support = self.store.critical_block_support(block, &self.committee);
        let Some((critical_block, support)) = critical_block_support else {
            return Ok(());
        };
        if support < self.committee.f_threshold {
            bail!("Block {} failed DAG validation: critical block {:?} has support {}, minimal requirement is {}",
                block.reference(), critical_block, support, self.committee.f_threshold);
        }
        Ok(())
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
    use crate::protocol_config::ProtocolConfigBuilder;
    use crate::store::memory_store::MemoryBlockStore;
    use crate::store::BlockReader;
    use std::collections::HashSet;

    #[test]
    pub fn block_manager_test() {
        let committee = Committee::new_test(vec![1; 3]);
        let store = MemoryBlockStore::from_committee(&committee);
        let mut protocol_config = ProtocolConfigBuilder::default();
        protocol_config.with_critical_block_check(false);
        let protocol_config = protocol_config.build();
        let metrics = Metrics::new_test();
        let mut block_store =
            BlockManager::new(store, metrics.clone(), Arc::new(committee), protocol_config);
        let r = block_store.add_block(blk(1, 1, vec![br(0, 0), br(1, 0)]));
        r.assert_added_empty();
        r.assert_new_missing(vec![br(0, 0), br(1, 0)]);
        assert_eq!(metrics.block_manager_added.get(), 0);
        let r = block_store.add_block(blk(2, 2, vec![br(0, 0), br(1, 1)]));
        r.assert_added_empty();
        r.assert_no_new_missing();
        r.assert_previously_missing(vec![br(1, 1), br(0, 0)]);

        // Attempt to add same block again
        let r = block_store.add_block(blk(2, 2, vec![br(0, 0), br(1, 1)]));
        assert_eq!(metrics.block_manager_added.get(), 0);
        r.assert_added_empty();
        r.assert_no_new_missing();
        r.assert_no_previously_missing();

        let r = block_store.add_block(blk(1, 0, vec![]));
        r.assert_added_multi(vec![br(1, 0)]);
        r.assert_no_new_missing();
        assert_eq!(metrics.block_manager_added.get(), 1);
        assert!(block_store.store.exists(&br(1, 0)));
        let r = block_store.add_block(blk(0, 0, vec![]));
        r.assert_added_multi(vec![br(0, 0), br(1, 1), br(2, 2)]);
        r.assert_no_new_missing();
        assert_eq!(block_store.missing_inverse.len(), 0);
        assert_eq!(block_store.suspended.len(), 0);
        assert_eq!(
            block_store.metrics.block_manager_missing_inverse_len.get(),
            0
        );
        assert_eq!(metrics.block_manager_added.get(), 4);
        assert!(block_store.store.exists(&br(0, 0)));
        assert!(block_store.store.exists(&br(1, 1)));
        assert!(block_store.store.exists(&br(2, 2)));
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
        pub fn assert_no_previously_missing(&self) {
            assert!(self.previously_missing.is_empty())
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

        #[track_caller]
        pub fn assert_previously_missing(&self, v: Vec<BlockReference>) {
            assert_eq!(
                HashSet::<BlockReference>::from_iter(self.previously_missing.iter().cloned()),
                HashSet::from_iter(v.into_iter())
            )
        }
    }
}
