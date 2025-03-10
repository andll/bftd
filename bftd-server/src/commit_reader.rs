use bftd_core::block::Block;
use bftd_core::consensus::Commit;
use bftd_core::store::{BlockReader, CommitStore};
use bftd_core::syncer::Syncer;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

pub struct CommitReader {
    jh: JoinHandle<()>,
    receiver: mpsc::Receiver<FullCommit>,
}

pub struct FullCommit {
    pub info: Commit,
    pub blocks: Vec<Arc<Block>>,
}

struct CommitReaderTask<B> {
    store: B,
    commit_receiver: watch::Receiver<Option<u64>>,
    sender: mpsc::Sender<FullCommit>,
}

impl CommitReader {
    pub fn start<B: BlockReader + CommitStore>(
        store: B,
        syncer: &Syncer,
        index_from_included: u64,
    ) -> Self {
        let commit_receiver = syncer.last_commit_receiver().clone();
        let (sender, receiver) = mpsc::channel(128);
        let task = CommitReaderTask {
            store,
            commit_receiver,
            sender,
        };
        let jh = tokio::spawn(task.run(index_from_included));
        Self { jh, receiver }
    }

    /// Blocks until the next commit is available.
    /// Returns None when consensus core stops
    pub async fn recv_commit(&mut self) -> Option<FullCommit> {
        self.receiver.recv().await
    }
}

impl Drop for CommitReader {
    fn drop(&mut self) {
        self.jh.abort();
    }
}

impl<B: BlockReader + CommitStore> CommitReaderTask<B> {
    pub async fn run(mut self, mut next_index: u64) {
        loop {
            let last_excluded = self
                .commit_receiver
                .borrow_and_update()
                .map(|c| c + 1)
                .unwrap_or_default();
            for index in next_index..last_excluded {
                let info = self
                    .store
                    .get_commit(index)
                    .expect("Not found expected commit in store");
                let blocks = info
                    .all_blocks()
                    .iter()
                    .map(|r| {
                        self.store
                            .get(r)
                            .expect("Block from the commit not found in the store")
                    })
                    .collect();
                let commit = FullCommit { info, blocks };
                if self.sender.send(commit).await.is_err() {
                    return;
                }
            }
            next_index = last_excluded;
            if self.commit_receiver.changed().await.is_err() {
                return;
            }
        }
    }
}
