use bftd_types::types::{ChannelElementKey, ChannelId};
use bincode::DefaultOptions;
use bincode::Options as BincodeOptions;
use parking_lot::Mutex;
use rocksdb::{ColumnFamily, Direction, IteratorMode, Options, WriteBatch, DB};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

pub struct ChannelStore {
    db: DB,
    notifiers: Mutex<HashMap<ChannelId, watch::Sender<u64>>>,
}

impl ChannelStore {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let db = DB::open_cf(&Self::options(), path, Self::cfs())?;
        let notifiers = Default::default();
        Ok(Self { db, notifiers })
    }

    pub fn update_channels(&self, commit_index: u64, channels: HashSet<ChannelId>) {
        let mut batch = WriteBatch::default();
        for channel in &channels {
            let key = (*channel, commit_index);
            let key = serialize_options()
                .serialize(&key)
                .expect("Failed to serialize key");
            batch.put_cf(self.channel_data(), key, &[]);
        }
        batch.put_cf(
            self.last_commit_index(),
            [],
            serialize_options()
                .serialize(&commit_index)
                .expect("Failed to serialize last commit index"),
        );
        self.db.write(batch).expect("Failed to write batch");
        let mut notifiers = self.notifiers.lock();
        for touched_channel in channels {
            if let Entry::Occupied(oc) = notifiers.entry(touched_channel) {
                if oc.get().send(commit_index).is_err() {
                    oc.remove();
                }
            }
        }
    }

    pub fn subscribe_channel(self: &Arc<Self>, from: ChannelElementKey) -> mpsc::Receiver<u64> {
        let notifier = self.register(from.channel);
        let (sender, receiver) = mpsc::channel(16);
        tokio::spawn(self.clone().stream_channel(
            (from.channel, from.commit_index),
            notifier,
            sender,
        ));
        receiver
    }

    fn register(&self, channel: ChannelId) -> watch::Receiver<u64> {
        let mut notifier = self.notifiers.lock();
        match notifier.entry(channel) {
            Entry::Occupied(oc) => oc.get().subscribe(),
            Entry::Vacant(va) => {
                let (sender, receiver) = watch::channel(0);
                va.insert(sender);
                receiver
            }
        }
    }

    async fn stream_channel(
        self: Arc<Self>,
        mut next: (ChannelId, u64),
        mut notifier: watch::Receiver<u64>,
        sender: mpsc::Sender<u64>,
    ) {
        loop {
            let key = serialize_options()
                .serialize(&next)
                .expect("Failed to serialize key");
            let mut iterator = self.db.iterator_cf(
                self.channel_data(),
                IteratorMode::From(&key, Direction::Forward),
            );
            while let Some(r) = iterator.next() {
                let (key, _) = r.expect("Storage error");
                let (channel, commit_index): (ChannelId, u64) = serialize_options()
                    .deserialize(&key)
                    .expect("Failed to deserialize key");
                if channel != next.0 {
                    break;
                }
                if sender.send(commit_index).await.is_err() {
                    return;
                }
                next = (channel, commit_index + 1);
            }
            loop {
                if notifier.changed().await.is_err() {
                    return;
                }
                let last_notified_commit = *notifier.borrow_and_update();
                if last_notified_commit >= next.1 {
                    // break the loop when commit notifier advanced to or past next expected commit
                    break;
                }
            }
        }
    }

    pub fn last_processed_commit_index(&self) -> Option<u64> {
        let r = self
            .db
            .get_cf(self.last_commit_index(), [])
            .expect("DB error");
        let r = r?;
        Some(
            serialize_options()
                .deserialize(&r)
                .expect("Failed to deserialize last commit index"),
        )
    }

    fn channel_data(&self) -> &ColumnFamily {
        self.db.cf_handle("channel_data").unwrap()
    }

    fn last_commit_index(&self) -> &ColumnFamily {
        self.db.cf_handle("last_commit_index").unwrap()
    }

    fn cfs() -> &'static [&'static str] {
        &["last_commit_index", "channel_data"]
    }

    fn options() -> Options {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options
    }
}

fn serialize_options() -> impl BincodeOptions {
    DefaultOptions::new()
        .with_big_endian()
        .with_fixint_encoding()
        .allow_trailing_bytes()
}
