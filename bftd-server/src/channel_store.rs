use bytes::Bytes;
use parking_lot::Mutex;
use rocksdb::{ColumnFamily, Direction, IteratorMode, Options, WriteBatch, DB};
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::num::ParseIntError;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

pub struct ChannelStore {
    db: DB,
    notifiers: Mutex<HashMap<ChannelId, watch::Sender<u64>>>,
}

#[derive(Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Hash, Debug)]
pub struct ChannelId(u32);

impl ChannelStore {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let db = DB::open_cf(&Self::options(), path, Self::cfs())?;
        let notifiers = Default::default();
        Ok(Self { db, notifiers })
    }

    pub fn update_channels(
        &self,
        commit_index: u64,
        channels: impl Iterator<Item = (ChannelId, Bytes)>,
    ) {
        let mut batch = WriteBatch::default();
        let mut touched_channels = HashSet::new();
        for (element, (channel, data)) in channels.enumerate() {
            let element = element as u32;
            touched_channels.insert(channel);
            let key = ChannelElementKey {
                channel,
                commit_index,
                element,
            };
            let key = bincode::serialize(&key).expect("Failed to serialize key");
            batch.put_cf(self.channel_data(), key, data);
        }
        batch.put_cf(
            self.last_commit_index(),
            [],
            bincode::serialize(&commit_index).expect("Failed to serialize last commit index"),
        );
        self.db.write(batch).expect("Failed to write batch");
        let mut notifiers = self.notifiers.lock();
        for touched_channel in touched_channels {
            if let Entry::Occupied(oc) = notifiers.entry(touched_channel) {
                if oc.get().send(commit_index).is_err() {
                    oc.remove();
                }
            }
        }
    }

    pub fn subscribe_channel(
        self: &Arc<Self>,
        from_exclusive: ChannelElementKey,
    ) -> mpsc::Receiver<(ChannelElementKey, Bytes)> {
        let notifier = self.register(from_exclusive.channel);
        let (sender, receiver) = mpsc::channel(16);
        tokio::spawn(
            self.clone()
                .stream_channel(from_exclusive.increment(), notifier, sender),
        );
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
        mut next: ChannelElementKey,
        mut notifier: watch::Receiver<u64>,
        sender: mpsc::Sender<(ChannelElementKey, Bytes)>,
    ) {
        loop {
            let key = bincode::serialize(&next).expect("Failed to serialize key");
            let mut iterator = self.db.iterator_cf(
                self.channel_data(),
                IteratorMode::From(&key, Direction::Forward),
            );
            while let Some(r) = iterator.next() {
                let (key, value) = r.expect("Storage error");
                let key: ChannelElementKey =
                    bincode::deserialize(&key).expect("Failed to deserialize key");
                if key.channel != next.channel {
                    break;
                }
                if sender.send((key, value.into())).await.is_err() {
                    return;
                }
                next = key.increment();
            }
            if notifier.changed().await.is_err() {
                return;
            }
        }
    }

    pub fn last_processed_commit_index(&self) -> Option<u64> {
        let r = self
            .db
            .get_cf(self.last_commit_index(), [])
            .expect("DB error");
        let r = r?;
        Some(bincode::deserialize(&r).expect("Failed to deserialize last commit index"))
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChannelElementKey {
    channel: ChannelId,
    commit_index: u64,
    element: u32,
}

impl ChannelElementKey {
    pub fn increment(&self) -> Self {
        let mut this = *self;
        this.element += 1;
        this
    }
}

impl ChannelId {
    pub const LENGTH: usize = 4;

    pub fn from_slice(slice: &[u8]) -> Self {
        let arr = slice
            .try_into()
            .expect("ChannelId::from_slice incorrect slice size");
        Self(u32::from_be_bytes(arr))
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

impl fmt::Display for ChannelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:08x}", self.0)
    }
}

impl FromStr for ChannelId {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(u32::from_str_radix(s, 16)?))
    }
}

impl fmt::Display for ChannelElementKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:08x}-{:08x}-{:08x}-{:08x}",
            self.channel.0, 0, self.commit_index, self.element
        )
    }
}

impl Serialize for ChannelElementKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ChannelElementKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = ChannelElementKey;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "ChannelElementKey")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                let mut split = v.split('-');
                let channel = parse::<E>(split.next())?;
                let epoch = split.next();
                if epoch.is_none() {
                    return Ok(ChannelElementKey {
                        channel: ChannelId(channel),
                        commit_index: 0,
                        element: 0,
                    });
                }
                let epoch = parse::<E>(epoch)?;
                let commit = parse::<E>(split.next())?;
                let element = parse::<E>(split.next())?;
                if epoch != 0 {
                    return Err(E::custom("epoch is not 0"));
                }
                Ok(ChannelElementKey {
                    channel: ChannelId(channel),
                    commit_index: commit as u64,
                    element,
                })
            }
        }
        deserializer.deserialize_str(V)
    }
}

fn parse<E: Error>(s: Option<&str>) -> Result<u32, E> {
    if let Some(s) = s {
        u32::from_str_radix(s, 16).map_err(|_| E::custom("Failed to parse element"))
    } else {
        Err(E::custom("Not enough elements"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_element_serde() {
        assert_eq!(
            p("\"15\""),
            ChannelElementKey {
                channel: ChannelId(0x15),
                commit_index: 0,
                element: 0,
            }
        );
        let k = ChannelElementKey {
            channel: ChannelId(20),
            commit_index: 30,
            element: 40,
        };
        assert_eq!(p(&serde_json::to_string(&k).unwrap()), k)
    }

    fn p(s: &str) -> ChannelElementKey {
        serde_json::from_str(s).unwrap()
    }
}
