use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::num::ParseIntError;
use std::str::FromStr;

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug, Ord, PartialOrd)]
pub struct ChannelId(pub u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct ChannelElementKey {
    pub channel: ChannelId,
    pub commit_index: u64,
    pub element: u32,
}

impl ChannelElementKey {
    pub fn increment(&self) -> Self {
        let mut this = *self;
        this.element += 1;
        this
    }

    pub fn channel_start(channel: ChannelId) -> Self {
        // todo confirm commit 0 never have any payload
        Self {
            channel,
            commit_index: 0,
            element: 0,
        }
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

impl FromStr for ChannelElementKey {
    type Err = ChannelElementKeyParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut split = s.split('-');
        let channel = parse(split.next())?;
        let epoch = split.next();
        if epoch.is_none() {
            return Ok(ChannelElementKey {
                channel: ChannelId(channel),
                commit_index: 0,
                element: 0,
            });
        }
        let epoch = parse(epoch)?;
        let commit = parse(split.next())?;
        let element = parse(split.next())?;
        if epoch != 0 {
            return Err(ChannelElementKeyParseError::InvalidEpoch);
        }
        if split.next().is_some() {
            return Err(ChannelElementKeyParseError::TooManyElements);
        }
        Ok(ChannelElementKey {
            channel: ChannelId(channel),
            commit_index: commit as u64,
            element,
        })
    }
}

#[derive(Clone, Debug)]
pub enum ChannelElementKeyParseError {
    ElementParseError(ParseIntError),
    NotEnoughElements,
    TooManyElements,
    InvalidEpoch, // temporary
}

impl From<ParseIntError> for ChannelElementKeyParseError {
    fn from(value: ParseIntError) -> Self {
        Self::ElementParseError(value)
    }
}

impl fmt::Display for ChannelElementKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}-{:08x}-{:08x}-{:08x}",
            self.channel, 0, self.commit_index, self.element
        )
    }
}

impl Serialize for ChannelId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ChannelId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = ChannelId;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "ChannelId")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                let v = u32::from_str_radix(v, 16)
                    .map_err(|_| E::custom("Failed to parse channel id"))?;
                Ok(ChannelId(v))
            }
        }
        deserializer.deserialize_str(V)
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
                Self::Value::from_str(v).map_err(E::custom)
            }
        }
        deserializer.deserialize_str(V)
    }
}

fn parse(s: Option<&str>) -> Result<u32, ChannelElementKeyParseError> {
    if let Some(s) = s {
        Ok(u32::from_str_radix(s, 16)?)
    } else {
        Err(ChannelElementKeyParseError::NotEnoughElements)
    }
}

impl fmt::Display for ChannelElementKeyParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChannelElementKeyParseError::ElementParseError(e) => {
                write!(f, "Element parse error: {e}")
            }
            ChannelElementKeyParseError::NotEnoughElements => write!(f, "Not enough elements"),
            ChannelElementKeyParseError::TooManyElements => write!(f, "Too many elements"),
            ChannelElementKeyParseError::InvalidEpoch => write!(f, "Invalid epoch"),
        }
    }
}

impl std::error::Error for ChannelElementKeyParseError {}

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

    #[test]
    fn test_channel_serde() {
        let ch = ChannelId(0x2478);
        let serialized = serde_json::to_string(&ch).unwrap();
        let deserialized: ChannelId = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, ch)
    }

    fn p(s: &str) -> ChannelElementKey {
        serde_json::from_str(s).unwrap()
    }
}
