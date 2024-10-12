use crate::types::{ChannelElementKey, ChannelId};
use serde::{Deserialize, Serialize};
use serde_with::formats::CommaSeparator;
use serde_with::serde_as;
use serde_with::StringWithSeparator;
use std::fmt;
use std::str::FromStr;

#[serde_as]
#[derive(Serialize, Deserialize)]
pub struct SubmitQuery {
    #[serde_as(as = "StringWithSeparator::<CommaSeparator, ChannelId>")]
    pub channels: Vec<ChannelId>,
}

#[serde_as]
#[derive(Serialize, Deserialize)]
pub struct TailQuery {
    pub from: ChannelElementKey,
    #[serde_as(as = "StringWithSeparator::<CommaSeparator, Metadata>")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub metadata: Vec<Metadata>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Metadata {
    Timestamp,
    Block,
    Commit,
}

#[serde_as]
#[derive(Clone, Serialize, Deserialize)]
pub struct ChannelElementWithMetadata {
    pub key: ChannelElementKey,
    #[serde_as(as = "serde_with::base64::Base64")]
    pub data: Vec<u8>,
    #[serde(skip_serializing_if = "ChannelElementMetadata::is_empty")]
    #[serde(default)]
    pub metadata: ChannelElementMetadata,
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct ChannelElementMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub timestamp_ns: Option<u64>,
    #[serde(skip_serializing_if = "String::is_empty")]
    #[serde(default)]
    pub block: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    #[serde(default)]
    pub commit: String,
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct NetworkInfo {
    pub chain_id: String,
    pub committee: Vec<String>,
}

impl ChannelElementMetadata {
    pub fn is_empty(&self) -> bool {
        self.timestamp_ns.is_none() && self.block.is_empty() && self.commit.is_empty()
    }
}

impl FromStr for Metadata {
    type Err = MetadataParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // not the most optimal hack using serde_json to parse enum variant...
        serde_json::from_str(&format!("\"{}\"", s)).map_err(|_| MetadataParseError(s.to_string()))
    }
}

impl fmt::Display for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // another hack to match display<->from_str
        write!(f, "{}", format!("{:?}", self).to_lowercase())
    }
}

#[derive(Debug)]
pub struct MetadataParseError(String);

impl fmt::Display for MetadataParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Unknown metadata field: {}", self.0)
    }
}

impl std::error::Error for MetadataParseError {}
