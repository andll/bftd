use crate::crypto::{Blake2Hasher, Hasher, SignatureVerifier, Signer};
use crate::metrics::Metrics;
use crate::network::MAX_NETWORK_PAYLOAD;
use anyhow::ensure;
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Add;
use std::sync::Arc;

/*
   Block verification:
   * Basic structure - Block::from_bytes:
     * reference.hash matches block content
     * parents are specified as a correct vector
     * signature matches block content
   * Correctness against current the current committee - Committee::verify_block:
     * reference.author specified in the block exists in committee
     * signature on the block belongs to reference.author
     * block reference matches source of the block:
       * for subscription, reference.author matches subscribed validator
       * for response for get_block RPC - reference matches requested reference
     * time_ns upper bound - time_ns is not in the future comparing to current time
     * chain_id of a block matches chain_id of the committee
   * Block stored locally has all parents - BlockManager
   * Block payload matches application-specific format - BlockFilter
   * TODO:
     * parents vector clock
     * time_ns lower bound
*/
pub struct Block {
    /// Reference of the current block
    reference: BlockReference,
    /// Signature over block data
    signature: BlockSignature,
    /// Id of the chain
    chain_id: ChainId,
    /// Timestamp when block was created. Number of nanoseconds form unix epoch.
    time_ns: u64, // todo verify
    /// References to parent blocks
    parents: Vec<BlockReference>,
    payload_offset: usize,
    data: Bytes,
    metrics: Option<Arc<Metrics>>,
}

#[derive(Clone, Copy, PartialOrd, PartialEq, Ord, Eq, Hash, Serialize, Deserialize)]
pub struct BlockReference {
    /// Round of the block
    pub round: Round,
    /// Author of the block
    pub author: ValidatorIndex,
    /// Hash of the block
    pub hash: BlockHash,
}

#[derive(Default, Clone, Copy, PartialOrd, PartialEq, Ord, Eq, Hash, Serialize, Deserialize)]
pub struct ValidatorIndex(pub u64);
#[derive(
    Default, Debug, Clone, Copy, PartialOrd, PartialEq, Ord, Eq, Hash, Serialize, Deserialize,
)]
pub struct Round(pub u64);

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChainId(pub [u8; CHAIN_ID_LENGTH]);

#[derive(Default, Clone, Copy, PartialOrd, PartialEq, Ord, Eq, Hash, Serialize, Deserialize)]
pub struct AuthorRound {
    pub author: ValidatorIndex,
    pub round: Round,
}

const SIGNATURE_LENGTH: usize = 64;
pub const BLOCK_HASH_LENGTH: usize = 32;
const CHAIN_ID_LENGTH: usize = 32;
pub const MAX_PARENTS: usize = 128;

pub const MAX_BLOCK_PAYLOAD: usize =
    MAX_BLOCK_SIZE - (Block::PARENTS_OFFSET + BlockReference::SIZE * MAX_PARENTS);
pub const MAX_BLOCK_SIZE: usize = MAX_NETWORK_PAYLOAD - 256;

#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Ord, Eq)]
pub struct BlockSignature(pub [u8; SIGNATURE_LENGTH]);
#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Ord, Eq, Hash, Serialize, Deserialize)]
pub struct BlockHash(pub [u8; BLOCK_HASH_LENGTH]);

impl Block {
    const HASH_OFFSET: usize = 0;
    const SIGNATURE_OFFSET: usize = Self::HASH_OFFSET + BLOCK_HASH_LENGTH;
    const ROUND_OFFSET: usize = Self::SIGNATURE_OFFSET + SIGNATURE_LENGTH;
    const AUTHOR_OFFSET: usize = Self::ROUND_OFFSET + 8;
    const CHAIN_ID_OFFSET: usize = Self::AUTHOR_OFFSET + 8;
    const TIME_OFFSET: usize = Self::CHAIN_ID_OFFSET + CHAIN_ID_LENGTH;
    const PARENTS_COUNT_OFFSET: usize = Self::TIME_OFFSET + 8;
    const PARENTS_OFFSET: usize = Self::PARENTS_COUNT_OFFSET + 4;

    pub fn reference(&self) -> &BlockReference {
        &self.reference
    }

    pub fn signature(&self) -> &BlockSignature {
        &self.signature
    }

    pub fn payload(&self) -> &[u8] {
        &self.data[self.payload_offset..]
    }

    pub fn payload_bytes(&self) -> Bytes {
        self.data.slice(self.payload_offset..)
    }

    pub fn data(&self) -> &Bytes {
        &self.data
    }

    pub fn parents(&self) -> &[BlockReference] {
        &self.parents
    }

    pub fn author(&self) -> ValidatorIndex {
        self.reference.author
    }

    pub fn round(&self) -> Round {
        self.reference.round
    }

    pub fn block_hash(&self) -> &BlockHash {
        &self.reference.hash
    }

    pub fn author_round(&self) -> AuthorRound {
        self.reference.author_round()
    }

    pub fn time_ns(&self) -> u64 {
        self.time_ns
    }

    pub fn reference_from_block_bytes(data: &[u8]) -> anyhow::Result<BlockReference> {
        ensure!(data.len() >= Self::PARENTS_OFFSET, "Block too small");
        let hash = BlockHash(
            data[Self::HASH_OFFSET..Self::HASH_OFFSET + BLOCK_HASH_LENGTH]
                .try_into()
                .unwrap(),
        );
        let round = Round(u64::from_be_bytes(
            data[Self::ROUND_OFFSET..Self::ROUND_OFFSET + 8]
                .try_into()
                .unwrap(),
        ));
        let author = ValidatorIndex(u64::from_be_bytes(
            data[Self::AUTHOR_OFFSET..Self::AUTHOR_OFFSET + 8]
                .try_into()
                .unwrap(),
        ));
        Ok(BlockReference {
            round,
            author,
            hash,
        })
    }

    pub fn chain_id(&self) -> &ChainId {
        &self.chain_id
    }

    pub fn new(
        round: Round,
        author: ValidatorIndex,
        chain_id: ChainId,
        time_ns: u64,
        payload: &[u8],
        parents: Vec<BlockReference>,
        signer: &impl Signer,
        hasher: &impl Hasher,
        metrics: Option<Arc<Metrics>>,
    ) -> Self {
        assert!(parents.len() <= MAX_PARENTS);
        assert!(payload.len() <= MAX_BLOCK_PAYLOAD);
        let parents_len = BlockReference::SIZE * parents.len();
        let payload_offset = Self::PARENTS_OFFSET + parents_len;
        let mut data = BytesMut::with_capacity(payload.len() + payload_offset);
        data.put_bytes(0, Self::ROUND_OFFSET);
        // todo change all encoding to be
        data.put_u64(round.0);
        data.put_u64(author.0);
        data.put_slice(&chain_id.0);
        data.put_u64(time_ns);
        data.put_u32(parents.len() as u32);
        for parent in &parents {
            assert!(
                parent.round < round,
                "Including parent {}, our round {}",
                parent,
                round
            );
            parent.write(&mut data);
        }
        data.put_slice(payload);
        let signature = signer.sign_bytes(&data[Self::ROUND_OFFSET..]);
        data[Self::SIGNATURE_OFFSET..Self::SIGNATURE_OFFSET + SIGNATURE_LENGTH]
            .copy_from_slice(&signature.0);
        let hash = hasher.hash_bytes(&data[Self::SIGNATURE_OFFSET..]);
        data[Self::HASH_OFFSET..Self::HASH_OFFSET + BLOCK_HASH_LENGTH].copy_from_slice(&hash.0);
        let data = data.into();

        Self::metrics_block_created(&metrics, &data);

        Self {
            reference: BlockReference {
                round,
                author,
                hash,
            },
            signature,
            chain_id,
            time_ns,
            parents,
            payload_offset,
            data,
            metrics,
        }
    }

    pub fn from_bytes(
        data: Bytes,
        hasher: &impl Hasher,
        verifier: &impl SignatureVerifier,
        metrics: Option<Arc<Metrics>>,
    ) -> anyhow::Result<Self> {
        ensure!(data.len() >= Self::PARENTS_OFFSET, "Block too small");
        let unverified = Self::from_bytes_unchecked(data, metrics)?;
        unverified.verify(hasher, verifier)
    }

    fn verify(
        self,
        hasher: &impl Hasher,
        verifier: &impl SignatureVerifier,
    ) -> anyhow::Result<Self> {
        let hash = hasher.hash_bytes(&self.data[Self::SIGNATURE_OFFSET..]);
        ensure!(
            hash == self.reference.hash,
            "Block hash does not match block content"
        );
        ensure!(
            verifier.check_signature(&self.data[Self::ROUND_OFFSET..], &self.signature),
            "Block signature verification failed"
        );
        Ok(self)
    }

    pub fn from_bytes_unchecked(
        data: Bytes,
        metrics: Option<Arc<Metrics>>,
    ) -> anyhow::Result<Self> {
        assert!(data.len() >= Self::PARENTS_OFFSET);
        let hash = BlockHash(
            data[Self::HASH_OFFSET..Self::HASH_OFFSET + BLOCK_HASH_LENGTH]
                .try_into()
                .unwrap(),
        );
        let signature = BlockSignature(
            data[Self::SIGNATURE_OFFSET..Self::SIGNATURE_OFFSET + SIGNATURE_LENGTH]
                .try_into()
                .unwrap(),
        );
        let round = Round(u64::from_be_bytes(
            data[Self::ROUND_OFFSET..Self::ROUND_OFFSET + 8]
                .try_into()
                .unwrap(),
        ));
        let author = ValidatorIndex(u64::from_be_bytes(
            data[Self::AUTHOR_OFFSET..Self::AUTHOR_OFFSET + 8]
                .try_into()
                .unwrap(),
        ));
        let chain_id = ChainId(
            data[Self::CHAIN_ID_OFFSET..Self::CHAIN_ID_OFFSET + CHAIN_ID_LENGTH]
                .try_into()
                .unwrap(),
        );
        let time_ns = u64::from_be_bytes(
            data[Self::TIME_OFFSET..Self::TIME_OFFSET + 8]
                .try_into()
                .unwrap(),
        );
        let parents_count = u32::from_be_bytes(
            data[Self::PARENTS_COUNT_OFFSET..Self::PARENTS_COUNT_OFFSET + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        ensure!(
            parents_count <= MAX_PARENTS,
            "Block has too many parents {parents_count}"
        );
        ensure!(
            data.len() >= Self::PARENTS_OFFSET + parents_count * BlockReference::SIZE,
            "Block does not list all parents"
        );
        let mut parents = Vec::with_capacity(parents_count);
        let mut offset = Self::PARENTS_OFFSET;
        for _ in 0..parents_count {
            let parent = BlockReference::from_bytes(
                &data[offset..offset + BlockReference::SIZE]
                    .try_into()
                    .unwrap(),
            );
            parents.push(parent);
            offset += BlockReference::SIZE;
        }
        let reference = BlockReference {
            round,
            author,
            hash,
        };
        Self::metrics_block_created(&metrics, &data);
        Ok(Self {
            reference,
            signature,
            chain_id,
            time_ns,
            parents,
            payload_offset: offset,
            data,
            metrics,
        })
    }

    fn metrics_block_created(metrics: &Option<Arc<Metrics>>, data: &Bytes) {
        if let Some(metrics) = &metrics {
            metrics.blocks_loaded_bytes.add(data.len() as i64);
            metrics.blocks_loaded.inc();
        }
    }

    pub fn genesis(chain_id: ChainId, author: ValidatorIndex) -> Self {
        Self::new(
            Round::ZERO,
            author,
            chain_id,
            0, /* time_ns */
            &[],
            vec![],
            &EmptySignature,
            &Blake2Hasher,
            None,
        )
    }
}

impl Drop for Block {
    fn drop(&mut self) {
        if let Some(metrics) = &self.metrics {
            metrics.blocks_loaded_bytes.sub(self.data.len() as i64);
            metrics.blocks_loaded.dec();
        }
    }
}

impl BlockHash {
    pub const MIN: Self = Self([0x0; BLOCK_HASH_LENGTH]);
    pub const MAX: Self = Self([0xff; BLOCK_HASH_LENGTH]);
}

impl ChainId {
    pub const CHAIN_ID_TEST: Self = Self([1; CHAIN_ID_LENGTH]);
}

struct EmptySignature;

impl Signer for EmptySignature {
    fn sign_bytes(&self, _bytes: &[u8]) -> BlockSignature {
        BlockSignature::NONE
    }
}

impl BlockReference {
    const ROUND_OFFSET: usize = 0;
    const AUTHOR_OFFSET: usize = Self::ROUND_OFFSET + 8;
    const HASH_OFFSET: usize = Self::AUTHOR_OFFSET + 8;
    pub const SIZE: usize = Self::HASH_OFFSET + BLOCK_HASH_LENGTH;

    pub fn from_bytes(bytes: &[u8; Self::SIZE]) -> Self {
        // todo change to be bytes
        let round = Round(u64::from_be_bytes(
            bytes[Self::ROUND_OFFSET..Self::ROUND_OFFSET + 8]
                .try_into()
                .unwrap(),
        ));
        let author = ValidatorIndex(u64::from_be_bytes(
            bytes[Self::AUTHOR_OFFSET..Self::AUTHOR_OFFSET + 8]
                .try_into()
                .unwrap(),
        ));
        let hash = BlockHash(
            bytes[Self::HASH_OFFSET..Self::HASH_OFFSET + BLOCK_HASH_LENGTH]
                .try_into()
                .unwrap(),
        );
        Self {
            round,
            author,
            hash,
        }
    }

    pub fn write(&self, buf: &mut BytesMut) {
        buf.put_u64(self.round.0);
        buf.put_u64(self.author.0);
        buf.put_slice(&self.hash.0);
    }

    pub fn round_author_hash_encoding(&self) -> [u8; Self::SIZE] {
        let mut r = [0u8; Self::SIZE];
        r[..8].copy_from_slice(&self.round.0.to_be_bytes());
        r[8..16].copy_from_slice(&self.author.0.to_be_bytes());
        r[16..].copy_from_slice(&self.hash.0);
        r
    }

    pub fn author_round_hash_encoding(&self) -> [u8; Self::SIZE] {
        let mut r = [0u8; Self::SIZE];
        r[..8].copy_from_slice(&self.author.0.to_be_bytes());
        r[8..16].copy_from_slice(&self.round.0.to_be_bytes());
        r[16..].copy_from_slice(&self.hash.0);
        r
    }

    pub fn author_round(&self) -> AuthorRound {
        AuthorRound::new(self.author, self.round)
    }

    pub fn round(&self) -> Round {
        self.round
    }

    pub fn first_block_reference_for_round(round: Round) -> Self {
        Self {
            round,
            author: ValidatorIndex(0),
            hash: BlockHash::MIN,
        }
    }
    pub fn first_block_reference_for_round_author(round: Round, author: ValidatorIndex) -> Self {
        Self {
            round,
            author,
            hash: BlockHash::MIN,
        }
    }
    pub fn last_block_reference_for_round_author(round: Round, author: ValidatorIndex) -> Self {
        Self {
            round,
            author,
            hash: BlockHash::MAX,
        }
    }
}

impl Round {
    pub const ZERO: Round = Round(0);
    pub const MAX: Round = Round(u64::MAX);

    pub fn previous(&self) -> Self {
        assert!(self.0 > 0);
        Self(self.0 - 1)
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

impl AuthorRound {
    pub fn new(author: ValidatorIndex, round: Round) -> Self {
        Self { author, round }
    }
}

impl ValidatorIndex {
    pub fn slice_get<'a, T>(&self, v: &'a [T]) -> &'a T {
        &v[self.0 as usize]
    }
}

impl PartialEq for Block {
    fn eq(&self, other: &Self) -> bool {
        self.reference == other.reference
    }
}

impl fmt::Display for ValidatorIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 > 26 {
            write!(f, "V{}", self.0)
        } else {
            let l = ('A' as u8 + self.0 as u8) as char;
            write!(f, "{l}")
        }
    }
}

impl fmt::Debug for ValidatorIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl fmt::Display for Round {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:0>6}", self.0)
    }
}

impl fmt::Display for AuthorRound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", format_author_round(self.author, self.round))
    }
}

impl fmt::Debug for AuthorRound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl fmt::Display for BlockHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.0[..4]))
    }
}

impl fmt::Display for BlockReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.author_round(), self.hash)
    }
}

impl fmt::Debug for BlockReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl fmt::Display for Block {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}[", self.reference())?;
        for parent in &self.parents {
            write!(f, "{},", parent)?;
        }
        write!(f, "]")
    }
}

impl BlockSignature {
    const NONE: Self = Self([0; SIGNATURE_LENGTH]);
}

impl Add for Round {
    type Output = Round;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

impl Add<u64> for Round {
    type Output = Round;

    fn add(self, rhs: u64) -> Self::Output {
        Self(self.0 + rhs)
    }
}

pub fn format_author_round(index: ValidatorIndex, round: Round) -> String {
    format!("{}{}", index, round)
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    pub fn test_block_ser() {
        let round = Round(17);
        let author = ValidatorIndex(3);
        let payload = [1, 2, 3];
        let signature = [1u8; SIGNATURE_LENGTH];
        let hash = [2u8; BLOCK_HASH_LENGTH];
        let parent = BlockReference {
            round: Round(15),
            author: ValidatorIndex(16),
            hash: BlockHash([3u8; BLOCK_HASH_LENGTH]),
        };
        let block = Block::new(
            round,
            author,
            ChainId::CHAIN_ID_TEST,
            15,
            &payload,
            vec![parent],
            &signature,
            &hash,
            None,
        );

        let block2 = Block::from_bytes_unchecked(block.data().clone(), None).unwrap();
        assert_eq!(block2.reference().author, author);
        assert_eq!(block2.reference().round, round);
        assert_eq!(block2.reference().hash.0, hash);
        assert_eq!(block2.signature().0, signature);
        assert_eq!(block2.chain_id, ChainId::CHAIN_ID_TEST);
        assert_eq!(block2.time_ns, 15);
        assert_eq!(block2.parents().len(), 1);
        assert_eq!(block2.parents().get(0).unwrap(), &parent);
        assert_eq!(block2.payload(), &payload);

        assert!(Block::from_bytes(block.data().clone(), &hash, &signature, None).is_ok());
        let mut data = BytesMut::from(block.data().as_ref());
        data[0] = 5;
        assert!(Block::from_bytes(data.into(), &hash, &signature, None).is_err());
        let mut data = BytesMut::from(block.data().as_ref());
        data[Block::SIGNATURE_OFFSET + 1] = 5;
        assert!(Block::from_bytes(data.into(), &hash, &signature, None).is_err());
    }

    impl Hasher for [u8; BLOCK_HASH_LENGTH] {
        fn hash_bytes(&self, _bytes: &[u8]) -> BlockHash {
            BlockHash(*self)
        }
    }

    impl Signer for [u8; SIGNATURE_LENGTH] {
        fn sign_bytes(&self, _bytes: &[u8]) -> BlockSignature {
            BlockSignature(*self)
        }
    }

    impl SignatureVerifier for [u8; SIGNATURE_LENGTH] {
        fn check_signature(&self, _bytes: &[u8], signature: &BlockSignature) -> bool {
            signature.0 == *self
        }
    }

    const BR_BLOCK_HASH: [u8; BLOCK_HASH_LENGTH] = [2u8; BLOCK_HASH_LENGTH];
    const BR_SIGNATURE: [u8; SIGNATURE_LENGTH] = [3u8; SIGNATURE_LENGTH];

    pub fn br(author: u64, round: u64) -> BlockReference {
        BlockReference {
            author: ValidatorIndex(author),
            round: Round(round),
            hash: BlockHash(BR_BLOCK_HASH),
        }
    }

    pub fn blk(author: u64, round: u64, parents: Vec<BlockReference>) -> Arc<Block> {
        blk_p(author, round, parents, &[])
    }

    pub fn blk_p(
        author: u64,
        round: u64,
        parents: Vec<BlockReference>,
        payload: &[u8],
    ) -> Arc<Block> {
        Arc::new(Block::new(
            Round(round),
            ValidatorIndex(author),
            ChainId::CHAIN_ID_TEST,
            0, /* time_ns */
            payload,
            parents,
            &BR_SIGNATURE,
            &BR_BLOCK_HASH,
            None,
        ))
    }
}
