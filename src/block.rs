use crate::crypto::{Blake2Hasher, Hasher, SignatureVerifier, Signer};
use anyhow::ensure;
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Add;

pub struct Block {
    reference: BlockReference,
    signature: BlockSignature,
    parents: Vec<BlockReference>,
    payload_offset: usize,
    data: Bytes,
}

#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Ord, Eq, Hash, Serialize, Deserialize)]
pub struct BlockReference {
    pub round: Round,
    pub author: ValidatorIndex,
    pub hash: BlockHash,
}

#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Ord, Eq, Hash, Serialize, Deserialize)]
pub struct ValidatorIndex(pub u64);
#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Ord, Eq, Hash, Serialize, Deserialize)]
pub struct Round(pub u64);

#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Ord, Eq, Hash, Serialize, Deserialize)]
pub struct AuthorRound {
    pub author: ValidatorIndex,
    pub round: Round,
}

const SIGNATURE_LENGTH: usize = 64;
const BLOCK_HASH_LENGTH: usize = 32;
pub const MAX_PARENTS: usize = 1024;

#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Ord, Eq)]
pub struct BlockSignature(pub [u8; SIGNATURE_LENGTH]);
#[derive(
    Debug, Clone, Copy, PartialOrd, PartialEq, Ord, Eq, Hash, Serialize, Deserialize, Default,
)]
pub struct BlockHash(pub [u8; BLOCK_HASH_LENGTH]);

impl Block {
    const HASH_OFFSET: usize = 0;
    const SIGNATURE_OFFSET: usize = Self::HASH_OFFSET + BLOCK_HASH_LENGTH;
    const ROUND_OFFSET: usize = Self::SIGNATURE_OFFSET + SIGNATURE_LENGTH;
    const AUTHOR_OFFSET: usize = Self::ROUND_OFFSET + 8;
    const PARENTS_COUNT_OFFSET: usize = Self::AUTHOR_OFFSET + 8;
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

    pub fn author_from_bytes(data: &[u8]) -> anyhow::Result<ValidatorIndex> {
        ensure!(data.len() >= Self::PARENTS_OFFSET, "Block too small");
        Ok(ValidatorIndex(u64::from_le_bytes(
            data[Self::AUTHOR_OFFSET..Self::AUTHOR_OFFSET + 8]
                .try_into()
                .unwrap(),
        )))
    }

    pub fn new(
        round: Round,
        author: ValidatorIndex,
        payload: &[u8],
        parents: Vec<BlockReference>,
        signer: &impl Signer,
        hasher: &impl Hasher,
    ) -> Self {
        assert!(parents.len() < MAX_PARENTS);
        let parents_len = BlockReference::SIZE * parents.len();
        let payload_offset = Self::PARENTS_OFFSET + parents_len;
        let mut data = BytesMut::with_capacity(payload.len() + payload_offset);
        data.put_bytes(0, Self::ROUND_OFFSET);
        data.put_u64_le(round.0);
        data.put_u64_le(author.0);
        data.put_u32_le(parents.len() as u32);
        for parent in &parents {
            parent.write(&mut data);
        }
        data.put_slice(payload);
        let signature = signer.sign_bytes(&data[Self::ROUND_OFFSET..]);
        data[Self::SIGNATURE_OFFSET..Self::SIGNATURE_OFFSET + SIGNATURE_LENGTH]
            .copy_from_slice(&signature.0);
        let hash = hasher.hash_bytes(&data[Self::SIGNATURE_OFFSET..]);
        data[Self::HASH_OFFSET..Self::HASH_OFFSET + BLOCK_HASH_LENGTH].copy_from_slice(&hash.0);
        let data = data.into();

        Self {
            reference: BlockReference {
                round,
                author,
                hash,
            },
            signature,
            parents,
            payload_offset,
            data,
        }
    }

    pub fn from_bytes(
        data: Bytes,
        hasher: &impl Hasher,
        verifier: &impl SignatureVerifier,
    ) -> anyhow::Result<Self> {
        ensure!(data.len() >= Self::PARENTS_OFFSET, "Block too small");
        let unverified = Self::from_bytes_unchecked(data)?;
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

    pub fn from_bytes_unchecked(data: Bytes) -> anyhow::Result<Self> {
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
        let round = Round(u64::from_le_bytes(
            data[Self::ROUND_OFFSET..Self::ROUND_OFFSET + 8]
                .try_into()
                .unwrap(),
        ));
        let author = ValidatorIndex(u64::from_le_bytes(
            data[Self::AUTHOR_OFFSET..Self::AUTHOR_OFFSET + 8]
                .try_into()
                .unwrap(),
        ));
        let parents_count = u32::from_le_bytes(
            data[Self::PARENTS_COUNT_OFFSET..Self::PARENTS_COUNT_OFFSET + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        ensure!(
            parents_count < MAX_PARENTS,
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
        Ok(Self {
            reference,
            signature,
            parents,
            payload_offset: offset,
            data,
        })
    }

    pub fn genesis(author: ValidatorIndex) -> Self {
        Self::new(
            Round::ZERO,
            author,
            &[],
            vec![],
            &EmptySignature,
            &Blake2Hasher,
        )
    }
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
        let round = Round(u64::from_le_bytes(
            bytes[Self::ROUND_OFFSET..Self::ROUND_OFFSET + 8]
                .try_into()
                .unwrap(),
        ));
        let author = ValidatorIndex(u64::from_le_bytes(
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
        buf.put_u64_le(self.round.0);
        buf.put_u64_le(self.author.0);
        buf.put_slice(&self.hash.0);
    }

    pub fn author_round(&self) -> AuthorRound {
        AuthorRound::new(self.author, self.round)
    }

    pub fn round(&self) -> Round {
        self.round
    }
}

impl Round {
    pub const ZERO: Round = Round(0);

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

impl fmt::Display for Round {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:0>4}", self.0)
    }
}

impl fmt::Display for AuthorRound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", format_author_round(self.author, self.round))
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
        let round = Round(2);
        let author = ValidatorIndex(3);
        let payload = [1, 2, 3];
        let signature = [1u8; SIGNATURE_LENGTH];
        let hash = [2u8; BLOCK_HASH_LENGTH];
        let parent = BlockReference {
            round: Round(15),
            author: ValidatorIndex(16),
            hash: BlockHash([3u8; BLOCK_HASH_LENGTH]),
        };
        let block = Block::new(round, author, &payload, vec![parent], &signature, &hash);

        let block2 = Block::from_bytes_unchecked(block.data().clone()).unwrap();
        assert_eq!(block2.reference().author, author);
        assert_eq!(block2.reference().round, round);
        assert_eq!(block2.reference().hash.0, hash);
        assert_eq!(block2.signature().0, signature);
        assert_eq!(block2.parents().len(), 1);
        assert_eq!(block2.parents().get(0).unwrap(), &parent);
        assert_eq!(block2.payload(), &payload);

        assert!(Block::from_bytes(block.data().clone(), &hash, &signature).is_ok());
        let mut data = BytesMut::from(block.data().as_ref());
        data[0] = 5;
        assert!(Block::from_bytes(data.into(), &hash, &signature).is_err());
        let mut data = BytesMut::from(block.data().as_ref());
        data[Block::SIGNATURE_OFFSET + 1] = 5;
        assert!(Block::from_bytes(data.into(), &hash, &signature).is_err());
    }

    impl Hasher for [u8; BLOCK_HASH_LENGTH] {
        fn hash_bytes(&self, bytes: &[u8]) -> BlockHash {
            BlockHash(*self)
        }
    }

    impl Signer for [u8; SIGNATURE_LENGTH] {
        fn sign_bytes(&self, bytes: &[u8]) -> BlockSignature {
            BlockSignature(*self)
        }
    }

    impl SignatureVerifier for [u8; SIGNATURE_LENGTH] {
        fn check_signature(&self, bytes: &[u8], signature: &BlockSignature) -> bool {
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
        Arc::new(Block::new(
            Round(round),
            ValidatorIndex(author),
            &[],
            parents,
            &BR_SIGNATURE,
            &BR_BLOCK_HASH,
        ))
    }
}
