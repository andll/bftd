use crate::crypto::{Hasher, SignatureVerifier, Signer};
use anyhow::ensure;
use bytes::{BufMut, Bytes, BytesMut};

pub struct Block {
    reference: BlockReference,
    signature: BlockSignature,
    data: Bytes,
}

#[derive(Clone, Copy, PartialOrd, PartialEq, Ord, Eq)]
pub struct BlockReference {
    round: Round,
    author: ValidatorIndex,
    hash: BlockHash,
}

#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Ord, Eq)]
pub struct ValidatorIndex(pub u64);
#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Ord, Eq)]
pub struct Round(pub u64);

const SIGNATURE_LENGTH: usize = 32;
const BLOCK_HASH_LENGTH: usize = 32;

#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Ord, Eq)]
pub struct BlockSignature(pub [u8; SIGNATURE_LENGTH]);
#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Ord, Eq)]
pub struct BlockHash(pub [u8; BLOCK_HASH_LENGTH]);

impl Block {
    const HASH_OFFSET: usize = 0;
    const SIGNATURE_OFFSET: usize = Self::HASH_OFFSET + BLOCK_HASH_LENGTH;
    const ROUND_OFFSET: usize = Self::SIGNATURE_OFFSET + SIGNATURE_LENGTH;
    const AUTHOR_OFFSET: usize = Self::ROUND_OFFSET + 8;
    const PAYLOAD_OFFSET: usize = Self::AUTHOR_OFFSET + 8;

    pub fn reference(&self) -> &BlockReference {
        &self.reference
    }

    pub fn signature(&self) -> &BlockSignature {
        &self.signature
    }

    pub fn payload(&self) -> &[u8] {
        &self.data[Self::PAYLOAD_OFFSET..]
    }

    pub fn data(&self) -> &Bytes {
        &self.data
    }

    pub fn new(
        round: Round,
        author: ValidatorIndex,
        payload: &[u8],
        signer: &impl Signer,
        hasher: &impl Hasher,
    ) -> Self {
        let mut data = BytesMut::with_capacity(payload.len() + Self::PAYLOAD_OFFSET);
        data.put_bytes(0, Self::ROUND_OFFSET);
        data.put_u64_le(round.0);
        data.put_u64_le(author.0);
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
            data,
        }
    }

    pub fn from_bytes(
        data: Bytes,
        hasher: &impl Hasher,
        verifier: &impl SignatureVerifier,
    ) -> anyhow::Result<Self> {
        ensure!(data.len() >= Self::PAYLOAD_OFFSET, "Block too small");
        let unverified = Self::from_bytes_unchecked(data);
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

    pub fn from_bytes_unchecked(data: Bytes) -> Self {
        assert!(data.len() >= Self::PAYLOAD_OFFSET);
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
        let reference = BlockReference {
            round,
            author,
            hash,
        };
        Self {
            reference,
            signature,
            data,
        }
    }
}

impl BlockReference {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_block_ser() {
        let round = Round(2);
        let author = ValidatorIndex(3);
        let payload = [1, 2, 3];
        let signature = [1u8; SIGNATURE_LENGTH];
        let hash = [2u8; BLOCK_HASH_LENGTH];
        let block = Block::new(round, author, &payload, &signature, &hash);

        let block2 = Block::from_bytes_unchecked(block.data().clone());
        assert_eq!(block2.reference().author, author);
        assert_eq!(block2.reference().round, round);
        assert_eq!(block2.reference().hash.0, hash);
        assert_eq!(block2.signature().0, signature);
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
}
