use crate::block::{BlockHash, BlockSignature};

pub trait Signer {
    fn sign_bytes(&self, bytes: &[u8]) -> BlockSignature;
}

pub trait SignatureVerifier {
    fn check_signature(&self, bytes: &[u8], signature: &BlockSignature) -> bool;
}

pub trait Hasher {
    fn hash_bytes(&self, bytes: &[u8]) -> BlockHash;
}
