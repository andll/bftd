use crate::block::{BlockHash, BlockSignature};
use blake2::{Blake2b, Digest};
use ed25519_consensus::{SigningKey, VerificationKey};

pub trait Signer {
    fn sign_bytes(&self, bytes: &[u8]) -> BlockSignature;
}

pub trait SignatureVerifier {
    fn check_signature(&self, bytes: &[u8], signature: &BlockSignature) -> bool;
}

pub trait Hasher {
    fn hash_bytes(&self, bytes: &[u8]) -> BlockHash;
}

pub struct Blake2Hasher;

pub struct Ed25519Signer(SigningKey);
pub struct Ed25519Verifier(VerificationKey);

impl Signer for Ed25519Signer {
    fn sign_bytes(&self, bytes: &[u8]) -> BlockSignature {
        BlockSignature(self.0.sign(bytes).to_bytes())
    }
}

impl SignatureVerifier for Ed25519Verifier {
    fn check_signature(&self, bytes: &[u8], signature: &BlockSignature) -> bool {
        let signature = ed25519_consensus::Signature::from(signature.0);
        self.0.verify(&signature, bytes).is_ok()
    }
}

impl Hasher for Blake2Hasher {
    fn hash_bytes(&self, bytes: &[u8]) -> BlockHash {
        BlockHash(Blake2b::digest(bytes).into())
    }
}
