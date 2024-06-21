use crate::block::{BlockHash, BlockSignature, BLOCK_HASH_LENGTH};
use blake2::{Blake2b, Digest};
use ed25519_consensus::{SigningKey, VerificationKey};
use rand::{CryptoRng, RngCore};
use serde::{Deserialize, Serialize};

pub trait Signer: Send + 'static {
    fn sign_bytes(&self, bytes: &[u8]) -> BlockSignature;
}

pub trait SignatureVerifier {
    fn check_signature(&self, bytes: &[u8], signature: &BlockSignature) -> bool;
}

pub trait Hasher {
    fn hash_bytes(&self, bytes: &[u8]) -> BlockHash;
}

pub struct Blake2Hasher;

pub struct Ed25519Signer(Box<SigningKey>);
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Ed25519Verifier(VerificationKey);

pub fn generate_validator_key_pair(
    rng: &mut (impl RngCore + CryptoRng),
) -> (Ed25519Signer, Ed25519Verifier) {
    let signing_key = SigningKey::new(rng);
    let verification_key = signing_key.verification_key();
    (
        Ed25519Signer(Box::new(signing_key)),
        Ed25519Verifier(verification_key),
    )
}

impl Signer for Ed25519Signer {
    fn sign_bytes(&self, bytes: &[u8]) -> BlockSignature {
        BlockSignature(self.0.sign(bytes).to_bytes())
    }
}

impl AsRef<[u8]> for Ed25519Signer {
    fn as_ref(&self) -> &[u8] {
        (*self.0).as_ref()
    }
}

impl TryFrom<&[u8]> for Ed25519Signer {
    type Error = ed25519_consensus::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Ok(Self(Box::new(SigningKey::try_from(value)?)))
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
        BlockHash(blake2_hash(bytes))
    }
}

pub fn blake2_hash(bytes: &[u8]) -> [u8; BLOCK_HASH_LENGTH] {
    Blake2b::digest(bytes).into()
}

#[cfg(test)]
impl Default for Ed25519Verifier {
    fn default() -> Self {
        let k = SigningKey::from([0; 32]).verification_key();
        Ed25519Verifier(k)
    }
}

impl From<VerificationKey> for Ed25519Verifier {
    fn from(value: VerificationKey) -> Self {
        Self(value)
    }
}

impl From<SigningKey> for Ed25519Signer {
    fn from(value: SigningKey) -> Self {
        Self(Box::new(value))
    }
}
