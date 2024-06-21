use bytes::Bytes;
use crate::block::ChainId;
use crate::committee::ValidatorInfo;
use crate::crypto::{Blake2Hasher, Hasher};

const GENERATION_LENGTH: usize = 32;

pub struct Genesis {
    chain_id: ChainId,
    data: Bytes,
    generation: [u8; GENERATION_LENGTH],
    validators: Vec<ValidatorInfo>,
}

impl Genesis {
    pub fn new(generation: [u8; GENERATION_LENGTH], validators: Vec<ValidatorInfo>) -> Self {
        let data = Bytes::from(bincode::serialize(&(&generation, &validators)).unwrap());
        let chain_id = ChainId(Blake2Hasher.hash_bytes(&data).0);
        Self {
            data,
            chain_id,
            generation,
            validators,
        }
    }

    pub fn load(data: Bytes) -> bincode::Result<Self> {
        let (generation, validators) = bincode::deserialize(&data)?;
        let chain_id = ChainId(Blake2Hasher.hash_bytes(&data).0);
        Ok(Self {
            data,
            chain_id,
            generation,
            validators,
        })
    }

    pub fn validators(&self) -> &[ValidatorInfo] {
        &self.validators
    }

    #[allow(dead_code)]
    pub fn generation(&self) -> &[u8; GENERATION_LENGTH] {
        &self.generation
    }

    pub fn chain_id(&self) -> &ChainId {
        &self.chain_id
    }

    pub fn data(&self) -> &Bytes {
        &self.data
    }
}