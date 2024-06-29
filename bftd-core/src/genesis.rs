use crate::block::ChainId;
use crate::committee::{Committee, ValidatorInfo};
use crate::crypto::blake2_hash;
use crate::protocol_config::ProtocolConfig;
use bytes::Bytes;
use std::sync::Arc;

const GENERATION_LENGTH: usize = 32;

pub struct Genesis {
    chain_id: ChainId,
    data: Bytes,
    generation: [u8; GENERATION_LENGTH],
    validators: Vec<ValidatorInfo>,
    protocol_config: ProtocolConfig,
}

impl Genesis {
    pub fn new(
        generation: [u8; GENERATION_LENGTH],
        validators: Vec<ValidatorInfo>,
        protocol_config: ProtocolConfig,
    ) -> Self {
        let data =
            Bytes::from(bincode::serialize(&(&generation, &validators, &protocol_config)).unwrap());
        let chain_id = ChainId(blake2_hash(&data));
        Self {
            data,
            chain_id,
            generation,
            validators,
            protocol_config,
        }
    }

    pub fn load(data: Bytes) -> bincode::Result<Self> {
        let (generation, validators, protocol_config) = bincode::deserialize(&data)?;
        let chain_id = ChainId(blake2_hash(&data));
        Ok(Self {
            data,
            chain_id,
            generation,
            validators,
            protocol_config,
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

    pub fn protocol_config(&self) -> &ProtocolConfig {
        &self.protocol_config
    }

    pub fn make_committee(&self) -> Arc<Committee> {
        Arc::new(Committee::new(self.chain_id, self.validators.clone()))
    }
}
