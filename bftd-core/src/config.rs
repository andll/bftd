use crate::block::ValidatorIndex;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct BftdConfig {
    pub bind: Option<String>,
    pub validator_index: ValidatorIndex,
}
