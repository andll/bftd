use bftd_core::block::ValidatorIndex;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Serialize, Deserialize)]
pub struct BftdConfig {
    pub bind: Option<String>,
    pub validator_index: ValidatorIndex,
    pub prometheus_bind: Option<SocketAddr>,
    pub http_server_bind: Option<SocketAddr>,
    // Load gen config in form transaction_size[::tps_limit]
    pub load_gen: Option<String>,
}
