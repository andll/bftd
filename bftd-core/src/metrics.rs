use prometheus::{
    exponential_buckets, register_histogram_with_registry, register_int_gauge_with_registry,
    Histogram, IntGauge, Registry,
};
use std::sync::Arc;

pub struct Metrics {
    pub core_last_proposed_round: IntGauge,
    pub syncer_received_block_age_ms: Histogram,
    pub rpc_connected_peers: IntGauge,
}

macro_rules! gauge (
    ($name:expr, $r:expr) => {register_int_gauge_with_registry!($name, $name, $r).unwrap()};
);
macro_rules! histogram (
    ($name:expr, $buck:expr, $r:expr) => {register_histogram_with_registry!($name, $name, $buck.unwrap(), $r).unwrap()};
);

impl Metrics {
    pub fn new() -> Arc<Self> {
        Self::new_in_registry(&Registry::default())
    }

    pub fn new_in_registry(registry: &Registry) -> Arc<Self> {
        Arc::new(Self {
            core_last_proposed_round: gauge!("core_last_proposed_round", registry),
            rpc_connected_peers: gauge!("rpc_connected_peers", registry),
            syncer_received_block_age_ms: histogram!(
                "syncer_received_block_age_ms",
                exponential_buckets(1., 2., 14),
                registry
            ),
        })
    }
}
