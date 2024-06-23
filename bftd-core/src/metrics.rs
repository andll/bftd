use prometheus::{exponential_buckets, Histogram, IntCounter, IntGauge, Registry};
use std::sync::Arc;

pub struct Metrics {
    pub core_last_proposed_round: IntGauge,
    pub syncer_last_committed_round: IntGauge,
    pub syncer_last_commit_index: IntGauge,
    pub syncer_leader_timeouts: IntCounter,
    pub syncer_received_block_age_ms: Histogram,
    pub rpc_connected_peers: IntGauge,
}

#[macro_export]
macro_rules! gauge (
    ($name:expr, $r:expr) => {prometheus::register_int_gauge_with_registry!($name, $name, $r).unwrap()};
);
#[macro_export]
macro_rules! counter (
    ($name:expr, $r:expr) => {prometheus::register_int_counter_with_registry!($name, $name, $r).unwrap()};
);
#[macro_export]
macro_rules! histogram (
    ($name:expr, $buck:expr, $r:expr) => {prometheus::register_histogram_with_registry!($name, $name, $buck.unwrap(), $r).unwrap()};
);

impl Metrics {
    pub fn new() -> Arc<Self> {
        Self::new_in_registry(&Registry::default())
    }

    pub fn new_in_registry(registry: &Registry) -> Arc<Self> {
        Arc::new(Self {
            core_last_proposed_round: gauge!("core_last_proposed_round", registry),
            syncer_last_committed_round: gauge!("syncer_last_committed_round", registry),
            syncer_last_commit_index: gauge!("syncer_last_commit_index", registry),
            syncer_leader_timeouts: counter!("syncer_leader_timeouts", registry),
            syncer_received_block_age_ms: histogram!(
                "syncer_received_block_age_ms",
                exponential_buckets(1., 2., 14),
                registry
            ),
            rpc_connected_peers: gauge!("rpc_connected_peers", registry),
        })
    }
}
