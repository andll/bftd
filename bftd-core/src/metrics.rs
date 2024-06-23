use crate::block::ValidatorIndex;
use crate::committee::Committee;
use prometheus::{exponential_buckets, HistogramVec, IntCounter, IntGauge, Registry};
use std::sync::Arc;

pub struct Metrics {
    pub core_last_proposed_round: IntGauge,
    pub syncer_last_committed_round: IntGauge,
    pub syncer_last_commit_index: IntGauge,
    pub syncer_leader_timeouts: IntCounter,
    pub syncer_received_block_age_ms: HistogramVec,
    pub rpc_connected_peers: IntGauge,
    validator_labels: Vec<String>,
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
macro_rules! histogram_vec (
    ($name:expr, $labels:expr, $buck:expr, $r:expr) => {prometheus::register_histogram_vec_with_registry!($name, $name, $labels, $buck.unwrap(), $r).unwrap()};
);

#[macro_export]
macro_rules! histogram (
    ($name:expr, $buck:expr, $r:expr) => {prometheus::register_histogram_with_registry!($name, $name, $buck.unwrap(), $r).unwrap()}
);

impl Metrics {
    pub fn new_test() -> Arc<Self> {
        Self::new_inner(&Registry::default(), vec![])
    }

    pub fn new_in_registry(registry: &Registry, committee: &Committee) -> Arc<Self> {
        let validator_labels = committee
            .enumerate_indexes()
            .map(|i| i.to_string())
            .collect();
        Self::new_inner(registry, validator_labels)
    }

    pub fn validator_label(&self, i: ValidatorIndex) -> &str {
        &i.slice_get(&self.validator_labels)
    }

    fn new_inner(registry: &Registry, validator_labels: Vec<String>) -> Arc<Self> {
        Arc::new(Self {
            core_last_proposed_round: gauge!("core_last_proposed_round", registry),
            syncer_last_committed_round: gauge!("syncer_last_committed_round", registry),
            syncer_last_commit_index: gauge!("syncer_last_commit_index", registry),
            syncer_leader_timeouts: counter!("syncer_leader_timeouts", registry),
            syncer_received_block_age_ms: histogram_vec!(
                "syncer_received_block_age_ms",
                &["source"],
                exponential_buckets(1., 2., 14),
                registry
            ),
            rpc_connected_peers: gauge!("rpc_connected_peers", registry),
            validator_labels,
        })
    }
}
