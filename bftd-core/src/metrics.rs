use crate::block::ValidatorIndex;
use crate::committee::Committee;
use prometheus::{
    exponential_buckets, Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge, Registry,
};
use std::sync::Arc;
use std::time::Instant;

pub struct Metrics {
    pub block_manager_missing_inverse_len: IntGauge,
    pub block_manager_added: IntCounter,
    pub block_manager_rejected: IntCounter,

    pub rocks_store_put_bytes: IntCounterVec,

    pub blocks_loaded: IntGauge,
    pub blocks_loaded_bytes: IntGauge,

    pub commit_rule_utilization: Histogram,

    pub core_last_proposed_round: IntGauge,
    pub core_last_proposed_block_size: IntGauge,

    pub fetcher_missing_block_tasks: IntGauge,
    pub fetcher_inflight_requests: IntGauge,
    pub fetcher_requests_total: IntCounter,

    pub syncer_last_committed_round: IntGauge,
    pub syncer_last_commit_index: IntGauge,
    pub syncer_leader_timeouts: IntCounter,
    pub syncer_received_block_age_ms: HistogramVec,
    pub syncer_own_block_commit_age_ms: Histogram,
    pub syncer_main_loop_util_ns: IntCounter,
    pub syncer_main_loop_calls: IntCounter,
    pub syncer_uncommitted_non_empty_blocks: IntGauge,

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
macro_rules! counter_vec (
    ($name:expr, $labels:expr, $r:expr) => {prometheus::register_int_counter_vec_with_registry!($name, $name, $labels, $r).unwrap()};
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
    #[cfg(test)]
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
            block_manager_missing_inverse_len: gauge!(
                "block_manager_missing_inverse_len",
                registry
            ),
            block_manager_added: counter!("block_manager_added", registry),
            block_manager_rejected: counter!("block_manager_rejected", registry),

            rocks_store_put_bytes: counter_vec!("rocks_store_put_bytes", &["cf"], registry),

            blocks_loaded: gauge!("blocks_loaded", registry),
            blocks_loaded_bytes: gauge!("blocks_loaded_bytes", registry),

            commit_rule_utilization: histogram!(
                "commit_rule_utilization",
                exponential_buckets(0.001, 2., 10),
                registry
            ),

            core_last_proposed_round: gauge!("core_last_proposed_round", registry),
            core_last_proposed_block_size: gauge!("core_last_proposed_block_size", registry),

            fetcher_missing_block_tasks: gauge!("fetcher_missing_block_tasks", registry),
            fetcher_inflight_requests: gauge!("fetcher_inflight_requests", registry),
            fetcher_requests_total: counter!("fetcher_requests_total", registry),

            syncer_last_committed_round: gauge!("syncer_last_committed_round", registry),
            syncer_last_commit_index: gauge!("syncer_last_commit_index", registry),
            syncer_leader_timeouts: counter!("syncer_leader_timeouts", registry),
            syncer_own_block_commit_age_ms: histogram!(
                "syncer_own_block_commit_age_ms",
                exponential_buckets(1., 2., 14),
                registry
            ),
            syncer_received_block_age_ms: histogram_vec!(
                "syncer_received_block_age_ms",
                &["source"],
                exponential_buckets(1., 2., 14),
                registry
            ),
            syncer_main_loop_util_ns: counter!("syncer_main_loop_util_ns", registry),
            syncer_main_loop_calls: counter!("syncer_main_loop_calls", registry),
            syncer_uncommitted_non_empty_blocks: gauge!(
                "syncer_uncommitted_non_empty_blocks",
                registry
            ),

            rpc_connected_peers: gauge!("rpc_connected_peers", registry),
            validator_labels,
        })
    }
}

pub struct UtilizationTimer {
    counter: IntCounter,
    start: Instant,
}

impl UtilizationTimer {
    pub fn new(counter: &IntCounter) -> Self {
        let start = Instant::now();
        let counter = counter.clone();
        Self { counter, start }
    }
}

impl Drop for UtilizationTimer {
    fn drop(&mut self) {
        self.counter.inc_by(self.start.elapsed().as_nanos() as u64);
    }
}

pub trait UtilizationTimerExt {
    fn utilization_timer(&self) -> UtilizationTimer;
}

impl UtilizationTimerExt for IntCounter {
    fn utilization_timer(&self) -> UtilizationTimer {
        UtilizationTimer::new(self)
    }
}

pub struct EntranceGauge {
    gauge: IntGauge,
}

impl EntranceGauge {
    pub fn new(gauge: &IntGauge) -> Self {
        let gauge = gauge.clone();
        gauge.inc();
        Self { gauge }
    }
}

impl Drop for EntranceGauge {
    fn drop(&mut self) {
        self.gauge.dec();
    }
}

pub trait EntranceGaugeExt {
    fn entrance_gauge(&self) -> EntranceGauge;
}

impl EntranceGaugeExt for IntGauge {
    fn entrance_gauge(&self) -> EntranceGauge {
        EntranceGauge::new(self)
    }
}
