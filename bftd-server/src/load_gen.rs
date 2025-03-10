use crate::mempool::{BasicMempoolClient, MAX_TRANSACTION};
use anyhow::{bail, ensure};
use bftd_core::syncer::{Clock, SystemTimeClock};
use bftd_core::{counter, gauge};
use prometheus::{IntCounter, IntGauge, Registry};
use rand::rngs::ThreadRng;
use rand::RngCore;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::log;

pub struct LoadGenConfig {
    transaction_size: usize,
    tps: Option<usize>,
    increase_interval_s: Option<usize>,
    increase_multiplier: Option<f64>,
}

impl LoadGenConfig {
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        let mut s = s.split("::");
        let Some(transaction_size) = s.next() else {
            bail!("Load gen specification too short")
        };
        let transaction_size = transaction_size.parse()?;
        ensure!(
            transaction_size < MAX_TRANSACTION,
            "Transaction size is too large"
        );
        let tps = s.next();
        let tps = tps.map(str::parse).transpose()?;
        let increase_interval_s = s.next();
        let increase_interval_s = increase_interval_s.map(str::parse).transpose()?;
        let increase_multiplier = s.next();
        let increase_multiplier = increase_multiplier.map(str::parse).transpose()?;
        Ok(Self {
            transaction_size,
            tps,
            increase_interval_s,
            increase_multiplier,
        })
    }
}

pub struct LoadGen {
    config: LoadGenConfig,
    mempool_client: BasicMempoolClient,
    metrics: Arc<LoadGenMetrics>,
    buf: Vec<u8>,
}

impl LoadGen {
    pub fn start(
        config: LoadGenConfig,
        mempool_client: BasicMempoolClient,
        metrics: Arc<LoadGenMetrics>,
    ) -> JoinHandle<()> {
        let mut rng = ThreadRng::default();
        let mut buf = vec![0u8; config.transaction_size];
        rng.fill_bytes(&mut buf[..]);
        let this = Self {
            config,
            mempool_client,
            metrics,
            buf,
        };
        tokio::spawn(this.run())
    }

    pub async fn run(mut self) {
        log::info!(
            "Starting load gen with transaction size {} and tps {:?}",
            self.config.transaction_size,
            self.config.tps
        );
        let clock = SystemTimeClock::new();
        let mut tps = self.config.tps;
        self.metrics
            .load_gen_target_tps
            .set(tps.unwrap_or_default() as i64);
        let mut tps_limit = tps.map(|limit| TpsLimit::new(limit as u64, 100));
        let mut increase_start = Instant::now();
        loop {
            if let Some(increase_interval_s) = self.config.increase_interval_s {
                if increase_start.elapsed().as_secs() >= increase_interval_s as u64 {
                    let increase_multiplier = self.config.increase_multiplier.unwrap_or(2.);
                    tps = Some((tps.unwrap() as f64 * increase_multiplier) as usize);
                    tps_limit = Some(TpsLimit::new(tps.unwrap() as u64, 100));
                    increase_start = Instant::now();
                    self.metrics
                        .load_gen_target_tps
                        .set(tps.unwrap_or_default() as i64);
                }
            }
            if let Some(tps_limit) = &mut tps_limit {
                tps_limit.increment(&clock).await;
            }
            let time = clock.time_ns();
            self.buf[..8].copy_from_slice(&time.to_be_bytes());
            if self
                .mempool_client
                .send_transaction(self.buf.clone())
                .await
                .is_err()
            {
                log::warn!("Load gen is stopped because mempool client is closed");
                return;
            }
            self.metrics.load_gen_sent_transactions.inc();
        }
    }
}

struct TpsLimit {
    limit: u64,
    used: u64,
    bucket_size_ms: u64,
    current: u64,
}

impl TpsLimit {
    pub fn new(limit_per_sec: u64, bucket_size_ms: u64) -> Self {
        assert!(bucket_size_ms > 0);
        assert!(bucket_size_ms <= 1000);
        let limit = limit_per_sec / (1000 / bucket_size_ms);
        Self {
            limit,
            bucket_size_ms,
            current: 0,
            used: 0,
        }
    }

    pub async fn increment(&mut self, clock: &impl Clock) {
        let d = self.maybe_reset(clock);
        if self.used == self.limit {
            tokio::time::sleep(d).await;
            self.maybe_reset(clock);
        }
        self.used += 1;
    }

    /// Returns time until the end of the current cycle
    fn maybe_reset(&mut self, clock: &impl Clock) -> Duration {
        let time_ms = clock.time().as_millis() as u64;
        let current = time_ms / self.bucket_size_ms;
        if self.current != current {
            self.current = current;
            self.used = 0;
        }
        Duration::from_millis(self.bucket_size_ms * (current + 1) - time_ms)
    }
}

pub struct LoadGenMetrics {
    load_gen_sent_transactions: IntCounter,
    load_gen_target_tps: IntGauge,
}

impl LoadGenMetrics {
    pub fn new_in_registry(registry: &Registry) -> Arc<Self> {
        Arc::new(Self {
            load_gen_sent_transactions: counter!("load_gen_sent_transactions", &registry),
            load_gen_target_tps: gauge!("load_gen_target_tps", &registry),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future;
    #[tokio::test(start_paused = true)]
    async fn tps_limit_test() {
        let mut limit = TpsLimit::new(200, 10);
        assert_eq!(limit.limit, 2); // 2 per bucket
        let clock = 1u64 * 1000 * 1000;
        future::poll_immediate(limit.increment(&clock))
            .await
            .unwrap();
        future::poll_immediate(limit.increment(&clock))
            .await
            .unwrap();
        let clock = 11u64 * 1000 * 1000;
        future::poll_immediate(limit.increment(&clock))
            .await
            .unwrap();
        future::poll_immediate(limit.increment(&clock))
            .await
            .unwrap();
        assert!(future::poll_immediate(limit.increment(&clock))
            .await
            .is_none());
        tokio::time::advance(Duration::from_millis(10)).await;
        let clock = 21u64 * 1000 * 1000;
        future::poll_immediate(limit.increment(&clock))
            .await
            .unwrap();
        future::poll_immediate(limit.increment(&clock))
            .await
            .unwrap();
        assert!(future::poll_immediate(limit.increment(&clock))
            .await
            .is_none());
    }
}
