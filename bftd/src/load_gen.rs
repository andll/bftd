use crate::mempool::{BasicMempoolClient, MAX_TRANSACTION};
use anyhow::{bail, ensure};
use bftd_core::counter;
use bftd_core::syncer::{Clock, SystemTimeClock};
use prometheus::{IntCounter, Registry};
use rand::rngs::ThreadRng;
use rand::RngCore;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::log;

pub struct LoadGenConfig {
    transaction_size: usize,
    tps: Option<usize>,
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
        let tps = if let Some(tps) = tps {
            Some(tps.parse()?)
        } else {
            None
        };
        Ok(Self {
            transaction_size,
            tps,
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
        loop {
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

pub struct LoadGenMetrics {
    load_gen_sent_transactions: IntCounter,
}

impl LoadGenMetrics {
    pub fn new_in_registry(registry: &Registry) -> Arc<Self> {
        Arc::new(Self {
            load_gen_sent_transactions: counter!("load_gen_sent_transactions", &registry),
        })
    }
}
