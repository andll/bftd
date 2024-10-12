use axum::{http::StatusCode, routing::get, Extension, Router};
use prometheus::{Registry, TextEncoder};
use std::future::IntoFuture;
use std::io;
use std::net::SocketAddr;
use tokio::net::TcpListener;

use tokio::task::JoinHandle;
use tracing::log;

pub const METRICS_ROUTE: &str = "/metrics";

pub type PrometheusJoinHandle = JoinHandle<io::Result<()>>;

pub use bftd_core::counter;
pub use bftd_core::gauge;
pub use bftd_core::histogram;
pub use bftd_core::histogram_vec;

pub async fn start_prometheus_server(
    address: SocketAddr,
    registry: &Registry,
) -> anyhow::Result<PrometheusJoinHandle> {
    let app = Router::new()
        .route(METRICS_ROUTE, get(metrics))
        .layer(Extension(registry.clone()));

    tracing::info!("Prometheus server started on {address}");
    let listener = TcpListener::bind(&address).await?;
    Ok(tokio::spawn(axum::serve(listener, app).into_future()))
}

async fn metrics(registry: Extension<Registry>) -> (StatusCode, String) {
    let metrics_families = registry.gather();
    match TextEncoder.encode_to_string(&metrics_families) {
        Ok(metrics) => (StatusCode::OK, metrics),
        Err(error) => {
            log::warn!("Error encoding metrics: {error}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unable to encode metrics".to_string(),
            )
        }
    }
}
