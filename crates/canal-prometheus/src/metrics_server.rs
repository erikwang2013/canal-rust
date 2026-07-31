use axum::{http::HeaderMap, response::IntoResponse, routing::get, Router};
use metrics::{counter, describe_counter, describe_gauge, gauge};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::OnceLock;
use tracing::info;

static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

fn init_metrics() -> &'static PrometheusHandle {
    PROMETHEUS_HANDLE.get_or_init(|| {
        describe_counter!(
            "canal_events_parsed_total",
            "Total number of binlog events parsed from MySQL"
        );
        describe_counter!(
            "canal_events_filtered_total",
            "Total number of events dropped by filter"
        );
        describe_counter!(
            "canal_events_dispatched_total",
            "Total number of events dispatched to connectors"
        );
        describe_counter!(
            "canal_dispatch_errors_total",
            "Total number of connector dispatch failures"
        );
        describe_gauge!("canal_instances_active", "Number of active Canal instances");

        PrometheusBuilder::new()
            .install_recorder()
            .expect("Failed to install Prometheus recorder — ensure only one CanalMetrics is created per process")
    })
}

/// Lightweight metrics facade backed by prometheus global recorder.
/// All values are queryable via the /metrics HTTP endpoint.
#[derive(Clone)]
pub struct CanalMetrics {
    handle: PrometheusHandle,
}

impl CanalMetrics {
    pub fn new() -> Self {
        let handle = init_metrics().clone();
        Self { handle }
    }

    pub fn inc_parsed(&self, count: u64) {
        counter!("canal_events_parsed_total").increment(count);
    }

    pub fn inc_filtered(&self, count: u64) {
        counter!("canal_events_filtered_total").increment(count);
    }

    pub fn inc_dispatched(&self, count: u64) {
        counter!("canal_events_dispatched_total").increment(count);
    }

    pub fn inc_dispatch_errors(&self, count: u64) {
        counter!("canal_dispatch_errors_total").increment(count);
    }

    pub fn set_instances_active(&self, count: u64) {
        gauge!("canal_instances_active").set(count as f64);
    }
}

impl Default for CanalMetrics {
    fn default() -> Self {
        Self::new()
    }
}

pub struct MetricsServer {
    auth_token: Option<String>,
    bind_addr: SocketAddr,
    metrics: Arc<CanalMetrics>,
}

impl MetricsServer {
    pub fn new(bind_addr: SocketAddr, metrics: Arc<CanalMetrics>) -> Self {
        Self {
            bind_addr,
            metrics,
            auth_token: None,
        }
    }

    pub fn with_auth(mut self, token: String) -> Self {
        self.auth_token = Some(token);
        self
    }

    pub async fn start(self) -> std::io::Result<tokio::task::JoinHandle<()>> {
        let handle = self.metrics.handle.clone();
        let auth_token = self.auth_token.clone();

        if self.bind_addr.ip().is_unspecified() {
            tracing::warn!(
                "Metrics server binding to {} — ensure firewall protection",
                self.bind_addr
            );
        }
        if auth_token.is_none() {
            tracing::warn!("Metrics endpoint /metrics has no authentication configured");
        }

        info!("Metrics server starting on {}", self.bind_addr);

        let app = Router::new().route(
            "/metrics",
            get(move |headers: axum::http::HeaderMap| {
                let handle = handle.clone();
                let token = auth_token.clone();
                async move {
                    if !check_metrics_auth(&headers, &token) {
                        return (axum::http::StatusCode::UNAUTHORIZED, "unauthorized")
                            .into_response();
                    }
                    handle.render().into_response()
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind(&self.bind_addr).await?;
        let task = tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                tracing::error!("Metrics server error: {}", e);
            }
        });

        Ok(task)
    }
}

fn check_metrics_auth(headers: &HeaderMap, expected: &Option<String>) -> bool {
    match expected {
        None => true,
        Some(token) => {
            let auth = headers
                .get("Authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            let expected_bearer = format!("Bearer {}", token);
            // Constant-time comparison
            let a = auth.as_bytes();
            let b = expected_bearer.as_bytes();
            let t = token.as_bytes();
            (a.len() == b.len() && a.iter().zip(b.iter()).fold(0, |acc, (x, y)| acc | (x ^ y)) == 0)
                || (a.len() == t.len()
                    && a.iter().zip(t.iter()).fold(0, |acc, (x, y)| acc | (x ^ y)) == 0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_creates_metrics() {
        let m = CanalMetrics::new();
        // Methods should not panic — values go to global prometheus recorder
        m.inc_parsed(10);
        m.inc_filtered(5);
        m.inc_dispatched(5);
        m.inc_dispatch_errors(0);
        m.set_instances_active(3);
    }

    #[test]
    fn test_default_works() {
        let m = CanalMetrics::default();
        m.inc_parsed(1);
    }

    #[tokio::test]
    async fn test_metrics_server_starts_on_random_port() {
        let metrics = Arc::new(CanalMetrics::new());
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = MetricsServer::new(addr, metrics);

        let task = server.start().await.unwrap();
        assert!(!task.is_finished());

        task.abort();
    }
}
