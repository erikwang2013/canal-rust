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
        let handle = PrometheusBuilder::new().install_recorder().expect(
            "Failed to install Prometheus recorder — ensure only one CanalMetrics is created per process",
        );
        // Descriptions must be registered after the recorder is installed,
        // otherwise the exporter never renders them as HELP lines.
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
        handle
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
            if token.is_empty() {
                return false;
            }
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

    // The prometheus recorder is process-global, so tests that read/write
    // metric values must be serialized against each other.
    static METRIC_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn test_new_creates_metrics() {
        let _guard = METRIC_LOCK.lock().unwrap();
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
        let _guard = METRIC_LOCK.lock().unwrap();
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

    #[tokio::test]
    async fn test_metrics_server_start_on_unspecified_ip() {
        // 0.0.0.0 warns but still binds locally and serves.
        let metrics = Arc::new(CanalMetrics::new());
        let server = MetricsServer::new("0.0.0.0:0".parse().unwrap(), metrics);
        let task = server.start().await.unwrap();
        assert!(!task.is_finished());
        task.abort();
    }

    #[test]
    fn test_metrics_server_with_auth_flag() {
        let metrics = Arc::new(CanalMetrics::new());
        let server = MetricsServer::new("127.0.0.1:0".parse().unwrap(), metrics);
        assert!(server.auth_token.is_none());
        let server = server.with_auth("tok".into());
        assert_eq!(server.auth_token.as_deref(), Some("tok"));
    }

    // ── counter/gauge rendering (delta assertions: the global recorder
    //    is shared across tests in this process) ──────────────

    fn metric_value(render: &str, name: &str) -> f64 {
        // A metric only appears in the render once it has been touched at
        // least once (counters/gauge register lazily), so missing = 0.
        render
            .lines()
            .find_map(|line| {
                let line = line.trim_start();
                if line.starts_with('#') {
                    return None;
                }
                let rest = line.strip_prefix(name)?;
                if !rest.starts_with(' ') {
                    return None;
                }
                rest.trim().parse::<f64>().ok()
            })
            .unwrap_or(0.0)
    }

    fn render_for(handle: &PrometheusHandle) -> String {
        handle.render()
    }

    #[test]
    fn test_inc_parsed_renders() {
        let _guard = METRIC_LOCK.lock().unwrap();
        let m = CanalMetrics::new();
        let before = metric_value(&render_for(&m.handle), "canal_events_parsed_total");
        m.inc_parsed(7);
        let after = metric_value(&render_for(&m.handle), "canal_events_parsed_total");
        assert_eq!(after, before + 7.0);
    }

    #[test]
    fn test_inc_filtered_and_dispatched_renders() {
        let _guard = METRIC_LOCK.lock().unwrap();
        let m = CanalMetrics::new();
        let before_f = metric_value(&render_for(&m.handle), "canal_events_filtered_total");
        let before_d = metric_value(&render_for(&m.handle), "canal_events_dispatched_total");
        m.inc_filtered(2);
        m.inc_dispatched(3);
        let render = render_for(&m.handle);
        assert_eq!(
            metric_value(&render, "canal_events_filtered_total"),
            before_f + 2.0
        );
        assert_eq!(
            metric_value(&render, "canal_events_dispatched_total"),
            before_d + 3.0
        );
    }

    #[test]
    fn test_inc_dispatch_errors_and_zero() {
        let _guard = METRIC_LOCK.lock().unwrap();
        let m = CanalMetrics::new();
        let before = metric_value(&render_for(&m.handle), "canal_dispatch_errors_total");
        m.inc_dispatch_errors(0); // zero increment is a no-op
        m.inc_dispatch_errors(1);
        assert_eq!(
            metric_value(&render_for(&m.handle), "canal_dispatch_errors_total"),
            before + 1.0
        );
    }

    #[test]
    fn test_set_instances_active_gauge() {
        let _guard = METRIC_LOCK.lock().unwrap();
        let m = CanalMetrics::new();
        m.set_instances_active(4);
        assert_eq!(
            metric_value(&render_for(&m.handle), "canal_instances_active"),
            4.0
        );
        m.set_instances_active(0);
        assert_eq!(
            metric_value(&render_for(&m.handle), "canal_instances_active"),
            0.0
        );
    }

    #[test]
    fn test_metrics_have_help_descriptions() {
        let _guard = METRIC_LOCK.lock().unwrap();
        // Touch each metric so it is registered, then check descriptions.
        let m = CanalMetrics::new();
        m.inc_parsed(1);
        m.inc_filtered(1);
        m.inc_dispatched(1);
        m.inc_dispatch_errors(1);
        m.set_instances_active(1);
        let render = render_for(&m.handle);
        assert!(render.contains("canal_events_parsed_total"));
        assert!(render.contains("Total number of binlog events parsed from MySQL"));
        assert!(render.contains("Total number of events dropped by filter"));
        assert!(render.contains("Total number of connector dispatch failures"));
        assert!(render.contains("Number of active Canal instances"));
    }

    // ── metrics auth ─────────────────────────────────────────

    #[test]
    fn test_metrics_auth_no_token_always_allowed() {
        assert!(check_metrics_auth(&HeaderMap::new(), &None));
    }

    #[test]
    fn test_metrics_auth_bearer_and_raw_token() {
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", "Bearer tok".parse().unwrap());
        assert!(check_metrics_auth(&headers, &Some("tok".into())));
        headers.insert("Authorization", "tok".parse().unwrap());
        assert!(check_metrics_auth(&headers, &Some("tok".into())));
    }

    #[test]
    fn test_metrics_auth_rejections() {
        let headers = HeaderMap::new();
        assert!(!check_metrics_auth(&headers, &Some("tok".into())));
        let mut wrong = HeaderMap::new();
        wrong.insert("Authorization", "Bearer wrong".parse().unwrap());
        assert!(!check_metrics_auth(&wrong, &Some("tok".into())));
        let mut prefix = HeaderMap::new();
        prefix.insert("Authorization", "Bearer to".parse().unwrap());
        assert!(!check_metrics_auth(&prefix, &Some("tok".into())));
    }

    #[test]
    fn test_metrics_auth_empty_token_rejected() {
        // An empty configured token must not accept a missing header.
        assert!(!check_metrics_auth(&HeaderMap::new(), &Some(String::new())));
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", "Bearer ".parse().unwrap());
        assert!(!check_metrics_auth(&headers, &Some(String::new())));
    }
}
