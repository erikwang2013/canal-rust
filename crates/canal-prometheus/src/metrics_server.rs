use axum::{Router, response::IntoResponse, routing::get};
use metrics::{counter, describe_counter, describe_gauge, gauge};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
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
        describe_gauge!(
            "canal_instances_active",
            "Number of active Canal instances"
        );

        PrometheusBuilder::new()
            .install_recorder()
            .expect("Failed to install Prometheus recorder")
    })
}

pub struct CanalMetrics {
    handle: PrometheusHandle,
    events_parsed: AtomicU64,
    events_filtered: AtomicU64,
    events_dispatched: AtomicU64,
    dispatch_errors: AtomicU64,
    instances_active: AtomicU64,
}

impl CanalMetrics {
    pub fn new() -> Self {
        let handle = init_metrics().clone();

        Self {
            handle,
            events_parsed: AtomicU64::new(0),
            events_filtered: AtomicU64::new(0),
            events_dispatched: AtomicU64::new(0),
            dispatch_errors: AtomicU64::new(0),
            instances_active: AtomicU64::new(0),
        }
    }
}

impl Default for CanalMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl CanalMetrics {
    pub fn inc_parsed(&self, count: u64) {
        counter!("canal_events_parsed_total").increment(count);
        self.events_parsed.fetch_add(count, Ordering::SeqCst);
    }

    pub fn inc_filtered(&self, count: u64) {
        counter!("canal_events_filtered_total").increment(count);
        self.events_filtered.fetch_add(count, Ordering::SeqCst);
    }

    pub fn inc_dispatched(&self, count: u64) {
        counter!("canal_events_dispatched_total").increment(count);
        self.events_dispatched.fetch_add(count, Ordering::SeqCst);
    }

    pub fn inc_dispatch_errors(&self, count: u64) {
        counter!("canal_dispatch_errors_total").increment(count);
        self.dispatch_errors.fetch_add(count, Ordering::SeqCst);
    }

    pub fn set_instances_active(&self, count: u64) {
        gauge!("canal_instances_active").set(count as f64);
        self.instances_active.store(count, Ordering::SeqCst);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            events_parsed: self.events_parsed.load(Ordering::SeqCst),
            events_filtered: self.events_filtered.load(Ordering::SeqCst),
            events_dispatched: self.events_dispatched.load(Ordering::SeqCst),
            dispatch_errors: self.dispatch_errors.load(Ordering::SeqCst),
            instances_active: self.instances_active.load(Ordering::SeqCst),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub events_parsed: u64,
    pub events_filtered: u64,
    pub events_dispatched: u64,
    pub dispatch_errors: u64,
    pub instances_active: u64,
}

pub struct MetricsServer {
    bind_addr: SocketAddr,
    metrics: Arc<CanalMetrics>,
}

impl MetricsServer {
    pub fn new(bind_addr: SocketAddr, metrics: Arc<CanalMetrics>) -> Self {
        Self { bind_addr, metrics }
    }

    pub async fn start(self) -> std::io::Result<tokio::task::JoinHandle<()>> {
        let handle = self.metrics.handle.clone();
        info!("Metrics server starting on {}", self.bind_addr);

        let app = Router::new().route("/metrics", get(move || metrics_handler(handle.clone())));

        let listener = tokio::net::TcpListener::bind(&self.bind_addr).await?;
        let task = tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                tracing::error!("Metrics server error: {}", e);
            }
        });

        Ok(task)
    }
}

async fn metrics_handler(handle: PrometheusHandle) -> impl IntoResponse {
    handle.render()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter_increments() {
        let m = CanalMetrics::new();
        assert_eq!(m.snapshot().events_parsed, 0);

        m.inc_parsed(10);
        assert_eq!(m.snapshot().events_parsed, 10);

        m.inc_parsed(5);
        assert_eq!(m.snapshot().events_parsed, 15);
    }

    #[test]
    fn test_gauge_updates() {
        let m = CanalMetrics::new();
        assert_eq!(m.snapshot().instances_active, 0);

        m.set_instances_active(3);
        assert_eq!(m.snapshot().instances_active, 3);

        m.set_instances_active(1);
        assert_eq!(m.snapshot().instances_active, 1);
    }

    #[test]
    fn test_multiple_counters() {
        let m = CanalMetrics::new();
        m.inc_parsed(100);
        m.inc_filtered(30);
        m.inc_dispatched(70);
        m.inc_dispatch_errors(2);

        let snap = m.snapshot();
        assert_eq!(snap.events_parsed, 100);
        assert_eq!(snap.events_filtered, 30);
        assert_eq!(snap.events_dispatched, 70);
        assert_eq!(snap.dispatch_errors, 2);
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

    #[test]
    fn test_snapshot_is_cloneable() {
        let snap = MetricsSnapshot {
            events_parsed: 1,
            events_filtered: 2,
            events_dispatched: 3,
            dispatch_errors: 0,
            instances_active: 1,
        };
        let cloned = snap.clone();
        assert_eq!(cloned.events_parsed, 1);
    }
}
