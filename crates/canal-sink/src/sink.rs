use async_trait::async_trait;
use canal_common::{CanalResult, Events};
use canal_filter::EventFilter;
use canal_prometheus::CanalMetrics;
use canal_store::memory::MemoryEventStore;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};

use crate::connector::SinkConnector;

type SharedConnector = Arc<dyn SinkConnector>;

/// A dispatch request sent to the background FIFO worker.
struct DispatchRequest {
    events: Vec<canal_common::CanalEvent>,
    filtered_count: u64,
}

/// Event dispatch pipeline.
/// Fans out parsed binlog events to:
///   1. Memory store (for client subscription via canal-server)
///   2. External connectors (Kafka, RocketMQ, etc.)
///
/// Corresponds to Java's CanalEventSink + EntryEventSink.
#[async_trait]
pub trait EventSink: Send + Sync {
    /// Dispatch a batch of raw binlog events.
    /// Returns the filtered Events with their batch_id for client consumption.
    async fn sink(&self, events: Vec<canal_common::CanalEvent>) -> CanalResult<Events>;

    /// Start the sink
    async fn start(&self) -> CanalResult<()> {
        Ok(())
    }

    /// Stop the sink gracefully
    async fn stop(&self) -> CanalResult<()> {
        Ok(())
    }
}

/// The default event sink implementation.
/// Filters events, stores them in memory, and fans out to connectors
/// via a background FIFO worker that preserves batch ordering.
pub struct DefaultEventSink {
    store: Arc<MemoryEventStore>,
    filter: EventFilter,
    connectors: Vec<SharedConnector>,
    metrics: Arc<CanalMetrics>,
    /// Lazy-initialized FIFO dispatch worker
    dispatch_tx: std::sync::Mutex<Option<mpsc::UnboundedSender<DispatchRequest>>>,
    _worker: std::sync::Mutex<Option<JoinHandle<()>>>,
}

impl DefaultEventSink {
    /// Create a sink that writes to both a MemoryEventStore and external connectors.
    pub fn new(
        store: Arc<MemoryEventStore>,
        filter: EventFilter,
        connectors: Vec<SharedConnector>,
    ) -> Self {
        Self {
            store,
            filter,
            connectors,
            metrics: Arc::new(CanalMetrics::new()),
            dispatch_tx: std::sync::Mutex::new(None),
            _worker: std::sync::Mutex::new(None),
        }
    }

    /// Create a sink with custom metrics instance.
    pub fn with_metrics(
        store: Arc<MemoryEventStore>,
        filter: EventFilter,
        connectors: Vec<SharedConnector>,
        metrics: Arc<CanalMetrics>,
    ) -> Self {
        Self {
            store,
            filter,
            connectors,
            metrics,
            dispatch_tx: std::sync::Mutex::new(None),
            _worker: std::sync::Mutex::new(None),
        }
    }

    /// Create a sink with only the memory store (no external connectors)
    pub fn store_only(store: Arc<MemoryEventStore>, filter: EventFilter) -> Self {
        Self {
            store,
            filter,
            connectors: vec![],
            metrics: Arc::new(CanalMetrics::new()),
            dispatch_tx: std::sync::Mutex::new(None),
            _worker: std::sync::Mutex::new(None),
        }
    }

    /// Add a connector to the sink. Must be called before first `sink()`.
    pub fn add_connector(&mut self, connector: SharedConnector) {
        self.connectors.push(connector);
    }

    /// Ensure the dispatch worker is running (lazy init).
    fn ensure_worker(&self) -> mpsc::UnboundedSender<DispatchRequest> {
        let mut guard = self.dispatch_tx.lock().unwrap();
        if let Some(ref tx) = *guard {
            return tx.clone();
        }

        let (tx, mut rx) = mpsc::unbounded_channel::<DispatchRequest>();
        let connectors: Vec<_> = self.connectors.iter().map(Arc::clone).collect();
        let metrics = self.metrics.clone();

        let handle = tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                for conn in &connectors {
                    match conn.dispatch(&req.events).await {
                        Ok(()) => metrics.inc_dispatched(req.filtered_count as u64),
                        Err(e) => {
                            error!("Connector {} dispatch failed: {}", conn.name(), e);
                            metrics.inc_dispatch_errors(1);
                        }
                    }
                }
            }
            info!("Dispatch worker shutting down");
        });

        *self._worker.lock().unwrap() = Some(handle);
        *guard = Some(tx.clone());
        tx
    }
}

impl Drop for DefaultEventSink {
    fn drop(&mut self) {
        // Drop sender to signal worker shutdown
        *self.dispatch_tx.lock().unwrap() = None;
        if let Some(handle) = self._worker.lock().unwrap().take() {
            handle.abort();
        }
    }
}

#[async_trait]
impl EventSink for DefaultEventSink {
    async fn sink(&self, events: Vec<canal_common::CanalEvent>) -> CanalResult<Events> {
        let total = events.len();
        self.metrics.inc_parsed(total as u64);

        // Phase 1: Filter events
        let filtered: Vec<canal_common::CanalEvent> = events
            .into_iter()
            .filter(|e| self.filter.matches(e))
            .collect();

        let filtered_count = filtered.len();
        let dropped = (total - filtered_count) as u64;
        if dropped > 0 {
            self.metrics.inc_filtered(dropped);
        }
        debug!("Filtered {} of {} events", filtered_count, total);

        if filtered.is_empty() {
            return Ok(Events::new(0));
        }

        let filtered_count = filtered.len();
        // Phase 2: Store events in memory immediately (don't block on connectors).
        let batch_id = self.store.put_batch(filtered.clone()).await?;

        // Phase 3: Enqueue dispatch to FIFO worker (preserves batch ordering).
        if !self.connectors.is_empty() {
            let tx = self.ensure_worker();
            let _ = tx.send(DispatchRequest {
                events: filtered,
                filtered_count: filtered_count as u64,
            });
        }

        let batch = Events::new(batch_id);
        info!(
            "Sinked batch_id={} with {} events",
            batch_id, filtered_count
        );

        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::SinkConnector;
    use canal_common::{CanalEvent, EventType};
    use std::sync::Mutex;

    /// A mock connector that collects dispatched events for verification
    struct MockConnector {
        name: String,
        dispatched: Mutex<Vec<Vec<CanalEvent>>>,
    }

    impl MockConnector {
        fn new(name: &str) -> Self {
            Self {
                name: name.into(),
                dispatched: Mutex::new(vec![]),
            }
        }
    }

    #[async_trait]
    impl SinkConnector for MockConnector {
        fn name(&self) -> &str {
            &self.name
        }
        async fn connect(&self) -> CanalResult<()> {
            Ok(())
        }
        async fn dispatch(&self, events: &[CanalEvent]) -> CanalResult<()> {
            self.dispatched.lock().unwrap().push(events.to_vec());
            Ok(())
        }
        async fn close(&self) -> CanalResult<()> {
            Ok(())
        }
    }

    fn make_event(schema: &str, table: &str, pos: u64) -> CanalEvent {
        CanalEvent {
            journal_name: "mysql-bin.000001".into(),
            position: pos,
            server_id: 1,
            execute_time: 0,
            entry_type: EventType::Insert,
            schema_name: schema.into(),
            table_name: table.into(),
            row_change: None,
            ddl_sql: None,
            gtid: None,
            raw_bytes: vec![],
        }
    }

    #[tokio::test]
    async fn test_sink_stores_and_returns_events() {
        let store = Arc::new(MemoryEventStore::new(1024));
        let filter = EventFilter::new(".*\\..*").unwrap();
        let sink = DefaultEventSink::store_only(store.clone(), filter);

        let events = vec![make_event("test_db", "users", 100)];
        let batch = sink.sink(events).await.unwrap();

        assert!(batch.batch_id >= 0);
        // Events are stored in the store; verify they're retrievable
        let stored = store
            .get_batch(&canal_common::LogPosition::new("mysql-bin.000001", 0), 10)
            .await
            .unwrap();
        assert_eq!(stored.len(), 1);
    }

    #[tokio::test]
    async fn test_filter_excludes_non_matching() {
        let store = Arc::new(MemoryEventStore::new(1024));
        let filter = EventFilter::new("test_db\\.users").unwrap();
        let sink = DefaultEventSink::store_only(store.clone(), filter);

        let events = vec![
            make_event("test_db", "users", 100),
            make_event("test_db", "orders", 200),
        ];
        let batch = sink.sink(events).await.unwrap();

        assert!(batch.batch_id >= 0);
        let stored = store
            .get_batch(&canal_common::LogPosition::new("mysql-bin.000001", 0), 10)
            .await
            .unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored.events[0].table_name, "users");
    }

    #[tokio::test]
    async fn test_connector_receives_events() {
        let store = Arc::new(MemoryEventStore::new(1024));
        let filter = EventFilter::new(".*\\..*").unwrap();
        let connector = Arc::new(MockConnector::new("test-connector"));
        let conn_clone = connector.clone();

        let mut sink = DefaultEventSink::new(store.clone(), filter, vec![]);
        sink.add_connector(connector);

        let events = vec![make_event("db", "tbl", 100)];
        sink.sink(events).await.unwrap();

        // Give the FIFO worker time to process
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let dispatched = conn_clone.dispatched.lock().unwrap();
        assert_eq!(dispatched.len(), 1);
        assert_eq!(dispatched[0].len(), 1);
        assert_eq!(dispatched[0][0].table_name, "tbl");
    }

    #[tokio::test]
    async fn test_fifo_ordering_preserved() {
        let store = Arc::new(MemoryEventStore::new(1024));
        let filter = EventFilter::new(".*\\..*").unwrap();
        let connector = Arc::new(MockConnector::new("fifo-test"));
        let conn_clone = connector.clone();

        let mut sink = DefaultEventSink::new(store.clone(), filter, vec![]);
        sink.add_connector(connector);

        // Send 3 batches in order
        for i in 1..=3 {
            sink.sink(vec![make_event("db", "tbl", i * 100)]).await.unwrap();
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let dispatched = conn_clone.dispatched.lock().unwrap();
        assert_eq!(dispatched.len(), 3, "should receive all 3 batches in order");
        assert_eq!(dispatched[0][0].position, 100);
        assert_eq!(dispatched[1][0].position, 200);
        assert_eq!(dispatched[2][0].position, 300);
    }
}
