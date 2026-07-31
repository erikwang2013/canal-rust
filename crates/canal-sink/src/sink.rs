use async_trait::async_trait;
use canal_common::{CanalEvent, CanalResult, Events};
use canal_filter::EventFilter;
use canal_prometheus::CanalMetrics;
use canal_store::memory::MemoryEventStore;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::connector::SinkConnector;

type SharedConnector = Arc<dyn SinkConnector>;

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
    async fn sink(&self, events: Vec<CanalEvent>) -> CanalResult<Events>;

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
/// Filters events, stores them in memory, and fans out to connectors.
pub struct DefaultEventSink {
    store: Arc<MemoryEventStore>,
    filter: EventFilter,
    connectors: Vec<SharedConnector>,
    metrics: Arc<CanalMetrics>,
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
        }
    }

    /// Create a sink with only the memory store (no external connectors)
    pub fn store_only(store: Arc<MemoryEventStore>, filter: EventFilter) -> Self {
        Self {
            store,
            filter,
            connectors: vec![],
            metrics: Arc::new(CanalMetrics::new()),
        }
    }

    /// Add a connector to the sink
    pub fn add_connector(&mut self, connector: SharedConnector) {
        self.connectors.push(connector);
    }
}

#[async_trait]
impl EventSink for DefaultEventSink {
    async fn sink(&self, events: Vec<CanalEvent>) -> CanalResult<Events> {
        let total = events.len();
        self.metrics.inc_parsed(total as u64);

        // Phase 1: Filter events
        let filtered: Vec<CanalEvent> = events
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

        // Wrap in Arc so we share a single allocation with the store and connectors.
        let filtered = Arc::new(filtered);

        // Phase 2: Store in memory for client subscription.
        // put_batch returns the batch_id, avoiding a race on get_batch.
        let batch_id = self.store.put_batch(Vec::clone(&filtered)).await?;

        // Phase 3: Fan out to external connectors (fire and forget pattern)
        let mut join_handles = Vec::new();
        for connector in &self.connectors {
            let events = Arc::clone(&filtered);
            let conn = Arc::clone(connector);
            join_handles.push(tokio::spawn(async move {
                match conn.dispatch(&events).await {
                    Ok(()) => (conn.name().to_string(), 0u64),
                    Err(e) => {
                        error!("Connector {} dispatch failed: {}", conn.name(), e);
                        (conn.name().to_string(), 1u64)
                    }
                }
            }));
        }
        for handle in join_handles {
            match handle.await {
                Ok((name, failures)) if failures > 0 => {
                    self.metrics.inc_dispatch_errors(failures);
                    warn!("Connector {} had {} failures in this batch", name, failures);
                }
                Ok((_name, _)) => {
                    self.metrics.inc_dispatched(1);
                }
                Err(e) => {
                    self.metrics.inc_dispatch_errors(1);
                    error!("Connector task panicked: {}", e);
                }
            }
        }

        // Phase 4: Build the response (no re-read from store)
        let events = Arc::try_unwrap(filtered).unwrap_or_else(|arc| (*arc).clone());
        let batch = Events::with_events(events, batch_id);
        info!(
            "Sinked batch_id={} with {} events",
            batch.batch_id,
            batch.len()
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

        assert_eq!(batch.len(), 1);
        assert!(batch.batch_id >= 0);
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

        assert_eq!(batch.len(), 1);
        assert_eq!(batch.events[0].table_name, "users");
    }

    #[tokio::test]
    async fn test_connector_receives_events() {
        let store = Arc::new(MemoryEventStore::new(1024));
        let filter = EventFilter::new(".*\\..*").unwrap();
        let connector = Arc::new(MockConnector::new("test-connector"));

        let mut sink = DefaultEventSink::new(store.clone(), filter, vec![]);
        sink.add_connector(connector);

        let events = vec![make_event("db", "tbl", 100)];
        sink.sink(events).await.unwrap();

        // Give the spawned task time to complete
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}
