//! Integration tests for canal-sink: DefaultEventSink + SinkConnector fan-out.
//! Pure local tests — no external services.

use async_trait::async_trait;
use canal_common::{CanalEvent, CanalResult, EventType};
use canal_filter::EventFilter;
use canal_prometheus::CanalMetrics;
use canal_sink::connector::SinkConnector;
use canal_sink::sink::{DefaultEventSink, EventSink};
use canal_store::memory::MemoryEventStore;
use std::sync::{Arc, Mutex};

struct MockConnector {
    name: String,
    dispatched: Mutex<Vec<Vec<CanalEvent>>>,
    fail: bool,
}

impl MockConnector {
    fn new(name: &str) -> Self {
        Self {
            name: name.into(),
            dispatched: Mutex::new(vec![]),
            fail: false,
        }
    }

    fn failing(name: &str) -> Self {
        Self {
            name: name.into(),
            dispatched: Mutex::new(vec![]),
            fail: true,
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
        if self.fail {
            return Err(canal_common::CanalError::Internal("mock failure".into()));
        }
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
async fn test_sink_empty_events_returns_zero_batch() {
    let store = Arc::new(MemoryEventStore::new(1024));
    let filter = EventFilter::new(".*\\..*").unwrap();
    let sink = DefaultEventSink::store_only(store.clone(), filter);

    let batch = sink.sink(vec![]).await.unwrap();
    assert_eq!(batch.batch_id, 0);
    assert!(batch.is_empty());
    assert!(store.latest_position().is_none());
}

#[tokio::test]
async fn test_sink_all_filtered_out_returns_empty_batch() {
    let store = Arc::new(MemoryEventStore::new(1024));
    let filter = EventFilter::new("nomatch\\..*").unwrap();
    let sink = DefaultEventSink::store_only(store.clone(), filter);

    let batch = sink.sink(vec![make_event("db", "tbl", 100)]).await.unwrap();
    assert_eq!(batch.batch_id, 0);
    assert!(store.latest_position().is_none());
}

#[tokio::test]
async fn test_sink_blacklist_filter_drops_events() {
    let store = Arc::new(MemoryEventStore::new(1024));
    let filter = EventFilter::with_blacklist(".*\\..*", "db\\.logs").unwrap();
    let sink = DefaultEventSink::store_only(store.clone(), filter);

    sink.sink(vec![
        make_event("db", "users", 100),
        make_event("db", "logs", 200),
    ])
    .await
    .unwrap();

    let stored = store
        .get_batch(&canal_common::LogPosition::new("mysql-bin.000001", 0), 10)
        .await
        .unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored.events[0].table_name, "users");
}

#[tokio::test]
async fn test_sink_connector_failure_does_not_fail_sink() {
    let store = Arc::new(MemoryEventStore::new(1024));
    let filter = EventFilter::new(".*\\..*").unwrap();
    let mut sink = DefaultEventSink::new(store.clone(), filter, vec![]);
    sink.add_connector(Arc::new(MockConnector::failing("broken")));

    let batch = sink.sink(vec![make_event("db", "tbl", 100)]).await.unwrap();
    assert!(batch.batch_id >= 0);
    // events still stored for clients despite connector failure
    assert!(store.latest_position().is_some());

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
}

#[tokio::test]
async fn test_sink_with_metrics_constructor() {
    let store = Arc::new(MemoryEventStore::new(1024));
    let filter = EventFilter::new(".*\\..*").unwrap();
    let sink = DefaultEventSink::with_metrics(
        store.clone(),
        filter,
        vec![],
        Arc::new(CanalMetrics::new()),
    );

    let batch = sink.sink(vec![make_event("db", "tbl", 100)]).await.unwrap();
    assert!(batch.batch_id >= 0);
}

#[tokio::test]
async fn test_event_sink_trait_default_lifecycle() {
    let store = Arc::new(MemoryEventStore::new(1024));
    let filter = EventFilter::new(".*\\..*").unwrap();
    let sink = DefaultEventSink::store_only(store, filter);

    assert!(sink.start().await.is_ok());
    assert!(sink.stop().await.is_ok());
}

#[tokio::test]
async fn test_sink_fans_out_to_multiple_connectors() {
    let store = Arc::new(MemoryEventStore::new(1024));
    let filter = EventFilter::new(".*\\..*").unwrap();
    let c1 = Arc::new(MockConnector::new("kafka"));
    let c2 = Arc::new(MockConnector::new("rocketmq"));
    let c1_clone = c1.clone();
    let c2_clone = c2.clone();

    let mut sink = DefaultEventSink::new(store.clone(), filter, vec![]);
    sink.add_connector(c1);
    sink.add_connector(c2);

    sink.sink(vec![make_event("db", "tbl", 100)]).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    assert_eq!(c1_clone.dispatched.lock().unwrap().len(), 1);
    assert_eq!(c2_clone.dispatched.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn test_sink_returns_store_batch_id() {
    let store = Arc::new(MemoryEventStore::new(1024));
    let filter = EventFilter::new(".*\\..*").unwrap();
    let sink = DefaultEventSink::store_only(store.clone(), filter);

    let batch1 = sink.sink(vec![make_event("db", "t", 100)]).await.unwrap();
    let batch2 = sink.sink(vec![make_event("db", "t", 200)]).await.unwrap();
    assert!(batch2.batch_id > batch1.batch_id);
    // the returned batch carries only the batch_id; events land in the store
    assert!(batch1.is_empty());
    let stored = store
        .get_batch(&canal_common::LogPosition::new("mysql-bin.000001", 0), 10)
        .await
        .unwrap();
    assert_eq!(stored.len(), 2);
}
