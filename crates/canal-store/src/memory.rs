use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use canal_common::lifecycle::CanalLifecycle;
use canal_common::{CanalEvent, CanalResult, Events, LogPosition};
use tokio::sync::Notify;
use tracing::{debug, info};

/// Memory-backed event store using a ring buffer
/// Corresponds to Java MemoryEventStoreWithBuffer
pub struct MemoryEventStore {
    buffer: Mutex<VecDeque<CanalEvent>>,
    capacity: usize,
    batch_id_seq: AtomicI64,
    latest_position: Mutex<Option<LogPosition>>,
    first_position: Mutex<Option<LogPosition>>,
    running: AtomicBool,
    notify: Notify,
}

/// Timeout for get_batch() waiting: 5 seconds
const GET_BATCH_TIMEOUT_MS: u64 = 5000;

impl MemoryEventStore {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "MemoryEventStore capacity must be > 0");
        Self {
            buffer: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            batch_id_seq: AtomicI64::new(0),
            latest_position: Mutex::new(None),
            first_position: Mutex::new(None),
            running: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    /// Append a batch of events to the store
    pub async fn put_batch(&self, events: Vec<CanalEvent>) -> CanalResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        let first = LogPosition::new(&events[0].journal_name, events[0].position);
        let last_pos = events.last().unwrap();

        let mut buffer = self.buffer.lock().unwrap_or_else(|e| e.into_inner());

        // Evict old events if buffer is full (ring buffer behavior)
        while buffer.len() + events.len() > self.capacity {
            buffer.pop_front();
        }

        // Track position boundaries — update first_position after eviction too
        let mut first_pos = self
            .first_position
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(front) = buffer.front() {
            *first_pos = Some(LogPosition::new(&front.journal_name, front.position));
        } else {
            *first_pos = Some(first);
        }
        *self
            .latest_position
            .lock()
            .unwrap_or_else(|e| e.into_inner()) =
            Some(LogPosition::new(&last_pos.journal_name, last_pos.position));

        buffer.extend(events);
        self.notify.notify_waiters();
        debug!("buffer size after put: {}", buffer.len());
        Ok(())
    }

    /// Get a batch of events starting after the given position.
    /// Blocks (async) until events are available.
    pub async fn get_batch(&self, start: &LogPosition, batch_size: usize) -> CanalResult<Events> {
        loop {
            // Scoped block ensures MutexGuard is dropped before .await below
            let result = {
                let buffer = self.buffer.lock().unwrap_or_else(|e| e.into_inner());

                // Find first event after the requested start position.
                // Use lexicographic (journal, position) comparison so binlog
                // rotation (e.g. mysql-bin.000001 → mysql-bin.000002) works.
                let start_idx = buffer.iter().position(|e| {
                    (e.journal_name.as_str(), e.position)
                        > (start.journal_name.as_str(), start.position)
                });

                if let Some(idx) = start_idx {
                    let batch_id = self.batch_id_seq.fetch_add(1, Ordering::SeqCst);
                    let events: Vec<CanalEvent> =
                        buffer.iter().skip(idx).take(batch_size).cloned().collect();

                    if !events.is_empty() {
                        Some(Events::with_events(events, batch_id))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }; // MutexGuard dropped here

            if let Some(events) = result {
                return Ok(events);
            }

            // Wait for new events to be put, with timeout to prevent
            // permanent blocking when no more events are available
            tokio::select! {
                _ = self.notify.notified() => {},
                _ = tokio::time::sleep(std::time::Duration::from_millis(GET_BATCH_TIMEOUT_MS)) => {
                    return Ok(Events::new(0));
                }
            }
        }
    }

    /// Get the latest position in the store
    pub fn latest_position(&self) -> Option<LogPosition> {
        self.latest_position
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Get the first position in the store
    pub fn first_position(&self) -> Option<LogPosition> {
        self.first_position
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

#[async_trait]
impl CanalLifecycle for MemoryEventStore {
    async fn start(&self) -> CanalResult<()> {
        self.running.store(true, Ordering::SeqCst);
        info!("MemoryEventStore started, capacity={}", self.capacity);
        Ok(())
    }

    async fn stop(&self) -> CanalResult<()> {
        self.running.store(false, Ordering::SeqCst);
        info!("MemoryEventStore stopped");
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use canal_common::EventType;

    fn make_event(journal: &str, pos: u64) -> CanalEvent {
        CanalEvent {
            journal_name: journal.to_string(),
            position: pos,
            server_id: 1,
            execute_time: 0,
            entry_type: EventType::Insert,
            schema_name: "test_db".to_string(),
            table_name: "users".to_string(),
            row_change: None,
            ddl_sql: None,
            gtid: None,
            raw_bytes: vec![],
        }
    }

    #[tokio::test]
    async fn test_put_and_get_batch() {
        let store = MemoryEventStore::new(1024);

        store
            .put_batch(vec![make_event("mysql-bin.000001", 100)])
            .await
            .unwrap();

        let start = LogPosition::new("mysql-bin.000001", 4);
        let batch = store.get_batch(&start, 10).await.unwrap();

        assert_eq!(batch.len(), 1);
        assert_eq!(batch.events[0].position, 100);
    }

    #[tokio::test]
    async fn test_latest_position_tracks_head() {
        let store = MemoryEventStore::new(1024);
        assert!(store.latest_position().is_none());

        store
            .put_batch(vec![
                make_event("mysql-bin.000001", 100),
                make_event("mysql-bin.000001", 200),
            ])
            .await
            .unwrap();

        let latest = store.latest_position().unwrap();
        assert_eq!(latest.position, 200);
    }

    #[tokio::test]
    async fn test_buffer_overflow_evicts_oldest() {
        let store = MemoryEventStore::new(2);

        store
            .put_batch(vec![
                make_event("mysql-bin.000001", 100),
                make_event("mysql-bin.000001", 200),
            ])
            .await
            .unwrap();

        // This should evict event at position 100
        store
            .put_batch(vec![make_event("mysql-bin.000001", 300)])
            .await
            .unwrap();

        // Position 100 should have been evicted
        let early = LogPosition::new("mysql-bin.000001", 50);
        let batch = store.get_batch(&early, 10).await.unwrap();
        assert_eq!(batch.events[0].position, 200);
    }

    #[tokio::test]
    async fn test_empty_put_is_noop() {
        let store = MemoryEventStore::new(1024);
        store.put_batch(vec![]).await.unwrap();
        assert!(store.latest_position().is_none());
    }

    #[tokio::test]
    async fn test_lifecycle_start_stop() {
        let store = MemoryEventStore::new(1024);
        assert!(!store.is_running());

        store.start().await.unwrap();
        assert!(store.is_running());

        store.stop().await.unwrap();
        assert!(!store.is_running());
    }
}
