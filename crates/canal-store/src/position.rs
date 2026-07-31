use canal_common::LogPosition;
use dashmap::DashMap;

/// Tracks per-client binlog positions for ACK management.
/// Uses DashMap for lock-free reads (P4 fix).
#[derive(Debug, Default)]
pub struct PositionTracker {
    positions: DashMap<String, LogPosition>,
}

impl PositionTracker {
    pub fn new() -> Self {
        Self {
            positions: DashMap::new(),
        }
    }

    /// Record a client's acknowledged position
    pub fn update(&self, client_id: &str, position: LogPosition) {
        self.positions.insert(client_id.to_string(), position);
    }

    /// Get a client's last acknowledged position
    pub fn get(&self, client_id: &str) -> Option<LogPosition> {
        self.positions.get(client_id).map(|r| r.clone())
    }

    /// Remove a disconnected client's position tracking
    pub fn remove(&self, client_id: &str) {
        self.positions.remove(client_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_and_get() {
        let tracker = PositionTracker::new();
        let pos = LogPosition::new("mysql-bin.000001", 100);
        tracker.update("client-1", pos.clone());
        let stored = tracker.get("client-1").unwrap();
        assert_eq!(stored.position, 100);
    }

    #[test]
    fn test_remove() {
        let tracker = PositionTracker::new();
        tracker.update("client-1", LogPosition::new("mysql-bin.000001", 100));
        tracker.remove("client-1");
        assert!(tracker.get("client-1").is_none());
    }

    #[test]
    fn test_missing_client() {
        let tracker = PositionTracker::new();
        assert!(tracker.get("no-such-client").is_none());
    }
}
