use canal_common::LogPosition;
use std::collections::HashMap;
use std::sync::RwLock;

/// Tracks per-client binlog positions for ACK management
#[derive(Debug)]
pub struct PositionTracker {
    positions: RwLock<HashMap<String, LogPosition>>,
}

impl Default for PositionTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl PositionTracker {
    pub fn new() -> Self {
        Self {
            positions: RwLock::new(HashMap::new()),
        }
    }

    /// Record a client's acknowledged position
    pub fn update(&self, client_id: &str, position: LogPosition) {
        self.positions
            .write()
            .unwrap()
            .insert(client_id.to_string(), position);
    }

    /// Get a client's last acknowledged position
    pub fn get(&self, client_id: &str) -> Option<LogPosition> {
        self.positions.read().unwrap().get(client_id).cloned()
    }

    /// Remove a disconnected client's position tracking
    pub fn remove(&self, client_id: &str) {
        self.positions.write().unwrap().remove(client_id);
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
