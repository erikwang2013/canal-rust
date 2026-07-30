use canal_common::{FilterPattern, LogPosition};
use chrono::Utc;
use std::collections::HashMap;
use std::sync::RwLock;

/// A connected Canal client session.
/// Mirrors Java's ClientIdentity + session state.
#[derive(Debug, Clone)]
pub struct ClientSession {
    pub client_id: String,
    pub destination: String,
    pub filter: FilterPattern,
    pub last_position: Option<LogPosition>,
    pub last_ack_position: Option<LogPosition>,
    pub connected_at: chrono::DateTime<Utc>,
    pub last_heartbeat: chrono::DateTime<Utc>,
}

impl ClientSession {
    pub fn new(client_id: &str, destination: &str, filter: FilterPattern) -> Self {
        let now = Utc::now();
        Self {
            client_id: client_id.to_string(),
            destination: destination.to_string(),
            filter,
            last_position: None,
            last_ack_position: None,
            connected_at: now,
            last_heartbeat: now,
        }
    }

    /// Update heartbeat timestamp (client is still alive)
    pub fn heartbeat(&mut self) {
        self.last_heartbeat = Utc::now();
    }
}

/// Manages all connected client sessions.
/// Thread-safe; can be shared across Tokio tasks via Arc.
#[derive(Debug, Default)]
pub struct SessionManager {
    sessions: RwLock<HashMap<String, ClientSession>>,
}

impl SessionManager {
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
        }
    }

    /// Register a new client session
    pub fn register(&self, client_id: &str, destination: &str, filter: FilterPattern) {
        let session = ClientSession::new(client_id, destination, filter);
        self.sessions
            .write()
            .unwrap()
            .insert(client_id.to_string(), session);
    }

    /// Remove a client session (on disconnect)
    pub fn unregister(&self, client_id: &str) {
        self.sessions.write().unwrap().remove(client_id);
    }

    /// Get a session by client_id
    pub fn get(&self, client_id: &str) -> Option<ClientSession> {
        self.sessions.read().unwrap().get(client_id).cloned()
    }

    /// Update a client's current read position
    pub fn update_position(&self, client_id: &str, pos: LogPosition) {
        if let Some(s) = self.sessions.write().unwrap().get_mut(client_id) {
            s.last_position = Some(pos);
        }
    }

    /// Update a client's acknowledged position
    pub fn update_ack(&self, client_id: &str, pos: LogPosition) {
        if let Some(s) = self.sessions.write().unwrap().get_mut(client_id) {
            s.last_ack_position = Some(pos);
        }
    }

    /// Record a heartbeat from a client
    pub fn heartbeat(&self, client_id: &str) {
        if let Some(s) = self.sessions.write().unwrap().get_mut(client_id) {
            s.heartbeat();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_lifecycle() {
        let mgr = SessionManager::new();
        let filter = FilterPattern::default();

        mgr.register("client-1", "example", filter);
        assert!(mgr.get("client-1").is_some());

        mgr.unregister("client-1");
        assert!(mgr.get("client-1").is_none());
    }

    #[test]
    fn test_position_tracking() {
        let mgr = SessionManager::new();
        mgr.register("client-1", "example", FilterPattern::default());

        let pos = LogPosition::new("mysql-bin.000001", 500);
        mgr.update_position("client-1", pos.clone());
        mgr.update_ack("client-1", pos);

        let session = mgr.get("client-1").unwrap();
        assert_eq!(session.last_position.unwrap().position, 500);
        assert_eq!(session.last_ack_position.unwrap().position, 500);
    }

    #[test]
    fn test_heartbeat_updates_timestamp() {
        let mgr = SessionManager::new();
        mgr.register("client-1", "example", FilterPattern::default());

        let before = mgr.get("client-1").unwrap().last_heartbeat;
        std::thread::sleep(std::time::Duration::from_millis(10));
        mgr.heartbeat("client-1");
        let after = mgr.get("client-1").unwrap().last_heartbeat;

        assert!(after > before);
    }
}
