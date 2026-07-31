use canal_common::{FilterPattern, LogPosition};
use chrono::Utc;
use dashmap::DashMap;

/// A connected Canal client session.
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

    pub fn heartbeat(&mut self) {
        self.last_heartbeat = Utc::now();
    }
}

/// Manages all connected client sessions. Uses DashMap for lock-free reads.
#[derive(Debug, Default)]
pub struct SessionManager {
    sessions: DashMap<String, ClientSession>,
}

impl SessionManager {
    pub fn new() -> Self {
        Self {
            sessions: DashMap::new(),
        }
    }

    pub fn register(&self, client_id: &str, destination: &str, filter: FilterPattern) {
        let session = ClientSession::new(client_id, destination, filter);
        self.sessions.insert(client_id.to_string(), session);
    }

    pub fn unregister(&self, client_id: &str) {
        self.sessions.remove(client_id);
    }

    pub fn get(&self, client_id: &str) -> Option<ClientSession> {
        self.sessions.get(client_id).map(|r| r.clone())
    }

    pub fn update_position(&self, client_id: &str, pos: LogPosition) {
        if let Some(mut s) = self.sessions.get_mut(client_id) {
            s.last_position = Some(pos);
        }
    }

    pub fn update_ack(&self, client_id: &str, pos: LogPosition) {
        if let Some(mut s) = self.sessions.get_mut(client_id) {
            s.last_ack_position = Some(pos);
        }
    }

    pub fn heartbeat(&self, client_id: &str) {
        if let Some(mut s) = self.sessions.get_mut(client_id) {
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
