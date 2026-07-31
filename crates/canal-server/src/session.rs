use canal_common::{FilterPattern, LogPosition, MutexLockExt};
use chrono::Utc;
use dashmap::DashMap;
use regex::Regex;
use std::sync::{Arc, Mutex};

/// A connected Canal client session.
pub struct ClientSession {
    pub client_id: String,
    pub destination: String,
    pub filter: FilterPattern,
    pub last_position: Mutex<Option<LogPosition>>,
    pub last_ack_position: Mutex<Option<LogPosition>>,
    pub connected_at: chrono::DateTime<Utc>,
    pub last_heartbeat: Mutex<chrono::DateTime<Utc>>,
    pub compiled_pattern: Option<Regex>,
    pub compiled_black_list: Option<Regex>,
}

impl std::fmt::Debug for ClientSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientSession")
            .field("client_id", &self.client_id)
            .field("destination", &self.destination)
            .field("filter", &self.filter)
            .field("connected_at", &self.connected_at)
            .finish_non_exhaustive()
    }
}

impl ClientSession {
    pub fn new(client_id: &str, destination: &str, filter: FilterPattern) -> Self {
        let now = Utc::now();
        let compiled_pattern = Regex::new(&filter.pattern).ok();
        let compiled_black_list = if filter.black_list.is_empty() {
            None
        } else {
            Regex::new(&filter.black_list).ok()
        };
        Self {
            client_id: client_id.to_string(),
            destination: destination.to_string(),
            filter,
            last_position: Mutex::new(None),
            last_ack_position: Mutex::new(None),
            connected_at: now,
            last_heartbeat: Mutex::new(now),
            compiled_pattern,
            compiled_black_list,
        }
    }
}

/// Manages all connected client sessions. Uses DashMap for lock-free reads.
#[derive(Debug, Default)]
pub struct SessionManager {
    sessions: DashMap<String, Arc<ClientSession>>,
}

impl SessionManager {
    pub fn new() -> Self {
        Self {
            sessions: DashMap::new(),
        }
    }

    pub fn register(&self, client_id: &str, destination: &str, filter: FilterPattern) {
        let session = ClientSession::new(client_id, destination, filter);
        self.sessions
            .insert(client_id.to_string(), Arc::new(session));
    }

    pub fn unregister(&self, client_id: &str) {
        self.sessions.remove(client_id);
    }

    pub fn get(&self, client_id: &str) -> Option<Arc<ClientSession>> {
        self.sessions.get(client_id).map(|r| Arc::clone(&*r))
    }

    pub fn update_position(&self, client_id: &str, pos: LogPosition) {
        if let Some(s) = self.sessions.get(client_id) {
            *s.last_position.lock_or_recover() = Some(pos);
        }
    }

    pub fn update_ack(&self, client_id: &str, pos: LogPosition) {
        if let Some(s) = self.sessions.get(client_id) {
            *s.last_ack_position.lock_or_recover() = Some(pos);
        }
    }

    pub fn heartbeat(&self, client_id: &str) {
        if let Some(s) = self.sessions.get(client_id) {
            *s.last_heartbeat.lock_or_recover() = Utc::now();
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
        assert_eq!(
            session
                .last_position
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .position,
            500
        );
        assert_eq!(
            session
                .last_ack_position
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .position,
            500
        );
    }

    #[test]
    fn test_heartbeat_updates_timestamp() {
        let mgr = SessionManager::new();
        mgr.register("client-1", "example", FilterPattern::default());

        let before = *mgr.get("client-1").unwrap().last_heartbeat.lock().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        mgr.heartbeat("client-1");
        let after = *mgr.get("client-1").unwrap().last_heartbeat.lock().unwrap();

        assert!(after > before);
    }
}
