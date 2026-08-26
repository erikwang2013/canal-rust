//! Integration tests for canal-admin's public construction API.

use canal_admin::{AdminServer, AdminState};
use canal_instance::instance::InstanceManager;
use std::sync::Arc;

#[test]
fn test_admin_server_construction_and_auth_flag() {
    let mgr = Arc::new(InstanceManager::new());
    let server = AdminServer::new("127.0.0.1:12345", mgr);
    assert_eq!(server.bind_addr, "127.0.0.1:12345");

    let server = server.with_auth("hunter2".into());
    assert_eq!(server.bind_addr, "127.0.0.1:12345");
    // Debug must expose the bind address but never the token.
    let debug_str = format!("{:?}", server);
    assert!(debug_str.contains("127.0.0.1:12345"));
    assert!(!debug_str.contains("hunter2"));
}

#[test]
fn test_admin_state_debug_never_leaks_token() {
    let mgr = Arc::new(InstanceManager::new());
    let state = AdminState {
        instance_manager: mgr,
        started_at: std::time::Instant::now(),
        admin_token: Some("sup3r-s3cret".into()),
    };
    let debug_str = format!("{:?}", state);
    assert!(debug_str.contains("has_auth: true"));
    assert!(!debug_str.contains("sup3r-s3cret"));
}

#[test]
fn test_admin_state_debug_reports_no_auth() {
    let mgr = Arc::new(InstanceManager::new());
    let state = AdminState {
        instance_manager: mgr,
        started_at: std::time::Instant::now(),
        admin_token: None,
    };
    let debug_str = format!("{:?}", state);
    assert!(debug_str.contains("has_auth: false"));
}
