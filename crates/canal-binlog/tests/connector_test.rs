//! Integration tests for DefaultBinlogConnector construction and lifecycle.
//! No MySQL connection is attempted: connect() requires an external server and
//! is exercised only via its pre-connect validation and state transitions.
use canal_binlog::{BinlogConnector, DefaultBinlogConnector};

fn make_connector() -> DefaultBinlogConnector {
    DefaultBinlogConnector::new("127.0.0.1", 3306, "canal", "secret", 1).unwrap()
}

#[test]
fn test_new_ok() {
    let conn = make_connector();
    assert!(conn.current_position().is_none());
}

#[test]
fn test_new_server_id_at_u32_max_ok() {
    assert!(DefaultBinlogConnector::new("h", 3306, "u", "p", u32::MAX as u64).is_ok());
}

#[test]
fn test_new_server_id_overflow_errors() {
    let result = DefaultBinlogConnector::new("h", 3306, "u", "p", u32::MAX as u64 + 1);
    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("server_id above u32::MAX must be rejected"),
    };
    assert!(matches!(err, canal_common::CanalError::Config(_)));
    assert!(err.to_string().contains("server_id"));
}

#[test]
fn test_with_channel_returns_receiver() {
    let (conn, mut rx) = make_connector().with_channel();
    assert!(conn.current_position().is_none());
    // Receiver is live but no events arrive without connect()
    let received = std::pin::pin!(async {
        tokio::time::timeout(std::time::Duration::from_millis(50), rx.recv()).await
    });
    let result = tokio::runtime::Runtime::new().unwrap().block_on(received);
    assert!(result.is_err(), "no events expected before connect()");
}

#[test]
fn test_connect_twice_guard_via_take_receiver() {
    // take_receiver must not be callable after with_channel reserved the sender
    let (conn, _rx) = make_connector().with_channel();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut conn = conn;
        conn.take_receiver();
    }));
    assert!(
        result.is_err(),
        "take_receiver after with_channel must panic"
    );
}

#[test]
fn test_take_receiver_ok() {
    let mut conn = make_connector();
    let rx = conn.take_receiver();
    assert!(conn.current_position().is_none());
    // Channel is usable (empty until connect)
    drop(rx);
}

#[test]
fn test_disconnect_without_connect_is_ok() {
    let mut conn = make_connector();
    let result = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(conn.disconnect());
    assert!(result.is_ok());
    assert!(conn.current_position().is_none());
}

#[test]
fn test_disconnect_twice_is_ok() {
    let mut conn = make_connector();
    let rt = tokio::runtime::Runtime::new().unwrap();
    assert!(rt.block_on(conn.disconnect()).is_ok());
    assert!(rt.block_on(conn.disconnect()).is_ok());
}

#[test]
fn test_connect_without_channel_errors() {
    // No sender configured -> connect fails fast with Internal error,
    // before any network activity
    let mut conn = make_connector();
    let result = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(conn.connect(&canal_common::LogPosition::new("mysql-bin.000001", 4)));
    assert!(matches!(result, Err(canal_common::CanalError::Internal(_))));
    assert!(conn.current_position().is_none());
}

#[test]
fn test_connect_after_disconnect_rejects_reconnect() {
    // disconnect() clears the sender; a second connect has no channel
    let mut conn = make_connector();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(conn.disconnect()).unwrap();
    let result = rt.block_on(conn.connect(&canal_common::LogPosition::new("bin.001", 4)));
    assert!(result.is_err());
}

#[test]
fn test_builder_chaining() {
    // Builder methods are chainable and keep the connector usable
    let (mut conn, _rx) = make_connector().with_connect_timeout(5).with_channel();
    let result = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(conn.disconnect());
    assert!(result.is_ok());
}
