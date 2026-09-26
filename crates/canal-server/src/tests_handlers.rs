//! Handler-level tests for canal-server (auth / subscription / get).
//! Shared helpers are pub(crate) for use by tests_e2e.
use super::*;
use canal_common::{CanalEvent, EventType};
use canal_proto::Entry;
use prost::Message;
use tokio::io::DuplexStream;
use tokio_util::codec::Framed;

pub(crate) type TestTransport = Framed<DuplexStream, CanalCodec>;

pub(crate) fn transport() -> (TestTransport, TestTransport) {
    let (a, b) = tokio::io::duplex(16384);
    (
        Framed::new(a, CanalCodec::new()),
        Framed::new(b, CanalCodec::new()),
    )
}

fn frame(ptype: PacketType, body: Vec<u8>) -> Vec<u8> {
    Packet {
        r#type: ptype as i32,
        body,
        ..Default::default()
    }
    .encode_to_vec()
}

pub(crate) fn auth_frame(client_id: &str, filter: &str, password: &[u8]) -> Vec<u8> {
    frame(
        PacketType::Clientauthentication,
        canal_proto::ClientAuth {
            username: "canal".into(),
            password: password.to_vec(),
            destination: "example".into(),
            client_id: client_id.into(),
            filter: filter.into(),
            ..Default::default()
        }
        .encode_to_vec(),
    )
}

pub(crate) fn sub_frame(client_id: &str, filter: &str) -> Vec<u8> {
    frame(
        PacketType::Subscription,
        canal_proto::Sub {
            destination: "example".into(),
            client_id: client_id.into(),
            filter: filter.into(),
        }
        .encode_to_vec(),
    )
}

pub(crate) fn get_frame(client_id: &str, fetch_size: i32) -> Vec<u8> {
    frame(
        PacketType::Get,
        canal_proto::Get {
            destination: "example".into(),
            client_id: client_id.into(),
            fetch_size,
            ..Default::default()
        }
        .encode_to_vec(),
    )
}

pub(crate) async fn read_packet(reader: &mut TestTransport) -> Packet {
    let bytes = reader
        .next()
        .await
        .expect("stream should yield a frame")
        .expect("no codec error");
    Packet::decode(&bytes[..]).expect("valid packet payload")
}

pub(crate) async fn read_ack(reader: &mut TestTransport) -> canal_proto::Ack {
    let pkt = read_packet(reader).await;
    assert_eq!(pkt.r#type, PacketType::Ack as i32);
    canal_proto::Ack::decode(&pkt.body[..]).unwrap()
}

pub(crate) async fn auth_state(
    tx: &mut TestTransport,
    rx: &mut TestTransport,
    client_id: &str,
    filter: &str,
) -> (ClientState, SessionManager) {
    let mut state = ClientState::default();
    let sessions = SessionManager::new();
    let packet = Packet::decode(&auth_frame(client_id, filter, &[])[..]).unwrap();
    handle_auth(tx, &packet, &mut state, &None, &sessions)
        .await
        .unwrap();
    read_ack(rx).await;
    (state, sessions)
}

pub(crate) fn make_stored_event(schema: &str, table: &str, pos: u64) -> CanalEvent {
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

// ---------------------------------------------------------------- auth

#[tokio::test]
async fn test_handle_auth_success_no_token() {
    let (mut tx, mut rx) = transport();
    let mut state = ClientState::default();
    let sessions = SessionManager::new();

    let packet = Packet::decode(&auth_frame("alice", ".*\\..*", &[])[..]).unwrap();
    handle_auth(&mut tx, &packet, &mut state, &None, &sessions)
        .await
        .unwrap();

    assert!(state.authenticated);
    assert_eq!(state.client_id.as_deref(), Some("alice"));
    let session = sessions.get("alice").unwrap();
    assert_eq!(session.destination, "example");

    let ack = read_ack(&mut rx).await;
    assert!(ack.error_message.is_empty());
    assert!(ack.error_code_present.is_none());
}

#[tokio::test]
async fn test_handle_auth_with_matching_token() {
    let (mut tx, mut rx) = transport();
    let mut state = ClientState::default();
    let sessions = SessionManager::new();

    let packet = Packet::decode(&auth_frame("bob", ".*\\..*", b"secret")[..]).unwrap();
    handle_auth(
        &mut tx,
        &packet,
        &mut state,
        &Some("secret".into()),
        &sessions,
    )
    .await
    .unwrap();
    assert!(state.authenticated);

    let ack = read_ack(&mut rx).await;
    assert!(ack.error_message.is_empty());
}

#[tokio::test]
async fn test_handle_auth_wrong_password_rejected() {
    let (mut tx, mut rx) = transport();
    let mut state = ClientState::default();
    let sessions = SessionManager::new();

    let packet = Packet::decode(&auth_frame("mallory", ".*\\..*", b"wrong")[..]).unwrap();
    handle_auth(
        &mut tx,
        &packet,
        &mut state,
        &Some("secret".into()),
        &sessions,
    )
    .await
    .unwrap();
    assert!(!state.authenticated);
    assert_eq!(state.auth_error_count, 1);
    assert!(sessions.get("mallory").is_none());

    let ack = read_ack(&mut rx).await;
    assert!(ack.error_message.contains("authentication failed"));
    assert!(ack.error_code_present.is_some());
}

#[tokio::test]
async fn test_handle_auth_three_failures_disconnect() {
    let (mut tx, mut rx) = transport();
    let mut state = ClientState::default();
    let sessions = SessionManager::new();

    let packet = Packet::decode(&auth_frame("mallory", ".*\\..*", b"wrong")[..]).unwrap();
    for _ in 0..2 {
        handle_auth(
            &mut tx,
            &packet,
            &mut state,
            &Some("secret".into()),
            &sessions,
        )
        .await
        .unwrap();
        read_ack(&mut rx).await;
    }
    let err = handle_auth(
        &mut tx,
        &packet,
        &mut state,
        &Some("secret".into()),
        &sessions,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, CanalError::AuthFailed(_)));
}

#[tokio::test]
async fn test_handle_auth_empty_client_id_uses_anonymous() {
    let (mut tx, mut rx) = transport();
    let mut state = ClientState::default();
    let sessions = SessionManager::new();

    let packet = Packet::decode(&auth_frame("", ".*\\..*", &[])[..]).unwrap();
    handle_auth(&mut tx, &packet, &mut state, &None, &sessions)
        .await
        .unwrap();
    assert_eq!(state.client_id.as_deref(), Some("anonymous"));
    assert!(sessions.get("anonymous").is_some());
    read_ack(&mut rx).await;
}

#[tokio::test]
async fn test_handle_auth_filter_too_long_rejected() {
    let (mut tx, mut rx) = transport();
    let mut state = ClientState::default();
    let sessions = SessionManager::new();

    let long_filter = "a".repeat(257);
    let packet = Packet::decode(&auth_frame("alice", &long_filter, &[])[..]).unwrap();
    handle_auth(&mut tx, &packet, &mut state, &None, &sessions)
        .await
        .unwrap();

    assert!(!state.authenticated);
    assert!(sessions.get("alice").is_none());
    let ack = read_ack(&mut rx).await;
    assert!(ack.error_message.contains("filter pattern too long"));
}

#[tokio::test]
async fn test_handle_auth_invalid_filter_regex_rejected() {
    let (mut tx, mut rx) = transport();
    let mut state = ClientState::default();
    let sessions = SessionManager::new();

    let packet = Packet::decode(&auth_frame("alice", "[invalid", &[])[..]).unwrap();
    handle_auth(&mut tx, &packet, &mut state, &None, &sessions)
        .await
        .unwrap();

    assert!(!state.authenticated);
    assert!(sessions.get("alice").is_none());
    let ack = read_ack(&mut rx).await;
    assert!(ack.error_message.contains("invalid filter pattern"));
}

#[tokio::test]
async fn test_handle_auth_garbage_body_returns_protocol_error() {
    let (mut tx, _rx) = transport();
    let mut state = ClientState::default();
    let sessions = SessionManager::new();

    let packet = Packet {
        r#type: PacketType::Clientauthentication as i32,
        body: vec![0xFF, 0x00, 0xDE, 0xAD],
        ..Default::default()
    };
    let err = handle_auth(&mut tx, &packet, &mut state, &None, &sessions)
        .await
        .unwrap_err();
    assert!(matches!(err, CanalError::Protocol(_)));
}

#[tokio::test]
async fn test_handle_auth_start_timestamp_sets_position() {
    let (mut tx, mut rx) = transport();
    let mut state = ClientState::default();
    let sessions = SessionManager::new();

    let auth = canal_proto::ClientAuth {
        client_id: "ts".into(),
        start_timestamp: 1234567890,
        ..Default::default()
    };
    let packet = Packet {
        r#type: PacketType::Clientauthentication as i32,
        body: auth.encode_to_vec(),
        ..Default::default()
    };
    handle_auth(&mut tx, &packet, &mut state, &None, &sessions)
        .await
        .unwrap();

    assert_eq!(
        state.current_pos.as_ref().unwrap().timestamp,
        Some(1234567890)
    );
    assert_eq!(state.current_pos.as_ref().unwrap().position, 0);
    read_ack(&mut rx).await;
}

#[tokio::test]
async fn test_handle_auth_start_timestamp_then_get_returns_matching_events() {
    // Regression (F2): a non-zero start_timestamp used to set an empty
    // journal_name, whose binlog_suffix is u64::MAX — every event sorted at or
    // before that cursor, so the client was served empty batches forever.
    let (mut tx, mut rx) = transport();
    let store = Arc::new(MemoryEventStore::new(1024));
    let mut early = make_stored_event("db", "t", 100);
    early.execute_time = 1_000;
    let mut late = make_stored_event("db", "t", 200);
    late.execute_time = 2_000;
    store.put_batch(vec![early, late]).await.unwrap();

    let mut state = ClientState::default();
    let sessions = SessionManager::new();
    let auth = canal_proto::ClientAuth {
        client_id: "ts".into(),
        start_timestamp: 1_500,
        ..Default::default()
    };
    let packet = Packet {
        r#type: PacketType::Clientauthentication as i32,
        body: auth.encode_to_vec(),
        ..Default::default()
    };
    handle_auth(&mut tx, &packet, &mut state, &None, &sessions)
        .await
        .unwrap();
    read_ack(&mut rx).await;

    let packet = Packet::decode(&get_frame("ts", 100)[..]).unwrap();
    handle_get(&mut tx, &packet, &mut state, &store, &sessions)
        .await
        .unwrap();

    let resp = read_packet(&mut rx).await;
    assert_eq!(resp.r#type, PacketType::Messages as i32);
    let msgs = canal_proto::Messages::decode(&resp.body[..]).unwrap();
    assert_eq!(msgs.messages.len(), 1, "timestamp cursor must not starve");
    let entry = Entry::decode(&msgs.messages[0][..]).unwrap();
    assert_eq!(entry.header.unwrap().logfile_offset, 200);

    // After the first batch the cursor is positional, not timed
    let pos = state.current_pos.as_ref().unwrap();
    assert_eq!(pos.timestamp, None);
    assert_eq!(pos.position, 200);
}

#[tokio::test]
async fn test_handle_auth_second_auth_on_same_connection_rejected() {
    // Regression (F6): a second ClientAuthentication registered another session
    // entry that was never unregistered, leaking sessions with a live cursor.
    let (mut tx, mut rx) = transport();
    let mut state = ClientState::default();
    let sessions = SessionManager::new();

    let packet = Packet::decode(&auth_frame("alice", ".*\\..*", &[])[..]).unwrap();
    handle_auth(&mut tx, &packet, &mut state, &None, &sessions)
        .await
        .unwrap();
    read_ack(&mut rx).await;

    let packet = Packet::decode(&auth_frame("bob", ".*\\..*", &[])[..]).unwrap();
    handle_auth(&mut tx, &packet, &mut state, &None, &sessions)
        .await
        .unwrap();

    let ack = read_ack(&mut rx).await;
    assert!(ack.error_message.contains("already authenticated"));
    assert_eq!(state.client_id.as_deref(), Some("alice"));
    assert!(sessions.get("bob").is_none());
    assert!(sessions.get("alice").is_some());
}

// ---------------------------------------------------------------- subscription

#[tokio::test]
async fn test_handle_sub_client_id_mismatch_rejected() {
    let (mut tx, mut rx) = transport();
    let mut state = ClientState {
        client_id: Some("alice".into()),
        authenticated: true,
        ..Default::default()
    };
    let sessions = SessionManager::new();

    let packet = Packet::decode(&sub_frame("bob", "db\\..*")[..]).unwrap();
    handle_sub(&mut tx, &packet, &mut state, &sessions)
        .await
        .unwrap();

    let ack = read_ack(&mut rx).await;
    assert!(ack.error_message.contains("client_id mismatch"));
    assert!(sessions.get("bob").is_none());
}

#[tokio::test]
async fn test_handle_sub_success_updates_filter() {
    let (mut tx, mut rx) = transport();
    let mut state = ClientState {
        client_id: Some("alice".into()),
        authenticated: true,
        ..Default::default()
    };
    let sessions = SessionManager::new();

    let packet = Packet::decode(&sub_frame("alice", "db\\..*")[..]).unwrap();
    handle_sub(&mut tx, &packet, &mut state, &sessions)
        .await
        .unwrap();

    let ack = read_ack(&mut rx).await;
    assert!(ack.error_message.is_empty());
    let session = sessions.get("alice").unwrap();
    assert_eq!(session.filter.pattern, "db\\..*");
    assert_eq!(state.client_id.as_deref(), Some("alice"));
}

#[tokio::test]
async fn test_handle_sub_filter_too_long_rejected() {
    let (mut tx, mut rx) = transport();
    let mut state = ClientState {
        client_id: Some("alice".into()),
        authenticated: true,
        ..Default::default()
    };
    let sessions = SessionManager::new();

    let packet = Packet::decode(&sub_frame("alice", &"x".repeat(257))[..]).unwrap();
    handle_sub(&mut tx, &packet, &mut state, &sessions)
        .await
        .unwrap();

    let ack = read_ack(&mut rx).await;
    assert!(ack.error_message.contains("filter pattern too long"));
}

#[tokio::test]
async fn test_handle_sub_invalid_filter_regex_rejected() {
    let (mut tx, mut rx) = transport();
    let mut state = ClientState {
        client_id: Some("alice".into()),
        authenticated: true,
        ..Default::default()
    };
    let sessions = SessionManager::new();

    let packet = Packet::decode(&sub_frame("alice", "[bad")[..]).unwrap();
    handle_sub(&mut tx, &packet, &mut state, &sessions)
        .await
        .unwrap();

    let ack = read_ack(&mut rx).await;
    assert!(ack.error_message.contains("invalid filter pattern"));
}

// ---------------------------------------------------------------- get

#[tokio::test]
async fn test_handle_get_without_client_id_errors() {
    let (mut tx, _rx) = transport();
    let mut state = ClientState::default();
    let sessions = SessionManager::new();
    let store = Arc::new(MemoryEventStore::new(1024));

    let packet = Packet::decode(&get_frame("c1", 100)[..]).unwrap();
    let err = handle_get(&mut tx, &packet, &mut state, &store, &sessions)
        .await
        .unwrap_err();
    assert!(matches!(err, CanalError::Protocol(_)));
}

#[tokio::test]
async fn test_handle_get_returns_events_as_messages() {
    let (mut tx, mut rx) = transport();
    let store = Arc::new(MemoryEventStore::new(1024));
    store
        .put_batch(vec![make_stored_event("db", "t", 100)])
        .await
        .unwrap();

    let (mut state, sessions) = auth_state(&mut tx, &mut rx, "c1", ".*\\..*").await;

    let packet = Packet::decode(&get_frame("c1", 100)[..]).unwrap();
    handle_get(&mut tx, &packet, &mut state, &store, &sessions)
        .await
        .unwrap();

    let resp = read_packet(&mut rx).await;
    assert_eq!(resp.r#type, PacketType::Messages as i32);
    let msgs = canal_proto::Messages::decode(&resp.body[..]).unwrap();
    assert_eq!(msgs.messages.len(), 1);

    let entry = Entry::decode(&msgs.messages[0][..]).unwrap();
    let hdr = entry.header.unwrap();
    assert_eq!(hdr.logfile_name, "mysql-bin.000001");
    assert_eq!(hdr.logfile_offset, 100);

    // state tracks the end position for subsequent acks
    assert_eq!(state.last_get_end_pos.as_ref().unwrap().position, 100);
    assert!(state.current_pos.is_some());
}

#[tokio::test]
async fn test_handle_get_all_filtered_sends_empty_messages() {
    let (mut tx, mut rx) = transport();
    let store = Arc::new(MemoryEventStore::new(1024));
    store
        .put_batch(vec![make_stored_event("db", "t", 100)])
        .await
        .unwrap();

    // session filter matches nothing in the store
    let (mut state, sessions) = auth_state(&mut tx, &mut rx, "c1", "nomatch\\..*").await;

    let packet = Packet::decode(&get_frame("c1", 100)[..]).unwrap();
    handle_get(&mut tx, &packet, &mut state, &store, &sessions)
        .await
        .unwrap();

    let resp = read_packet(&mut rx).await;
    assert_eq!(resp.r#type, PacketType::Messages as i32);
    let msgs = canal_proto::Messages::decode(&resp.body[..]).unwrap();
    assert!(msgs.messages.is_empty());
}

#[tokio::test]
async fn test_handle_get_fetch_size_zero_clamped_to_one() {
    let (mut tx, mut rx) = transport();
    let store = Arc::new(MemoryEventStore::new(1024));
    store
        .put_batch(vec![
            make_stored_event("db", "t", 100),
            make_stored_event("db", "t", 200),
        ])
        .await
        .unwrap();

    let (mut state, sessions) = auth_state(&mut tx, &mut rx, "c1", ".*\\..*").await;
    let packet = Packet::decode(&get_frame("c1", 0)[..]).unwrap();
    handle_get(&mut tx, &packet, &mut state, &store, &sessions)
        .await
        .unwrap();
    let resp = read_packet(&mut rx).await;
    let msgs = canal_proto::Messages::decode(&resp.body[..]).unwrap();
    assert_eq!(msgs.messages.len(), 1);
}

#[tokio::test]
async fn test_handle_get_negative_fetch_size_clamped() {
    let (mut tx, mut rx) = transport();
    let store = Arc::new(MemoryEventStore::new(1024));
    store
        .put_batch(vec![
            make_stored_event("db", "t", 100),
            make_stored_event("db", "t", 200),
        ])
        .await
        .unwrap();

    let (mut state, sessions) = auth_state(&mut tx, &mut rx, "c1", ".*\\..*").await;
    let packet = Packet::decode(&get_frame("c1", -5)[..]).unwrap();
    handle_get(&mut tx, &packet, &mut state, &store, &sessions)
        .await
        .unwrap();
    let resp = read_packet(&mut rx).await;
    let msgs = canal_proto::Messages::decode(&resp.body[..]).unwrap();
    assert_eq!(msgs.messages.len(), 2);
}

/// Store with capacity 2 holding positions 200 and 300 — position 100 was evicted.
async fn store_with_evicted_position_100() -> Arc<MemoryEventStore> {
    let store = Arc::new(MemoryEventStore::new(2));
    store
        .put_batch(vec![
            make_stored_event("db", "t", 100),
            make_stored_event("db", "t", 200),
        ])
        .await
        .unwrap();
    store
        .put_batch(vec![make_stored_event("db", "t", 300)])
        .await
        .unwrap();
    assert_eq!(store.evicted_watermark().unwrap().position, 100);
    store
}

#[tokio::test]
async fn test_handle_get_cursor_below_watermark_reports_dropped_events() {
    // Regression (F7): a client whose cursor fell behind eviction used to get a
    // normal-looking contiguous batch that silently skipped the dropped events.
    let (mut tx, mut rx) = transport();
    let store = store_with_evicted_position_100().await;

    let (mut state, sessions) = auth_state(&mut tx, &mut rx, "c1", ".*\\..*").await;
    state.current_pos = Some(LogPosition::new("mysql-bin.000001", 50));

    let packet = Packet::decode(&get_frame("c1", 100)[..]).unwrap();
    handle_get(&mut tx, &packet, &mut state, &store, &sessions)
        .await
        .unwrap();

    let ack = read_ack(&mut rx).await;
    assert!(
        ack.error_message.contains("eviction watermark"),
        "expected a dropped-events error, got {:?}",
        ack.error_message
    );
    // the cursor is not advanced past the gap
    assert_eq!(state.current_pos.as_ref().unwrap().position, 50);
}

#[tokio::test]
async fn test_handle_get_fresh_client_below_watermark_is_served() {
    // A fresh client starts at the default cursor, which is also below the
    // watermark, but nothing was ever promised to it — it must not be errored.
    let (mut tx, mut rx) = transport();
    let store = store_with_evicted_position_100().await;

    let (mut state, sessions) = auth_state(&mut tx, &mut rx, "c1", ".*\\..*").await;
    assert!(state.current_pos.is_none());

    let packet = Packet::decode(&get_frame("c1", 100)[..]).unwrap();
    handle_get(&mut tx, &packet, &mut state, &store, &sessions)
        .await
        .unwrap();

    let resp = read_packet(&mut rx).await;
    assert_eq!(resp.r#type, PacketType::Messages as i32);
    let msgs = canal_proto::Messages::decode(&resp.body[..]).unwrap();
    assert_eq!(msgs.messages.len(), 2, "surviving events must be delivered");
}
