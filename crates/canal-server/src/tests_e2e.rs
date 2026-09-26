//! canal-server tests: ack/rollback/heartbeat handlers + TCP end-to-end flow.
use super::tests_handlers::{
    auth_frame, auth_state, get_frame, make_stored_event, read_ack, read_packet, sub_frame,
    transport,
};
use super::*;
use bytes::BytesMut;
use canal_proto::{Ack, Entry, Messages};
use prost::Message;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Encoder;

// ---------------------------------------------------------------- ack / rollback / heartbeat

#[tokio::test]
async fn test_handle_client_ack_updates_session() {
    let sessions = SessionManager::new();
    sessions.register("c1", "example", FilterPattern::default());
    let end = LogPosition::new("mysql-bin.000001", 500);
    let mut state = ClientState {
        client_id: Some("c1".into()),
        last_get_batch_id: 7,
        last_get_end_pos: Some(end.clone()),
        ..Default::default()
    };

    let ack = canal_proto::ClientAck {
        destination: "example".into(),
        client_id: "c1".into(),
        batch_id: 7,
    };
    let packet = Packet {
        r#type: PacketType::Clientack as i32,
        body: ack.encode_to_vec(),
        ..Default::default()
    };
    handle_client_ack(&packet, &mut state, &sessions);

    assert_eq!(state.last_ack_pos.as_ref(), Some(&end));
    let session = sessions.get("c1").unwrap();
    assert_eq!(
        session.last_ack_position.lock().unwrap().as_ref(),
        Some(&end)
    );
}

#[tokio::test]
async fn test_handle_client_ack_mismatched_batch_is_ignored() {
    let sessions = SessionManager::new();
    sessions.register("c1", "example", FilterPattern::default());
    let end = LogPosition::new("mysql-bin.000001", 500);
    let mut state = ClientState {
        client_id: Some("c1".into()),
        last_get_batch_id: 7,
        last_get_end_pos: Some(end.clone()),
        ..Default::default()
    };

    let ack = canal_proto::ClientAck {
        destination: "example".into(),
        client_id: "c1".into(),
        batch_id: 999, // stale/unknown batch id
    };
    let packet = Packet {
        r#type: PacketType::Clientack as i32,
        body: ack.encode_to_vec(),
        ..Default::default()
    };
    handle_client_ack(&packet, &mut state, &sessions);
    assert!(
        state.last_ack_pos.is_none(),
        "a stale batch_id must not advance the ack cursor"
    );
    assert!(sessions
        .get("c1")
        .unwrap()
        .last_ack_position
        .lock()
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn test_handle_client_ack_stale_batch_does_not_advance_durable_ack() {
    // Regression (F3): Get -> batch 1 (end 100), Get -> batch 2 (end 200), then
    // Ack{batch_id: 1} used to jump the durable ack to 200 — the end of a batch
    // the client had not confirmed.
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

    for _ in 0..2 {
        let packet = Packet::decode(&get_frame("c1", 1)[..]).unwrap();
        handle_get(&mut tx, &packet, &mut state, &store, &sessions)
            .await
            .unwrap();
        read_packet(&mut rx).await;
    }
    assert_eq!(state.last_get_end_pos.as_ref().unwrap().position, 200);
    let stale_batch = state.last_get_batch_id - 1;

    let ack = canal_proto::ClientAck {
        destination: "example".into(),
        client_id: "c1".into(),
        batch_id: stale_batch,
    };
    let packet = Packet {
        r#type: PacketType::Clientack as i32,
        body: ack.encode_to_vec(),
        ..Default::default()
    };
    handle_client_ack(&packet, &mut state, &sessions);

    assert!(state.last_ack_pos.is_none());
    assert!(sessions
        .get("c1")
        .unwrap()
        .last_ack_position
        .lock()
        .unwrap()
        .is_none());

    // the batch the client was actually last sent still acks
    let ack = canal_proto::ClientAck {
        destination: "example".into(),
        client_id: "c1".into(),
        batch_id: state.last_get_batch_id,
    };
    let packet = Packet {
        r#type: PacketType::Clientack as i32,
        body: ack.encode_to_vec(),
        ..Default::default()
    };
    handle_client_ack(&packet, &mut state, &sessions);
    assert_eq!(
        state.last_ack_pos.as_ref().unwrap().position,
        200,
        "the current batch must still be ackable"
    );
}

#[tokio::test]
async fn test_handle_client_ack_garbage_body_noop() {
    let sessions = SessionManager::new();
    let mut state = ClientState::default();
    let packet = Packet {
        r#type: PacketType::Clientack as i32,
        body: vec![0xFF, 0xFF],
        ..Default::default()
    };
    handle_client_ack(&packet, &mut state, &sessions);
    assert!(state.last_ack_pos.is_none());
}

#[tokio::test]
async fn test_handle_client_ack_without_batch_does_nothing() {
    let sessions = SessionManager::new();
    let mut state = ClientState {
        client_id: Some("c1".into()),
        ..Default::default()
    };
    let ack = canal_proto::ClientAck {
        destination: "example".into(),
        client_id: "c1".into(),
        batch_id: 1,
    };
    let packet = Packet {
        r#type: PacketType::Clientack as i32,
        body: ack.encode_to_vec(),
        ..Default::default()
    };
    handle_client_ack(&packet, &mut state, &sessions);
    assert!(state.last_ack_pos.is_none());
    assert!(state.current_pos.is_none());
}

#[tokio::test]
async fn test_handle_client_rollback_restores_ack_position() {
    let ack_pos = LogPosition::new("mysql-bin.000001", 100);
    let mut state = ClientState {
        current_pos: Some(LogPosition::new("mysql-bin.000001", 999)),
        last_ack_pos: Some(ack_pos.clone()),
        ..Default::default()
    };

    let rollback = canal_proto::ClientRollback {
        destination: "example".into(),
        client_id: "c1".into(),
        batch_id: 1,
    };
    let packet = Packet {
        r#type: PacketType::Clientrollback as i32,
        body: rollback.encode_to_vec(),
        ..Default::default()
    };
    handle_client_rollback(&packet, &mut state);
    assert_eq!(state.current_pos.as_ref(), Some(&ack_pos));
}

#[tokio::test]
async fn test_handle_client_rollback_without_ack_clears_position() {
    let mut state = ClientState {
        current_pos: Some(LogPosition::new("mysql-bin.000001", 999)),
        ..Default::default()
    };
    let rollback = canal_proto::ClientRollback {
        destination: "example".into(),
        client_id: "c1".into(),
        batch_id: 1,
    };
    let packet = Packet {
        r#type: PacketType::Clientrollback as i32,
        body: rollback.encode_to_vec(),
        ..Default::default()
    };
    handle_client_rollback(&packet, &mut state);
    assert!(state.current_pos.is_none());
}

#[tokio::test]
async fn test_handle_client_rollback_garbage_body_noop() {
    let mut state = ClientState {
        current_pos: Some(LogPosition::new("mysql-bin.000001", 999)),
        ..Default::default()
    };
    let packet = Packet {
        r#type: PacketType::Clientrollback as i32,
        body: vec![0xFF],
        ..Default::default()
    };
    handle_client_rollback(&packet, &mut state);
    assert!(state.current_pos.is_some());
}

#[tokio::test]
async fn test_handle_heartbeat_acks_and_updates_session() {
    let (mut tx, mut rx) = transport();
    let (state, sessions) = auth_state(&mut tx, &mut rx, "c1", ".*\\..*").await;
    let before = *sessions.get("c1").unwrap().last_heartbeat.lock().unwrap();

    tokio::time::sleep(Duration::from_millis(5)).await;
    handle_heartbeat(&mut tx, &state, &sessions).await.unwrap();

    let ack = read_ack(&mut rx).await;
    assert!(ack.error_message.is_empty());
    let after = *sessions.get("c1").unwrap().last_heartbeat.lock().unwrap();
    assert!(after > before);
}

#[tokio::test]
async fn test_handle_get_blacklist_filters_events() {
    let (mut tx, mut rx) = transport();
    let store = Arc::new(MemoryEventStore::new(1024));
    store
        .put_batch(vec![
            make_stored_event("db", "users", 100),
            make_stored_event("db", "logs", 200),
        ])
        .await
        .unwrap();

    let sessions = SessionManager::new();
    let mut state = ClientState::default();
    let auth = canal_proto::ClientAuth {
        client_id: "c1".into(),
        filter: ".*\\..*".into(),
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
    // re-register with a blacklist that excludes db.logs
    let blacklisted = FilterPattern {
        pattern: ".*\\..*".into(),
        black_list: "db\\.logs".into(),
    };
    sessions.register("c1", "example", blacklisted);

    let packet = Packet::decode(&get_frame("c1", 100)[..]).unwrap();
    handle_get(&mut tx, &packet, &mut state, &store, &sessions)
        .await
        .unwrap();

    let resp = read_packet(&mut rx).await;
    let msgs = Messages::decode(&resp.body[..]).unwrap();
    assert_eq!(msgs.messages.len(), 1);
    let entry = Entry::decode(&msgs.messages[0][..]).unwrap();
    assert_eq!(entry.header.unwrap().table_name, "users");
}

// ---------------------------------------------------------------- server e2e

async fn spawn_server(
    auth: Option<String>,
) -> (
    SocketAddr,
    tokio_util::sync::CancellationToken,
    tokio::task::JoinHandle<CanalResult<()>>,
    Arc<MemoryEventStore>,
    Arc<CanalServer>,
) {
    let store = Arc::new(MemoryEventStore::new(1024));
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = probe.local_addr().unwrap();
    drop(probe);

    let mut server = CanalServer::new(addr, store.clone());
    if let Some(token) = auth {
        server = server.with_auth(token);
    }
    let token = server.shutdown_token();
    let server = Arc::new(server);
    let serving = server.clone();
    let handle = tokio::spawn(async move { serving.serve().await });
    (addr, token, handle, store, server)
}

async fn connect_with_retry(addr: SocketAddr) -> TcpStream {
    for _ in 0..200 {
        if let Ok(s) = TcpStream::connect(addr).await {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("failed to connect to {addr}");
}

async fn client_send(stream: &mut TcpStream, payload: &[u8]) {
    let mut buf = BytesMut::new();
    CanalCodec::new()
        .encode(payload.to_vec(), &mut buf)
        .unwrap();
    stream.write_all(&buf).await.unwrap();
}

async fn client_recv(stream: &mut TcpStream) -> Packet {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.unwrap();
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await.unwrap();
    Packet::decode(&payload[..]).unwrap()
}

#[tokio::test]
async fn test_server_builder_and_shutdown_token() {
    let store = Arc::new(MemoryEventStore::new(1024));
    let server = CanalServer::new("127.0.0.1:0".parse().unwrap(), store)
        .with_auth("secret".into())
        .with_idle_timeout(30);
    let token = server.shutdown_token();
    assert!(!token.is_cancelled());
    token.cancel();
    assert!(token.is_cancelled());
}

#[tokio::test]
async fn test_with_auth_ignores_empty_token() {
    // Regression (F5): config `auth_token: ""` deserializes to Some("") and used
    // to enable auth with an empty secret — any client got in while the operator
    // believed authentication was on.
    let store = Arc::new(MemoryEventStore::new(1024));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = CanalServer::new(addr, store)
        .with_auth(String::new())
        .with_auth("   ".into());
    assert!(
        server.auth_token.is_none(),
        "an empty/whitespace token must not enable authentication"
    );

    let store = Arc::new(MemoryEventStore::new(1024));
    let server = CanalServer::new(addr, store).with_auth("secret".into());
    assert_eq!(server.auth_token.as_deref(), Some("secret"));
}

#[tokio::test]
async fn test_server_serve_shuts_down_on_cancel() {
    let (addr, token, handle, _store, _server) = spawn_server(None).await;
    assert!(addr.port() > 0);
    token.cancel();
    let result = handle.await.unwrap();
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_completed_client_tasks_are_pruned() {
    // Regression (F9): the JoinSet was only drained at shutdown, so one finished
    // task (with its retained output) accumulated per connection ever served.
    let (addr, token, handle, _store, server) = spawn_server(None).await;

    for _ in 0..3 {
        let mut stream = connect_with_retry(addr).await;
        client_send(&mut stream, &auth_frame("c1", ".*\\..*", &[])).await;
        let pkt = client_recv(&mut stream).await;
        assert_eq!(pkt.r#type, PacketType::Ack as i32);
        drop(stream);
    }

    for _ in 0..100 {
        if server.client_tasks.lock().await.len() < 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let live = server.client_tasks.lock().await.len();
    assert!(
        live < 3,
        "completed client tasks must be pruned from the JoinSet, {} left",
        live
    );

    token.cancel();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_server_rejects_wrong_auth_token_over_tcp() {
    let (addr, token, handle, _store, _server) = spawn_server(Some("secret".into())).await;
    let mut stream = connect_with_retry(addr).await;

    client_send(&mut stream, &auth_frame("c1", ".*\\..*", b"wrong")).await;
    let pkt = client_recv(&mut stream).await;
    assert_eq!(pkt.r#type, PacketType::Ack as i32);
    let ack = Ack::decode(&pkt.body[..]).unwrap();
    assert!(ack.error_message.contains("authentication failed"));

    // unauthenticated client is refused for data packets too
    client_send(&mut stream, &get_frame("c1", 10)).await;
    let pkt = client_recv(&mut stream).await;
    assert_eq!(pkt.r#type, PacketType::Ack as i32);
    let ack = Ack::decode(&pkt.body[..]).unwrap();
    assert!(ack.error_message.contains("not authenticated"));

    drop(stream);
    token.cancel();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_server_full_client_flow_over_tcp() {
    let (addr, token, handle, store, _server) = spawn_server(None).await;
    store
        .put_batch(vec![make_stored_event("db", "t", 100)])
        .await
        .unwrap();

    let mut stream = connect_with_retry(addr).await;

    // authenticate
    client_send(&mut stream, &auth_frame("c1", ".*\\..*", &[])).await;
    let pkt = client_recv(&mut stream).await;
    assert_eq!(pkt.r#type, PacketType::Ack as i32);
    assert!(Ack::decode(&pkt.body[..]).unwrap().error_message.is_empty());

    // subscribe
    client_send(&mut stream, &sub_frame("c1", ".*\\..*")).await;
    let pkt = client_recv(&mut stream).await;
    assert_eq!(pkt.r#type, PacketType::Ack as i32);

    // get events
    client_send(&mut stream, &get_frame("c1", 10)).await;
    let pkt = client_recv(&mut stream).await;
    assert_eq!(pkt.r#type, PacketType::Messages as i32);
    let msgs = Messages::decode(&pkt.body[..]).unwrap();
    assert_eq!(msgs.messages.len(), 1);
    let entry = Entry::decode(&msgs.messages[0][..]).unwrap();
    let hdr = entry.header.unwrap();
    assert_eq!(hdr.logfile_name, "mysql-bin.000001");
    assert_eq!(hdr.logfile_offset, 100);

    // heartbeat still works after a get
    client_send(
        &mut stream,
        &Packet {
            r#type: PacketType::Heartbeat as i32,
            body: vec![],
            ..Default::default()
        }
        .encode_to_vec(),
    )
    .await;
    let pkt = client_recv(&mut stream).await;
    assert_eq!(pkt.r#type, PacketType::Ack as i32);

    drop(stream);
    token.cancel();
    handle.await.unwrap().unwrap();
}
