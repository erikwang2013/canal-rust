//! End-to-end handshake tests: full ClientAuth → Sub → Get → Messages →
//! ClientAck flow against an in-process mock server on the loopback
//! interface (no external services).

use canal_client::{read_packet, send_packet, CanalClient};
use canal_common::{CanalError, FilterPattern, LogPosition};
use canal_proto::{
    header::EventTypePresent, Ack, Entry, Header, Messages, Packet, PacketType, RowChange,
};
use prost::Message;
use tokio::net::TcpListener;

fn test_header(event_type: i32) -> Header {
    Header {
        logfile_name: "mysql-bin.000001".into(),
        logfile_offset: 100,
        server_id: 1,
        serveren_code: "utf8".into(),
        execute_time: 1700000000,
        schema_name: "test_db".into(),
        table_name: "users".into(),
        event_length: 0,
        props: vec![],
        gtid: "abc123:1-5".into(),
        version_present: None,
        source_type_present: None,
        event_type_present: Some(EventTypePresent::EventType(event_type)),
    }
}

fn insert_entry() -> Vec<u8> {
    let row_change = RowChange {
        table_id: 0,
        sql: String::new(),
        row_datas: vec![canal_proto::RowData {
            before_columns: vec![],
            after_columns: vec![canal_proto::Column {
                index: 0,
                sql_type: 3,
                name: "id".into(),
                is_key: true,
                updated: true,
                props: vec![],
                value: "42".into(),
                length: 0,
                mysql_type: "int".into(),
                is_null_present: None,
            }],
            props: vec![],
        }],
        props: vec![],
        ddl_schema_name: String::new(),
        event_type_present: Some(canal_proto::row_change::EventTypePresent::EventType(1)),
        is_ddl_present: None,
    };
    let entry = Entry {
        header: Some(test_header(1)),
        store_value: row_change.encode_to_vec(),
        entry_type_present: None,
    };
    entry.encode_to_vec()
}

fn ack_packet(error_message: &str) -> Packet {
    Packet {
        r#type: PacketType::Ack as i32,
        body: Ack {
            error_message: error_message.into(),
            error_code_present: None,
        }
        .encode_to_vec(),
        ..Default::default()
    }
}

/// Mock server: ClientAuth → Ack; Sub → Ack; Get → Messages(1 entry); ClientAck; Get → terminal Ack.
async fn spawn_full_mock() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        // ClientAuth → Ack
        let pkt = read_packet(&mut stream).await.unwrap();
        assert_eq!(pkt.r#type, PacketType::Clientauthentication as i32);
        send_packet(&mut stream, &ack_packet("")).await.unwrap();
        // Sub → Ack
        let pkt = read_packet(&mut stream).await.unwrap();
        assert_eq!(pkt.r#type, PacketType::Subscription as i32);
        send_packet(&mut stream, &ack_packet("")).await.unwrap();
        // Get → Messages
        let pkt = read_packet(&mut stream).await.unwrap();
        assert_eq!(pkt.r#type, PacketType::Get as i32);
        let msgs = Messages {
            batch_id: 7,
            messages: vec![insert_entry()],
        };
        send_packet(
            &mut stream,
            &Packet {
                r#type: PacketType::Messages as i32,
                body: msgs.encode_to_vec(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        // ClientAck → discard
        let pkt = read_packet(&mut stream).await.unwrap();
        assert_eq!(pkt.r#type, PacketType::Clientack as i32);
        // Get → terminal Ack (ends the poll loop)
        let pkt = read_packet(&mut stream).await.unwrap();
        assert_eq!(pkt.r#type, PacketType::Get as i32);
        send_packet(&mut stream, &ack_packet("")).await.unwrap();
        // drain until the client closes
        let _ = read_packet(&mut stream).await;
    });
    addr.to_string()
}

#[tokio::test]
async fn connect_and_subscribe_full_flow() {
    let addr = spawn_full_mock().await;
    let (host, port) = addr.rsplit_once(':').unwrap();
    let mut client = CanalClient::new(host, port.parse().unwrap())
        .with_destination("test-dest")
        .with_filter(FilterPattern::default());
    client.connect().await.unwrap();
    // Ids start at 1001 and only increase (process-global counter).
    assert!(client.client_id() >= 1001);

    let mut stream = client
        .subscribe(Some(LogPosition::new("mysql-bin.000001", 4)))
        .await
        .unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next_event())
        .await
        .expect("timed out waiting for event")
        .expect("stream ended early");
    let event = result.unwrap();
    assert_eq!(event.schema_name, "test_db");
    assert_eq!(event.table_name, "users");
    // bg task ends after terminal Ack; next_event returns None
    let done = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next_event())
        .await
        .expect("timed out waiting for stream end");
    assert!(done.is_none());
}

#[tokio::test]
async fn connect_auth_failure() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _ = read_packet(&mut stream).await;
        send_packet(&mut stream, &ack_packet("invalid credentials"))
            .await
            .unwrap();
        let _ = read_packet(&mut stream).await;
    });
    let mut client = CanalClient::new("127.0.0.1", addr.port());
    let err = client.connect().await.unwrap_err();
    assert!(matches!(err, CanalError::AuthFailed(_)));
    assert!(err.to_string().contains("invalid credentials"));
}

#[tokio::test]
async fn connect_wrong_ack_type() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _ = read_packet(&mut stream).await;
        send_packet(
            &mut stream,
            &Packet {
                r#type: PacketType::Messages as i32,
                body: vec![],
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let _ = read_packet(&mut stream).await;
    });
    let mut client = CanalClient::new("127.0.0.1", addr.port());
    let err = client.connect().await.unwrap_err();
    assert!(matches!(err, CanalError::Protocol(_)));
    assert!(err.to_string().contains("expected Ack"));
}

#[tokio::test]
async fn connect_refused() {
    // Bind then drop to get a port that is not listening.
    let port = {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap().port()
    };
    let mut client = CanalClient::new("127.0.0.1", port);
    let err = client.connect().await.unwrap_err();
    assert!(matches!(err, CanalError::BinlogConnection(_)));
}

#[tokio::test]
async fn subscribe_without_connect_fails() {
    let mut client = CanalClient::new("127.0.0.1", 11111);
    match client.subscribe(None).await {
        Ok(_) => panic!("subscribe should fail without a connection"),
        Err(e) => {
            assert!(matches!(e, CanalError::Internal(_)));
            assert!(e.to_string().contains("not connected"));
        }
    }
}

// Note: CanalEventStream when the background task ends is covered by the
// inline `test_canal_event_stream_drop` unit test in src/lib.rs.
