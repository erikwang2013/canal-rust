//! Integration tests for canal-client: Entry decoding and packet framing.
//! The full ClientAuth → Sub → Get → Messages → ClientAck handshake flow
//! lives in tests/e2e.rs (both use only in-process loopback connections).

use canal_client::{entry_bytes_to_event, read_packet, send_packet};
use canal_common::{CanalError, DmlType, EventType};
use canal_proto::{
    header::EventTypePresent, row_change::IsDdlPresent, Entry, Header, Packet, PacketType,
    RowChange as ProtoRowChange, RowData as ProtoRowData,
};
use prost::Message;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

// ── Helpers ─────────────────────────────────────────────────

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
    let row_change = ProtoRowChange {
        table_id: 0,
        sql: String::new(),
        row_datas: vec![ProtoRowData {
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

// ── entry_bytes_to_event unit cases ─────────────────────────

#[test]
fn decode_insert_event() {
    let event = entry_bytes_to_event(&insert_entry()).unwrap();
    assert_eq!(event.journal_name, "mysql-bin.000001");
    assert_eq!(event.position, 100);
    assert_eq!(event.schema_name, "test_db");
    assert_eq!(event.table_name, "users");
    assert_eq!(event.entry_type, EventType::Insert);
    assert_eq!(event.gtid.as_deref(), Some("abc123:1-5"));
    let rc = event.row_change.expect("row change");
    assert_eq!(rc.dml_type, DmlType::Insert);
    assert_eq!(rc.before, None);
    let after = rc.after.unwrap();
    assert_eq!(after.columns.len(), 1);
    assert_eq!(after.columns[0].name, "id");
    assert_eq!(after.columns[0].value.as_deref(), Some("42"));
    assert!(after.columns[0].is_key);
    assert!(after.columns[0].updated);
    assert!(event.ddl_sql.is_none());
    assert!(!event.raw_bytes.is_empty());
}

#[test]
fn decode_update_event_with_before_and_after() {
    let rc = ProtoRowChange {
        table_id: 0,
        sql: String::new(),
        row_datas: vec![ProtoRowData {
            before_columns: vec![canal_proto::Column {
                index: 0,
                sql_type: 3,
                name: "id".into(),
                is_key: true,
                updated: false,
                props: vec![],
                value: "1".into(),
                length: 0,
                mysql_type: "int".into(),
                is_null_present: None,
            }],
            after_columns: vec![canal_proto::Column {
                index: 0,
                sql_type: 3,
                name: "id".into(),
                is_key: true,
                updated: false,
                props: vec![],
                value: "2".into(),
                length: 0,
                mysql_type: "int".into(),
                is_null_present: None,
            }],
            props: vec![],
        }],
        props: vec![],
        ddl_schema_name: String::new(),
        event_type_present: Some(canal_proto::row_change::EventTypePresent::EventType(2)),
        is_ddl_present: None,
    };
    let entry = Entry {
        header: Some(test_header(2)),
        store_value: rc.encode_to_vec(),
        entry_type_present: None,
    };
    let event = entry_bytes_to_event(&entry.encode_to_vec()).unwrap();
    assert_eq!(event.entry_type, EventType::Update);
    let rc = event.row_change.unwrap();
    assert_eq!(rc.dml_type, DmlType::Update);
    assert_eq!(rc.before.unwrap().columns[0].value.as_deref(), Some("1"));
    assert_eq!(rc.after.unwrap().columns[0].value.as_deref(), Some("2"));
}

#[test]
fn decode_delete_event() {
    let rc = ProtoRowChange {
        table_id: 0,
        sql: String::new(),
        row_datas: vec![ProtoRowData {
            before_columns: vec![canal_proto::Column {
                index: 0,
                sql_type: 3,
                name: "id".into(),
                is_key: true,
                updated: false,
                props: vec![],
                value: "9".into(),
                length: 0,
                mysql_type: "int".into(),
                is_null_present: None,
            }],
            after_columns: vec![],
            props: vec![],
        }],
        props: vec![],
        ddl_schema_name: String::new(),
        event_type_present: Some(canal_proto::row_change::EventTypePresent::EventType(3)),
        is_ddl_present: None,
    };
    let entry = Entry {
        header: Some(test_header(3)),
        store_value: rc.encode_to_vec(),
        entry_type_present: None,
    };
    let event = entry_bytes_to_event(&entry.encode_to_vec()).unwrap();
    assert_eq!(event.entry_type, EventType::Delete);
    assert_eq!(event.row_change.unwrap().dml_type, DmlType::Delete);
}

#[test]
fn decode_ddl_event() {
    let rc = ProtoRowChange {
        table_id: 0,
        sql: "ALTER TABLE users ADD COLUMN age INT".into(),
        row_datas: vec![],
        props: vec![],
        ddl_schema_name: String::new(),
        event_type_present: Some(canal_proto::row_change::EventTypePresent::EventType(7)),
        is_ddl_present: Some(IsDdlPresent::IsDdl(true)),
    };
    let entry = Entry {
        header: Some(test_header(7)),
        store_value: rc.encode_to_vec(),
        entry_type_present: None,
    };
    let event = entry_bytes_to_event(&entry.encode_to_vec()).unwrap();
    assert_eq!(event.entry_type, EventType::Ddl);
    assert_eq!(
        event.ddl_sql.as_deref(),
        Some("ALTER TABLE users ADD COLUMN age INT")
    );
    assert!(event.row_change.is_none());
}

#[test]
fn decode_null_column_becomes_none() {
    let rc = ProtoRowChange {
        table_id: 0,
        sql: String::new(),
        row_datas: vec![ProtoRowData {
            before_columns: vec![],
            after_columns: vec![canal_proto::Column {
                index: 0,
                sql_type: 253,
                name: "nickname".into(),
                is_key: false,
                updated: false,
                props: vec![],
                value: String::new(),
                length: 0,
                mysql_type: "varchar".into(),
                is_null_present: Some(canal_proto::column::IsNullPresent::IsNull(true)),
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
        store_value: rc.encode_to_vec(),
        entry_type_present: None,
    };
    let event = entry_bytes_to_event(&entry.encode_to_vec()).unwrap();
    let after = event.row_change.unwrap().after.unwrap();
    assert_eq!(after.columns[0].value, None);
}

#[test]
fn decode_empty_store_value_ok() {
    let entry = Entry {
        header: Some(test_header(5)),
        store_value: vec![],
        entry_type_present: None,
    };
    let event = entry_bytes_to_event(&entry.encode_to_vec()).unwrap();
    assert_eq!(event.entry_type, EventType::Query);
    assert!(event.row_change.is_none());
    assert!(event.ddl_sql.is_none());
}

#[test]
fn decode_invalid_store_value_falls_back_to_none() {
    let entry = Entry {
        header: Some(test_header(1)),
        store_value: vec![0xff, 0x00, 0x01], // not valid RowChange
        entry_type_present: None,
    };
    let event = entry_bytes_to_event(&entry.encode_to_vec()).unwrap();
    assert!(event.row_change.is_none());
    assert!(event.ddl_sql.is_none());
}

#[test]
fn decode_missing_header_is_error() {
    let entry = Entry {
        header: None,
        store_value: vec![],
        entry_type_present: None,
    };
    let err = entry_bytes_to_event(&entry.encode_to_vec()).unwrap_err();
    assert!(matches!(err, CanalError::Protocol(_)));
    assert!(err.to_string().contains("header"));
}

#[test]
fn decode_garbage_bytes_is_error() {
    let err = entry_bytes_to_event(&[0xde, 0xad, 0xbe, 0xef]).unwrap_err();
    assert!(matches!(err, CanalError::Protocol(_)));
}

#[test]
fn decode_event_type_mapping() {
    // proto values the server uses: 7=Query(→Ddl), 13=Xacommit(→Xid), 15=Mheartbeat(→Heartbeat)
    for (proto, expected) in [(7, EventType::Ddl), (13, EventType::Xid), (15, EventType::Heartbeat)] {
        let entry = Entry {
            header: Some(test_header(proto)),
            store_value: vec![],
            entry_type_present: None,
        };
        let event = entry_bytes_to_event(&entry.encode_to_vec()).unwrap();
        assert_eq!(event.entry_type, expected);
    }
    let entry = Entry {
        header: Some(test_header(99)),
        store_value: vec![],
        entry_type_present: None,
    };
    let event = entry_bytes_to_event(&entry.encode_to_vec()).unwrap();
    assert_eq!(event.entry_type, EventType::Unknown(99));
}

#[test]
fn decode_gtid_absent_becomes_none() {
    let mut header = test_header(1);
    header.gtid = String::new();
    let entry = Entry {
        header: Some(header),
        store_value: vec![],
        entry_type_present: None,
    };
    let event = entry_bytes_to_event(&entry.encode_to_vec()).unwrap();
    assert_eq!(event.gtid, None);
}

// ── packet framing (loopback TCP pair, no external resources) ──

async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let client = TcpStream::connect(addr).await.unwrap();
    let (server, _) = listener.accept().await.unwrap();
    (client, server)
}

#[tokio::test]
async fn packet_roundtrip() {
    let (mut a, mut b) = tcp_pair().await;
    let packet = Packet {
        r#type: PacketType::Messages as i32,
        body: vec![1, 2, 3],
        ..Default::default()
    };
    send_packet(&mut a, &packet).await.unwrap();
    let got = read_packet(&mut b).await.unwrap();
    assert_eq!(got.r#type, packet.r#type);
    assert_eq!(got.body, packet.body);
}

#[tokio::test]
async fn read_packet_rejects_zero_length() {
    let (mut a, mut b) = tcp_pair().await;
    a.write_all(&[0u8; 4]).await.unwrap();
    let err = read_packet(&mut b).await.unwrap_err();
    assert!(matches!(err, CanalError::Protocol(_)));
    assert!(err.to_string().contains("zero-length"));
}

#[tokio::test]
async fn read_packet_rejects_oversized() {
    let (mut a, mut b) = tcp_pair().await;
    // 8MB + 1 header without body; client must fail on the header alone
    a.write_all(&((8 * 1024 * 1024 + 1) as u32).to_be_bytes())
        .await
        .unwrap();
    let err = read_packet(&mut b).await.unwrap_err();
    assert!(matches!(err, CanalError::Protocol(_)));
    assert!(err.to_string().contains("too large"));
}

#[tokio::test]
async fn read_packet_rejects_invalid_protobuf() {
    let (mut a, mut b) = tcp_pair().await;
    let body = [0xffu8; 16];
    let mut buf = Vec::with_capacity(4 + body.len());
    buf.extend_from_slice(&(body.len() as u32).to_be_bytes());
    buf.extend_from_slice(&body);
    a.write_all(&buf).await.unwrap();
    let err = read_packet(&mut b).await.unwrap_err();
    assert!(matches!(err, CanalError::Protocol(_)));
}

#[tokio::test]
async fn read_packet_eof_is_io_error() {
    let (mut a, mut b) = tcp_pair().await;
    a.write_all(&100u32.to_be_bytes()).await.unwrap();
    drop(a); // close before body arrives → EOF on body read
    let err = read_packet(&mut b).await.unwrap_err();
    assert!(matches!(err, CanalError::Io(_)));
}

