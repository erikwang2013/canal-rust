use super::*;
use canal_common::CanalEvent;

#[tokio::test]
async fn test_handle_client_registers_and_sends_events() {
    let store = Arc::new(MemoryEventStore::new(1024));
    let sessions = Arc::new(SessionManager::new());

    let event = CanalEvent {
        journal_name: "mysql-bin.000001".into(),
        position: 200,
        server_id: 1,
        execute_time: 0,
        entry_type: canal_common::EventType::Insert,
        schema_name: "test".into(),
        table_name: "t".into(),
        row_change: None,
        ddl_sql: None,
        gtid: None,
        raw_bytes: vec![],
    };
    store.put_batch(vec![event]).await.unwrap();

    sessions.register("test-client", "example", FilterPattern::default());
    let session = sessions.get("test-client").unwrap();
    assert_eq!(session.client_id, "test-client");

    sessions.unregister("test-client");
    assert!(sessions.get("test-client").is_none());

    let start_pos = LogPosition::new("mysql-bin.000001", 4);
    let batch = store.get_batch(&start_pos, 10).await.unwrap();
    assert_eq!(batch.events.len(), 1);
}

#[tokio::test]
async fn test_server_binds_to_port() {
    let store = Arc::new(MemoryEventStore::new(1024));
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = CanalServer::new(addr, store);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bound = listener.local_addr().unwrap();
    assert!(bound.port() > 0);
    drop(listener);
    drop(server);
}

#[test]
fn test_canal_event_to_entry_header_fields() {
    let event = CanalEvent {
        journal_name: "mysql-bin.000001".into(),
        position: 1234,
        server_id: 42,
        execute_time: 1690000000,
        entry_type: EventType::Insert,
        schema_name: "mydb".into(),
        table_name: "users".into(),
        row_change: None,
        ddl_sql: None,
        gtid: Some("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5".into()),
        raw_bytes: vec![0u8; 100],
    };

    let entry = canal_event_to_entry(&event);

    let h = entry.header.unwrap();
    assert_eq!(h.logfile_name, "mysql-bin.000001");
    assert_eq!(h.logfile_offset, 1234);
    assert_eq!(h.server_id, 42);
    assert_eq!(h.execute_time, 1690000000);
    assert_eq!(h.schema_name, "mydb");
    assert_eq!(h.table_name, "users");
    assert_eq!(h.gtid, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5");
    assert_eq!(h.event_length, 100);

    match h.event_type_present {
        Some(header::EventTypePresent::EventType(v)) => {
            assert_eq!(v, ProtoEventType::Insert as i32);
        }
        _ => panic!("expected EventTypePresent::EventType"),
    }
}

#[test]
fn test_canal_event_to_entry_with_row_change() {
    let row_change = canal_common::RowChange {
        table_name: "users".into(),
        schema_name: "mydb".into(),
        before: None,
        after: Some(canal_common::RowData {
            columns: vec![
                ColumnValue {
                    name: "id".into(),
                    value: Some("1".into()),
                    column_type: 3,
                    is_key: true,
                    updated: false,
                },
                ColumnValue {
                    name: "name".into(),
                    value: Some("alice".into()),
                    column_type: 253,
                    is_key: false,
                    updated: false,
                },
            ],
        }),
        dml_type: DmlType::Insert,
    };

    let event = CanalEvent {
        journal_name: "mysql-bin.000001".into(),
        position: 567,
        server_id: 1,
        execute_time: 0,
        entry_type: EventType::Insert,
        schema_name: "mydb".into(),
        table_name: "users".into(),
        row_change: Some(row_change),
        ddl_sql: None,
        gtid: None,
        raw_bytes: vec![],
    };

    let entry = canal_event_to_entry(&event);

    assert!(!entry.store_value.is_empty());
    let rc = RowChange::decode(&entry.store_value[..]).unwrap();
    assert_eq!(rc.row_datas.len(), 1);

    let rd = &rc.row_datas[0];
    assert_eq!(rd.before_columns.len(), 0);
    assert_eq!(rd.after_columns.len(), 2);

    assert_eq!(rd.after_columns[0].name, "id");
    assert_eq!(rd.after_columns[0].value, "1");
    assert!(rd.after_columns[0].is_key);

    assert_eq!(rd.after_columns[1].name, "name");
    assert_eq!(rd.after_columns[1].value, "alice");
    assert!(!rd.after_columns[1].is_key);
}

#[test]
fn test_canal_event_to_entry_update_with_before_and_after() {
    let row_change = canal_common::RowChange {
        table_name: "users".into(),
        schema_name: "mydb".into(),
        before: Some(canal_common::RowData {
            columns: vec![ColumnValue {
                name: "name".into(),
                value: Some("alice".into()),
                column_type: 253,
                is_key: false,
                updated: false,
            }],
        }),
        after: Some(canal_common::RowData {
            columns: vec![ColumnValue {
                name: "name".into(),
                value: Some("bob".into()),
                column_type: 253,
                is_key: false,
                updated: true,
            }],
        }),
        dml_type: DmlType::Update,
    };

    let event = CanalEvent {
        journal_name: "mysql-bin.000001".into(),
        position: 789,
        server_id: 1,
        execute_time: 0,
        entry_type: EventType::Update,
        schema_name: "mydb".into(),
        table_name: "users".into(),
        row_change: Some(row_change),
        ddl_sql: None,
        gtid: None,
        raw_bytes: vec![],
    };

    let entry = canal_event_to_entry(&event);
    assert!(!entry.store_value.is_empty());

    let rc = RowChange::decode(&entry.store_value[..]).unwrap();
    let rd = &rc.row_datas[0];

    assert_eq!(rd.before_columns.len(), 1);
    assert_eq!(rd.before_columns[0].value, "alice");
    assert!(!rd.before_columns[0].updated);

    assert_eq!(rd.after_columns.len(), 1);
    assert_eq!(rd.after_columns[0].value, "bob");
    assert!(rd.after_columns[0].updated);
}

#[test]
fn test_canal_event_to_entry_delete() {
    let row_change = canal_common::RowChange {
        table_name: "users".into(),
        schema_name: "mydb".into(),
        before: Some(canal_common::RowData {
            columns: vec![ColumnValue {
                name: "id".into(),
                value: Some("99".into()),
                column_type: 3,
                is_key: true,
                updated: false,
            }],
        }),
        after: None,
        dml_type: DmlType::Delete,
    };

    let event = CanalEvent {
        journal_name: "mysql-bin.000001".into(),
        position: 999,
        server_id: 1,
        execute_time: 0,
        entry_type: EventType::Delete,
        schema_name: "mydb".into(),
        table_name: "users".into(),
        row_change: Some(row_change),
        ddl_sql: None,
        gtid: None,
        raw_bytes: vec![],
    };

    let entry = canal_event_to_entry(&event);
    let rc = RowChange::decode(&entry.store_value[..]).unwrap();
    let rd = &rc.row_datas[0];

    assert_eq!(rd.before_columns.len(), 1);
    assert_eq!(rd.after_columns.len(), 0);
    assert_eq!(rd.before_columns[0].value, "99");
}

#[test]
fn test_canal_event_to_entry_ddl() {
    let event = CanalEvent {
        journal_name: "mysql-bin.000001".into(),
        position: 200,
        server_id: 1,
        execute_time: 0,
        entry_type: EventType::Ddl,
        schema_name: "mydb".into(),
        table_name: "users".into(),
        row_change: None,
        ddl_sql: Some("ALTER TABLE users ADD COLUMN age INT".into()),
        gtid: None,
        raw_bytes: vec![],
    };

    let entry = canal_event_to_entry(&event);
    let rc = RowChange::decode(&entry.store_value[..]).unwrap();
    assert_eq!(rc.sql, "ALTER TABLE users ADD COLUMN age INT");
    assert_eq!(rc.ddl_schema_name, "mydb");

    match rc.is_ddl_present {
        Some(row_change::IsDdlPresent::IsDdl(true)) => {}
        _ => panic!("expected IsDdl(true)"),
    }
}

#[test]
fn test_column_value_to_proto_with_null() {
    let col = ColumnValue {
        name: "email".into(),
        value: None,
        column_type: 253,
        is_key: false,
        updated: false,
    };

    let proto_col = column_value_to_proto(&col, false);
    assert_eq!(proto_col.name, "email");
    assert_eq!(proto_col.value, "");
    assert!(proto_col.is_null_present.is_some());
}

#[test]
fn test_send_ack_packet_structure() {
    let ack = Ack::default();
    let packet = Packet {
        r#type: PacketType::Ack as i32,
        body: ack.encode_to_vec(),
        ..Default::default()
    };

    let wire = packet.encode_to_vec();
    let decoded = Packet::decode(&wire[..]).unwrap();

    assert_eq!(decoded.r#type, PacketType::Ack as i32);
    let decoded_ack = Ack::decode(&decoded.body[..]).unwrap();
    assert_eq!(decoded_ack.error_code_present, None);
    assert_eq!(decoded_ack.error_message, "");
}

#[test]
fn test_send_ack_packet_with_error() {
    let ack = Ack {
        error_message: "something went wrong".into(),
        error_code_present: Some(canal_proto::ack::ErrorCodePresent::ErrorCode(5)),
    };

    let packet = Packet {
        r#type: PacketType::Ack as i32,
        body: ack.encode_to_vec(),
        ..Default::default()
    };

    let wire = packet.encode_to_vec();
    let decoded = Packet::decode(&wire[..]).unwrap();
    assert_eq!(decoded.r#type, PacketType::Ack as i32);

    let decoded_ack = Ack::decode(&decoded.body[..]).unwrap();
    assert_eq!(decoded_ack.error_message, "something went wrong");
    match decoded_ack.error_code_present {
        Some(canal_proto::ack::ErrorCodePresent::ErrorCode(5)) => {}
        _ => panic!("expected ErrorCode(5)"),
    }
}

#[test]
fn test_messages_packet_roundtrip() {
    let event = CanalEvent {
        journal_name: "mysql-bin.000001".into(),
        position: 42,
        server_id: 1,
        execute_time: 1000,
        entry_type: EventType::Insert,
        schema_name: "db".into(),
        table_name: "t".into(),
        row_change: None,
        ddl_sql: None,
        gtid: None,
        raw_bytes: vec![],
    };

    let entry = canal_event_to_entry(&event);
    let entry_bytes = entry.encode_to_vec();

    let mut msgs = Messages {
        batch_id: 1,
        ..Default::default()
    };
    msgs.messages.push(entry_bytes);

    let packet = Packet {
        r#type: PacketType::Messages as i32,
        body: msgs.encode_to_vec(),
        ..Default::default()
    };

    let wire = packet.encode_to_vec();
    let decoded_packet = Packet::decode(&wire[..]).unwrap();
    assert_eq!(decoded_packet.r#type, PacketType::Messages as i32);

    let decoded_msgs = Messages::decode(&decoded_packet.body[..]).unwrap();
    assert_eq!(decoded_msgs.batch_id, 1);
    assert_eq!(decoded_msgs.messages.len(), 1);

    let decoded_entry = Entry::decode(&decoded_msgs.messages[0][..]).unwrap();
    let h = decoded_entry.header.unwrap();
    assert_eq!(h.logfile_name, "mysql-bin.000001");
    assert_eq!(h.logfile_offset, 42);
}

#[test]
fn test_client_auth_encoding_roundtrip() {
    let auth = ClientAuth {
        username: "canal".into(),
        password: vec![0x12, 0x34],
        destination: "example".into(),
        client_id: "1001".into(),
        filter: ".*\\..*".into(),
        start_timestamp: 0,
        ..Default::default()
    };

    let packet = Packet {
        r#type: PacketType::Clientauthentication as i32,
        body: auth.encode_to_vec(),
        ..Default::default()
    };

    let wire = packet.encode_to_vec();
    let decoded = Packet::decode(&wire[..]).unwrap();
    assert_eq!(decoded.r#type, PacketType::Clientauthentication as i32);

    let decoded_auth = ClientAuth::decode(&decoded.body[..]).unwrap();
    assert_eq!(decoded_auth.username, "canal");
    assert_eq!(decoded_auth.client_id, "1001");
    assert_eq!(decoded_auth.filter, ".*\\..*");
}

#[test]
fn test_sub_encoding_roundtrip() {
    let sub = Sub {
        destination: "example".into(),
        client_id: "1001".into(),
        filter: "mydb\\.users".into(),
    };

    let packet = Packet {
        r#type: PacketType::Subscription as i32,
        body: sub.encode_to_vec(),
        ..Default::default()
    };

    let wire = packet.encode_to_vec();
    let decoded = Packet::decode(&wire[..]).unwrap();
    assert_eq!(decoded.r#type, PacketType::Subscription as i32);

    let decoded_sub = Sub::decode(&decoded.body[..]).unwrap();
    assert_eq!(decoded_sub.client_id, "1001");
    assert_eq!(decoded_sub.filter, "mydb\\.users");
}

#[test]
fn test_get_encoding_roundtrip() {
    let get = Get {
        destination: "example".into(),
        client_id: "1001".into(),
        fetch_size: 100,
        ..Default::default()
    };

    let packet = Packet {
        r#type: PacketType::Get as i32,
        body: get.encode_to_vec(),
        ..Default::default()
    };

    let wire = packet.encode_to_vec();
    let decoded = Packet::decode(&wire[..]).unwrap();
    assert_eq!(decoded.r#type, PacketType::Get as i32);

    let decoded_get = Get::decode(&decoded.body[..]).unwrap();
    assert_eq!(decoded_get.fetch_size, 100);
    assert_eq!(decoded_get.client_id, "1001");
}

#[test]
fn test_client_ack_encoding_roundtrip() {
    let ack = ClientAck {
        destination: "example".into(),
        client_id: "1001".into(),
        batch_id: 42,
    };

    let packet = Packet {
        r#type: PacketType::Clientack as i32,
        body: ack.encode_to_vec(),
        ..Default::default()
    };

    let wire = packet.encode_to_vec();
    let decoded = Packet::decode(&wire[..]).unwrap();
    assert_eq!(decoded.r#type, PacketType::Clientack as i32);

    let decoded_ack = ClientAck::decode(&decoded.body[..]).unwrap();
    assert_eq!(decoded_ack.batch_id, 42);
}

#[test]
fn test_client_rollback_encoding_roundtrip() {
    let rollback = ClientRollback {
        destination: "example".into(),
        client_id: "1001".into(),
        batch_id: 10,
    };

    let packet = Packet {
        r#type: PacketType::Clientrollback as i32,
        body: rollback.encode_to_vec(),
        ..Default::default()
    };

    let wire = packet.encode_to_vec();
    let decoded = Packet::decode(&wire[..]).unwrap();
    assert_eq!(decoded.r#type, PacketType::Clientrollback as i32);

    let decoded_rollback = ClientRollback::decode(&decoded.body[..]).unwrap();
    assert_eq!(decoded_rollback.batch_id, 10);
}
