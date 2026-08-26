//! Tests for canal_event_to_entry / column_value_to_proto (canal-server conversion).
use super::*;
use crate::conversion::{canal_event_to_entry, column_value_to_proto};
use canal_common::{CanalEvent, ColumnValue, EventType};
use canal_proto::{header, Entry, EventType as ProtoEventType, RowChange};
use prost::Message;

fn make_event(
    entry_type: EventType,
    row_change: Option<canal_common::RowChange>,
    ddl: Option<String>,
) -> CanalEvent {
    CanalEvent {
        journal_name: "mysql-bin.000001".into(),
        position: 100,
        server_id: 1,
        execute_time: 0,
        entry_type,
        schema_name: "mydb".into(),
        table_name: "users".into(),
        row_change,
        ddl_sql: ddl,
        gtid: None,
        raw_bytes: vec![],
    }
}

fn assert_entry_type(entry: &Entry, expected: i32) {
    let hdr = entry.header.as_ref().unwrap();
    match hdr.event_type_present {
        Some(header::EventTypePresent::EventType(v)) => assert_eq!(v, expected),
        _ => panic!("expected EventTypePresent::EventType"),
    }
}

#[test]
fn test_canal_event_to_entry_unknown_type_returns_error() {
    let event = make_event(EventType::Unknown(99), None, None);
    let err = canal_event_to_entry(&event).unwrap_err();
    assert!(matches!(err, CanalError::Protocol(_)));
}

#[test]
fn test_canal_event_to_entry_xid_maps_to_xacommit() {
    let entry = canal_event_to_entry(&make_event(EventType::Xid, None, None)).unwrap();
    assert_entry_type(&entry, ProtoEventType::Xacommit as i32);
}

#[test]
fn test_canal_event_to_entry_heartbeat_maps_to_mheartbeat() {
    let entry = canal_event_to_entry(&make_event(EventType::Heartbeat, None, None)).unwrap();
    assert_entry_type(&entry, ProtoEventType::Mheartbeat as i32);
}

#[test]
fn test_canal_event_to_entry_ddl_query_rotate_map_to_query() {
    for t in [EventType::Ddl, EventType::Query, EventType::Rotate] {
        let entry = canal_event_to_entry(&make_event(t, None, None)).unwrap();
        assert_entry_type(&entry, ProtoEventType::Query as i32);
    }
}

#[test]
fn test_canal_event_to_entry_without_row_change_or_ddl() {
    // store_value is a RowChange carrying the entry event_type
    let entry = canal_event_to_entry(&make_event(EventType::Insert, None, None)).unwrap();
    assert!(!entry.store_value.is_empty());
    let rc = RowChange::decode(&entry.store_value[..]).unwrap();
    match rc.event_type_present {
        Some(canal_proto::row_change::EventTypePresent::EventType(v)) => {
            assert_eq!(v, ProtoEventType::Insert as i32)
        }
        _ => panic!("expected RowChange event type"),
    }
    // event_length falls back to raw_bytes when store_value is empty
    let mut event = make_event(EventType::Insert, None, None);
    event.raw_bytes = vec![1, 2, 3];
    let entry = canal_event_to_entry(&event).unwrap();
    assert!(entry.header.unwrap().event_length > 0);
}

#[test]
fn test_canal_event_to_entry_header_source_type_mysql() {
    let entry = canal_event_to_entry(&make_event(EventType::Insert, None, None)).unwrap();
    let hdr = entry.header.unwrap();
    match hdr.source_type_present {
        Some(header::SourceTypePresent::SourceType(v)) => {
            assert_eq!(v, canal_proto::Type::Mysql as i32)
        }
        _ => panic!("expected SourceTypePresent"),
    }
}

#[test]
fn test_column_value_to_proto_with_value() {
    let col = ColumnValue {
        name: "id".into(),
        value: Some("42".into()),
        column_type: 3,
        is_key: true,
        updated: true,
    };
    let proto_col = column_value_to_proto(&col, true);
    assert_eq!(proto_col.name, "id");
    assert_eq!(proto_col.value, "42");
    assert!(proto_col.is_key);
    assert!(proto_col.updated);
    assert!(proto_col.is_null_present.is_none());
    assert_eq!(proto_col.sql_type, 3);
}

#[test]
fn test_column_value_to_proto_null_value() {
    let col = ColumnValue {
        name: "email".into(),
        value: None,
        column_type: 253,
        is_key: false,
        updated: false,
    };
    let proto_col = column_value_to_proto(&col, false);
    assert_eq!(proto_col.value, "");
    assert!(proto_col.is_null_present.is_some());
    assert!(!proto_col.updated);
}
