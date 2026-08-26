//! Integration tests for canal-common public API: types, error, lifecycle, utils.
use canal_common::{
    binlog_suffix, CanalError, CanalEvent, DmlType, EventType, Events, FilterPattern, LogPosition,
    MutexLockExt, RowChange, RowData, RwLockExt,
};
use std::sync::{Mutex, RwLock};

// ---------- binlog_suffix ----------

#[test]
fn test_binlog_suffix_basic() {
    assert_eq!(binlog_suffix("mysql-bin.000001"), 1);
    assert_eq!(binlog_suffix("mysql-bin.000123"), 123);
    assert_eq!(binlog_suffix("bin.0000000000001"), 1);
}

#[test]
fn test_binlog_suffix_edge_cases() {
    // No dot -> no numeric suffix -> u64::MAX (sorts last)
    assert_eq!(binlog_suffix("relaylog"), u64::MAX);
    // Empty string
    assert_eq!(binlog_suffix(""), u64::MAX);
    // Trailing dot -> empty suffix segment
    assert_eq!(binlog_suffix("bin."), u64::MAX);
    // Non-numeric suffix
    assert_eq!(binlog_suffix("bin.abc"), u64::MAX);
    // Numeric overflow -> parse fails -> u64::MAX
    assert_eq!(binlog_suffix("bin.99999999999999999999"), u64::MAX);
    // Negative is not a valid suffix either
    assert_eq!(binlog_suffix("bin.-1"), u64::MAX);
}

// ---------- LogPosition ----------

#[test]
fn test_log_position_ord_equal_positions() {
    let p1 = LogPosition::new("mysql-bin.000001", 100);
    let p2 = LogPosition::new("mysql-bin.000001", 100);
    assert_eq!(p1.cmp(&p2), std::cmp::Ordering::Equal);
    assert!(!(p1 < p2));
    assert!(p1 <= p2);
}

#[test]
fn test_log_position_ord_across_files_same_position() {
    // File suffix dominates: bin.000002 at position 0 is after bin.000001 at u64::MAX
    let p1 = LogPosition::new("mysql-bin.000001", u64::MAX);
    let p2 = LogPosition::new("mysql-bin.000002", 0);
    assert!(p1 < p2);
}

#[test]
fn test_log_position_ord_non_numeric_suffixes_tie() {
    let p1 = LogPosition::new("relay-log", 100);
    let p2 = LogPosition::new("other", 50);
    // Both suffix to u64::MAX -> tie broken by position
    assert!(p2 < p1);
}

#[test]
fn test_log_position_display_defaults() {
    let pos = LogPosition::new("mysql-bin.000001", 0);
    assert_eq!(pos.timestamp, None);
    assert_eq!(pos.server_id, None);
    assert_eq!(pos.gtid, None);
    assert_eq!(pos.to_string(), "mysql-bin.000001:0");
}

#[test]
fn test_log_position_partial_ord() {
    let p1 = LogPosition::new("mysql-bin.000001", 100);
    let p2 = LogPosition::new("mysql-bin.000002", 10);
    assert_eq!(p1.partial_cmp(&p2), Some(std::cmp::Ordering::Less));
}

// ---------- EventType / DmlType ----------

#[test]
fn test_event_type_from_proto_mapping() {
    assert_eq!(EventType::from_proto(1), EventType::Insert);
    assert_eq!(EventType::from_proto(2), EventType::Update);
    assert_eq!(EventType::from_proto(3), EventType::Delete);
    assert_eq!(EventType::from_proto(4), EventType::Ddl);
    assert_eq!(EventType::from_proto(5), EventType::Query);
    // Protocol limitation: Ddl/Query/Rotate all arrive as proto Query=7
    assert_eq!(EventType::from_proto(7), EventType::Ddl);
    assert_eq!(EventType::from_proto(13), EventType::Xid);
    assert_eq!(EventType::from_proto(15), EventType::Heartbeat);
    // Unknown values map to Unknown
    assert_eq!(EventType::from_proto(0), EventType::Unknown(0));
    assert_eq!(EventType::from_proto(99), EventType::Unknown(99));
    assert_eq!(EventType::from_proto(-1), EventType::Unknown(-1));
}

#[test]
fn test_event_type_from_i32_remaining() {
    assert_eq!(EventType::from(6), EventType::Rotate);
    assert_eq!(EventType::from(7), EventType::Xid);
    assert_eq!(EventType::from(8), EventType::Heartbeat);
}

#[test]
fn test_event_type_as_str_all_variants() {
    assert_eq!(EventType::Insert.as_str(), "INSERT");
    assert_eq!(EventType::Update.as_str(), "UPDATE");
    assert_eq!(EventType::Delete.as_str(), "DELETE");
    assert_eq!(EventType::Ddl.as_str(), "DDL");
    assert_eq!(EventType::Query.as_str(), "QUERY");
    assert_eq!(EventType::Rotate.as_str(), "ROTATE");
    assert_eq!(EventType::Xid.as_str(), "XID");
    assert_eq!(EventType::Heartbeat.as_str(), "HEARTBEAT");
    assert_eq!(EventType::Unknown(7).as_str(), "UNKNOWN");
}

#[test]
fn test_dml_type_as_str_all_variants() {
    assert_eq!(DmlType::Insert.as_str(), "INSERT");
    assert_eq!(DmlType::Update.as_str(), "UPDATE");
    assert_eq!(DmlType::Delete.as_str(), "DELETE");
}

// ---------- FilterPattern ----------

#[test]
fn test_filter_pattern_validate_valid() {
    let fp = FilterPattern {
        pattern: "^test_db\\.users$".into(),
        black_list: "test_db\\.logs".into(),
    };
    assert!(fp.validate().is_ok());
}

#[test]
fn test_filter_pattern_validate_invalid_pattern() {
    let fp = FilterPattern {
        pattern: "[".into(),
        black_list: String::new(),
    };
    let err = fp.validate().unwrap_err();
    assert!(err.contains("invalid pattern"), "got: {}", err);
}

#[test]
fn test_filter_pattern_validate_invalid_blacklist() {
    let fp = FilterPattern {
        pattern: ".*\\..*".into(),
        black_list: "[".into(),
    };
    let err = fp.validate().unwrap_err();
    assert!(err.contains("invalid blacklist"), "got: {}", err);
}

#[test]
fn test_filter_pattern_validate_empty_blacklist() {
    let fp = FilterPattern {
        pattern: ".*\\..*".into(),
        black_list: String::new(),
    };
    assert!(fp.validate().is_ok());
}

#[test]
fn test_filter_pattern_validate_default() {
    assert!(FilterPattern::default().validate().is_ok());
}

// ---------- Events ----------

#[test]
fn test_events_with_events_empty_range() {
    let batch = Events::with_events(vec![], 7);
    assert!(batch.is_empty());
    assert_eq!(batch.len(), 0);
    assert_eq!(batch.batch_id, 7);
    assert_eq!(batch.position_range.start.journal_name, "");
    assert_eq!(batch.position_range.end.position, 0);
}

#[test]
fn test_events_multi_journal_range() {
    let e1 = CanalEvent {
        journal_name: "mysql-bin.000001".into(),
        position: 100,
        server_id: 1,
        execute_time: 0,
        entry_type: EventType::Insert,
        schema_name: "db".into(),
        table_name: "t".into(),
        row_change: None,
        ddl_sql: None,
        gtid: None,
        raw_bytes: vec![],
    };
    let e2 = CanalEvent {
        journal_name: "mysql-bin.000002".into(),
        position: 50,
        server_id: 1,
        execute_time: 0,
        entry_type: EventType::Insert,
        schema_name: "db".into(),
        table_name: "t".into(),
        row_change: None,
        ddl_sql: None,
        gtid: None,
        raw_bytes: vec![],
    };
    let batch = Events::with_events(vec![e1, e2], 3);
    assert_eq!(batch.position_range.start.journal_name, "mysql-bin.000001");
    assert_eq!(batch.position_range.end.journal_name, "mysql-bin.000002");
}

// ---------- Serde roundtrips ----------

#[test]
fn test_serde_log_position_roundtrip() {
    let pos = LogPosition {
        journal_name: "bin.001".into(),
        position: 42,
        timestamp: Some(1700000000),
        server_id: Some(2),
        gtid: Some("uuid:1-5".into()),
    };
    let json = serde_json::to_string(&pos).unwrap();
    let back: LogPosition = serde_json::from_str(&json).unwrap();
    assert_eq!(back, pos);
}

#[test]
fn test_serde_event_type_roundtrip() {
    for t in [
        EventType::Insert,
        EventType::Update,
        EventType::Delete,
        EventType::Ddl,
        EventType::Query,
        EventType::Rotate,
        EventType::Xid,
        EventType::Heartbeat,
        EventType::Unknown(99),
    ] {
        let json = serde_json::to_string(&t).unwrap();
        let back: EventType = serde_json::from_str(&json).unwrap();
        assert_eq!(back, t);
    }
}

#[test]
fn test_serde_canal_event_roundtrip() {
    let event = CanalEvent {
        journal_name: "bin.001".into(),
        position: 100,
        server_id: 1,
        execute_time: 1700000000,
        entry_type: EventType::Insert,
        schema_name: "db".into(),
        table_name: "users".into(),
        row_change: Some(RowChange {
            table_name: "users".into(),
            schema_name: "db".into(),
            before: None,
            after: Some(RowData { columns: vec![] }),
            dml_type: DmlType::Insert,
        }),
        ddl_sql: None,
        gtid: Some("uuid:1-5".into()),
        raw_bytes: vec![1, 2, 3],
    };
    let json = serde_json::to_string(&event).unwrap();
    let back: CanalEvent = serde_json::from_str(&json).unwrap();
    assert_eq!(back, event);
}

#[test]
fn test_serde_canal_event_raw_bytes_default() {
    // raw_bytes has #[serde(default)]: missing field decodes as empty
    let json = r#"{"journal_name":"bin.001","position":1,"server_id":1,"execute_time":0,
        "entry_type":"Insert","schema_name":"db","table_name":"t",
        "row_change":null,"ddl_sql":null,"gtid":null}"#;
    let back: CanalEvent = serde_json::from_str(json).unwrap();
    assert!(back.raw_bytes.is_empty());
}

#[test]
fn test_serde_events_roundtrip() {
    let batch = Events::with_events(
        vec![CanalEvent {
            journal_name: "bin.001".into(),
            position: 100,
            server_id: 1,
            execute_time: 0,
            entry_type: EventType::Delete,
            schema_name: "db".into(),
            table_name: "t".into(),
            row_change: None,
            ddl_sql: None,
            gtid: None,
            raw_bytes: vec![],
        }],
        9,
    );
    let json = serde_json::to_string(&batch).unwrap();
    let back: Events = serde_json::from_str(&json).unwrap();
    assert_eq!(back, batch);
}

// ---------- CanalError ----------

#[test]
fn test_error_display_remaining_variants() {
    assert_eq!(
        format!("{}", CanalError::AuthFailed("bob".into())),
        "authentication failed for client bob"
    );
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "no file");
    assert_eq!(
        format!("{}", CanalError::Io(io_err)),
        "io: no file"
    );
    assert_eq!(format!("{}", CanalError::Store("full".into())), "store: full");
    assert_eq!(
        format!("{}", CanalError::Config("bad".into())),
        "configuration: bad"
    );
    assert_eq!(format!("{}", CanalError::NotFound("x".into())), "not found: x");
}

#[test]
fn test_error_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<CanalError>();
    assert_send_sync::<canal_common::CanalResult<()>>();
}

// ---------- MutexLockExt / RwLockExt ----------

#[test]
fn test_mutex_lock_or_recover_poisoned() {
    let m = Mutex::new(10u32);
    let result = std::panic::catch_unwind(|| {
        let mut guard = m.lock().unwrap();
        *guard = 20;
        panic!("boom");
    });
    assert!(result.is_err());
    // Lock is poisoned; recover and read the last value written before the panic
    assert_eq!(*m.lock_or_recover(), 20);
}

#[test]
fn test_mutex_lock_or_recover_healthy() {
    let m = Mutex::new(5u32);
    assert_eq!(*m.lock_or_recover(), 5);
}

#[test]
fn test_rwlock_read_or_recover_poisoned() {
    let rw = RwLock::new(vec![1, 2, 3]);
    let result = std::panic::catch_unwind(|| {
        let mut guard = rw.write().unwrap();
        guard.push(4);
        panic!("boom");
    });
    assert!(result.is_err());
    assert_eq!(*rw.read_or_recover(), vec![1, 2, 3, 4]);
}

#[test]
fn test_rwlock_write_or_recover_poisoned() {
    let rw = RwLock::new(String::from("a"));
    let result = std::panic::catch_unwind(|| {
        let mut guard = rw.write().unwrap();
        guard.push('b');
        panic!("boom");
    });
    assert!(result.is_err());
    assert_eq!(*rw.write_or_recover(), "ab");
}

#[test]
fn test_rwlock_healthy() {
    let rw = RwLock::new(7u32);
    assert_eq!(*rw.read_or_recover(), 7);
}
