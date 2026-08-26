//! Extra tests for canal-connector: KafkaConfig handling and event serialization.
//! Serialization is pure-local; connect()/dispatch() against a broker need a
//! running Kafka and are covered only for the pre-connect error paths.
use super::*;
use canal_common::{CanalEvent, ColumnValue, DmlType, EventType, RowChange, RowData};

fn make_event(schema: &str, table: &str, pos: u64) -> CanalEvent {
    CanalEvent {
        journal_name: "mysql-bin.000001".into(),
        position: pos,
        server_id: 1,
        execute_time: 1234567890,
        entry_type: EventType::Insert,
        schema_name: schema.into(),
        table_name: table.into(),
        row_change: Some(RowChange {
            table_name: table.into(),
            schema_name: schema.into(),
            before: None,
            after: Some(RowData {
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
                        value: Some("Alice".into()),
                        column_type: 253,
                        is_key: false,
                        updated: false,
                    },
                ],
            }),
            dml_type: DmlType::Insert,
        }),
        ddl_sql: None,
        gtid: Some("uuid:1-100".into()),
        raw_bytes: vec![],
    }
}

fn base_event(schema: &str, table: &str, pos: u64, entry_type: EventType) -> CanalEvent {
    CanalEvent {
        journal_name: "mysql-bin.000001".into(),
        position: pos,
        server_id: 1,
        execute_time: 1234567890,
        entry_type,
        schema_name: schema.into(),
        table_name: table.into(),
        row_change: None,
        ddl_sql: None,
        gtid: None,
        raw_bytes: vec![],
    }
}

fn col(name: &str, value: Option<&str>, updated: bool) -> ColumnValue {
    ColumnValue {
        name: name.into(),
        value: value.map(String::from),
        column_type: 253,
        is_key: false,
        updated,
    }
}

fn serialize_one(connector: &KafkaConnector, event: &CanalEvent) -> serde_json::Value {
    let messages = connector.serialize_events(std::slice::from_ref(event));
    assert_eq!(messages.len(), 1);
    serde_json::from_str(&messages[0].1).unwrap()
}

#[test]
fn test_config_defaults_clone_and_debug() {
    let config = KafkaConfig::new("kafka:9092", "topic-a");
    assert_eq!(config.servers, "kafka:9092");
    assert_eq!(config.topic, "topic-a");
    assert!(config.ssl_ca_location.is_none());
    assert!(config.sasl_username.is_none());

    let mut with_sasl = config.clone();
    with_sasl.sasl_username = Some("user".into());
    with_sasl.sasl_password = Some("hunter2".into());
    assert_eq!(with_sasl.servers, config.servers);
    let dbg = format!("{:?}", with_sasl);
    assert!(dbg.contains("<redacted>"));
    assert!(!dbg.contains("hunter2"));
}

#[test]
fn test_serialize_ddl_event() {
    let connector = KafkaConnector::new("t", KafkaConfig::new("k:9092", "topic")).unwrap();
    let mut event = base_event("db", "users", 100, EventType::Ddl);
    event.ddl_sql = Some("ALTER TABLE users ADD COLUMN age INT".into());

    let v = serialize_one(&connector, &event);
    assert_eq!(v["schema"], "db");
    assert_eq!(v["table"], "users");
    assert_eq!(v["type"], "DDL");
    assert_eq!(v["ddl_sql"], "ALTER TABLE users ADD COLUMN age INT");
    assert_eq!(v["position"], 100);
    assert_eq!(v["journal"], "mysql-bin.000001");
    assert_eq!(v["server_id"], 1);
    assert_eq!(v["execute_time"], 1234567890);
    assert!(v.get("row_change").is_none());
}

#[test]
fn test_serialize_update_with_before_and_after() {
    let connector = KafkaConnector::new("t", KafkaConfig::new("k:9092", "topic")).unwrap();
    let mut event = base_event("db", "users", 200, EventType::Update);
    event.row_change = Some(RowChange {
        table_name: "users".into(),
        schema_name: "db".into(),
        before: Some(RowData {
            columns: vec![col("name", Some("alice"), false)],
        }),
        after: Some(RowData {
            columns: vec![col("name", Some("bob"), true)],
        }),
        dml_type: DmlType::Update,
    });

    let v = serialize_one(&connector, &event);
    assert_eq!(v["type"], "UPDATE");
    let rc = &v["row_change"];
    assert_eq!(rc["dml_type"], "UPDATE");
    assert_eq!(rc["after"][0]["name"], "name");
    assert_eq!(rc["after"][0]["value"], "bob");
    assert_eq!(rc["after"][0]["updated"], true);
    // before-image columns are never marked updated
    assert_eq!(rc["before"][0]["value"], "alice");
    assert!(rc["before"][0].get("updated").is_none());
}

#[test]
fn test_serialize_delete_event() {
    let connector = KafkaConnector::new("t", KafkaConfig::new("k:9092", "topic")).unwrap();
    let mut event = base_event("db", "users", 300, EventType::Delete);
    event.row_change = Some(RowChange {
        table_name: "users".into(),
        schema_name: "db".into(),
        before: Some(RowData {
            columns: vec![col("id", Some("99"), false)],
        }),
        after: None,
        dml_type: DmlType::Delete,
    });

    let v = serialize_one(&connector, &event);
    assert_eq!(v["row_change"]["dml_type"], "DELETE");
    assert_eq!(v["row_change"]["before"][0]["value"], "99");
    assert!(v["row_change"].get("after").is_none());
}

#[test]
fn test_serialize_null_column_value() {
    let connector = KafkaConnector::new("t", KafkaConfig::new("k:9092", "topic")).unwrap();
    let mut event = base_event("db", "users", 400, EventType::Insert);
    event.row_change = Some(RowChange {
        table_name: "users".into(),
        schema_name: "db".into(),
        before: None,
        after: Some(RowData {
            columns: vec![col("email", None, false)],
        }),
        dml_type: DmlType::Insert,
    });

    let v = serialize_one(&connector, &event);
    assert_eq!(
        v["row_change"]["after"][0]["value"],
        serde_json::Value::Null
    );
}

#[test]
fn test_serialize_event_without_row_change_or_ddl() {
    let connector = KafkaConnector::new("t", KafkaConfig::new("k:9092", "topic")).unwrap();
    let event = base_event("db", "users", 500, EventType::Insert);

    let v = serialize_one(&connector, &event);
    assert_eq!(v["type"], "INSERT");
    assert!(v.get("row_change").is_none());
    assert!(v.get("ddl_sql").is_none());
    assert!(v.get("gtid").is_none());
}

#[test]
fn test_serialize_with_gtid() {
    let connector = KafkaConnector::new("t", KafkaConfig::new("k:9092", "topic")).unwrap();
    let mut event = base_event("db", "users", 600, EventType::Insert);
    event.gtid = Some("uuid:1-100".into());

    let v = serialize_one(&connector, &event);
    assert_eq!(v["gtid"], "uuid:1-100");
}

#[tokio::test]
async fn test_dispatch_empty_events_ok_without_connect() {
    let connector = KafkaConnector::new("t", KafkaConfig::new("k:9092", "topic")).unwrap();
    connector.dispatch(&[]).await.unwrap();
}

#[tokio::test]
async fn test_dispatch_without_connect_errors() {
    let connector = KafkaConnector::new("t", KafkaConfig::new("k:9092", "topic")).unwrap();
    let events = vec![make_event("db", "tbl", 100)];
    let err = connector.dispatch(&events).await.unwrap_err();
    assert!(matches!(err, CanalError::Internal(_)));
}

#[tokio::test]
async fn test_close_without_connect_ok() {
    let connector = KafkaConnector::new("t", KafkaConfig::new("k:9092", "topic")).unwrap();
    connector.close().await.unwrap();
    // still safe to close twice
    connector.close().await.unwrap();
}
