use async_trait::async_trait;
use canal_common::{CanalError, CanalResult};
use canal_sink::connector::SinkConnector;
use canal_common::CanalEvent;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use serde_json;
use std::time::Duration;
use tracing::{debug, error, info};

/// Kafka connector implementing the SinkConnector trait.
/// Uses rdkafka's FutureProducer for async message delivery.
pub struct KafkaConnector {
    name: String,
    producer: FutureProducer,
    topic: String,
    servers: String,
}

impl KafkaConnector {
    /// Create a new Kafka connector.
    ///
    /// # Arguments
    /// * `name` - Connector name for logging/metrics
    /// * `servers` - Comma-separated bootstrap servers (e.g., "localhost:9092")
    /// * `topic` - Target Kafka topic
    pub fn new(name: &str, servers: &str, topic: &str) -> Self {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", servers)
            .set("message.timeout.ms", "5000")
            .set("acks", "1")
            .create()
            .expect("Failed to create Kafka producer");

        Self {
            name: name.to_string(),
            producer,
            topic: topic.to_string(),
            servers: servers.to_string(),
        }
    }

    /// Serialize a CanalEvent batch to flat JSON messages for Kafka.
    /// Each CanalEvent becomes one Kafka message (flat format similar to Canal's FlatMessage).
    fn serialize_events(&self, events: &[CanalEvent]) -> Vec<String> {
        events
            .iter()
            .filter_map(|event| {
                let payload = serde_json::json!({
                    "schema": event.schema_name,
                    "table": event.table_name,
                    "type": format!("{:?}", event.entry_type),
                    "position": event.position,
                    "journal": event.journal_name,
                    "server_id": event.server_id,
                    "execute_time": event.execute_time,
                    "gtid": event.gtid,
                    "row_change": event.row_change.as_ref().map(|rc| {
                        serde_json::json!({
                            "dml_type": format!("{:?}", rc.dml_type),
                            "before": rc.before.as_ref().map(|r| {
                                r.columns.iter().map(|c| {
                                    serde_json::json!({
                                        "name": c.name,
                                        "value": c.value,
                                        "is_key": c.is_key,
                                    })
                                }).collect::<Vec<_>>()
                            }),
                            "after": rc.after.as_ref().map(|r| {
                                r.columns.iter().map(|c| {
                                    serde_json::json!({
                                        "name": c.name,
                                        "value": c.value,
                                        "is_key": c.is_key,
                                        "updated": c.updated,
                                    })
                                }).collect::<Vec<_>>()
                            }),
                        })
                    }),
                    "ddl_sql": event.ddl_sql,
                });

                serde_json::to_string(&payload).ok()
            })
            .collect()
    }
}

#[async_trait]
impl SinkConnector for KafkaConnector {
    fn name(&self) -> &str {
        &self.name
    }

    async fn connect(&self) -> CanalResult<()> {
        info!(
            "Kafka connector '{}' connecting to {}",
            self.name, self.servers
        );

        // Verify connectivity by getting metadata
        self.producer
            .client()
            .fetch_metadata(Some(&self.topic), Timeout::After(Duration::from_secs(10)))
            .map_err(|e| CanalError::Internal(format!("Kafka metadata fetch failed: {}", e)))?;

        info!("Kafka connector '{}' connected to topic '{}'", self.name, self.topic);
        Ok(())
    }

    async fn dispatch(&self, events: Vec<CanalEvent>) -> CanalResult<()> {
        let total = events.len();
        if total == 0 {
            return Ok(());
        }

        let messages = self.serialize_events(&events);
        let mut delivered = 0u64;

        for msg in messages {
            let record = FutureRecord::to(&self.topic)
                .payload(&msg)
                .key(&events[0].schema_name); // partition by schema

            match self.producer.send(record, Timeout::After(Duration::from_secs(5))).await {
                Ok((partition, offset)) => {
                    debug!(
                        "Kafka delivered: topic={}, partition={}, offset={}",
                        self.topic, partition, offset
                    );
                    delivered += 1;
                }
                Err((e, _msg)) => {
                    error!("Kafka delivery failed: {}", e);
                }
            }
        }

        info!("Kafka '{}': dispatched {}/{} messages", self.name, delivered, total);
        Ok(())
    }

    async fn close(&self) -> CanalResult<()> {
        info!("Kafka connector '{}' closed", self.name);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
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
                        ColumnValue { name: "id".into(), value: Some("1".into()), column_type: 3, is_key: true, updated: false },
                        ColumnValue { name: "name".into(), value: Some("Alice".into()), column_type: 253, is_key: false, updated: false },
                    ],
                }),
                dml_type: DmlType::Insert,
            }),
            ddl_sql: None,
            gtid: Some("uuid:1-100".into()),
            raw_bytes: vec![],
        }
    }

    #[test]
    fn test_serialize_insert_event() {
        // Test with a mock connector that doesn't actually connect to Kafka.
        // We just verify JSON serialization works.
        let connector = KafkaConnector::new("test", "localhost:9092", "test-topic");
        let events = vec![make_event("test_db", "users", 100)];
        let messages = connector.serialize_events(&events);
        assert_eq!(messages.len(), 1);

        let parsed: serde_json::Value = serde_json::from_str(&messages[0]).unwrap();
        assert_eq!(parsed["schema"], "test_db");
        assert_eq!(parsed["table"], "users");
        assert_eq!(parsed["type"], "Insert");
        assert_eq!(parsed["position"], 100);
    }

    #[test]
    fn test_serialize_multiple_events() {
        let connector = KafkaConnector::new("test", "localhost:9092", "test-topic");
        let events = vec![
            make_event("db", "t1", 100),
            make_event("db", "t2", 200),
        ];
        let messages = connector.serialize_events(&events);
        assert_eq!(messages.len(), 2);
    }

    #[test]
    fn test_connector_name() {
        let connector = KafkaConnector::new("canal-kafka-1", "localhost:9092", "topic");
        assert_eq!(connector.name(), "canal-kafka-1");
    }

    #[test]
    fn test_empty_events_produces_empty_messages() {
        let connector = KafkaConnector::new("test", "localhost:9092", "test-topic");
        let messages = connector.serialize_events(&[]);
        assert_eq!(messages.len(), 0);
    }
}
