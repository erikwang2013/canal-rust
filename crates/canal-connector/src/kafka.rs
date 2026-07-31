use async_trait::async_trait;
use canal_common::{CanalError, CanalResult};
use canal_sink::connector::SinkConnector;
use canal_common::CanalEvent;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use serde_json;
use std::sync::Mutex;
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// Kafka connector configuration.
#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub servers: String,
    pub topic: String,
    /// Optional TLS: path to CA certificate PEM file
    pub ssl_ca_location: Option<String>,
    /// Optional SASL: username for PLAIN/SCRAM auth
    pub sasl_username: Option<String>,
    /// Optional SASL: password for PLAIN/SCRAM auth
    pub sasl_password: Option<String>,
    /// Optional SASL: mechanism (e.g. "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512")
    pub sasl_mechanism: Option<String>,
}

impl KafkaConfig {
    pub fn new(servers: &str, topic: &str) -> Self {
        Self {
            servers: servers.to_string(),
            topic: topic.to_string(),
            ssl_ca_location: None,
            sasl_username: None,
            sasl_password: None,
            sasl_mechanism: None,
        }
    }
}

/// Kafka connector implementing the SinkConnector trait.
pub struct KafkaConnector {
    name: String,
    producer: Mutex<Option<FutureProducer>>,
    config: KafkaConfig,
}

impl KafkaConnector {
    pub fn new(name: &str, config: KafkaConfig) -> CanalResult<Self> {
        Ok(Self {
            name: name.to_string(),
            producer: Mutex::new(None),
            config,
        })
    }

    /// Serialize a CanalEvent batch to flat JSON messages for Kafka.
    fn serialize_events(&self, events: &[CanalEvent]) -> Vec<(String, String)> {
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

                serde_json::to_string(&payload)
                    .inspect_err(|e| warn!("Kafka '{}': failed to serialize: {}", self.name, e))
                    .ok()
                    .map(|json| (event.schema_name.clone(), json))
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
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", &self.config.servers)
           .set("message.timeout.ms", "5000")
           .set("acks", "1");

        // TLS config
        if let Some(ref ca_path) = self.config.ssl_ca_location {
            cfg.set("security.protocol", "SASL_SSL");
            cfg.set("ssl.ca.location", ca_path);
        }

        // SASL config
        if let Some(ref user) = self.config.sasl_username {
            if !self.config.ssl_ca_location.is_some() {
                cfg.set("security.protocol", "SASL_PLAINTEXT");
            }
            cfg.set("sasl.username", user);
            if let Some(ref pass) = self.config.sasl_password {
                cfg.set("sasl.password", pass);
            }
            cfg.set("sasl.mechanism", self.config.sasl_mechanism.as_deref().unwrap_or("PLAIN"));
        }

        let producer: FutureProducer = cfg
            .create()
            .map_err(|e| CanalError::Internal(format!("Kafka producer: {}", e)))?;

        info!("Kafka connector '{}' connecting to {}", self.name, self.config.servers);

        producer
            .client()
            .fetch_metadata(Some(&self.config.topic), Timeout::After(Duration::from_secs(10)))
            .map_err(|e| CanalError::Internal(format!("Kafka metadata: {}", e)))?;

        *self.producer.lock().unwrap_or_else(|e| e.into_inner()) = Some(producer);
        info!("Kafka connector '{}' connected to '{}'", self.name, self.config.topic);
        Ok(())
    }

    async fn dispatch(&self, events: Vec<CanalEvent>) -> CanalResult<()> {
        let total = events.len();
        if total == 0 {
            return Ok(());
        }

        let messages = self.serialize_events(&events);
        let mut delivered = 0u64;
        let mut failed = 0u64;

        // Partition by each event's own schema_name
        for (key, msg) in messages {
            let record = FutureRecord::to(&self.config.topic)
                .payload(&msg)
                .key(&key);

            let producer = self.producer.lock().unwrap_or_else(|e| e.into_inner())
                .as_ref()
                .expect("KafkaConnector: not connected")
                .clone();
            match producer.send(record, Timeout::After(Duration::from_secs(5))).await {
                Ok((partition, offset)) => {
                    debug!(
                        "Kafka delivered: topic={}, partition={}, offset={}",
                        self.config.topic, partition, offset
                    );
                    delivered += 1;
                }
                Err((e, _msg)) => {
                    error!("Kafka delivery failed: {}", e);
                    failed += 1;
                }
            }
        }

        if failed > 0 {
            return Err(CanalError::Internal(format!(
                "Kafka '{}': {}/{} messages failed",
                self.name, failed, total
            )));
        }

        info!("Kafka '{}': dispatched {}/{} messages", self.name, delivered, total);
        Ok(())
    }

    async fn close(&self) -> CanalResult<()> {
        *self.producer.lock().unwrap_or_else(|e| e.into_inner()) = None;
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
        let config = KafkaConfig::new("localhost:9092", "test-topic");
        let connector = KafkaConnector::new("test", config).unwrap();
        let events = vec![make_event("test_db", "users", 100)];
        let messages = connector.serialize_events(&events);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].0, "test_db"); // key is schema_name

        let parsed: serde_json::Value = serde_json::from_str(&messages[0].1).unwrap();
        assert_eq!(parsed["schema"], "test_db");
        assert_eq!(parsed["table"], "users");
    }

    #[test]
    fn test_serialize_multiple_events() {
        let config = KafkaConfig::new("localhost:9092", "test-topic");
        let connector = KafkaConnector::new("test", config).unwrap();
        let events = vec![
            make_event("db", "t1", 100),
            make_event("db", "t2", 200),
        ];
        let messages = connector.serialize_events(&events);
        assert_eq!(messages.len(), 2);
    }

    #[test]
    fn test_connector_name() {
        let config = KafkaConfig::new("localhost:9092", "topic");
        let connector = KafkaConnector::new("canal-kafka-1", config).unwrap();
        assert_eq!(connector.name(), "canal-kafka-1");
    }

    #[test]
    fn test_empty_events_produces_empty_messages() {
        let config = KafkaConfig::new("localhost:9092", "test-topic");
        let connector = KafkaConnector::new("test", config).unwrap();
        let messages = connector.serialize_events(&[]);
        assert_eq!(messages.len(), 0);
    }
}
