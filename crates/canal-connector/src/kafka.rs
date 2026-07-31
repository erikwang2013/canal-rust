use async_trait::async_trait;
use canal_common::CanalEvent;
use canal_common::{CanalError, CanalResult, MutexLockExt};
use canal_sink::connector::SinkConnector;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use serde::Serialize;
use std::sync::Mutex;
use std::time::Duration;
use tracing::{error, info, warn};

#[derive(Serialize)]
struct KafkaColumn<'a> {
    name: &'a str,
    value: &'a Option<String>,
    is_key: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    updated: bool,
}

#[derive(Serialize)]
struct KafkaRowChange<'a> {
    dml_type: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    before: Option<Vec<KafkaColumn<'a>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    after: Option<Vec<KafkaColumn<'a>>>,
}

#[derive(Serialize)]
struct KafkaPayload<'a> {
    schema: &'a str,
    table: &'a str,
    #[serde(rename = "type")]
    event_type: &'a str,
    position: u64,
    journal: &'a str,
    server_id: u64,
    execute_time: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    gtid: &'a Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    row_change: Option<KafkaRowChange<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ddl_sql: &'a Option<String>,
}

pub struct KafkaConfig {
    pub servers: String,
    pub topic: String,
    pub ssl_ca_location: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
    pub sasl_mechanism: Option<String>,
}

impl std::fmt::Debug for KafkaConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaConfig")
            .field("servers", &self.servers)
            .field("topic", &self.topic)
            .field("ssl_ca_location", &self.ssl_ca_location)
            .field("sasl_username", &self.sasl_username)
            .field("sasl_password", &"<redacted>")
            .field("sasl_mechanism", &self.sasl_mechanism)
            .finish()
    }
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

impl Clone for KafkaConfig {
    fn clone(&self) -> Self {
        Self {
            servers: self.servers.clone(),
            topic: self.topic.clone(),
            ssl_ca_location: self.ssl_ca_location.clone(),
            sasl_username: self.sasl_username.clone(),
            sasl_password: self.sasl_password.clone(),
            sasl_mechanism: self.sasl_mechanism.clone(),
        }
    }
}

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

    fn serialize_events(&self, events: &[CanalEvent]) -> Vec<(String, String)> {
        let total = events.len();
        let messages: Vec<_> = events
            .iter()
            .filter_map(|event| {
                let row_change = event.row_change.as_ref().map(|rc| KafkaRowChange {
                    dml_type: rc.dml_type.as_str(),
                    before: rc.before.as_ref().map(|r| {
                        r.columns
                            .iter()
                            .map(|c| KafkaColumn {
                                name: &c.name,
                                value: &c.value,
                                is_key: c.is_key,
                                updated: false,
                            })
                            .collect()
                    }),
                    after: rc.after.as_ref().map(|r| {
                        r.columns
                            .iter()
                            .map(|c| KafkaColumn {
                                name: &c.name,
                                value: &c.value,
                                is_key: c.is_key,
                                updated: c.updated,
                            })
                            .collect()
                    }),
                });

                let payload = KafkaPayload {
                    schema: &event.schema_name,
                    table: &event.table_name,
                    event_type: event.entry_type.as_str(),
                    position: event.position,
                    journal: &event.journal_name,
                    server_id: event.server_id,
                    execute_time: event.execute_time,
                    gtid: &event.gtid,
                    row_change,
                    ddl_sql: &event.ddl_sql,
                };

                serde_json::to_string(&payload)
                    .inspect_err(|e| warn!("Kafka '{}': failed to serialize: {}", self.name, e))
                    .ok()
                    .map(|json| (event.schema_name.clone(), json))
            })
            .collect();

        let dropped = total - messages.len();
        if dropped > 0 {
            warn!(
                "Kafka '{}': {} of {} events dropped due to serialization failure",
                self.name, dropped, total
            );
        }
        messages
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

        let has_tls = self.config.ssl_ca_location.is_some();
        let has_sasl = self.config.sasl_username.is_some();

        // Set security.protocol based on combination
        match (has_tls, has_sasl) {
            (true, true) => {
                cfg.set("security.protocol", "SASL_SSL");
            }
            (true, false) => {
                cfg.set("security.protocol", "SSL");
            }
            (false, true) => {
                cfg.set("security.protocol", "SASL_PLAINTEXT");
            }
            (false, false) => {}
        }

        if let Some(ref ca_path) = self.config.ssl_ca_location {
            cfg.set("ssl.ca.location", ca_path);
        }

        if let Some(ref user) = self.config.sasl_username {
            cfg.set("sasl.username", user);
            if let Some(ref pass) = self.config.sasl_password {
                cfg.set("sasl.password", pass);
            }
            cfg.set(
                "sasl.mechanism",
                self.config.sasl_mechanism.as_deref().unwrap_or("PLAIN"),
            );
        }

        let producer: FutureProducer = cfg
            .create()
            .map_err(|e| CanalError::Internal(format!("Kafka producer: {}", e)))?;

        info!(
            "Kafka connector '{}' connecting to {}",
            self.name, self.config.servers
        );

        let topic = self.config.topic.clone();
        let p_clone = producer.clone();
        tokio::task::spawn_blocking(move || {
            p_clone
                .client()
                .fetch_metadata(Some(&topic), Timeout::After(Duration::from_secs(10)))
        })
        .await
        .map_err(|e| CanalError::Internal(format!("Kafka metadata join error: {}", e)))?
        .map_err(|e| CanalError::Internal(format!("Kafka metadata: {}", e)))?;

        *self.producer.lock_or_recover() = Some(producer);
        info!(
            "Kafka connector '{}' connected to '{}'",
            self.name, self.config.topic
        );
        Ok(())
    }

    async fn dispatch(&self, events: &[CanalEvent]) -> CanalResult<()> {
        let total = events.len();
        if total == 0 {
            return Ok(());
        }

        // Check connectivity before expensive serialization
        let producer = {
            let guard = self.producer.lock_or_recover();
            match guard.as_ref() {
                Some(p) => p.clone(),
                None => return Err(CanalError::Internal("KafkaConnector: not connected".into())),
            }
        };

        let messages = self.serialize_events(events);

        let topic = &self.config.topic;
        // Limit concurrent in-flight sends to avoid overwhelming the broker
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(50));
        let send_futures: Vec<_> = messages
            .into_iter()
            .map(|(key, msg)| {
                let p = producer.clone();
                let t = topic.clone();
                let permit = semaphore.clone();
                async move {
                    let _guard = permit.acquire().await;
                    let record = FutureRecord::to(&t).payload(&msg).key(&key);
                    p.send(record, Timeout::After(Duration::from_secs(5))).await
                }
            })
            .collect();

        let results = futures::future::join_all(send_futures).await;

        let delivered = results.iter().filter(|r| r.is_ok()).count() as u64;
        let failed = results.iter().filter(|r| r.is_err()).count() as u64;

        for result in results.iter().filter(|r| r.is_err()) {
            if let Err((e, _)) = result {
                error!("Kafka delivery failed: {}", e);
            }
        }

        if failed > 0 {
            return Err(CanalError::Internal(format!(
                "Kafka '{}': {}/{} messages failed",
                self.name, failed, total
            )));
        }

        info!(
            "Kafka '{}': dispatched {}/{} messages",
            self.name, delivered, total
        );
        Ok(())
    }

    async fn close(&self) -> CanalResult<()> {
        *self.producer.lock_or_recover() = None;
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

    #[test]
    fn test_serialize_insert_event() {
        let config = KafkaConfig::new("localhost:9092", "test-topic");
        let connector = KafkaConnector::new("test", config).unwrap();
        let events = vec![make_event("test_db", "users", 100)];
        let messages = connector.serialize_events(&events);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].0, "test_db");

        let parsed: serde_json::Value = serde_json::from_str(&messages[0].1).unwrap();
        assert_eq!(parsed["schema"], "test_db");
        assert_eq!(parsed["table"], "users");
    }

    #[test]
    fn test_serialize_multiple_events() {
        let config = KafkaConfig::new("localhost:9092", "test-topic");
        let connector = KafkaConnector::new("test", config).unwrap();
        let events = vec![make_event("db", "t1", 100), make_event("db", "t2", 200)];
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
