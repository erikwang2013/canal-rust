use std::collections::HashMap;
use std::sync::Arc;

use canal_common::{
    lifecycle::CanalLifecycle, CanalError, CanalEvent, CanalResult, FilterPattern, LogPosition,
};
use canal_filter::EventFilter;
use canal_sink::connector::SinkConnector;
use canal_sink::sink::{DefaultEventSink, EventSink};
use canal_store::memory::MemoryEventStore;
use tokio::sync::RwLock;
use tracing::info;

#[derive(Clone)]
pub struct InstanceConfig {
    pub destination: String,
    pub mysql_host: String,
    pub mysql_port: u16,
    pub mysql_username: String,
    pub mysql_password: String,
    pub mysql_server_id: u64,
    pub start_position: LogPosition,
    pub filter: FilterPattern,
    pub store_buffer_size: usize,
    pub connector_names: Vec<String>,
}

impl std::fmt::Debug for InstanceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstanceConfig")
            .field("destination", &self.destination)
            .field("mysql_host", &self.mysql_host)
            .field("mysql_port", &self.mysql_port)
            .field("mysql_username", &self.mysql_username)
            .field("mysql_password", &"<redacted>")
            .field("mysql_server_id", &self.mysql_server_id)
            .finish()
    }
}

pub struct CanalInstance {
    config: InstanceConfig,
    sink: Arc<DefaultEventSink>,
    store: Arc<MemoryEventStore>,
    running: std::sync::atomic::AtomicBool,
}

impl CanalInstance {
    pub fn new(
        config: InstanceConfig,
        connectors: Vec<Arc<dyn SinkConnector>>,
    ) -> CanalResult<Self> {
        let store = Arc::new(MemoryEventStore::new(config.store_buffer_size));
        let filter = EventFilter::with_blacklist(&config.filter.pattern, &config.filter.black_list)
            .map_err(|e| {
                CanalError::Config(format!(
                    "Invalid filter pattern '{}': {}",
                    config.filter.pattern, e
                ))
            })?;

        let sink = Arc::new(DefaultEventSink::new(store.clone(), filter, connectors));

        Ok(Self {
            config,
            sink,
            store,
            running: std::sync::atomic::AtomicBool::new(false),
        })
    }

    pub fn destination(&self) -> &str {
        &self.config.destination
    }

    pub fn store(&self) -> Arc<MemoryEventStore> {
        self.store.clone()
    }

    pub fn sink(&self) -> Arc<DefaultEventSink> {
        self.sink.clone()
    }

    pub async fn feed(&self, events: Vec<CanalEvent>) -> CanalResult<()> {
        if !self.running.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(());
        }
        self.sink.sink(events).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl CanalLifecycle for CanalInstance {
    async fn start(&self) -> CanalResult<()> {
        use std::sync::atomic::Ordering;
        self.running.store(true, Ordering::SeqCst);
        info!("CanalInstance '{}' started", self.config.destination);
        Ok(())
    }

    async fn stop(&self) -> CanalResult<()> {
        use std::sync::atomic::Ordering;
        self.running.store(false, Ordering::SeqCst);
        info!("CanalInstance '{}' stopped", self.config.destination);
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.running.load(std::sync::atomic::Ordering::SeqCst)
    }
}

pub struct InstanceManager {
    instances: RwLock<HashMap<String, Arc<CanalInstance>>>,
}

impl InstanceManager {
    pub fn new() -> Self {
        Self {
            instances: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for InstanceManager {
    fn default() -> Self {
        Self::new()
    }
}

impl InstanceManager {
    pub async fn register(&self, instance: CanalInstance) {
        let dest = instance.destination().to_string();
        self.instances
            .write()
            .await
            .insert(dest.clone(), Arc::new(instance));
        info!("Registered instance '{}'", dest);
    }

    pub async fn get(&self, destination: &str) -> Option<Arc<CanalInstance>> {
        self.instances.read().await.get(destination).cloned()
    }

    pub async fn remove(&self, destination: &str) -> Option<Arc<CanalInstance>> {
        let removed = self.instances.write().await.remove(destination);
        if removed.is_some() {
            info!("Removed instance '{}'", destination);
        }
        removed
    }

    pub async fn list(&self) -> Vec<String> {
        self.instances.read().await.keys().cloned().collect()
    }

    pub async fn running_count(&self) -> usize {
        self.instances
            .read()
            .await
            .values()
            .filter(|i| i.is_running())
            .count()
    }

    /// Start all instances via CanalLifecycle trait methods.
    pub async fn start_all(&self) -> CanalResult<()> {
        for (dest, instance) in self.instances.read().await.iter() {
            info!("Starting instance '{}'", dest);
            instance.start().await?;
        }
        Ok(())
    }

    /// Stop all instances via CanalLifecycle trait methods.
    pub async fn stop_all(&self) -> CanalResult<()> {
        for (dest, instance) in self.instances.read().await.iter() {
            info!("Stopping instance '{}'", dest);
            instance.stop().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use canal_common::FilterPattern;

    fn make_config(destination: &str) -> InstanceConfig {
        InstanceConfig {
            destination: destination.to_string(),
            mysql_host: "localhost".into(),
            mysql_port: 3306,
            mysql_username: "root".into(),
            mysql_password: "pass".into(),
            mysql_server_id: 1001,
            start_position: LogPosition::new("mysql-bin.000001", 4),
            filter: FilterPattern::default(),
            store_buffer_size: 1024,
            connector_names: vec![],
        }
    }

    #[tokio::test]
    async fn test_instance_creation_and_lifecycle() {
        let config = make_config("test-dest");
        let instance = CanalInstance::new(config, vec![]).unwrap();
        assert_eq!(instance.destination(), "test-dest");
        assert!(!instance.is_running());
    }

    #[tokio::test]
    async fn test_invalid_filter_returns_error() {
        let mut config = make_config("bad-filter");
        config.filter.pattern = "[invalid".to_string();
        let result = CanalInstance::new(config, vec![]);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_manager_register_and_lookup() {
        let manager = InstanceManager::new();
        let instance = CanalInstance::new(make_config("my-dest"), vec![]).unwrap();
        manager.register(instance).await;
        let found = manager.get("my-dest").await;
        assert!(found.is_some());
    }

    #[tokio::test]
    async fn test_manager_list_instances() {
        let manager = InstanceManager::new();
        manager.register(CanalInstance::new(make_config("a"), vec![]).unwrap()).await;
        manager.register(CanalInstance::new(make_config("b"), vec![]).unwrap()).await;
        manager.register(CanalInstance::new(make_config("c"), vec![]).unwrap()).await;
        assert_eq!(manager.list().await.len(), 3);
    }

    #[tokio::test]
    async fn test_manager_remove() {
        let manager = InstanceManager::new();
        manager.register(CanalInstance::new(make_config("x"), vec![]).unwrap()).await;
        assert!(manager.remove("x").await.is_some());
        assert!(manager.get("x").await.is_none());
    }

    #[tokio::test]
    async fn test_feed_events_to_instance() {
        let config = make_config("feed-test");
        let instance = CanalInstance::new(config, vec![]).unwrap();
        instance.start().await.unwrap();
        instance.feed(vec![CanalEvent {
            journal_name: "mysql-bin.000001".into(), position: 100,
            server_id: 1, execute_time: 0,
            entry_type: canal_common::EventType::Insert,
            schema_name: "db".into(), table_name: "t".into(),
            row_change: None, ddl_sql: None, gtid: None, raw_bytes: vec![],
        }]).await.unwrap();
        assert!(instance.store().latest_position().is_some());
    }

    #[test]
    fn test_config_clone() {
        let config = make_config("clone-test");
        let cloned = config.clone();
        assert_eq!(config.destination, cloned.destination);
    }
}
