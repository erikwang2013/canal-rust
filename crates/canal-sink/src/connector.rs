use async_trait::async_trait;
use canal_common::{CanalEvent, CanalResult};

/// Trait for external data sinks (Kafka, RocketMQ, file, etc.).
/// Implement this to add a new downstream target.
#[async_trait]
pub trait SinkConnector: Send + Sync {
    /// Human-readable name for logging/metrics
    fn name(&self) -> &str;

    /// Initialize the connector (e.g., establish connections, create topics)
    async fn connect(&self) -> CanalResult<()>;

    /// Dispatch a batch of filtered events to the downstream system
    async fn dispatch(&self, events: Vec<CanalEvent>) -> CanalResult<()>;

    /// Close the connector gracefully
    async fn close(&self) -> CanalResult<()>;
}
