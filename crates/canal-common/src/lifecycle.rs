use crate::error::CanalResult;

/// Canal component lifecycle management
/// Corresponds to Java AbstractCanalLifeCycle + CanalLifeCycle interface
#[async_trait::async_trait]
pub trait CanalLifecycle: Send + Sync {
    /// Start the component
    async fn start(&mut self) -> CanalResult<()> {
        Ok(())
    }

    /// Stop the component gracefully
    async fn stop(&mut self) -> CanalResult<()> {
        Ok(())
    }

    /// Check if the component is currently running
    fn is_running(&self) -> bool;
}
