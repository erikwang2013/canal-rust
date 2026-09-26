pub mod error;
pub mod lifecycle;
pub mod pet;
pub mod types;
pub mod utils;

pub use error::{CanalError, CanalResult};
pub use lifecycle::CanalLifecycle;
pub use types::{
    binlog_suffix, CanalEvent, ColumnValue, DmlType, EventType, Events, FilterPattern, LogPosition,
    PositionRange, RowChange, RowData,
};
pub use utils::{MutexLockExt, RwLockExt};

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct MockComponent {
        running: AtomicBool,
    }

    #[async_trait::async_trait]
    impl CanalLifecycle for MockComponent {
        fn is_running(&self) -> bool {
            self.running.load(Ordering::SeqCst)
        }
    }

    #[tokio::test]
    async fn test_lifecycle_default_impls() {
        let comp = MockComponent {
            running: AtomicBool::new(false),
        };
        // Default start/stop return Ok without overriding state
        comp.start().await.unwrap();
        comp.stop().await.unwrap();
        assert!(!comp.is_running());
    }
}
