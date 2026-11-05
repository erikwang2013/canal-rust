// clippy 1.99 false positive: async_trait-generated futures are already #[must_use].
#![allow(clippy::double_must_use)]

pub mod kafka;

pub use kafka::KafkaConnector;
