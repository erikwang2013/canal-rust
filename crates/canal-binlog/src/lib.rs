// clippy 1.99 false positive: async_trait-generated futures are already #[must_use].
#![allow(clippy::double_must_use)]

mod column_serde;
pub mod connector;
pub mod converter;
pub mod table_map;

pub use connector::{BinlogConnector, DefaultBinlogConnector};
pub use converter::EventConverter;
pub use table_map::{ColumnInfo, TableMapCache};
