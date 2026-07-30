use serde::{Deserialize, Serialize};

/// Binlog position (corresponds to Java LogPosition/EntryPosition)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogPosition {
    pub journal_name: String,
    pub position: u64,
    pub timestamp: Option<i64>,
    pub server_id: Option<u64>,
    pub gtid: Option<String>,
}

impl LogPosition {
    pub fn new(journal_name: &str, position: u64) -> Self {
        Self {
            journal_name: journal_name.to_string(),
            position,
            timestamp: None,
            server_id: None,
            gtid: None,
        }
    }
}

impl std::fmt::Display for LogPosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref gtid) = self.gtid {
            write!(f, "{}:{}:{}", self.journal_name, self.position, gtid)
        } else {
            write!(f, "{}:{}", self.journal_name, self.position)
        }
    }
}

/// Position range for a batch of events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionRange {
    pub start: LogPosition,
    pub end: LogPosition,
}

/// Event type classifications
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventType {
    Insert,
    Update,
    Delete,
    Ddl,
    Query,
    Rotate,
    Xid,
    Heartbeat,
    Unknown(i32),
}

impl From<i32> for EventType {
    fn from(v: i32) -> Self {
        match v {
            1 => EventType::Insert,
            2 => EventType::Update,
            3 => EventType::Delete,
            4 => EventType::Ddl,
            5 => EventType::Query,
            6 => EventType::Rotate,
            7 => EventType::Xid,
            8 => EventType::Heartbeat,
            _ => EventType::Unknown(v),
        }
    }
}

/// DML operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DmlType {
    Insert,
    Update,
    Delete,
}

/// A single column's value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnValue {
    pub name: String,
    pub value: Option<String>,
    pub column_type: i32,
    pub is_key: bool,
    pub updated: bool,
}

/// A row's data (set of columns)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowData {
    pub columns: Vec<ColumnValue>,
}

/// A change to a row (before/after images and DML type)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowChange {
    pub table_name: String,
    pub schema_name: String,
    pub before: Option<RowData>,
    pub after: Option<RowData>,
    pub dml_type: DmlType,
}

/// A single binlog event as stored in Canal's event store
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanalEvent {
    pub journal_name: String,
    pub position: u64,
    pub server_id: u64,
    pub execute_time: i64,
    pub entry_type: EventType,
    pub schema_name: String,
    pub table_name: String,
    pub row_change: Option<RowChange>,
    pub ddl_sql: Option<String>,
    pub gtid: Option<String>,
    pub raw_bytes: Vec<u8>,
}

/// A batch of events returned to a client
/// Corresponds to Java Events<Event>
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Events {
    pub position_range: PositionRange,
    pub events: Vec<CanalEvent>,
    pub batch_id: i64,
}

impl Events {
    pub fn new(batch_id: i64) -> Self {
        Self {
            position_range: PositionRange {
                start: LogPosition::new("", 0),
                end: LogPosition::new("", 0),
            },
            events: Vec::new(),
            batch_id,
        }
    }

    pub fn with_events(events: Vec<CanalEvent>, batch_id: i64) -> Self {
        let (first, last) = if let (Some(f), Some(l)) = (events.first(), events.last()) {
            (
                LogPosition::new(&f.journal_name, f.position),
                LogPosition::new(&l.journal_name, l.position),
            )
        } else {
            (LogPosition::new("", 0), LogPosition::new("", 0))
        };

        Self {
            position_range: PositionRange {
                start: first,
                end: last,
            },
            events,
            batch_id,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }
}

/// Client subscription filter (regex patterns for included/excluded tables)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterPattern {
    pub pattern: String,
    pub black_list: String,
}

impl Default for FilterPattern {
    fn default() -> Self {
        Self {
            pattern: ".*\\..*".to_string(),
            black_list: String::new(),
        }
    }
}
