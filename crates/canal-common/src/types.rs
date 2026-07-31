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
impl PartialOrd for LogPosition {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LogPosition {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let suffix_self = binlog_suffix(&self.journal_name);
        let suffix_other = binlog_suffix(&other.journal_name);
        suffix_self
            .cmp(&suffix_other)
            .then_with(|| self.position.cmp(&other.position))
    }
}

/// Extract the numeric suffix from a binlog filename for ordering.
/// Returns u64::MAX for non-numeric suffixes so they sort last.
pub fn binlog_suffix(journal_name: &str) -> u64 {
    journal_name
        .rsplit('.')
        .next()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(u64::MAX)
}

/// Position range for a batch of events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionRange {
    pub start: LogPosition,
    pub end: LogPosition,
}

/// Event type classifications
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
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

impl EventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventType::Insert => "INSERT",
            EventType::Update => "UPDATE",
            EventType::Delete => "DELETE",
            EventType::Ddl => "DDL",
            EventType::Query => "QUERY",
            EventType::Rotate => "ROTATE",
            EventType::Xid => "XID",
            EventType::Heartbeat => "HEARTBEAT",
            EventType::Unknown(_) => "UNKNOWN",
        }
    }
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

impl DmlType {
    pub fn as_str(&self) -> &'static str {
        match self {
            DmlType::Insert => "INSERT",
            DmlType::Update => "UPDATE",
            DmlType::Delete => "DELETE",
        }
    }
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
    #[serde(default)]
    pub raw_bytes: Vec<u8>,
}

/// A batch of events returned to a client
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

    #[must_use]
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

/// Client subscription filter
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

impl FilterPattern {
    pub fn validate(&self) -> Result<(), String> {
        regex::Regex::new(&self.pattern).map_err(|e| format!("invalid pattern: {}", e))?;
        if !self.black_list.is_empty() {
            regex::Regex::new(&self.black_list).map_err(|e| format!("invalid blacklist: {}", e))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_position_new() {
        let pos = LogPosition::new("mysql-bin.000001", 12345);
        assert_eq!(pos.journal_name, "mysql-bin.000001");
        assert_eq!(pos.position, 12345);
    }

    #[test]
    fn test_log_position_display() {
        let pos = LogPosition::new("mysql-bin.000001", 999);
        assert_eq!(format!("{}", pos), "mysql-bin.000001:999");
    }

    #[test]
    fn test_log_position_display_with_gtid() {
        let pos = LogPosition {
            journal_name: "bin.001".into(),
            position: 42,
            timestamp: Some(1000),
            server_id: Some(1),
            gtid: Some("uuid:1-100".into()),
        };
        assert_eq!(format!("{}", pos), "bin.001:42:uuid:1-100");
    }

    #[test]
    fn test_log_position_ord() {
        // Same file: compare by position
        let p1 = LogPosition::new("mysql-bin.000001", 100);
        let p2 = LogPosition::new("mysql-bin.000001", 200);
        assert!(p1 < p2);

        // Across files: compare by numeric suffix
        let p3 = LogPosition::new("mysql-bin.000001", 999);
        let p4 = LogPosition::new("mysql-bin.000002", 4);
        assert!(p3 < p4);
    }

    #[test]
    fn test_log_position_ord_suffix_fallback() {
        // Non-numeric suffix sorts last
        let p1 = LogPosition::new("mysql-bin.000001", 100);
        let p2 = LogPosition::new("relay-log", 100);
        assert!(p1 < p2);
    }

    #[test]
    fn test_event_type_from_i32() {
        assert_eq!(EventType::from(1), EventType::Insert);
        assert_eq!(EventType::from(2), EventType::Update);
        assert_eq!(EventType::from(3), EventType::Delete);
        assert_eq!(EventType::from(99), EventType::Unknown(99));
    }

    #[test]
    fn test_events_new_is_empty() {
        let batch = Events::new(42);
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);
    }

    #[test]
    fn test_events_with_events_populates_range() {
        let e1 = CanalEvent {
            journal_name: "bin.001".into(),
            position: 100,
            server_id: 1,
            execute_time: 0,
            entry_type: EventType::Insert,
            schema_name: "db".into(),
            table_name: "t".into(),
            row_change: None,
            ddl_sql: None,
            gtid: None,
            raw_bytes: vec![],
        };
        let e2 = CanalEvent {
            journal_name: "bin.001".into(),
            position: 200,
            server_id: 1,
            execute_time: 0,
            entry_type: EventType::Update,
            schema_name: "db".into(),
            table_name: "t".into(),
            row_change: None,
            ddl_sql: None,
            gtid: None,
            raw_bytes: vec![],
        };
        let batch = Events::with_events(vec![e1, e2], 1);
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.position_range.start.position, 100);
        assert_eq!(batch.position_range.end.position, 200);
    }

    #[test]
    fn test_filter_pattern_default() {
        let fp = FilterPattern::default();
        assert_eq!(fp.pattern, ".*\\..*");
        assert!(fp.black_list.is_empty());
    }

    #[test]
    fn test_column_value_key_detection() {
        let pk = ColumnValue {
            name: "id".into(),
            value: Some("1".into()),
            column_type: 3,
            is_key: true,
            updated: false,
        };
        assert!(pk.is_key);
        let non_pk = ColumnValue {
            name: "name".into(),
            value: None,
            column_type: 253,
            is_key: false,
            updated: false,
        };
        assert!(!non_pk.is_key);
    }

    #[test]
    fn test_row_change_roundtrip() {
        let change = RowChange {
            table_name: "users".into(),
            schema_name: "db".into(),
            before: None,
            after: Some(RowData { columns: vec![] }),
            dml_type: DmlType::Insert,
        };
        assert_eq!(change.dml_type, DmlType::Insert);
    }
}
