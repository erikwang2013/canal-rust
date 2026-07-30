use canal_common::CanalEvent;
use regex::Regex;
use tracing::debug;

/// Table/schema filter for binlog events.
/// Corresponds to Java Canal's AviaterRegexFilter.
///
/// Supports include pattern (whitelist) and exclude pattern (blacklist).
/// The include pattern is matched as "schema.table" against the event.
#[derive(Debug, Clone)]
pub struct EventFilter {
    include: Regex,
    exclude: Option<Regex>,
}

impl EventFilter {
    /// Create a new filter with the given include pattern.
    /// The default pattern ".*\\..*" matches all tables in all databases.
    pub fn new(pattern: &str) -> Result<Self, regex::Error> {
        Ok(Self {
            include: Regex::new(pattern)?,
            exclude: None,
        })
    }

    /// Create a filter with both include and exclude patterns.
    pub fn with_blacklist(pattern: &str, black_list: &str) -> Result<Self, regex::Error> {
        let exclude = if black_list.is_empty() {
            None
        } else {
            Some(Regex::new(black_list)?)
        };
        Ok(Self { include: Regex::new(pattern)?, exclude })
    }

    /// Check whether a CanalEvent passes this filter.
    /// Returns true if the event's schema.table matches.
    pub fn matches(&self, event: &CanalEvent) -> bool {
        let full_name = format!("{}.{}", event.schema_name, event.table_name);

        // Check exclude (blacklist) first
        if let Some(ref exclude) = self.exclude {
            if exclude.is_match(&full_name) {
                debug!("Filtered out (blacklist): {}", full_name);
                return false;
            }
        }

        // Check include (whitelist)
        let matched = self.include.is_match(&full_name);
        if !matched {
            debug!("Filtered out: {}", full_name);
        }
        matched
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use canal_common::{CanalEvent, EventType};

    fn make_event(schema: &str, table: &str) -> CanalEvent {
        CanalEvent {
            journal_name: "mysql-bin.000001".into(),
            position: 100,
            server_id: 1,
            execute_time: 0,
            entry_type: EventType::Insert,
            schema_name: schema.into(),
            table_name: table.into(),
            row_change: None,
            ddl_sql: None,
            gtid: None,
            raw_bytes: vec![],
        }
    }

    #[test]
    fn test_match_all_pattern() {
        let filter = EventFilter::new(".*\\..*").unwrap();
        assert!(filter.matches(&make_event("test_db", "users")));
        assert!(filter.matches(&make_event("prod", "orders")));
    }

    #[test]
    fn test_specific_table() {
        let filter = EventFilter::new("test_db\\.users").unwrap();
        assert!(filter.matches(&make_event("test_db", "users")));
        assert!(!filter.matches(&make_event("test_db", "orders")));
        assert!(!filter.matches(&make_event("other_db", "users")));
    }

    #[test]
    fn test_wildcard_schema() {
        let filter = EventFilter::new(".*\\.users").unwrap();
        assert!(filter.matches(&make_event("test_db", "users")));
        assert!(filter.matches(&make_event("prod", "users")));
        assert!(!filter.matches(&make_event("test_db", "orders")));
    }

    #[test]
    fn test_blacklist_excludes() {
        let filter = EventFilter::with_blacklist(".*\\..*", "test_db\\.logs").unwrap();
        assert!(filter.matches(&make_event("test_db", "users")));
        assert!(!filter.matches(&make_event("test_db", "logs")));
    }

    #[test]
    fn test_empty_blacklist_passes_all() {
        let filter = EventFilter::with_blacklist(".*\\..*", "").unwrap();
        assert!(filter.matches(&make_event("test_db", "anything")));
    }

    #[test]
    fn test_invalid_regex_returns_error() {
        assert!(EventFilter::new("[").is_err());
    }
}
