use canal_common::CanalEvent;
use regex::{Regex, RegexBuilder};

const MAX_FILTER_LEN: usize = 256;
const MAX_REGEX_SIZE: usize = 1024 * 1024; // 1MB DFA size limit
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
        if pattern.len() > MAX_FILTER_LEN {
            return Err(regex::Error::Syntax(format!(
                "pattern too long: {} exceeds max {}",
                pattern.len(),
                MAX_FILTER_LEN
            )));
        }
        let include = RegexBuilder::new(pattern)
            .size_limit(MAX_REGEX_SIZE)
            .build()?;
        Ok(Self {
            include,
            exclude: None,
        })
    }

    /// Create a filter with both include and exclude patterns.
    pub fn with_blacklist(pattern: &str, black_list: &str) -> Result<Self, regex::Error> {
        if pattern.len() > MAX_FILTER_LEN {
            return Err(regex::Error::Syntax(format!(
                "pattern too long: {} exceeds max {}",
                pattern.len(),
                MAX_FILTER_LEN
            )));
        }
        let include = RegexBuilder::new(pattern)
            .size_limit(MAX_REGEX_SIZE)
            .build()?;
        let exclude = if black_list.is_empty() {
            None
        } else {
            if black_list.len() > MAX_FILTER_LEN {
                return Err(regex::Error::Syntax(format!(
                    "blacklist too long: {} exceeds max {}",
                    black_list.len(),
                    MAX_FILTER_LEN
                )));
            }
            Some(
                RegexBuilder::new(black_list)
                    .size_limit(MAX_REGEX_SIZE)
                    .build()?,
            )
        };
        Ok(Self {
            include,
            exclude,
        })
    }

    /// Check whether a CanalEvent passes this filter.
    /// Returns true if the event's schema.table matches.
    pub fn matches(&self, event: &CanalEvent) -> bool {
        // Thread-local buffer to avoid per-event String allocation
        std::thread_local! {
            static FULL_NAME: std::cell::RefCell<String> =
                std::cell::RefCell::new(String::with_capacity(128));
        }

        FULL_NAME.with(|buf| {
            let mut buf = buf.borrow_mut();
            buf.clear();
            buf.push_str(&event.schema_name);
            buf.push('.');
            buf.push_str(&event.table_name);
            let full_name: &str = &buf;

            // Check exclude (blacklist) first
            if let Some(ref exclude) = self.exclude {
                if exclude.is_match(full_name) {
                    debug!("Filtered out (blacklist): {}", full_name);
                    return false;
                }
            }

            // Check include (whitelist)
            let matched = self.include.is_match(full_name);
            if !matched {
                debug!("Filtered out: {}", full_name);
            }
            matched
        })
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
