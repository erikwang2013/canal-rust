use std::collections::HashMap;

/// Caches MySQL TableMap events: maps table_id → (schema_name, table_name)
/// MySQL sends TableMap events once, then Row events reference tables by numeric ID.
/// We must store these mappings to resolve table names for row-level changes.
#[derive(Debug, Default)]
pub struct TableMapCache {
    tables: HashMap<u64, (String, String)>,
}

impl TableMapCache {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Store a table_id → (schema, table) mapping from a TableMap event
    pub fn put(&mut self, table_id: u64, schema: String, table: String) {
        self.tables.insert(table_id, (schema, table));
    }

    /// Look up a table_id (from a Row event) to get (schema, table)
    pub fn get(&self, table_id: u64) -> Option<(String, String)> {
        self.tables.get(&table_id).cloned()
    }

    /// Clear all cached mappings (called on binlog RotateEvent)
    pub fn clear(&mut self) {
        self.tables.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_and_get() {
        let mut cache = TableMapCache::new();
        cache.put(100, "test_db".into(), "users".into());
        let (schema, table) = cache.get(100).unwrap();
        assert_eq!(schema, "test_db");
        assert_eq!(table, "users");
    }

    #[test]
    fn test_missing_table_id() {
        let cache = TableMapCache::new();
        assert!(cache.get(999).is_none());
    }

    #[test]
    fn test_clear_removes_all() {
        let mut cache = TableMapCache::new();
        cache.put(1, "db".into(), "t1".into());
        cache.put(2, "db".into(), "t2".into());
        cache.clear();
        assert!(cache.get(1).is_none());
        assert!(cache.get(2).is_none());
    }
}
