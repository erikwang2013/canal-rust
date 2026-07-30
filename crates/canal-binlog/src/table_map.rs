use std::collections::HashMap;

/// Metadata about a single table column learned from a TableMap event.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column name (from table metadata if available, otherwise "col_N")
    pub name: String,
    /// MySQL column type code (e.g. 3 = INT, 253 = VARCHAR)
    pub column_type: i32,
    /// Whether this column is part of the primary key
    pub is_key: bool,
    /// Whether the column is nullable
    pub is_nullable: bool,
}

/// Caches MySQL TableMap events: maps table_id → (schema_name, table_name)
/// MySQL sends TableMap events once, then Row events reference tables by numeric ID.
/// We must store these mappings to resolve table names for row-level changes.
#[derive(Debug, Default)]
pub struct TableMapCache {
    tables: HashMap<u64, (String, String)>,
    columns: HashMap<u64, Vec<ColumnInfo>>,
}

impl TableMapCache {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            columns: HashMap::new(),
        }
    }

    /// Store a table_id → (schema, table) mapping from a TableMap event
    pub fn put(&mut self, table_id: u64, schema: String, table: String) {
        self.tables.insert(table_id, (schema, table));
    }

    /// Store a table mapping with full column metadata.
    pub fn put_with_columns(
        &mut self,
        table_id: u64,
        schema: String,
        table: String,
        cols: Vec<ColumnInfo>,
    ) {
        self.tables.insert(table_id, (schema, table));
        self.columns.insert(table_id, cols);
    }

    /// Look up a table_id (from a Row event) to get (schema, table)
    pub fn get(&self, table_id: u64) -> Option<(String, String)> {
        self.tables.get(&table_id).cloned()
    }

    /// Look up column metadata for a table_id
    pub fn get_columns(&self, table_id: u64) -> Option<&Vec<ColumnInfo>> {
        self.columns.get(&table_id)
    }

    /// Clear all cached mappings (called on binlog RotateEvent)
    pub fn clear(&mut self) {
        self.tables.clear();
        self.columns.clear();
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
