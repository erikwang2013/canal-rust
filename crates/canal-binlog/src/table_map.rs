use rustc_hash::FxHashMap;

/// Column metadata extracted from a MySQL TableMapEvent.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub column_type: i32,
    pub is_key: bool,
    pub is_nullable: bool,
}

/// Maps MySQL table_id → (schema, table, columns).
pub struct TableMapCache {
    names: FxHashMap<u64, (String, String)>,
    columns: FxHashMap<u64, Vec<ColumnInfo>>,
}

impl Default for TableMapCache {
    fn default() -> Self {
        Self::new()
    }
}

impl TableMapCache {
    pub fn new() -> Self {
        Self {
            names: FxHashMap::default(),
            columns: FxHashMap::default(),
        }
    }

    pub fn put(&mut self, table_id: u64, schema: String, table: String) {
        self.names.insert(table_id, (schema, table));
    }

    pub fn put_with_columns(
        &mut self,
        table_id: u64,
        schema: String,
        table: String,
        cols: Vec<ColumnInfo>,
    ) {
        self.columns.insert(table_id, cols);
        self.names.insert(table_id, (schema, table));
    }

    pub fn get(&self, table_id: u64) -> Option<(&String, &String)> {
        self.names.get(&table_id).map(|(s, t)| (s, t))
    }

    pub fn get_columns(&self, table_id: u64) -> Option<&Vec<ColumnInfo>> {
        self.columns.get(&table_id)
    }

    pub fn clear(&mut self) {
        self.names.clear();
        self.columns.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_and_get() {
        let mut cache = TableMapCache::new();
        cache.put(42, "mydb".into(), "users".into());
        let result = cache.get(42).unwrap();
        assert_eq!(result.0.as_str(), "mydb");
        assert_eq!(result.1.as_str(), "users");
        assert_eq!(cache.get(99), None);
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

    #[test]
    fn test_missing_table_id() {
        let cache = TableMapCache::new();
        assert!(cache.get(42).is_none());
    }

    #[test]
    fn test_put_with_columns() {
        let mut cache = TableMapCache::new();
        let cols = vec![ColumnInfo {
            name: "id".into(),
            column_type: 3,
            is_key: true,
            is_nullable: false,
        }];
        cache.put_with_columns(1, "db".into(), "tbl".into(), cols);
        let result = cache.get(1).unwrap();
        assert_eq!(result.0.as_str(), "db");
        assert_eq!(result.1.as_str(), "tbl");
        assert_eq!(cache.get_columns(1).unwrap().len(), 1);
    }
}
