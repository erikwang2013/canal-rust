use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;
use tracing::debug;

/// Column metadata from a table schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMeta {
    pub name: String,
    pub column_type: i32, // MySQL column type code
    pub is_key: bool,     // primary key
    pub is_nullable: bool,
    pub position: usize, // ordinal position in table
}

/// Table schema: a list of columns with their metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMeta {
    pub schema_name: String,
    pub table_name: String,
    pub columns: Vec<ColumnMeta>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl TableMeta {
    /// Find the primary key column(s) of this table.
    pub fn primary_keys(&self) -> Vec<&ColumnMeta> {
        self.columns.iter().filter(|c| c.is_key).collect()
    }

    /// Count of columns in this table.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Look up a column by name.
    pub fn get_column(&self, name: &str) -> Option<&ColumnMeta> {
        self.columns.iter().find(|c| c.name == name)
    }
}

/// Thread-safe cache of table schemas, keyed by "schema.table".
/// Updated when DDL events (ALTER TABLE, CREATE TABLE, DROP TABLE) are received.
///
/// Corresponds to Java's TableMetaTSDB.
#[derive(Debug, Default)]
pub struct TableMetaCache {
    tables: RwLock<HashMap<String, TableMeta>>,
}

impl TableMetaCache {
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
        }
    }

    /// Store or update table metadata.
    pub fn put(&self, key: &str, meta: TableMeta) {
        self.tables
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key.to_string(), meta);
        debug!("Table metadata updated: {}", key);
    }

    /// Get table metadata by "schema.table" key.
    pub fn get(&self, key: &str) -> Option<TableMeta> {
        self.tables
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
            .cloned()
    }

    /// Remove table metadata (e.g., after DROP TABLE).
    pub fn remove(&self, key: &str) {
        self.tables
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(key);
        debug!("Table metadata removed: {}", key);
    }

    /// Check if metadata exists for a table.
    pub fn contains(&self, key: &str) -> bool {
        self.tables
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(key)
    }

    /// Number of cached tables.
    pub fn len(&self) -> usize {
        self.tables.read().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear all cached table metadata.
    pub fn clear(&self) {
        self.tables
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_column(name: &str, is_key: bool, pos: usize) -> ColumnMeta {
        ColumnMeta {
            name: name.into(),
            column_type: 253,
            is_key,
            is_nullable: false,
            position: pos,
        }
    }

    fn make_table_meta() -> TableMeta {
        TableMeta {
            schema_name: "test_db".into(),
            table_name: "users".into(),
            columns: vec![
                make_column("id", true, 0),
                make_column("name", false, 1),
                make_column("email", false, 2),
            ],
            updated_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn test_put_and_get() {
        let cache = TableMetaCache::new();
        cache.put("test_db.users", make_table_meta());

        let meta = cache.get("test_db.users").unwrap();
        assert_eq!(meta.schema_name, "test_db");
        assert_eq!(meta.column_count(), 3);
    }

    #[test]
    fn test_primary_keys() {
        let cache = TableMetaCache::new();
        cache.put("test_db.users", make_table_meta());

        let meta = cache.get("test_db.users").unwrap();
        let keys = meta.primary_keys();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].name, "id");
    }

    #[test]
    fn test_get_column_by_name() {
        let cache = TableMetaCache::new();
        cache.put("test_db.users", make_table_meta());

        let meta = cache.get("test_db.users").unwrap();
        let col = meta.get_column("email").unwrap();
        assert_eq!(col.name, "email");
        assert!(!col.is_key);
    }

    #[test]
    fn test_remove() {
        let cache = TableMetaCache::new();
        cache.put("test_db.users", make_table_meta());
        cache.remove("test_db.users");
        assert!(cache.get("test_db.users").is_none());
    }

    #[test]
    fn test_clear() {
        let cache = TableMetaCache::new();
        cache.put("a.b", make_table_meta());
        cache.put("a.c", make_table_meta());
        cache.clear();
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_contains() {
        let cache = TableMetaCache::new();
        assert!(!cache.contains("a.b"));
        cache.put("a.b", make_table_meta());
        assert!(cache.contains("a.b"));
    }
}
