use crate::table_map::{ColumnInfo, TableMapCache};
use canal_common::{CanalError, CanalResult, ColumnValue, DmlType, EventType, RowChange, RowData};

/// Converts MySQL binlog raw events into Canal's normalized event format.
pub struct EventConverter {
    table_map: TableMapCache,
}

impl Default for EventConverter {
    fn default() -> Self {
        Self::new()
    }
}

impl EventConverter {
    pub fn new() -> Self {
        Self {
            table_map: TableMapCache::new(),
        }
    }

    /// Store table name only (without column metadata).
    /// Prefer `handle_table_map_event` so column metadata is available for row events.
    pub fn handle_table_map(&mut self, table_id: u64, schema: &str, table: &str) {
        self.table_map
            .put(table_id, schema.to_string(), table.to_string());
    }

    /// Store table name with column info for row-event conversion.
    pub fn handle_table_map_event(
        &mut self,
        table_id: u64,
        schema: &str,
        table: &str,
        columns: Vec<ColumnInfo>,
    ) {
        self.table_map
            .put_with_columns(table_id, schema.to_string(), table.to_string(), columns);
    }

    pub fn get_columns(&self, table_id: u64) -> Option<&Vec<ColumnInfo>> {
        self.table_map.get_columns(table_id)
    }

    /// Process a Row event (INSERT / DELETE / single-image events).
    pub fn handle_row_event(
        &self,
        table_id: u64,
        event_type: EventType,
        columns: Vec<ColumnValue>,
    ) -> CanalResult<RowChange> {
        let schema_table = self.table_map.get(table_id).ok_or_else(|| {
            CanalError::NotFound(format!(
                "table_id {} not found in TableMap — did you call handle_table_map_event?",
                table_id
            ))
        })?;
        let (schema, table) = schema_table;

        let (before, after, dml_type) = match event_type {
            EventType::Insert => (None, Some(RowData { columns }), DmlType::Insert),
            EventType::Delete => (Some(RowData { columns }), None, DmlType::Delete),
            _ => {
                return Err(CanalError::Internal(format!(
                    "handle_row_event does not support {:?}; use handle_update_row_event",
                    event_type
                )));
            }
        };

        Ok(RowChange {
            table_name: table.clone(),
            schema_name: schema.clone(),
            before,
            after,
            dml_type,
        })
    }

    /// Process an UPDATE with separate before-image and after-image column vectors.
    pub fn handle_update_row_event(
        &self,
        table_id: u64,
        before_columns: Vec<ColumnValue>,
        mut after_columns: Vec<ColumnValue>,
    ) -> CanalResult<RowChange> {
        let schema_table = self.table_map.get(table_id).ok_or_else(|| {
            CanalError::NotFound(format!(
                "table_id {} not found in TableMap — did you call handle_table_map_event?",
                table_id
            ))
        })?;
        let (schema, table) = schema_table;

        // Only mark columns as updated when their value actually changed
        for (i, col) in after_columns.iter_mut().enumerate() {
            col.updated = before_columns
                .get(i)
                .is_none_or(|before| before.value != col.value);
        }

        Ok(RowChange {
            table_name: table.clone(),
            schema_name: schema.clone(),
            before: Some(RowData {
                columns: before_columns,
            }),
            after: Some(RowData {
                columns: after_columns,
            }),
            dml_type: DmlType::Update,
        })
    }

    pub fn clear_table_map(&mut self) {
        self.table_map.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_column(name: &str, value: &str) -> ColumnValue {
        ColumnValue {
            name: name.to_string(),
            value: Some(value.to_string()),
            column_type: 253,
            is_key: false,
            updated: false,
        }
    }

    #[test]
    fn test_insert_event() {
        let mut converter = EventConverter::new();
        converter.handle_table_map(10, "mydb", "products");

        let change = converter
            .handle_row_event(
                10,
                EventType::Insert,
                vec![make_column("id", "1"), make_column("name", "widget")],
            )
            .unwrap();

        assert_eq!(change.dml_type, DmlType::Insert);
        assert!(change.before.is_none());
        assert_eq!(change.after.unwrap().columns.len(), 2);
    }

    #[test]
    fn test_update_event_separate_before_after() {
        let mut converter = EventConverter::new();
        converter.handle_table_map(10, "mydb", "products");

        let change = converter
            .handle_update_row_event(
                10,
                vec![make_column("id", "1"), make_column("price", "10")],
                vec![make_column("id", "1"), make_column("price", "20")],
            )
            .unwrap();

        assert_eq!(change.dml_type, DmlType::Update);
        assert_eq!(change.before.as_ref().unwrap().columns.len(), 2);
        assert_eq!(change.after.as_ref().unwrap().columns.len(), 2);
        assert!(change.after.as_ref().unwrap().columns[1].updated);
    }

    #[test]
    fn test_delete_event() {
        let mut converter = EventConverter::new();
        converter.handle_table_map(10, "mydb", "products");

        let change = converter
            .handle_row_event(10, EventType::Delete, vec![make_column("id", "1")])
            .unwrap();

        assert_eq!(change.dml_type, DmlType::Delete);
        assert!(change.after.is_none());
        assert_eq!(
            change.before.unwrap().columns[0].value.as_deref(),
            Some("1")
        );
    }

    #[test]
    fn test_missing_table_map_errors() {
        let converter = EventConverter::new();
        let result = converter.handle_row_event(999, EventType::Insert, vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn test_clear_after_rotate() {
        let mut converter = EventConverter::new();
        converter.handle_table_map(1, "db", "tbl");
        converter.clear_table_map();

        let result = converter.handle_row_event(1, EventType::Insert, vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn test_update_unchanged_columns_not_marked() {
        let mut converter = EventConverter::new();
        converter.handle_table_map(10, "mydb", "products");

        let change = converter
            .handle_update_row_event(
                10,
                vec![make_column("id", "1"), make_column("price", "10")],
                vec![make_column("id", "1"), make_column("price", "10")],
            )
            .unwrap();

        let after = change.after.unwrap();
        assert!(
            after.columns.iter().all(|c| !c.updated),
            "identical before/after values must not be marked updated"
        );
    }

    #[test]
    fn test_update_extra_after_columns_marked_updated() {
        // After image has more columns than before: extras are considered updated
        let mut converter = EventConverter::new();
        converter.handle_table_map(10, "mydb", "products");

        let change = converter
            .handle_update_row_event(
                10,
                vec![make_column("id", "1")],
                vec![make_column("id", "1"), make_column("new_col", "x")],
            )
            .unwrap();

        let after = change.after.unwrap();
        assert!(!after.columns[0].updated);
        assert!(after.columns[1].updated, "extra after-column must be updated");
    }

    #[test]
    fn test_update_marks_changed_value() {
        let mut converter = EventConverter::new();
        converter.handle_table_map(10, "mydb", "products");
        let change = converter
            .handle_update_row_event(
                10,
                vec![make_column("name", "old")],
                vec![make_column("name", "new")],
            )
            .unwrap();
        assert!(change.after.unwrap().columns[0].updated);
    }

    #[test]
    fn test_row_event_update_unsupported() {
        let mut converter = EventConverter::new();
        converter.handle_table_map(10, "mydb", "products");
        let err = converter
            .handle_row_event(10, EventType::Update, vec![make_column("id", "1")])
            .unwrap_err();
        assert!(matches!(err, CanalError::Internal(_)));
    }

    #[test]
    fn test_update_missing_table_map_errors() {
        let converter = EventConverter::new();
        let result = converter.handle_update_row_event(999, vec![], vec![]);
        assert!(matches!(result, Err(CanalError::NotFound(_))));
    }

    #[test]
    fn test_columns_from_table_map_event() {
        let mut converter = EventConverter::new();
        let columns = vec![
            ColumnInfo {
                name: "id".into(),
                column_type: 3,
                is_key: true,
                is_nullable: false,
            },
            ColumnInfo {
                name: "name".into(),
                column_type: 253,
                is_key: false,
                is_nullable: true,
            },
        ];
        converter.handle_table_map_event(20, "db", "tbl", columns);
        let cols = converter.get_columns(20).unwrap();
        assert_eq!(cols.len(), 2);
        assert!(cols[0].is_key);
    }

    #[test]
    fn test_get_columns_none_without_event() {
        let converter = EventConverter::new();
        assert!(converter.get_columns(1).is_none());
    }

    #[test]
    fn test_insert_event_uses_table_map_names() {
        let mut converter = EventConverter::new();
        converter.handle_table_map_event(
            30,
            "inventory",
            "items",
            vec![ColumnInfo {
                name: "sku".into(),
                column_type: 253,
                is_key: true,
                is_nullable: false,
            }],
        );
        let change = converter
            .handle_row_event(30, EventType::Insert, vec![make_column("sku", "A-1")])
            .unwrap();
        assert_eq!(change.schema_name, "inventory");
        assert_eq!(change.table_name, "items");
        assert_eq!(change.after.unwrap().columns[0].name, "sku");
    }
}
