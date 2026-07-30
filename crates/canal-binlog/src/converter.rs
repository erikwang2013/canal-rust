use crate::table_map::TableMapCache;
use canal_common::{CanalError, CanalResult, ColumnValue, DmlType, EventType, RowChange, RowData};

/// Converts MySQL binlog raw events into Canal's normalized event format.
/// Manages TableMap state and resolves table_id references.
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

    /// Process a TableMap event: register a table_id → (schema, table) mapping
    pub fn handle_table_map(&mut self, table_id: u64, schema: &str, table: &str) {
        self.table_map
            .put(table_id, schema.to_string(), table.to_string());
    }

    /// Process a Row event (INSERT / UPDATE / DELETE):
    /// - Looks up the table name from TableMap
    /// - Separates before-image and after-image columns (for UPDATEs)
    /// - Produces a RowChange with the appropriate DML type
    pub fn handle_row_event(
        &self,
        table_id: u64,
        event_type: EventType,
        columns: Vec<ColumnValue>,
    ) -> CanalResult<RowChange> {
        let (schema, table) = self.table_map.get(table_id).ok_or_else(|| {
            CanalError::NotFound(format!("table_id {} not found in TableMap", table_id))
        })?;

        let (before, after, dml_type) = match event_type {
            EventType::Insert => (None, Some(RowData { columns }), DmlType::Insert),
            EventType::Delete => (Some(RowData { columns }), None, DmlType::Delete),
            EventType::Update => {
                // MySQL sends before-image columns followed by after-image columns
                let mid = columns.len() / 2;
                let before_cols = columns[..mid].to_vec();
                let mut after_cols = columns[mid..].to_vec();

                // Mark after-image columns as updated
                for col in &mut after_cols {
                    col.updated = true;
                }

                (
                    Some(RowData {
                        columns: before_cols,
                    }),
                    Some(RowData {
                        columns: after_cols,
                    }),
                    DmlType::Update,
                )
            }
            _ => {
                return Err(CanalError::Internal(format!(
                    "unexpected event type {:?} for row event",
                    event_type
                )));
            }
        };

        Ok(RowChange {
            table_name: table,
            schema_name: schema,
            before,
            after,
            dml_type,
        })
    }

    /// Must be called when a RotateEvent is received from binlog.
    /// Clears the TableMap cache because table_id mappings are
    /// no longer valid after log rotation.
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
    fn test_update_event_splits_before_after() {
        let mut converter = EventConverter::new();
        converter.handle_table_map(10, "mydb", "products");

        // 2 columns before + 2 columns after = 4 total
        let change = converter
            .handle_row_event(
                10,
                EventType::Update,
                vec![
                    make_column("id", "1"),
                    make_column("price", "10"),
                    make_column("id", "1"),
                    make_column("price", "20"),
                ],
            )
            .unwrap();

        assert_eq!(change.dml_type, DmlType::Update);
        assert_eq!(change.before.as_ref().unwrap().columns.len(), 2);
        assert_eq!(change.after.as_ref().unwrap().columns.len(), 2);
        assert!(change.after.unwrap().columns[1].updated);
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
        assert!(result.is_err()); // table_map cleared, table_id 1 unknown
    }
}
