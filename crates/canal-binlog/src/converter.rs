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

    pub fn handle_table_map(&mut self, table_id: u64, schema: &str, table: &str) {
        self.table_map
            .put(table_id, schema.to_string(), table.to_string());
    }

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
        let (schema, table) = self.table_map.get(table_id).ok_or_else(|| {
            CanalError::NotFound(format!("table_id {} not found in TableMap", table_id))
        })?;

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
            table_name: table,
            schema_name: schema,
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
        let (schema, table) = self.table_map.get(table_id).ok_or_else(|| {
            CanalError::NotFound(format!("table_id {} not found in TableMap", table_id))
        })?;

        for col in &mut after_columns {
            col.updated = true;
        }

        Ok(RowChange {
            table_name: table,
            schema_name: schema,
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
}
