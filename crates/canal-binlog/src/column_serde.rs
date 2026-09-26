use crate::table_map::ColumnInfo;
use canal_common::ColumnValue;
use mysql_cdc::events::row_events::mysql_value::MySqlValue;
use mysql_cdc::events::row_events::row_data::RowData;
use mysql_cdc::events::table_map_event::TableMapEvent;

pub(crate) fn build_column_infos(tm: &TableMapEvent) -> Vec<ColumnInfo> {
    let num_cols = tm.column_types.len();

    let column_names: Vec<String> = tm
        .table_metadata
        .as_ref()
        .and_then(|m| m.column_names.clone())
        .unwrap_or_else(|| (0..num_cols).map(|i| format!("col_{}", i)).collect());

    let mut is_key = vec![false; num_cols];
    if let Some(ref meta) = tm.table_metadata {
        if let Some(ref pks) = meta.simple_primary_keys {
            for &idx in pks {
                if (idx as usize) < num_cols {
                    is_key[idx as usize] = true;
                }
            }
        }
        if let Some(ref pks) = meta.primary_keys_with_prefix {
            for &(idx, _) in pks {
                if (idx as usize) < num_cols {
                    is_key[idx as usize] = true;
                }
            }
        }
    }

    (0..num_cols)
        .map(|i| ColumnInfo {
            name: column_names
                .get(i)
                .cloned()
                .unwrap_or_else(|| format!("col_{}", i)),
            column_type: tm.column_types.get(i).copied().unwrap_or(0) as i32,
            is_key: is_key[i],
            is_nullable: tm.null_bitmap.get(i).copied().unwrap_or(true),
        })
        .collect()
}

pub(crate) fn mysql_value_to_string(v: &MySqlValue) -> String {
    match v {
        MySqlValue::TinyInt(n) => n.to_string(),
        MySqlValue::SmallInt(n) => n.to_string(),
        MySqlValue::MediumInt(n) => n.to_string(),
        MySqlValue::Int(n) => n.to_string(),
        MySqlValue::BigInt(n) => n.to_string(),
        MySqlValue::Float(n) => n.to_string(),
        MySqlValue::Double(n) => n.to_string(),
        MySqlValue::Decimal(s) | MySqlValue::String(s) => s.clone(),
        MySqlValue::Blob(b) => {
            if let Ok(s) = std::str::from_utf8(b) {
                s.to_string()
            } else {
                use std::fmt::Write;
                let mut s = String::with_capacity(b.len() * 2);
                for byte in b {
                    write!(s, "{:02x}", byte).unwrap();
                }
                s
            }
        }
        MySqlValue::Bit(bits) => {
            let mut s = String::with_capacity(bits.len());
            for &b in bits {
                s.push(if b { '1' } else { '0' });
            }
            s
        }
        MySqlValue::Enum(n) => n.to_string(),
        MySqlValue::Set(n) => n.to_string(),
        MySqlValue::Year(n) => n.to_string(),
        MySqlValue::Date(d) => format!("{:04}-{:02}-{:02}", d.year, d.month, d.day),
        MySqlValue::Time(t) => format!("{:02}:{:02}:{:02}", t.hour, t.minute, t.second),
        MySqlValue::DateTime(dt) => {
            format!(
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second
            )
        }
        MySqlValue::Timestamp(ts) => ts.to_string(),
    }
}

pub(crate) fn extract_column_values(
    row: &RowData,
    column_infos: &[ColumnInfo],
) -> Vec<ColumnValue> {
    row.cells
        .iter()
        .enumerate()
        .map(|(i, cell)| {
            let info = column_infos.get(i);
            ColumnValue {
                name: info.map_or_else(|| format!("col_{}", i), |c| c.name.clone()),
                value: cell.as_ref().map(mysql_value_to_string),
                column_type: info.map_or(0, |c| c.column_type),
                is_key: info.is_some_and(|c| c.is_key),
                updated: false,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_cdc::events::row_events::mysql_value::{Date, DateTime, MySqlValue, Time};
    use mysql_cdc::metadata::table_metadata::TableMetadata;

    fn table_map_event(
        column_types: Vec<u8>,
        null_bitmap: Vec<bool>,
        meta: Option<TableMetadata>,
    ) -> TableMapEvent {
        TableMapEvent {
            table_id: 1,
            database_name: "db".into(),
            table_name: "t".into(),
            column_types,
            column_metadata: vec![],
            null_bitmap,
            table_metadata: meta,
        }
    }

    #[test]
    fn test_mysql_value_to_string_numerics() {
        assert_eq!(mysql_value_to_string(&MySqlValue::TinyInt(255)), "255");
        assert_eq!(mysql_value_to_string(&MySqlValue::SmallInt(65535)), "65535");
        assert_eq!(
            mysql_value_to_string(&MySqlValue::MediumInt(16_777_215)),
            "16777215"
        );
        assert_eq!(mysql_value_to_string(&MySqlValue::Int(42)), "42");
        assert_eq!(
            mysql_value_to_string(&MySqlValue::BigInt(u64::MAX)),
            "18446744073709551615"
        );
        assert_eq!(mysql_value_to_string(&MySqlValue::Float(1.5)), "1.5");
        assert_eq!(mysql_value_to_string(&MySqlValue::Double(-2.25)), "-2.25");
        assert_eq!(
            mysql_value_to_string(&MySqlValue::Decimal("123.45".into())),
            "123.45"
        );
        assert_eq!(
            mysql_value_to_string(&MySqlValue::String("hello".into())),
            "hello"
        );
        assert_eq!(mysql_value_to_string(&MySqlValue::Enum(3)), "3");
        assert_eq!(mysql_value_to_string(&MySqlValue::Set(5)), "5");
        assert_eq!(mysql_value_to_string(&MySqlValue::Year(2024)), "2024");
    }

    #[test]
    fn test_mysql_value_to_string_blob() {
        // Valid UTF-8 blobs are decoded as text
        assert_eq!(
            mysql_value_to_string(&MySqlValue::Blob(b"plain text".to_vec())),
            "plain text"
        );
        // Invalid UTF-8 blobs fall back to lowercase hex
        assert_eq!(
            mysql_value_to_string(&MySqlValue::Blob(vec![0xff, 0x00, 0xab])),
            "ff00ab"
        );
        // Empty blob
        assert_eq!(mysql_value_to_string(&MySqlValue::Blob(vec![])), "");
    }

    #[test]
    fn test_mysql_value_to_string_bit() {
        assert_eq!(mysql_value_to_string(&MySqlValue::Bit(vec![])), "");
        assert_eq!(
            mysql_value_to_string(&MySqlValue::Bit(vec![true, false, true])),
            "101"
        );
        assert_eq!(
            mysql_value_to_string(&MySqlValue::Bit(vec![false; 4])),
            "0000"
        );
    }

    #[test]
    fn test_mysql_value_to_string_temporal() {
        assert_eq!(
            mysql_value_to_string(&MySqlValue::Date(Date {
                year: 2024,
                month: 1,
                day: 31
            })),
            "2024-01-31"
        );
        assert_eq!(
            mysql_value_to_string(&MySqlValue::Date(Date {
                year: 7,
                month: 3,
                day: 2
            })),
            "0007-03-02"
        );
        assert_eq!(
            mysql_value_to_string(&MySqlValue::Time(Time {
                hour: 10,
                minute: 30,
                second: 59,
                millis: 0
            })),
            "10:30:59"
        );
        assert_eq!(
            mysql_value_to_string(&MySqlValue::DateTime(DateTime {
                year: 2024,
                month: 12,
                day: 31,
                hour: 23,
                minute: 59,
                second: 58,
                millis: 0
            })),
            "2024-12-31 23:59:58"
        );
        assert_eq!(
            mysql_value_to_string(&MySqlValue::Timestamp(1_700_000_000)),
            "1700000000"
        );
    }

    #[test]
    fn test_build_column_infos_without_metadata() {
        // No table metadata: generated col_N names, no primary keys, nullable default
        let tm = table_map_event(vec![3, 253, 4], vec![false, true, false], None);
        let infos = build_column_infos(&tm);
        assert_eq!(infos.len(), 3);
        assert_eq!(infos[0].name, "col_0");
        assert_eq!(infos[1].name, "col_1");
        assert_eq!(infos[0].column_type, 3);
        assert_eq!(infos[2].column_type, 4);
        assert!(!infos[0].is_key);
        assert!(infos[1].is_nullable);
        assert!(!infos[2].is_nullable);
    }

    #[test]
    fn test_build_column_infos_with_names_and_pks() {
        let meta = TableMetadata {
            signedness: None,
            default_charset: None,
            column_charsets: None,
            column_names: Some(vec!["id".into(), "name".into(), "age".into()]),
            set_string_values: None,
            enum_string_values: None,
            geometry_types: None,
            simple_primary_keys: Some(vec![0]),
            primary_keys_with_prefix: Some(vec![(1, 10)]),
            enum_and_set_default_charset: None,
            enum_and_set_column_charsets: None,
            column_visibility: None,
        };
        let tm = table_map_event(vec![3, 253, 3], vec![false, false, false], Some(meta));
        let infos = build_column_infos(&tm);
        assert_eq!(infos[0].name, "id");
        assert_eq!(infos[1].name, "name");
        assert!(infos[0].is_key, "simple_primary_keys idx 0 must be key");
        assert!(
            infos[1].is_key,
            "primary_keys_with_prefix idx 1 must be key"
        );
        assert!(!infos[2].is_key);
        assert!(!infos[2].is_nullable);
    }

    #[test]
    fn test_build_column_infos_pk_out_of_bounds_ignored() {
        let meta = TableMetadata {
            signedness: None,
            default_charset: None,
            column_charsets: None,
            column_names: Some(vec!["id".into()]),
            set_string_values: None,
            enum_string_values: None,
            geometry_types: None,
            simple_primary_keys: Some(vec![5]),
            primary_keys_with_prefix: Some(vec![(9, 4)]),
            enum_and_set_default_charset: None,
            enum_and_set_column_charsets: None,
            column_visibility: None,
        };
        let tm = table_map_event(vec![3], vec![true], Some(meta));
        let infos = build_column_infos(&tm);
        assert_eq!(infos.len(), 1);
        assert!(!infos[0].is_key, "out-of-bounds pk index must be ignored");
    }

    #[test]
    fn test_build_column_infos_short_vectors() {
        // column_types is authoritative for the column count; a shorter
        // null_bitmap leaves remaining columns nullable by default
        let meta = TableMetadata {
            signedness: None,
            default_charset: None,
            column_charsets: None,
            column_names: Some(vec!["a".into(), "b".into(), "c".into()]),
            set_string_values: None,
            enum_string_values: None,
            geometry_types: None,
            simple_primary_keys: None,
            primary_keys_with_prefix: None,
            enum_and_set_default_charset: None,
            enum_and_set_column_charsets: None,
            column_visibility: None,
        };
        let tm = table_map_event(vec![3, 253], vec![false], Some(meta));
        let infos = build_column_infos(&tm);
        assert_eq!(infos.len(), 2, "count follows column_types, not names");
        assert_eq!(infos[0].name, "a");
        assert_eq!(infos[1].name, "b");
        assert_eq!(infos[0].column_type, 3);
        assert_eq!(infos[1].column_type, 253);
        assert!(!infos[0].is_nullable);
        assert!(infos[1].is_nullable, "missing nullability defaults to true");
    }

    #[test]
    fn test_extract_column_values_with_infos() {
        let infos = vec![
            ColumnInfo {
                name: "id".into(),
                column_type: 3,
                is_key: true,
                is_nullable: false,
            },
            ColumnInfo {
                name: "note".into(),
                column_type: 253,
                is_key: false,
                is_nullable: true,
            },
        ];
        let row = RowData::new(vec![
            Some(MySqlValue::Int(7)),
            None,
            Some(MySqlValue::String("extra".into())),
        ]);
        let values = extract_column_values(&row, &infos);
        assert_eq!(values.len(), 3);
        // Index 0: from ColumnInfo
        assert_eq!(values[0].name, "id");
        assert_eq!(values[0].value.as_deref(), Some("7"));
        assert!(values[0].is_key);
        assert_eq!(values[0].column_type, 3);
        // Index 1: NULL cell
        assert_eq!(values[1].name, "note");
        assert!(values[1].value.is_none());
        // Index 2: beyond ColumnInfo -> generated fallback
        assert_eq!(values[2].name, "col_2");
        assert_eq!(values[2].value.as_deref(), Some("extra"));
        assert!(!values[2].is_key);
        assert_eq!(values[2].column_type, 0);
        // updated is never set during extraction
        assert!(values.iter().all(|v| !v.updated));
    }

    #[test]
    fn test_extract_column_values_empty() {
        let row = RowData::new(vec![]);
        assert!(extract_column_values(&row, &[]).is_empty());
    }
}
