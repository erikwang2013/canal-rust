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
                b.iter()
                    .map(|byte| format!("{:02x}", byte))
                    .collect::<Vec<_>>()
                    .join("")
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
