use async_trait::async_trait;
use canal_common::{CanalError, CanalEvent, CanalResult, ColumnValue, EventType, LogPosition};
use mysql_cdc::binlog_client::BinlogClient;
use mysql_cdc::binlog_options::BinlogOptions;
use mysql_cdc::events::binlog_event::BinlogEvent;
use mysql_cdc::events::event_header::EventHeader;
use mysql_cdc::events::row_events::mysql_value::MySqlValue;
use mysql_cdc::events::row_events::row_data::{RowData, UpdateRowData};
use mysql_cdc::events::table_map_event::TableMapEvent;
use mysql_cdc::replica_options::ReplicaOptions;
use mysql_cdc::ssl_mode::SslMode;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

use crate::table_map::ColumnInfo;
use crate::EventConverter;

/// Trait for MySQL binlog replication connectors.
/// Abstracts the underlying binlog client library so implementations
/// can be swapped (binlog crate, custom protocol, mock for testing).
#[async_trait]
pub trait BinlogConnector: Send {
    /// Connect to MySQL and start replicating from the given position
    async fn connect(&mut self, pos: &LogPosition) -> CanalResult<()>;

    /// Get the receiver end of the event channel.
    /// Events from MySQL binlog are streamed through this channel.
    fn take_receiver(&mut self) -> mpsc::Receiver<CanalResult<CanalEvent>>;

    /// Gracefully disconnect from MySQL
    async fn disconnect(&mut self) -> CanalResult<()>;

    /// Return the current binlog position, if connected
    fn current_position(&self) -> Option<LogPosition>;
}

/// Default binlog connector implementation using the `mysql_cdc` crate.
///
/// The connect() method spawns a blocking task that streams binlog events
/// from a real MySQL/MariaDB server and feeds them into the event channel.
pub struct DefaultBinlogConnector {
    host: String,
    port: u16,
    username: String,
    password: String,
    server_id: u64,
    sender: Option<mpsc::Sender<CanalResult<CanalEvent>>>,
    current_pos: Option<LogPosition>,
    running: bool,
}

impl DefaultBinlogConnector {
    pub fn new(host: &str, port: u16, username: &str, password: &str, server_id: u64) -> Self {
        Self {
            host: host.to_string(),
            port,
            username: username.to_string(),
            password: password.to_string(),
            server_id,
            sender: None,
            current_pos: None,
            running: false,
        }
    }

    /// Create a connector with a pre-built channel for event streaming
    pub fn with_channel(mut self) -> (Self, mpsc::Receiver<CanalResult<CanalEvent>>) {
        let (tx, rx) = mpsc::channel(4096);
        self.sender = Some(tx);
        (self, rx)
    }

    // ---------------------------------------------------------------------------
    // Internal helpers — all synchronous because they run inside spawn_blocking
    // ---------------------------------------------------------------------------

    /// Build ReplicaOptions from stored connection params and the given position.
    fn build_options(&self, pos: &LogPosition) -> ReplicaOptions {
        ReplicaOptions {
            hostname: self.host.clone(),
            port: self.port,
            username: self.username.clone(),
            password: self.password.clone(),
            server_id: self.server_id as u32,
            blocking: true,
            ssl_mode: SslMode::Disabled,
            binlog: BinlogOptions::from_position(pos.journal_name.clone(), pos.position as u32),
            ..Default::default()
        }
    }

    /// The synchronous replication loop. Runs inside tokio::task::spawn_blocking
    /// because mysql_cdc's replicate() is a blocking iterator over network packets.
    fn run_replication(
        options: ReplicaOptions,
        tx: mpsc::Sender<CanalResult<CanalEvent>>,
    ) {
        let mut client = BinlogClient::new(options);
        let mut converter = EventConverter::new();
        let mut current_binlog_file = String::new();

        let events = match client.replicate() {
            Ok(e) => e,
            Err(e) => {
                let _ = tx.blocking_send(Err(CanalError::BinlogConnection(format!(
                    "failed to start binlog replication: {:?}",
                    e
                ))));
                return;
            }
        };

        for result in events {
            let (header, event) = match result {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.blocking_send(Err(CanalError::Protocol(format!(
                        "binlog stream error: {:?}",
                        e
                    ))));
                    continue;
                }
            };

            // Keep track of the current binlog file name (updated on RotateEvent)
            if let BinlogEvent::RotateEvent(ref re) = event {
                current_binlog_file = re.binlog_filename.clone();
            }

            // Convert and send. Non-row events (TableMap, Rotate, heartbeat,
            // Xid, etc.) are handled internally and produce no CanalEvent.
            if let Err(e) = Self::process_and_send(
                &header,
                &event,
                &mut converter,
                &current_binlog_file,
                &tx,
            ) {
                let _ = tx.blocking_send(Err(e));
            }

            // Always commit so the connector tracks progress for reconnection
            client.commit(&header, &event);
        }

        info!("Binlog replication stream ended");
    }

    /// Process a single binlog event. Returns Ok(()) whether or not a
    /// CanalEvent was produced (only row events produce one).
    fn process_and_send(
        header: &EventHeader,
        event: &BinlogEvent,
        converter: &mut EventConverter,
        current_binlog_file: &str,
        tx: &mpsc::Sender<CanalResult<CanalEvent>>,
    ) -> CanalResult<()> {
        match event {
            // -- TableMap: register table metadata, no output event --
            BinlogEvent::TableMapEvent(e) => {
                let columns = build_column_infos(e);
                converter.handle_table_map_event(
                    e.table_id,
                    &e.database_name,
                    &e.table_name,
                    columns,
                );
                Ok(())
            }

            // -- RotateEvent: clear stale TableMap entries, no output event --
            BinlogEvent::RotateEvent(_) => {
                converter.clear_table_map();
                Ok(())
            }

            // -- Heartbeat: no output event --
            BinlogEvent::HeartbeatEvent(_) => Ok(()),

            // -- Xid / transaction boundary: no output event --
            BinlogEvent::XidEvent(_) => Ok(()),

            // -- DDL / Query events: emitted as a DDL CanalEvent --
            BinlogEvent::QueryEvent(q) => {
                let canal_event = CanalEvent {
                    journal_name: current_binlog_file.to_string(),
                    position: header.next_event_position as u64,
                    server_id: header.server_id as u64,
                    execute_time: header.timestamp as i64,
                    entry_type: EventType::Ddl,
                    schema_name: String::new(),
                    table_name: String::new(),
                    row_change: None,
                    ddl_sql: Some(q.sql_statement.clone()),
                    gtid: None,
                    raw_bytes: vec![],
                };
                let _ = tx.blocking_send(Ok(canal_event));
                Ok(())
            }

            // -- WriteRows (INSERT) --
            BinlogEvent::WriteRowsEvent(e) => {
                let columns = converter
                    .get_columns(e.table_id)
                    .cloned()
                    .unwrap_or_default();
                for row in &e.rows {
                    let values = extract_column_values(row, &columns);
                    match converter.handle_row_event(e.table_id, EventType::Insert, values) {
                        Ok(change) => {
                            let schema = change.schema_name.clone();
                            let table = change.table_name.clone();
                            let canal_event = Self::build_canal_event(
                                header,
                                current_binlog_file,
                                EventType::Insert,
                                &schema,
                                &table,
                                Some(change),
                                None,
                            );
                            let _ = tx.blocking_send(Ok(canal_event));
                        }
                        Err(err) => {
                            error!("Failed to convert WriteRows event: {:?}", err);
                            let _ = tx.blocking_send(Err(err));
                        }
                    }
                }
                Ok(())
            }

            // -- UpdateRows (UPDATE) --
            BinlogEvent::UpdateRowsEvent(e) => {
                let columns = converter
                    .get_columns(e.table_id)
                    .cloned()
                    .unwrap_or_default();
                for row in &e.rows {
                    let values = extract_update_column_values(row, &columns);
                    match converter.handle_row_event(e.table_id, EventType::Update, values) {
                        Ok(change) => {
                            let schema = change.schema_name.clone();
                            let table = change.table_name.clone();
                            let canal_event = Self::build_canal_event(
                                header,
                                current_binlog_file,
                                EventType::Update,
                                &schema,
                                &table,
                                Some(change),
                                None,
                            );
                            let _ = tx.blocking_send(Ok(canal_event));
                        }
                        Err(err) => {
                            error!("Failed to convert UpdateRows event: {:?}", err);
                            let _ = tx.blocking_send(Err(err));
                        }
                    }
                }
                Ok(())
            }

            // -- DeleteRows (DELETE) --
            BinlogEvent::DeleteRowsEvent(e) => {
                let columns = converter
                    .get_columns(e.table_id)
                    .cloned()
                    .unwrap_or_default();
                for row in &e.rows {
                    let values = extract_column_values(row, &columns);
                    match converter.handle_row_event(e.table_id, EventType::Delete, values) {
                        Ok(change) => {
                            let schema = change.schema_name.clone();
                            let table = change.table_name.clone();
                            let canal_event = Self::build_canal_event(
                                header,
                                current_binlog_file,
                                EventType::Delete,
                                &schema,
                                &table,
                                Some(change),
                                None,
                            );
                            let _ = tx.blocking_send(Ok(canal_event));
                        }
                        Err(err) => {
                            error!("Failed to convert DeleteRows event: {:?}", err);
                            let _ = tx.blocking_send(Err(err));
                        }
                    }
                }
                Ok(())
            }

            // Catch-all for unhandled events
            other => {
                debug!("Skipping unhandled binlog event: {:?}", other);
                Ok(())
            }
        }
    }

    /// Build a CanalEvent from binlog event metadata and an optional RowChange / DDL sql.
    fn build_canal_event(
        header: &EventHeader,
        journal_name: &str,
        entry_type: EventType,
        schema_name: &str,
        table_name: &str,
        row_change: Option<canal_common::RowChange>,
        ddl_sql: Option<String>,
    ) -> CanalEvent {
        CanalEvent {
            journal_name: journal_name.to_string(),
            position: header.next_event_position as u64,
            server_id: header.server_id as u64,
            execute_time: header.timestamp as i64,
            entry_type,
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
            row_change,
            ddl_sql,
            gtid: None,
            raw_bytes: vec![],
        }
    }
}

// ---------------------------------------------------------------------------
// Async trait implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl BinlogConnector for DefaultBinlogConnector {
    async fn connect(&mut self, pos: &LogPosition) -> CanalResult<()> {
        if self.running {
            return Err(CanalError::Internal(
                "already connected — disconnect first".to_string(),
            ));
        }

        let tx = self
            .sender
            .clone()
            .ok_or_else(|| CanalError::Internal("no sender configured".to_string()))?;

        let options = self.build_options(pos);

        info!(
            "Connecting to MySQL {}:{} at {}:{}",
            self.host, self.port, pos.journal_name, pos.position
        );

        self.current_pos = Some(pos.clone());
        self.running = true;

        tokio::task::spawn_blocking(move || {
            Self::run_replication(options, tx);
        });

        Ok(())
    }

    fn take_receiver(&mut self) -> mpsc::Receiver<CanalResult<CanalEvent>> {
        self.sender
            .take()
            .map(|_old_tx| {
                let (tx, rx) = mpsc::channel(4096);
                self.sender = Some(tx);
                rx
            })
            .unwrap_or_else(|| {
                let (tx, rx) = mpsc::channel(4096);
                self.sender = Some(tx);
                rx
            })
    }

    async fn disconnect(&mut self) -> CanalResult<()> {
        self.running = false;
        info!("Disconnected from MySQL");
        Ok(())
    }

    fn current_position(&self) -> Option<LogPosition> {
        self.current_pos.clone()
    }
}

// ---------------------------------------------------------------------------
// Column extraction helpers
// ---------------------------------------------------------------------------

/// Build a Vec<ColumnInfo> from a mysql_cdc TableMapEvent.
/// Uses column names from table metadata (MySQL 5.6+) when available,
/// otherwise synthesises "col_N" names.
fn build_column_infos(tm: &TableMapEvent) -> Vec<ColumnInfo> {
    let num_cols = tm.column_types.len();

    // Column names: prefer what the server sends, else fall back to col_N
    let column_names: Vec<String> = tm
        .table_metadata
        .as_ref()
        .and_then(|m| m.column_names.clone())
        .unwrap_or_else(|| (0..num_cols).map(|i| format!("col_{}", i)).collect());

    // Primary-key columns (from table metadata)
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

/// Convert a MySqlValue into its string representation.
fn mysql_value_to_string(v: &MySqlValue) -> String {
    match v {
        MySqlValue::TinyInt(n) => n.to_string(),
        MySqlValue::SmallInt(n) => n.to_string(),
        MySqlValue::MediumInt(n) => n.to_string(),
        MySqlValue::Int(n) => n.to_string(),
        MySqlValue::BigInt(n) => n.to_string(),
        MySqlValue::Float(n) => n.to_string(),
        MySqlValue::Double(n) => n.to_string(),
        MySqlValue::Decimal(s) | MySqlValue::String(s) => s.clone(),
        MySqlValue::Blob(b) => String::from_utf8_lossy(b).to_string(),
        MySqlValue::Bit(bits) => bits.iter().fold(String::new(), |mut s, b| {
            s.push(if *b { '1' } else { '0' });
            s
        }),
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

/// Convert a mysql_cdc RowData into a Vec<ColumnValue> using column metadata
/// for names, types, and key status.
fn extract_column_values(row: &RowData, column_infos: &[ColumnInfo]) -> Vec<ColumnValue> {
    row.cells
        .iter()
        .enumerate()
        .map(|(i, cell)| {
            let info = column_infos.get(i);
            ColumnValue {
                name: info.map_or_else(|| format!("col_{}", i), |c| c.name.clone()),
                value: cell.as_ref().map(mysql_value_to_string),
                column_type: info.map_or(0, |c| c.column_type),
                is_key: info.map_or(false, |c| c.is_key),
                updated: false,
            }
        })
        .collect()
}

/// Convert a mysql_cdc UpdateRowData (before + after images) into a flat
/// Vec<ColumnValue> suitable for EventConverter::handle_row_event.
fn extract_update_column_values(
    row: &UpdateRowData,
    column_infos: &[ColumnInfo],
) -> Vec<ColumnValue> {
    let mut values = extract_column_values(&row.before_update, column_infos);
    let after = extract_column_values(&row.after_update, column_infos);
    values.extend(after);
    values
}
