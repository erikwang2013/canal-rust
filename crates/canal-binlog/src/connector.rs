use std::sync::atomic::{AtomicBool, Ordering};

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
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::table_map::ColumnInfo;
use crate::EventConverter;

/// Trait for MySQL binlog replication connectors.
#[async_trait]
pub trait BinlogConnector: Send {
    /// Connect to MySQL and start replicating from the given position
    async fn connect(&mut self, pos: &LogPosition) -> CanalResult<()>;

    /// Get the receiver end of the event channel.
    /// Must be called BEFORE connect().
    fn take_receiver(&mut self) -> mpsc::Receiver<CanalResult<CanalEvent>>;

    /// Gracefully disconnect from MySQL
    async fn disconnect(&mut self) -> CanalResult<()>;

    /// Return the current binlog position, if connected
    fn current_position(&self) -> Option<LogPosition>;
}

/// Default binlog connector using `mysql_cdc`.
pub struct DefaultBinlogConnector {
    host: String,
    port: u16,
    username: String,
    password: String,
    server_id: u64,
    ssl_mode: SslMode,
    sender: Option<mpsc::Sender<CanalResult<CanalEvent>>>,
    current_pos: Option<LogPosition>,
    running: AtomicBool,
    cancel_token: Option<CancellationToken>,
    connected: AtomicBool,
}

impl DefaultBinlogConnector {
    pub fn new(host: &str, port: u16, username: &str, password: &str, server_id: u64) -> Self {
        assert!(server_id <= u32::MAX as u64, "server_id must fit in u32");
        Self {
            host: host.to_string(),
            port,
            username: username.to_string(),
            password: password.to_string(),
            server_id,
            ssl_mode: SslMode::IfAvailable,
            sender: None,
            current_pos: None,
            running: AtomicBool::new(false),
            cancel_token: None,
            connected: AtomicBool::new(false),
        }
    }

    /// Set the SSL mode for the MySQL connection.
    pub fn with_ssl_mode(mut self, mode: SslMode) -> Self {
        self.ssl_mode = mode;
        self
    }

    /// Create a connector with a pre-built channel for event streaming.
    pub fn with_channel(mut self) -> (Self, mpsc::Receiver<CanalResult<CanalEvent>>) {
        let (tx, rx) = mpsc::channel(4096);
        self.sender = Some(tx);
        (self, rx)
    }

    // -- Internal helpers --

    fn build_options(&self, pos: &LogPosition) -> ReplicaOptions {
        ReplicaOptions {
            hostname: self.host.clone(),
            port: self.port,
            username: self.username.clone(),
            password: self.password.clone(),
            server_id: self.server_id as u32,
            blocking: true,
            ssl_mode: self.ssl_mode,
            binlog: BinlogOptions::from_position(pos.journal_name.clone(), pos.position as u32),
            ..Default::default()
        }
    }

    /// The synchronous replication loop.
    fn run_replication(
        options: ReplicaOptions,
        tx: mpsc::Sender<CanalResult<CanalEvent>>,
        cancel: CancellationToken,
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
            if cancel.is_cancelled() {
                info!("Binlog replication cancelled");
                break;
            }

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

            if let BinlogEvent::RotateEvent(ref re) = event {
                current_binlog_file = re.binlog_filename.clone();
            }

            if let Err(e) = Self::process_and_send(
                &header,
                &event,
                &mut converter,
                &current_binlog_file,
                &tx,
            ) {
                let _ = tx.blocking_send(Err(e));
            }

            client.commit(&header, &event);
        }

        info!("Binlog replication stream ended");
    }

    /// Process a single binlog event.
    fn process_and_send(
        header: &EventHeader,
        event: &BinlogEvent,
        converter: &mut EventConverter,
        current_binlog_file: &str,
        tx: &mpsc::Sender<CanalResult<CanalEvent>>,
    ) -> CanalResult<()> {
        match event {
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

            BinlogEvent::RotateEvent(_) => {
                converter.clear_table_map();
                Ok(())
            }

            BinlogEvent::HeartbeatEvent(_) | BinlogEvent::XidEvent(_) => Ok(()),

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

            BinlogEvent::WriteRowsEvent(e) => {
                Self::send_row_events(
                    header, current_binlog_file, EventType::Insert,
                    e.table_id, &e.rows, converter, tx,
                    extract_column_values,
                )
            }

            BinlogEvent::UpdateRowsEvent(e) => {
                Self::send_update_events(
                    header, current_binlog_file,
                    e.table_id, &e.rows, converter, tx,
                )
            }

            BinlogEvent::DeleteRowsEvent(e) => {
                Self::send_row_events(
                    header, current_binlog_file, EventType::Delete,
                    e.table_id, &e.rows, converter, tx,
                    extract_column_values,
                )
            }

            other => {
                debug!("Skipping unhandled binlog event: {:?}", other);
                Ok(())
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn send_row_events(
        header: &EventHeader,
        current_binlog_file: &str,
        entry_type: EventType,
        table_id: u64,
        rows: &[RowData],
        converter: &mut EventConverter,
        tx: &mpsc::Sender<CanalResult<CanalEvent>>,
        extract: fn(&RowData, &[ColumnInfo]) -> Vec<ColumnValue>,
    ) -> CanalResult<()> {
        let columns = converter.get_columns(table_id).cloned().unwrap_or_default();
        for row in rows {
            let values = extract(row, &columns);
            match converter.handle_row_event(table_id, entry_type, values) {
                Ok(change) => {
                    let schema = change.schema_name.clone();
                    let table = change.table_name.clone();
                    let event = Self::build_canal_event(
                        header, current_binlog_file, entry_type,
                        &schema, &table,
                        Some(change), None,
                    );
                    let _ = tx.blocking_send(Ok(event));
                }
                Err(err) => {
                    error!("Failed to convert {:?} event: {:?}", entry_type, err);
                    let _ = tx.blocking_send(Err(err));
                }
            }
        }
        Ok(())
    }

    fn send_update_events(
        header: &EventHeader,
        current_binlog_file: &str,
        table_id: u64,
        rows: &[UpdateRowData],
        converter: &mut EventConverter,
        tx: &mpsc::Sender<CanalResult<CanalEvent>>,
    ) -> CanalResult<()> {
        let columns = converter.get_columns(table_id).cloned().unwrap_or_default();
        for row in rows {
            let before_values = extract_column_values(&row.before_update, &columns);
            let after_values = extract_column_values(&row.after_update, &columns);
            match converter.handle_update_row_event(table_id, before_values, after_values) {
                Ok(change) => {
                    let schema = change.schema_name.clone();
                    let table = change.table_name.clone();
                    let event = Self::build_canal_event(
                        header, current_binlog_file, EventType::Update,
                        &schema, &table,
                        Some(change), None,
                    );
                    let _ = tx.blocking_send(Ok(event));
                }
                Err(err) => {
                    error!("Failed to convert Update event: {:?}", err);
                    let _ = tx.blocking_send(Err(err));
                }
            }
        }
        Ok(())
    }

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

// -- Async trait implementation --

#[async_trait]
impl BinlogConnector for DefaultBinlogConnector {
    async fn connect(&mut self, pos: &LogPosition) -> CanalResult<()> {
        if self.running.load(Ordering::SeqCst) {
            return Err(CanalError::Internal(
                "already connected — disconnect first".to_string(),
            ));
        }

        let tx = self
            .sender
            .clone()
            .ok_or_else(|| CanalError::Internal("no sender configured".to_string()))?;

        let options = self.build_options(pos);
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        self.cancel_token = Some(cancel);

        info!(
            "Connecting to MySQL {}:{} at {}:{}",
            self.host, self.port, pos.journal_name, pos.position
        );

        self.current_pos = Some(pos.clone());
        self.running.store(true, Ordering::SeqCst);
        self.connected.store(true, Ordering::SeqCst);

        tokio::task::spawn_blocking(move || {
            Self::run_replication(options, tx, cancel_clone);
        });

        Ok(())
    }

    /// Take the receiver end of the event channel.
    ///
    /// Must be called BEFORE connect(). Panics if already connected
    /// to prevent silent data loss from channel mismatch.
    fn take_receiver(&mut self) -> mpsc::Receiver<CanalResult<CanalEvent>> {
        assert!(
            !self.connected.load(Ordering::SeqCst),
            "take_receiver must be called before connect()"
        );

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
        self.running.store(false, Ordering::SeqCst);
        if let Some(token) = self.cancel_token.take() {
            token.cancel();
        }
        info!("Disconnected from MySQL");
        Ok(())
    }

    fn current_position(&self) -> Option<LogPosition> {
        self.current_pos.clone()
    }
}

// -- Column extraction helpers --

fn build_column_infos(tm: &TableMapEvent) -> Vec<ColumnInfo> {
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
                is_key: info.is_some_and(|c| c.is_key),
                updated: false,
            }
        })
        .collect()
}
