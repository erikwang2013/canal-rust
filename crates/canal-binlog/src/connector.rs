use std::sync::atomic::{AtomicBool, Ordering};

const DDL_SQL_MAX_LEN: usize = 64 * 1024;

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
use tracing::{debug, error, info, warn};

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
    connect_timeout_secs: u64,
    sender: Option<mpsc::Sender<CanalResult<CanalEvent>>>,
    current_pos: Option<LogPosition>,
    running: AtomicBool,
    cancel_token: Option<CancellationToken>,
    connected: AtomicBool,
}

impl DefaultBinlogConnector {
    pub fn new(
        host: &str,
        port: u16,
        username: &str,
        password: &str,
        server_id: u64,
    ) -> CanalResult<Self> {
        if server_id > u32::MAX as u64 {
            return Err(CanalError::Config(format!(
                "server_id {} exceeds u32::MAX",
                server_id
            )));
        }
        Ok(Self {
            host: host.to_string(),
            port,
            username: username.to_string(),
            password: password.to_string(),
            server_id,
            ssl_mode: SslMode::Require,
            connect_timeout_secs: 30,
            sender: None,
            current_pos: None,
            running: AtomicBool::new(false),
            cancel_token: None,
            connected: AtomicBool::new(false),
        })
    }

    /// Set the SSL mode for the MySQL connection.
    pub fn with_ssl_mode(mut self, mode: SslMode) -> Self {
        self.ssl_mode = mode;
        self
    }

    /// Set the connection timeout in seconds (default: 30).
    pub fn with_connect_timeout(mut self, secs: u64) -> Self {
        self.connect_timeout_secs = secs;
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
        started: tokio::sync::oneshot::Sender<()>,
    ) {
        let mut client = BinlogClient::new(options);
        let mut converter = EventConverter::new();
        let mut current_binlog_file = String::new();

        let events = match client.replicate() {
            Ok(e) => e,
            Err(e) => {
                let send_err = CanalError::BinlogConnection(format!(
                    "failed to start binlog replication: {:?}",
                    e
                ));
                if tx.blocking_send(Err(send_err)).is_err() {
                    error!("Failed to send binlog connection error: channel closed");
                    return;
                }
                let _ = started.send(());
                return;
            }
        };

        // Signal that we've successfully connected and started replicating
        let _ = started.send(()); // oneshot — caller may have timed out, that's fine
        let mut current_gtid: Option<String> = None;

        for result in events {
            if cancel.is_cancelled() {
                info!("Binlog replication cancelled");
                break;
            }

            let (header, event) = match result {
                Ok(r) => r,
                Err(e) => {
                    let stream_err = CanalError::Protocol(format!("binlog stream error: {:?}", e));
                    if tx.blocking_send(Err(stream_err)).is_err() {
                        error!("Binlog stream error: channel closed, stopping replication");
                        break;
                    }
                    continue;
                }
            };

            // Track GTID from GTID events for inclusion in subsequent CanalEvents
            match &event {
                BinlogEvent::MySqlGtidEvent(ge) => {
                    current_gtid = Some(ge.gtid.to_string());
                }
                BinlogEvent::MariaDbGtidEvent(ge) => {
                    current_gtid = Some(ge.gtid.to_string());
                }
                _ => {}
            }
            let gtid_ref: Option<&str> = current_gtid.as_deref();

            if let BinlogEvent::RotateEvent(ref re) = event {
                current_binlog_file = re.binlog_filename.clone();
            }

            match Self::process_and_send(
                &header,
                &event,
                &mut converter,
                &current_binlog_file,
                gtid_ref,
                &tx,
            ) {
                Ok(()) => client.commit(&header, &event),
                Err(e) => {
                    if tx.blocking_send(Err(e)).is_err() {
                        error!("Channel closed during error delivery, stopping replication");
                        break;
                    }
                    // Still commit after conversion errors — the binlog event was
                    // consumed; we sent the error downstream for handling.
                    client.commit(&header, &event);
                }
            }
        }

        info!("Binlog replication stream ended");
    }

    /// Process a single binlog event.
    fn process_and_send(
        header: &EventHeader,
        event: &BinlogEvent,
        converter: &mut EventConverter,
        current_binlog_file: &str,
        gtid: Option<&str>,
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
                    schema_name: q.database_name.clone(),
                    table_name: String::new(),
                    row_change: None,
                    ddl_sql: {
                        let sql = q.sql_statement.clone();
                        if sql.len() > DDL_SQL_MAX_LEN {
                            warn!(
                                "DDL SQL truncated: {} bytes → {} bytes",
                                sql.len(),
                                DDL_SQL_MAX_LEN
                            );
                            Some(sql[..DDL_SQL_MAX_LEN].to_string())
                        } else {
                            Some(sql)
                        }
                    },
                    gtid: gtid.map(|s| s.to_string()),
                    raw_bytes: vec![],
                };
                if tx.blocking_send(Ok(canal_event)).is_err() {
                    error!("Channel closed during DDL event send");
                }
                Ok(())
            }

            BinlogEvent::WriteRowsEvent(e) => Self::send_row_events(
                header,
                current_binlog_file,
                EventType::Insert,
                e.table_id,
                &e.rows,
                converter,
                gtid,
                tx,
                extract_column_values,
            ),

            BinlogEvent::UpdateRowsEvent(e) => Self::send_update_events(
                header,
                current_binlog_file,
                e.table_id,
                &e.rows,
                converter,
                gtid,
                tx,
            ),

            BinlogEvent::DeleteRowsEvent(e) => Self::send_row_events(
                header,
                current_binlog_file,
                EventType::Delete,
                e.table_id,
                &e.rows,
                converter,
                gtid,
                tx,
                extract_column_values,
            ),

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
        gtid: Option<&str>,
        tx: &mpsc::Sender<CanalResult<CanalEvent>>,
        extract: fn(&RowData, &[ColumnInfo]) -> Vec<ColumnValue>,
    ) -> CanalResult<()> {
        let columns = converter.get_columns(table_id).cloned().unwrap_or_default();
        let gtid_owned = gtid.map(|s| s.to_string());
        for row in rows {
            let values = extract(row, &columns);
            match converter.handle_row_event(table_id, entry_type, values) {
                Ok(change) => {
                    let event = CanalEvent {
                        journal_name: current_binlog_file.to_string(),
                        position: header.next_event_position as u64,
                        server_id: header.server_id as u64,
                        execute_time: header.timestamp as i64,
                        entry_type,
                        schema_name: change.schema_name.clone(),
                        table_name: change.table_name.clone(),
                        row_change: Some(change),
                        ddl_sql: None,
                        gtid: gtid_owned.clone(),
                        raw_bytes: vec![],
                    };
                    if tx.blocking_send(Ok(event)).is_err() {
                        error!("Channel closed during row event send");
                    }
                }
                Err(err) => {
                    error!("Failed to convert {:?} event: {:?}", entry_type, err);
                    if tx.blocking_send(Err(err)).is_err() {
                        error!("Channel closed during error delivery");
                    }
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
        gtid: Option<&str>,
        tx: &mpsc::Sender<CanalResult<CanalEvent>>,
    ) -> CanalResult<()> {
        let columns = converter.get_columns(table_id).cloned().unwrap_or_default();
        let gtid_owned = gtid.map(|s| s.to_string());
        for row in rows {
            let before_values = extract_column_values(&row.before_update, &columns);
            let after_values = extract_column_values(&row.after_update, &columns);
            match converter.handle_update_row_event(table_id, before_values, after_values) {
                Ok(change) => {
                    let event = CanalEvent {
                        journal_name: current_binlog_file.to_string(),
                        position: header.next_event_position as u64,
                        server_id: header.server_id as u64,
                        execute_time: header.timestamp as i64,
                        entry_type: EventType::Update,
                        schema_name: change.schema_name.clone(),
                        table_name: change.table_name.clone(),
                        row_change: Some(change),
                        ddl_sql: None,
                        gtid: gtid_owned.clone(),
                        raw_bytes: vec![],
                    };
                    if tx.blocking_send(Ok(event)).is_err() {
                        error!("Channel closed during update event send");
                    }
                }
                Err(err) => {
                    error!("Failed to convert Update event: {:?}", err);
                    if tx.blocking_send(Err(err)).is_err() {
                        error!("Channel closed during error delivery");
                    }
                }
            }
        }
        Ok(())
    }
}

// -- Async trait implementation --

#[async_trait]
impl BinlogConnector for DefaultBinlogConnector {
    async fn connect(&mut self, pos: &LogPosition) -> CanalResult<()> {
        if self.running.load(Ordering::Acquire) {
            return Err(CanalError::Internal(
                "already connected — disconnect first".to_string(),
            ));
        }

        let tx = self
            .sender
            .clone()
            .ok_or_else(|| CanalError::Internal("no sender configured".to_string()))?;

        let options = self.build_options(pos);
        // Clear password from memory after use
        self.password.clear();
        self.password.shrink_to_fit();
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        self.cancel_token = Some(cancel);
        let timeout_secs = self.connect_timeout_secs;

        info!(
            "Connecting to MySQL {}:{} at {}:{} (timeout {}s)",
            self.host, self.port, pos.journal_name, pos.position, timeout_secs
        );

        self.current_pos = Some(pos.clone());
        self.running.store(true, Ordering::Release);

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();

        tokio::task::spawn_blocking(move || {
            Self::run_replication(options, tx, cancel_clone, started_tx);
        });

        let started_result =
            tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), started_rx)
                .await
                .map_err(|_| {
                    CanalError::BinlogConnection(format!(
                        "connection timed out after {}s",
                        timeout_secs
                    ))
                })?;

        match started_result {
            Ok(()) => self.connected.store(true, Ordering::Release),
            Err(_) => {
                // The oneshot sender was dropped without sending, meaning
                // run_replication returned early with an error — the error
                // was already sent via the event channel.
            }
        }

        Ok(())
    }

    /// Take the receiver end of the event channel.
    ///
    /// Must be called BEFORE connect() and BEFORE or INSTEAD OF with_channel().
    /// Panics if already connected or if a sender already exists.
    fn take_receiver(&mut self) -> mpsc::Receiver<CanalResult<CanalEvent>> {
        assert!(
            !self.connected.load(Ordering::Acquire),
            "take_receiver must be called before connect()"
        );
        assert!(
            self.sender.is_none(),
            "take_receiver must be called instead of with_channel(), not after it"
        );

        let (tx, rx) = mpsc::channel(4096);
        self.sender = Some(tx);
        rx
    }

    async fn disconnect(&mut self) -> CanalResult<()> {
        self.running.store(false, Ordering::Release);
        self.connected.store(false, Ordering::Release);
        self.sender = None;
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

impl Drop for DefaultBinlogConnector {
    fn drop(&mut self) {
        if let Some(token) = self.cancel_token.take() {
            token.cancel();
        }
    }
}
