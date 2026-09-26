use std::sync::atomic::{AtomicBool, Ordering};

const DDL_SQL_MAX_LEN: usize = 64 * 1024;

use async_trait::async_trait;
use canal_common::{CanalError, CanalEvent, CanalResult, EventType, LogPosition};
use mysql_cdc::binlog_client::BinlogClient;
use mysql_cdc::binlog_options::BinlogOptions;
use mysql_cdc::events::binlog_event::BinlogEvent;
use mysql_cdc::events::event_header::EventHeader;
use mysql_cdc::events::row_events::row_data::{RowData, UpdateRowData};
use mysql_cdc::replica_options::ReplicaOptions;
use mysql_cdc::ssl_mode::SslMode;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::column_serde::{build_column_infos, extract_column_values};
use crate::table_map::ColumnInfo;
use crate::EventConverter;
use canal_common::ColumnValue;

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
    original_password: Option<String>,
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
            original_password: Some(password.to_string()),
            server_id,
            // mysql_cdc 0.2.1 (the latest release) hard-panics with
            // `unimplemented!("Ssl encryption is not supported in this version")`
            // for ANY mode other than `Disabled`, so `Disabled` is not a
            // preference here — it is the only value that can connect at all.
            // See the unencrypted-connection warning in `run_replication`.
            ssl_mode: SslMode::Disabled,
            connect_timeout_secs: 30,
            sender: None,
            current_pos: None,
            running: AtomicBool::new(false),
            cancel_token: None,
            connected: AtomicBool::new(false),
        })
    }

    /// Set the SSL mode for the MySQL connection.
    ///
    /// NOTE: `mysql_cdc 0.2.1` does not implement TLS — constructing its client
    /// with any mode other than [`SslMode::Disabled`] panics. Overriding this is
    /// therefore only useful as a placeholder for a future dependency.
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
            // mysql_cdc uses u32 for binlog position (protocol limit)
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
        start_journal: &str,
    ) {
        // mysql_cdc has no TLS implementation, so this connection carries the
        // MySQL credentials and every binlog event in plaintext. Say so loudly
        // once per connection rather than letting it pass unremarked.
        warn!(
            "MySQL binlog connection to {}:{} is UNENCRYPTED — mysql_cdc 0.2.1 does not \
             implement TLS. Run it over a private network or an SSH tunnel.",
            options.hostname, options.port
        );

        let mut client = BinlogClient::new(options);
        let mut converter = EventConverter::new();
        let mut current_binlog_file = start_journal.to_string();
        // Livelock guard: skip permanently-undecodable events after N attempts
        let mut consecutive_errors: u32 = 0;
        let mut last_error_pos: Option<(String, u64)> = None;
        const MAX_CONSECUTIVE_ERRORS: u32 = 3;

        let mut events = match client.replicate() {
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
        // Position of the last event we decoded, for panic diagnostics.
        let mut last_pos: u64 = 0;

        // Driven by hand rather than `for result in events`: a `for` loop calls
        // `next()` inside its desugaring, where a panic from the decoder escapes
        // and kills this thread (closing the event channel and silently stopping
        // the CDC server). `next_or_panic` turns that into a value we can report.
        loop {
            let result = match next_or_panic(&mut events) {
                NextOutcome::Item(r) => r,
                NextOutcome::Done => break,
                NextOutcome::Panicked(msg) => {
                    error!(
                        "mysql_cdc panicked while decoding the binlog at {}:{} (last good position) — \
                         the upstream event is malformed or the decoder is buggy; stopping replication: {}",
                        current_binlog_file, last_pos, msg
                    );
                    let panic_err = CanalError::Protocol(format!(
                        "binlog decoder panicked near {}:{}: {}",
                        current_binlog_file, last_pos, msg
                    ));
                    if tx.blocking_send(Err(panic_err)).is_err() {
                        error!("Channel closed during panic error delivery");
                    }
                    break;
                }
            };

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
            last_pos = header.next_event_position as u64;

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
                Ok(()) => {
                    consecutive_errors = 0;
                    last_error_pos = None;
                    client.commit(&header, &event)
                }
                Err(e) => {
                    let current_pos = (
                        current_binlog_file.clone(),
                        header.next_event_position as u64,
                    );
                    if last_error_pos.as_ref() == Some(&current_pos) {
                        consecutive_errors += 1;
                    } else {
                        consecutive_errors = 1;
                        last_error_pos = Some(current_pos);
                    }
                    if tx.blocking_send(Err(e)).is_err() {
                        error!("Channel closed during error delivery, stopping replication");
                        break;
                    }
                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                        error!(
                            "Skipping permanently-undecodable event at {}:{} after {} attempts",
                            current_binlog_file, header.next_event_position, consecutive_errors,
                        );
                        consecutive_errors = 0;
                        last_error_pos = None;
                    }
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
                            Some(sql.chars().take(DDL_SQL_MAX_LEN).collect())
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

/// Result of pulling one item out of the binlog event stream.
enum NextOutcome<T> {
    /// The iterator yielded an item.
    Item(T),
    /// The iterator is exhausted — the stream ended normally.
    Done,
    /// The iterator panicked; carries the panic message.
    Panicked(String),
}

/// Advance `it` by one item, catching a panic instead of letting it escape.
///
/// `mysql_cdc 0.2.1` panics on malformed replication data — `row_parser.rs:109`
/// indexes `columns_present` with the *table map's* column count, so a row event
/// declaring fewer columns panics with an index-out-of-bounds. In `spawn_blocking`
/// that panic would kill the replication thread and close the event channel,
/// which the caller reads as a clean "stream ended" and shuts the server down.
/// Catching it here turns the crash into a value the caller can report.
///
/// The iterator is not used again after a caught panic (the caller must stop
/// driving it), so its possibly-inconsistent state is never observed.
fn next_or_panic<T>(it: &mut impl Iterator<Item = T>) -> NextOutcome<T> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| it.next())) {
        Ok(Some(item)) => NextOutcome::Item(item),
        Ok(None) => NextOutcome::Done,
        Err(payload) => NextOutcome::Panicked(
            payload
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "<non-string panic payload>".to_string()),
        ),
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
        let cancel_for_spawn = cancel.clone();
        let cancel_for_timeout = cancel.clone();
        self.cancel_token = Some(cancel);
        let timeout_secs = self.connect_timeout_secs;

        info!(
            "Connecting to MySQL {}:{} at {}:{} (timeout {}s)",
            self.host, self.port, pos.journal_name, pos.position, timeout_secs
        );

        self.current_pos = Some(pos.clone());
        self.running.store(true, Ordering::Release);

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();

        let journal_name = pos.journal_name.clone();
        let replication_handle = tokio::task::spawn_blocking(move || {
            Self::run_replication(options, tx, cancel_for_spawn, started_tx, &journal_name);
        });
        // Nothing else joins this task, so bind the handle and watch it: a
        // replication thread that dies (panic outside the guarded pull, or a
        // cancelled blocking read) must be loud, not a silently closed channel.
        tokio::spawn(async move {
            match replication_handle.await {
                Ok(()) => debug!("Binlog replication task finished"),
                Err(e) => error!("Binlog replication task ended abnormally: {}", e),
            }
        });

        let started_result =
            match tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), started_rx)
                .await
            {
                Ok(r) => r,
                Err(_) => {
                    cancel_for_timeout.cancel();
                    self.running.store(false, Ordering::Release);
                    return Err(CanalError::BinlogConnection(format!(
                        "connection timed out after {}s",
                        timeout_secs
                    )));
                }
            };

        match started_result {
            Ok(()) => self.connected.store(true, Ordering::Release),
            Err(_) => {
                self.running.store(false, Ordering::Release);
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
        // Restore password for reconnection
        if self.password.is_empty() {
            if let Some(ref orig) = self.original_password {
                self.password = orig.clone();
            }
        }
        info!("Disconnected from MySQL");
        Ok(())
    }

    fn current_position(&self) -> Option<LogPosition> {
        self.current_pos.clone()
    }
}

impl Drop for DefaultBinlogConnector {
    fn drop(&mut self) {
        if let Some(token) = self.cancel_token.take() {
            token.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Stand-in for `mysql_cdc`'s event iterator: its `next()` panics with an
    /// index-out-of-bounds, exactly like `row_parser.rs:109` does on a malformed
    /// row event. The real panic cannot be triggered from here — it lives inside
    /// the dependency — so this exercises the containment mechanism instead.
    struct PanicOnNext;

    impl Iterator for PanicOnNext {
        type Item = u8;

        fn next(&mut self) -> Option<u8> {
            panic!("index out of bounds: the len is 3 but the index is 5");
        }
    }

    // The caught panic still prints its message to stderr via the default panic
    // hook — expected noise, the hook is global and must not be swapped here.
    #[test]
    fn next_or_panic_converts_panic_into_outcome() {
        let mut it = PanicOnNext;
        match next_or_panic(&mut it) {
            NextOutcome::Panicked(msg) => {
                assert!(msg.contains("index out of bounds"), "message lost: {msg}")
            }
            _ => panic!("panic escaped containment"),
        }
    }

    #[test]
    fn next_or_panic_passes_items_and_end_through_unchanged() {
        let mut it = [1u8, 2, 3].into_iter();
        for expected in [1u8, 2, 3] {
            match next_or_panic(&mut it) {
                NextOutcome::Item(got) => assert_eq!(got, expected),
                _ => panic!("expected item {expected}"),
            }
        }
        assert!(
            matches!(next_or_panic(&mut it), NextOutcome::Done),
            "expected Done after the last item"
        );
    }

    #[test]
    fn default_ssl_mode_is_disabled() {
        // Regression: the default used to be `SslMode::Require`, but mysql_cdc
        // panics with `unimplemented!` for any non-Disabled mode — so every
        // connection attempt died before reaching MySQL. `Disabled` is the only
        // value that can connect; this pins that so it cannot silently regress.
        let connector = DefaultBinlogConnector::new("localhost", 3306, "u", "p", 1).unwrap();
        assert_eq!(
            connector.ssl_mode,
            SslMode::Disabled,
            "the only mode mysql_cdc can connect with is Disabled"
        );
    }
}
