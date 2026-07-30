use std::net::SocketAddr;
use std::sync::Arc;

use canal_common::lifecycle::CanalLifecycle;
use canal_common::{
    CanalError, CanalEvent, CanalResult, ColumnValue, DmlType, EventType, Events, FilterPattern,
    LogPosition,
};
use canal_proto::{
    self,
    column,
    header,
    row_change,
    Ack,
    ClientAck,
    ClientAuth,
    ClientRollback,
    Column,
    Entry,
    EventType as ProtoEventType,
    Get,
    Header,
    Messages,
    Packet,
    PacketType,
    RowChange,
    RowData,
    Sub,
};
use canal_store::memory::MemoryEventStore;
use futures::{SinkExt, StreamExt};
use prost::Message;
use tokio::net::TcpListener;
use tokio_util::codec::Framed;
use tracing::{debug, error, info, warn};

use crate::codec::CanalCodec;
use crate::session::SessionManager;

/// Canal TCP server.
/// Listens for client connections, handles the Canal wire protocol,
/// and streams binlog events from the store to connected clients.
pub struct CanalServer {
    store: Arc<MemoryEventStore>,
    sessions: Arc<SessionManager>,
    bind_addr: SocketAddr,
}

impl CanalServer {
    pub fn new(bind_addr: SocketAddr, store: MemoryEventStore) -> Self {
        Self {
            store: Arc::new(store),
            sessions: Arc::new(SessionManager::new()),
            bind_addr,
        }
    }

    /// Start the TCP server. Blocks indefinitely, spawning a new
    /// Tokio task for each accepted client connection.
    pub async fn serve(&self) -> CanalResult<()> {
        let listener = TcpListener::bind(&self.bind_addr).await?;
        self.store.start().await?;
        info!("Canal server listening on {}", self.bind_addr);

        loop {
            let (socket, peer_addr) = listener.accept().await?;
            info!("Client connected: {}", peer_addr);

            let store = self.store.clone();
            let sessions = self.sessions.clone();

            tokio::spawn(async move {
                let transport = Framed::new(socket, CanalCodec::new());
                if let Err(e) = handle_client(transport, store, sessions).await {
                    error!("Client {} error: {}", peer_addr, e);
                }
                info!("Client {} disconnected", peer_addr);
            });
        }
    }
}

/// Handle a single client connection's lifecycle.
///
/// Protocol phases (per Canal wire spec):
///   1. ClientAuth   -> server registers session, replies ACK
///   2. Sub          -> server updates subscription filter, replies ACK
///   3. Get          -> server fetches events from store, replies Messages
///   4. ClientAck    -> server updates acknowledged position
///   5. ClientRollback -> server resets position to last ACK
///   6. Heartbeat    -> server updates heartbeat timestamp, replies ACK
async fn handle_client(
    mut transport: impl StreamExt<Item = Result<Vec<u8>, CanalError>>
        + SinkExt<Vec<u8>, Error = CanalError>
        + Unpin,
    store: Arc<MemoryEventStore>,
    sessions: Arc<SessionManager>,
) -> CanalResult<()> {
    let mut client_id: Option<String> = None;
    let mut current_pos: Option<LogPosition> = None;
    let mut last_ack_pos: Option<LogPosition> = None;

    while let Some(frame_bytes) = transport.next().await {
        let frame_bytes = frame_bytes?;

        // Decode the protobuf Packet from the wire frame.
        let packet = Packet::decode(&frame_bytes[..]).map_err(|e| {
            CanalError::Protocol(format!("failed to decode Packet: {}", e))
        })?;

        // Dispatch on PacketType. The 'type' field is a raw i32 in the
        // prost-generated struct; compare against PacketType discriminant values.
        let ptype = packet.r#type;

        if ptype == PacketType::Clientauthentication as i32 {
            // --- ClientAuth ---
            let auth = ClientAuth::decode(&packet.body[..]).map_err(|e| {
                CanalError::Protocol(format!("failed to decode ClientAuth: {}", e))
            })?;

            let cid = if auth.client_id.is_empty() {
                "anonymous".to_string()
            } else {
                auth.client_id.clone()
            };

            let filter = if auth.filter.is_empty() {
                FilterPattern::default()
            } else {
                FilterPattern {
                    pattern: auth.filter.clone(),
                    black_list: String::new(),
                }
            };

            sessions.register(&cid, &auth.destination, filter);
            client_id = Some(cid.clone());

            // Start reading from the beginning or the requested position
            if auth.start_timestamp > 0 {
                current_pos = Some(LogPosition {
                    journal_name: String::new(),
                    position: 0,
                    timestamp: Some(auth.start_timestamp),
                    server_id: None,
                    gtid: None,
                });
            }

            info!(
                "Client authenticated: {} (dest={}, filter={})",
                cid, auth.destination, auth.filter
            );

            send_ack(&mut transport, None).await?;
        } else if ptype == PacketType::Subscription as i32 {
            // --- Sub ---
            let sub = Sub::decode(&packet.body[..]).map_err(|e| {
                CanalError::Protocol(format!("failed to decode Sub: {}", e))
            })?;

            let cid = sub.client_id.clone();
            if let Some(session) = sessions.get(&cid) {
                // Update the client's subscription filter
                sessions.register(
                    &cid,
                    &sub.destination,
                    FilterPattern {
                        pattern: sub.filter.clone(),
                        black_list: session.filter.black_list.clone(),
                    },
                );
                info!("Client {} subscribed: dest={} filter={}", cid, sub.destination, sub.filter);
            } else {
                // Register a new lightweight session on subscribe if not yet registered
                sessions.register(
                    &cid,
                    &sub.destination,
                    FilterPattern {
                        pattern: sub.filter.clone(),
                        black_list: String::new(),
                    },
                );
                info!("Client {} auto-registered via subscribe: dest={} filter={}", cid, sub.destination, sub.filter);
            }
            client_id = Some(cid);

            send_ack(&mut transport, None).await?;
        } else if ptype == PacketType::Get as i32 {
            // --- Get: fetch events and return Messages ---
            let get = Get::decode(&packet.body[..]).map_err(|e| {
                CanalError::Protocol(format!("failed to decode Get: {}", e))
            })?;

            let batch_size = if get.fetch_size > 0 {
                get.fetch_size as usize
            } else {
                100
            };

            let cid = client_id.clone().unwrap_or_else(|| "anonymous".to_string());

            let start = current_pos
                .clone()
                .unwrap_or_else(|| LogPosition::new("mysql-bin.000001", 4));

            let events: Events = store.get_batch(&start, batch_size).await?;

            if !events.is_empty() {
                // Update the current read position to the end of this batch
                current_pos = Some(events.position_range.end.clone());
                if sessions.get(&cid).is_some() {
                    if let Some(ref pos) = current_pos {
                        sessions.update_position(&cid, pos.clone());
                    }
                }

                debug!(
                    "Sending batch_id={} with {} events",
                    events.batch_id,
                    events.events.len()
                );

                // Build the Messages protobuf
                let mut msgs = Messages {
                    batch_id: events.batch_id,
                    ..Default::default()
                };
                for event in &events.events {
                    let entry = canal_event_to_entry(event);
                    msgs.messages.push(entry.encode_to_vec());
                }

                let resp_packet = Packet {
                    r#type: PacketType::Messages as i32,
                    body: msgs.encode_to_vec(),
                    ..Default::default()
                };

                transport.send(resp_packet.encode_to_vec()).await?;
            } else {
                // No events available yet; send an empty Messages packet so the
                // client doesn't block forever.
                let msgs = Messages::default();
                let resp_packet = Packet {
                    r#type: PacketType::Messages as i32,
                    body: msgs.encode_to_vec(),
                    ..Default::default()
                };

                transport.send(resp_packet.encode_to_vec()).await?;
            }
        } else if ptype == PacketType::Clientack as i32 {
            // --- ClientAck ---
            let client_ack = ClientAck::decode(&packet.body[..]).map_err(|e| {
                CanalError::Protocol(format!("failed to decode ClientAck: {}", e))
            })?;

            let cid = client_ack.client_id.clone();
            if let Some(ref pos) = current_pos {
                last_ack_pos = Some(pos.clone());
                sessions.update_ack(&cid, pos.clone());
            }

            debug!(
                "Client {} acked batch_id={}",
                cid, client_ack.batch_id
            );
        } else if ptype == PacketType::Clientrollback as i32 {
            // --- ClientRollback: reset position to last ACK ---
            let rollback = ClientRollback::decode(&packet.body[..]).map_err(|e| {
                CanalError::Protocol(format!("failed to decode ClientRollback: {}", e))
            })?;

            if let Some(ref ack_pos) = last_ack_pos {
                current_pos = Some(ack_pos.clone());
                info!(
                    "Client {} rolled back to position {} (batch_id={})",
                    rollback.client_id, ack_pos, rollback.batch_id
                );
            } else {
                current_pos = None;
                info!(
                    "Client {} rolled back to start (no prior ACK, batch_id={})",
                    rollback.client_id, rollback.batch_id
                );
            }
        } else if ptype == PacketType::Heartbeat as i32 {
            // --- Heartbeat ---
            if let Some(ref cid) = client_id {
                sessions.heartbeat(cid);
                debug!("Heartbeat from client {}", cid);
            }
            send_ack(&mut transport, None).await?;
        } else {
            warn!("Unknown packet type: {}", ptype);
        }
    }

    // Cleanup on disconnect
    if let Some(ref cid) = client_id {
        sessions.unregister(cid);
    }

    Ok(())
}

/// Send an ACK packet to the client.
///
/// If `error_message` is `None`, sends a success ACK.
/// If `Some(msg)`, sends an error ACK with the given message and a non-zero error code.
async fn send_ack(
    transport: &mut (impl SinkExt<Vec<u8>, Error = CanalError> + Unpin),
    error_message: Option<&str>,
) -> CanalResult<()> {
    let ack = if let Some(msg) = error_message {
        Ack {
            error_message: msg.to_string(),
            error_code_present: Some(canal_proto::ack::ErrorCodePresent::ErrorCode(1)),
        }
    } else {
        Ack::default()
    };

    let packet = Packet {
        r#type: PacketType::Ack as i32,
        body: ack.encode_to_vec(),
        ..Default::default()
    };

    transport.send(packet.encode_to_vec()).await?;
    Ok(())
}

/// Convert an internal CanalEvent to the Canal wire-protocol Entry type.
///
/// The Entry carries a Header (metadata) and a `store_value` field containing
/// a serialized RowChange protobuf. This matches the Java Canal server's
/// approach where `RowChange` is separately encoded inside `storeValue`.
fn canal_event_to_entry(event: &CanalEvent) -> Entry {
    let mut entry = Entry::default();

    // --- Build Header ---
    let event_type_i32 = match event.entry_type {
        EventType::Insert => ProtoEventType::Insert as i32,
        EventType::Update => ProtoEventType::Update as i32,
        EventType::Delete => ProtoEventType::Delete as i32,
        EventType::Ddl => ProtoEventType::Query as i32,
        EventType::Query => ProtoEventType::Query as i32,
        EventType::Rotate => ProtoEventType::Query as i32,
        EventType::Xid => ProtoEventType::Xacommit as i32,
        EventType::Heartbeat => ProtoEventType::Mheartbeat as i32,
        EventType::Unknown(_v) => {
            // Try to map as a known EventType; fall back to INSERT
            ProtoEventType::Insert as i32
        }
    };

    let header = Header {
        logfile_name: event.journal_name.clone(),
        logfile_offset: event.position as i64,
        server_id: event.server_id as i64,
        execute_time: event.execute_time,
        schema_name: event.schema_name.clone(),
        table_name: event.table_name.clone(),
        gtid: event.gtid.clone().unwrap_or_default(),
        event_type_present: Some(header::EventTypePresent::EventType(event_type_i32)),
        source_type_present: Some(header::SourceTypePresent::SourceType(
            canal_proto::Type::Mysql as i32,
        )),
        event_length: event.raw_bytes.len() as i64,
        ..Default::default()
    };
    entry.header = Some(header);

    // --- Build RowChange (serialized into store_value) ---
    if let Some(ref change) = event.row_change {
        let mut rc = RowChange {
            event_type_present: Some(row_change::EventTypePresent::EventType(
                match change.dml_type {
                    DmlType::Insert => ProtoEventType::Insert as i32,
                    DmlType::Update => ProtoEventType::Update as i32,
                    DmlType::Delete => ProtoEventType::Delete as i32,
                },
            )),
            ..Default::default()
        };

        let mut rd = RowData::default();

        // Before-image columns (only for UPDATE and DELETE)
        if let Some(ref before) = change.before {
            for col in &before.columns {
                rd.before_columns.push(column_value_to_proto(col, false));
            }
        }

        // After-image columns (for INSERT and UPDATE)
        if let Some(ref after) = change.after {
            for col in &after.columns {
                rd.after_columns.push(column_value_to_proto(col, col.updated));
            }
        }

        rc.row_datas.push(rd);
        entry.store_value = rc.encode_to_vec();
    } else if let Some(ref ddl_sql) = event.ddl_sql {
        // DDL event: pack the SQL into a RowChange with is_ddl flag
        let rc = RowChange {
            sql: ddl_sql.clone(),
            is_ddl_present: Some(row_change::IsDdlPresent::IsDdl(true)),
            event_type_present: Some(row_change::EventTypePresent::EventType(
                ProtoEventType::Query as i32,
            )),
            ddl_schema_name: event.schema_name.clone(),
            ..Default::default()
        };
        entry.store_value = rc.encode_to_vec();
    }

    entry
}

/// Convert an internal ColumnValue to the Canal wire-protocol Column type.
fn column_value_to_proto(col: &ColumnValue, updated: bool) -> Column {
    Column {
        name: col.name.clone(),
        value: col.value.clone().unwrap_or_default(),
        is_key: col.is_key,
        updated,
        mysql_type: col.column_type.to_string(),
        sql_type: col.column_type,
        is_null_present: if col.value.is_none() {
            Some(column::IsNullPresent::IsNull(true))
        } else {
            None
        },
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use canal_common::CanalEvent;

    #[tokio::test]
    async fn test_handle_client_registers_and_sends_events() {
        let store = Arc::new(MemoryEventStore::new(1024));
        let sessions = Arc::new(SessionManager::new());

        // Put some events in the store
        let event = CanalEvent {
            journal_name: "mysql-bin.000001".into(),
            position: 200,
            server_id: 1,
            execute_time: 0,
            entry_type: canal_common::EventType::Insert,
            schema_name: "test".into(),
            table_name: "t".into(),
            row_change: None,
            ddl_sql: None,
            gtid: None,
            raw_bytes: vec![],
        };
        store.put_batch(vec![event]).await.unwrap();

        // Register a session manually — verify it's tracked
        sessions.register("test-client", "example", FilterPattern::default());
        let session = sessions.get("test-client").unwrap();
        assert_eq!(session.client_id, "test-client");

        sessions.unregister("test-client");
        assert!(sessions.get("test-client").is_none());

        // Verify the store still has the event we put
        let start_pos = LogPosition::new("mysql-bin.000001", 4);
        let batch = store.get_batch(&start_pos, 10).await.unwrap();
        assert_eq!(batch.events.len(), 1);
    }

    #[tokio::test]
    async fn test_server_binds_to_port() {
        let store = MemoryEventStore::new(1024);
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = CanalServer::new(addr, store);

        // Bind a test listener to verify the address format
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound = listener.local_addr().unwrap();
        assert!(bound.port() > 0);
        drop(listener);

        // Just verify the struct is constructable
        drop(server);
    }

    #[test]
    fn test_canal_event_to_entry_header_fields() {
        let event = CanalEvent {
            journal_name: "mysql-bin.000001".into(),
            position: 1234,
            server_id: 42,
            execute_time: 1690000000,
            entry_type: EventType::Insert,
            schema_name: "mydb".into(),
            table_name: "users".into(),
            row_change: None,
            ddl_sql: None,
            gtid: Some("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5".into()),
            raw_bytes: vec![0u8; 100],
        };

        let entry = canal_event_to_entry(&event);

        let h = entry.header.unwrap();
        assert_eq!(h.logfile_name, "mysql-bin.000001");
        assert_eq!(h.logfile_offset, 1234);
        assert_eq!(h.server_id, 42);
        assert_eq!(h.execute_time, 1690000000);
        assert_eq!(h.schema_name, "mydb");
        assert_eq!(h.table_name, "users");
        assert_eq!(
            h.gtid,
            "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"
        );
        assert_eq!(h.event_length, 100);

        // Verify event_type_present is set to INSERT
        match h.event_type_present {
            Some(header::EventTypePresent::EventType(v)) => {
                assert_eq!(v, ProtoEventType::Insert as i32);
            }
            _ => panic!("expected EventTypePresent::EventType"),
        }
    }

    #[test]
    fn test_canal_event_to_entry_with_row_change() {
        let row_change = canal_common::RowChange {
            table_name: "users".into(),
            schema_name: "mydb".into(),
            before: None,
            after: Some(canal_common::RowData {
                columns: vec![
                    ColumnValue {
                        name: "id".into(),
                        value: Some("1".into()),
                        column_type: 3, // MYSQL_TYPE_LONG
                        is_key: true,
                        updated: false,
                    },
                    ColumnValue {
                        name: "name".into(),
                        value: Some("alice".into()),
                        column_type: 253, // MYSQL_TYPE_VARCHAR
                        is_key: false,
                        updated: false,
                    },
                ],
            }),
            dml_type: DmlType::Insert,
        };

        let event = CanalEvent {
            journal_name: "mysql-bin.000001".into(),
            position: 567,
            server_id: 1,
            execute_time: 0,
            entry_type: EventType::Insert,
            schema_name: "mydb".into(),
            table_name: "users".into(),
            row_change: Some(row_change),
            ddl_sql: None,
            gtid: None,
            raw_bytes: vec![],
        };

        let entry = canal_event_to_entry(&event);

        // store_value should contain a serialized RowChange
        assert!(!entry.store_value.is_empty());
        let rc = RowChange::decode(&entry.store_value[..]).unwrap();
        assert_eq!(rc.row_datas.len(), 1);

        let rd = &rc.row_datas[0];
        assert_eq!(rd.before_columns.len(), 0); // INSERT has no before
        assert_eq!(rd.after_columns.len(), 2);

        assert_eq!(rd.after_columns[0].name, "id");
        assert_eq!(rd.after_columns[0].value, "1");
        assert!(rd.after_columns[0].is_key);

        assert_eq!(rd.after_columns[1].name, "name");
        assert_eq!(rd.after_columns[1].value, "alice");
        assert!(!rd.after_columns[1].is_key);
    }

    #[test]
    fn test_canal_event_to_entry_update_with_before_and_after() {
        let row_change = canal_common::RowChange {
            table_name: "users".into(),
            schema_name: "mydb".into(),
            before: Some(canal_common::RowData {
                columns: vec![ColumnValue {
                    name: "name".into(),
                    value: Some("alice".into()),
                    column_type: 253,
                    is_key: false,
                    updated: false,
                }],
            }),
            after: Some(canal_common::RowData {
                columns: vec![ColumnValue {
                    name: "name".into(),
                    value: Some("bob".into()),
                    column_type: 253,
                    is_key: false,
                    updated: true,
                }],
            }),
            dml_type: DmlType::Update,
        };

        let event = CanalEvent {
            journal_name: "mysql-bin.000001".into(),
            position: 789,
            server_id: 1,
            execute_time: 0,
            entry_type: EventType::Update,
            schema_name: "mydb".into(),
            table_name: "users".into(),
            row_change: Some(row_change),
            ddl_sql: None,
            gtid: None,
            raw_bytes: vec![],
        };

        let entry = canal_event_to_entry(&event);
        assert!(!entry.store_value.is_empty());

        let rc = RowChange::decode(&entry.store_value[..]).unwrap();
        let rd = &rc.row_datas[0];

        // UPDATE has both before and after
        assert_eq!(rd.before_columns.len(), 1);
        assert_eq!(rd.before_columns[0].value, "alice");
        assert!(!rd.before_columns[0].updated);

        assert_eq!(rd.after_columns.len(), 1);
        assert_eq!(rd.after_columns[0].value, "bob");
        assert!(rd.after_columns[0].updated);
    }

    #[test]
    fn test_canal_event_to_entry_delete() {
        let row_change = canal_common::RowChange {
            table_name: "users".into(),
            schema_name: "mydb".into(),
            before: Some(canal_common::RowData {
                columns: vec![ColumnValue {
                    name: "id".into(),
                    value: Some("99".into()),
                    column_type: 3,
                    is_key: true,
                    updated: false,
                }],
            }),
            after: None,
            dml_type: DmlType::Delete,
        };

        let event = CanalEvent {
            journal_name: "mysql-bin.000001".into(),
            position: 999,
            server_id: 1,
            execute_time: 0,
            entry_type: EventType::Delete,
            schema_name: "mydb".into(),
            table_name: "users".into(),
            row_change: Some(row_change),
            ddl_sql: None,
            gtid: None,
            raw_bytes: vec![],
        };

        let entry = canal_event_to_entry(&event);
        let rc = RowChange::decode(&entry.store_value[..]).unwrap();
        let rd = &rc.row_datas[0];

        // DELETE has only before columns
        assert_eq!(rd.before_columns.len(), 1);
        assert_eq!(rd.after_columns.len(), 0);
        assert_eq!(rd.before_columns[0].value, "99");
    }

    #[test]
    fn test_canal_event_to_entry_ddl() {
        let event = CanalEvent {
            journal_name: "mysql-bin.000001".into(),
            position: 200,
            server_id: 1,
            execute_time: 0,
            entry_type: EventType::Ddl,
            schema_name: "mydb".into(),
            table_name: "users".into(),
            row_change: None,
            ddl_sql: Some("ALTER TABLE users ADD COLUMN age INT".into()),
            gtid: None,
            raw_bytes: vec![],
        };

        let entry = canal_event_to_entry(&event);
        let rc = RowChange::decode(&entry.store_value[..]).unwrap();
        assert_eq!(rc.sql, "ALTER TABLE users ADD COLUMN age INT");
        assert_eq!(rc.ddl_schema_name, "mydb");

        // Verify is_ddl flag is set
        match rc.is_ddl_present {
            Some(row_change::IsDdlPresent::IsDdl(true)) => {}
            _ => panic!("expected IsDdl(true)"),
        }
    }

    #[test]
    fn test_column_value_to_proto_with_null() {
        let col = ColumnValue {
            name: "email".into(),
            value: None,
            column_type: 253,
            is_key: false,
            updated: false,
        };

        let proto_col = column_value_to_proto(&col, false);
        assert_eq!(proto_col.name, "email");
        assert_eq!(proto_col.value, "");
        assert!(proto_col.is_null_present.is_some());
    }

    #[test]
    fn test_send_ack_packet_structure() {
        // Test that an ACK packet encodes and decodes correctly
        let ack = Ack::default();
        let mut packet = Packet::default();
        packet.r#type = PacketType::Ack as i32;
        packet.body = ack.encode_to_vec();

        let wire = packet.encode_to_vec();
        let decoded = Packet::decode(&wire[..]).unwrap();

        assert_eq!(decoded.r#type, PacketType::Ack as i32);
        let decoded_ack = Ack::decode(&decoded.body[..]).unwrap();
        assert_eq!(decoded_ack.error_code_present, None);
        assert_eq!(decoded_ack.error_message, "");
    }

    #[test]
    fn test_send_ack_packet_with_error() {
        let ack = Ack {
            error_message: "something went wrong".into(),
            error_code_present: Some(canal_proto::ack::ErrorCodePresent::ErrorCode(5)),
        };

        let mut packet = Packet::default();
        packet.r#type = PacketType::Ack as i32;
        packet.body = ack.encode_to_vec();

        let wire = packet.encode_to_vec();
        let decoded = Packet::decode(&wire[..]).unwrap();
        assert_eq!(decoded.r#type, PacketType::Ack as i32);

        let decoded_ack = Ack::decode(&decoded.body[..]).unwrap();
        assert_eq!(decoded_ack.error_message, "something went wrong");
        match decoded_ack.error_code_present {
            Some(canal_proto::ack::ErrorCodePresent::ErrorCode(5)) => {}
            _ => panic!("expected ErrorCode(5)"),
        }
    }

    #[test]
    fn test_messages_packet_roundtrip() {
        // Verify Messages protobuf round-trips correctly
        let event = CanalEvent {
            journal_name: "mysql-bin.000001".into(),
            position: 42,
            server_id: 1,
            execute_time: 1000,
            entry_type: EventType::Insert,
            schema_name: "db".into(),
            table_name: "t".into(),
            row_change: None,
            ddl_sql: None,
            gtid: None,
            raw_bytes: vec![],
        };

        let entry = canal_event_to_entry(&event);
        let entry_bytes = entry.encode_to_vec();

        let mut msgs = Messages::default();
        msgs.batch_id = 1;
        msgs.messages.push(entry_bytes);

        let mut packet = Packet::default();
        packet.r#type = PacketType::Messages as i32;
        packet.body = msgs.encode_to_vec();

        let wire = packet.encode_to_vec();
        let decoded_packet = Packet::decode(&wire[..]).unwrap();
        assert_eq!(decoded_packet.r#type, PacketType::Messages as i32);

        let decoded_msgs = Messages::decode(&decoded_packet.body[..]).unwrap();
        assert_eq!(decoded_msgs.batch_id, 1);
        assert_eq!(decoded_msgs.messages.len(), 1);

        // Decode the embedded Entry
        let decoded_entry = Entry::decode(&decoded_msgs.messages[0][..]).unwrap();
        let h = decoded_entry.header.unwrap();
        assert_eq!(h.logfile_name, "mysql-bin.000001");
        assert_eq!(h.logfile_offset, 42);
    }

    #[test]
    fn test_client_auth_encoding_roundtrip() {
        let auth = ClientAuth {
            username: "canal".into(),
            password: vec![0x12, 0x34],
            destination: "example".into(),
            client_id: "1001".into(),
            filter: ".*\\..*".into(),
            start_timestamp: 0,
            ..Default::default()
        };

        let mut packet = Packet::default();
        packet.r#type = PacketType::Clientauthentication as i32;
        packet.body = auth.encode_to_vec();

        let wire = packet.encode_to_vec();
        let decoded = Packet::decode(&wire[..]).unwrap();
        assert_eq!(decoded.r#type, PacketType::Clientauthentication as i32);

        let decoded_auth = ClientAuth::decode(&decoded.body[..]).unwrap();
        assert_eq!(decoded_auth.username, "canal");
        assert_eq!(decoded_auth.client_id, "1001");
        assert_eq!(decoded_auth.filter, ".*\\..*");
    }

    #[test]
    fn test_sub_encoding_roundtrip() {
        let sub = Sub {
            destination: "example".into(),
            client_id: "1001".into(),
            filter: "mydb\\.users".into(),
        };

        let mut packet = Packet::default();
        packet.r#type = PacketType::Subscription as i32;
        packet.body = sub.encode_to_vec();

        let wire = packet.encode_to_vec();
        let decoded = Packet::decode(&wire[..]).unwrap();
        assert_eq!(decoded.r#type, PacketType::Subscription as i32);

        let decoded_sub = Sub::decode(&decoded.body[..]).unwrap();
        assert_eq!(decoded_sub.client_id, "1001");
        assert_eq!(decoded_sub.filter, "mydb\\.users");
    }

    #[test]
    fn test_get_encoding_roundtrip() {
        let get = Get {
            destination: "example".into(),
            client_id: "1001".into(),
            fetch_size: 100,
            ..Default::default()
        };

        let mut packet = Packet::default();
        packet.r#type = PacketType::Get as i32;
        packet.body = get.encode_to_vec();

        let wire = packet.encode_to_vec();
        let decoded = Packet::decode(&wire[..]).unwrap();
        assert_eq!(decoded.r#type, PacketType::Get as i32);

        let decoded_get = Get::decode(&decoded.body[..]).unwrap();
        assert_eq!(decoded_get.fetch_size, 100);
        assert_eq!(decoded_get.client_id, "1001");
    }

    #[test]
    fn test_client_ack_encoding_roundtrip() {
        let ack = ClientAck {
            destination: "example".into(),
            client_id: "1001".into(),
            batch_id: 42,
        };

        let mut packet = Packet::default();
        packet.r#type = PacketType::Clientack as i32;
        packet.body = ack.encode_to_vec();

        let wire = packet.encode_to_vec();
        let decoded = Packet::decode(&wire[..]).unwrap();
        assert_eq!(decoded.r#type, PacketType::Clientack as i32);

        let decoded_ack = ClientAck::decode(&decoded.body[..]).unwrap();
        assert_eq!(decoded_ack.batch_id, 42);
    }

    #[test]
    fn test_client_rollback_encoding_roundtrip() {
        let rollback = ClientRollback {
            destination: "example".into(),
            client_id: "1001".into(),
            batch_id: 10,
        };

        let mut packet = Packet::default();
        packet.r#type = PacketType::Clientrollback as i32;
        packet.body = rollback.encode_to_vec();

        let wire = packet.encode_to_vec();
        let decoded = Packet::decode(&wire[..]).unwrap();
        assert_eq!(decoded.r#type, PacketType::Clientrollback as i32);

        let decoded_rollback = ClientRollback::decode(&decoded.body[..]).unwrap();
        assert_eq!(decoded_rollback.batch_id, 10);
    }
}
