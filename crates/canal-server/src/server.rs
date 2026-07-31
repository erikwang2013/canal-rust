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
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tokio_util::codec::Framed;
use tracing::{debug, error, info, warn};

use crate::codec::CanalCodec;
use crate::session::SessionManager;

/// Maximum concurrent client connections.
const MAX_CONNECTIONS: usize = 1024;

/// Canal TCP server.
pub struct CanalServer {
    store: Arc<MemoryEventStore>,
    sessions: Arc<SessionManager>,
    bind_addr: SocketAddr,
    shutdown_token: CancellationToken,
    client_tasks: Mutex<JoinSet<()>>,
    /// Optional shared secret for client authentication.
    auth_token: Option<String>,
}

impl CanalServer {
    pub fn new(bind_addr: SocketAddr, store: Arc<MemoryEventStore>) -> Self {
        Self {
            store,
            sessions: Arc::new(SessionManager::new()),
            bind_addr,
            shutdown_token: CancellationToken::new(),
            client_tasks: Mutex::new(JoinSet::new()),
            auth_token: None,
        }
    }

    /// Require clients to present this token as their password.
    /// When set, clients must send `password` matching this value.
    pub fn with_auth(mut self, token: String) -> Self {
        self.auth_token = Some(token);
        self
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown_token.clone()
    }

    pub async fn serve(&self) -> CanalResult<()> {
        let listener = TcpListener::bind(&self.bind_addr).await?;
        self.store.start().await?;
        info!("Canal server listening on {}", self.bind_addr);

        let semaphore = Arc::new(Semaphore::new(MAX_CONNECTIONS));

        loop {
            let (socket, peer_addr) = tokio::select! {
                result = listener.accept() => result?,
                _ = self.shutdown_token.cancelled() => {
                    info!("Canal server shutting down gracefully");
                    return Ok(());
                }
            };

            let permit = match semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    warn!("Connection limit reached, rejecting {}", peer_addr);
                    continue;
                }
            };

            info!("Client connected: {}", peer_addr);

            let store = self.store.clone();
            let sessions = self.sessions.clone();
            let auth_token = self.auth_token.clone();

            self.client_tasks.lock().await.spawn(async move {
                let _permit = permit;
                let transport = Framed::new(socket, CanalCodec::new());
                if let Err(e) = handle_client(transport, store, sessions, auth_token).await {
                    error!("Client {} error: {}", peer_addr, e);
                }
                info!("Client {} disconnected", peer_addr);
            });
        }
    }
}

/// Handle a single client connection's lifecycle.
async fn handle_client(
    mut transport: impl StreamExt<Item = Result<Vec<u8>, CanalError>>
        + SinkExt<Vec<u8>, Error = CanalError>
        + Unpin,
    store: Arc<MemoryEventStore>,
    sessions: Arc<SessionManager>,
    auth_token: Option<String>,
) -> CanalResult<()> {
    let mut client_id: Option<String> = None;
    let mut current_pos: Option<LogPosition> = None;
    let mut last_ack_pos: Option<LogPosition> = None;
    let mut authenticated = auth_token.is_none();
    let mut auth_error_count: u32 = 0;

    while let Some(frame_bytes) = transport.next().await {
        let frame_bytes = frame_bytes?;

        let packet = Packet::decode(&frame_bytes[..]).map_err(|e| {
            CanalError::Protocol(format!("failed to decode Packet: {}", e))
        })?;

        let ptype = packet.r#type;

        if ptype == PacketType::Clientauthentication as i32 {
            let auth = ClientAuth::decode(&packet.body[..]).map_err(|e| {
                CanalError::Protocol(format!("failed to decode ClientAuth: {}", e))
            })?;

            // Verify credentials if an auth token is configured
            if let Some(ref token) = auth_token {
                let pass = String::from_utf8_lossy(&auth.password);
                if pass.as_ref() != token.as_str() {
                    auth_error_count += 1;
                    warn!(
                        "Auth failed for client '{}': bad password",
                        auth.client_id
                    );
                    send_ack(&mut transport, Some("authentication failed")).await?;
                    if auth_error_count >= 3 {
                        info!("Too many auth failures, disconnecting");
                        return Ok(());
                    }
                    continue;
                }
            }
            authenticated = true;

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
        } else {
            // All other packet types require authentication
            if !authenticated {
                send_ack(&mut transport, Some("not authenticated")).await?;
                continue;
            }

            if ptype == PacketType::Subscription as i32 {
                let sub = Sub::decode(&packet.body[..]).map_err(|e| {
                    CanalError::Protocol(format!("failed to decode Sub: {}", e))
                })?;

                let cid = sub.client_id.clone();
                if let Some(session) = sessions.get(&cid) {
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
                    let msgs = Messages::default();
                    let resp_packet = Packet {
                        r#type: PacketType::Messages as i32,
                        body: msgs.encode_to_vec(),
                        ..Default::default()
                    };

                    transport.send(resp_packet.encode_to_vec()).await?;
                }
            } else if ptype == PacketType::Clientack as i32 {
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
                if let Some(ref cid) = client_id {
                    sessions.heartbeat(cid);
                    debug!("Heartbeat from client {}", cid);
                }
                send_ack(&mut transport, None).await?;
            } else {
                warn!("Unknown packet type: {}", ptype);
            }
        }
    }

    if let Some(ref cid) = client_id {
        sessions.unregister(cid);
    }

    Ok(())
}

/// Send an ACK packet to the client.
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
fn canal_event_to_entry(event: &CanalEvent) -> Entry {
    let mut entry = Entry::default();

    let event_type_i32 = match event.entry_type {
        EventType::Insert => ProtoEventType::Insert as i32,
        EventType::Update => ProtoEventType::Update as i32,
        EventType::Delete => ProtoEventType::Delete as i32,
        EventType::Ddl => ProtoEventType::Query as i32,
        EventType::Query => ProtoEventType::Query as i32,
        EventType::Rotate => ProtoEventType::Query as i32,
        EventType::Xid => ProtoEventType::Xacommit as i32,
        EventType::Heartbeat => ProtoEventType::Mheartbeat as i32,
        EventType::Unknown(_v) => ProtoEventType::Insert as i32,
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

        if let Some(ref before) = change.before {
            for col in &before.columns {
                rd.before_columns.push(column_value_to_proto(col, false));
            }
        }

        if let Some(ref after) = change.after {
            for col in &after.columns {
                rd.after_columns.push(column_value_to_proto(col, col.updated));
            }
        }

        rc.row_datas.push(rd);
        entry.store_value = rc.encode_to_vec();
    } else if let Some(ref ddl_sql) = event.ddl_sql {
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
#[path = "tests.rs"]
mod tests;
