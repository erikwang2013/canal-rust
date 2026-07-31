use std::net::SocketAddr;
use std::sync::Arc;

use canal_common::lifecycle::CanalLifecycle;
use canal_common::{
    CanalError, CanalEvent, CanalResult, ColumnValue, DmlType, EventType, Events, FilterPattern,
    LogPosition,
};
use canal_proto::{
    self, column, header, row_change, Ack, ClientAck, ClientAuth, ClientRollback, Column, Entry,
    EventType as ProtoEventType, Get, Header, Messages, Packet, PacketType, RowChange, RowData,
    Sub,
};
use canal_store::memory::MemoryEventStore;
use futures::{SinkExt, StreamExt};
use prost::Message;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::codec::CanalCodec;
use crate::session::SessionManager;

const MAX_CONNECTIONS: usize = 1024;

/// Canal TCP server.
pub struct CanalServer {
    store: Arc<MemoryEventStore>,
    sessions: Arc<SessionManager>,
    bind_addr: SocketAddr,
    shutdown_token: CancellationToken,
    client_tasks: Mutex<JoinSet<()>>,
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
                    break;
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

        // Await all client tasks to finish gracefully
        let mut tasks = self.client_tasks.lock().await;
        info!("Waiting for {} client tasks to complete...", tasks.len());
        while let Some(result) = tasks.join_next().await {
            if let Err(e) = result {
                error!("Client task panicked: {}", e);
            }
        }

        Ok(())
    }
}

async fn handle_client(
    mut transport: impl StreamExt<Item = Result<Vec<u8>, CanalError>>
        + SinkExt<Vec<u8>, Error = CanalError>
        + Unpin,
    store: Arc<MemoryEventStore>,
    sessions: Arc<SessionManager>,
    auth_token: Option<String>,
) -> CanalResult<()> {
    let mut state = ClientState::default();

    while let Some(frame_bytes) = transport.next().await {
        let frame_bytes = frame_bytes?;

        let packet = Packet::decode(&frame_bytes[..])
            .map_err(|e| CanalError::Protocol(format!("failed to decode Packet: {}", e)))?;

        let ptype = packet.r#type;

        if ptype == PacketType::Clientauthentication as i32 {
            handle_auth(&mut transport, &packet, &mut state, &auth_token, &sessions)
                .await?;
        } else if !state.authenticated {
            send_ack(&mut transport, Some("not authenticated")).await?;
            continue;
        } else if ptype == PacketType::Subscription as i32 {
            handle_sub(&mut transport, &packet, &mut state, &sessions).await?;
        } else if ptype == PacketType::Get as i32 {
            handle_get(&mut transport, &packet, &mut state, &store, &sessions).await?;
        } else if ptype == PacketType::Clientack as i32 {
            handle_client_ack(&packet, &mut state, &sessions);
        } else if ptype == PacketType::Clientrollback as i32 {
            handle_client_rollback(&packet, &mut state);
        } else if ptype == PacketType::Heartbeat as i32 {
            handle_heartbeat(&mut transport, &state, &sessions).await?;
        } else {
            warn!("Unknown packet type: {}", ptype);
        }
    }

    if let Some(ref cid) = state.client_id {
        sessions.unregister(cid);
    }

    Ok(())
}

#[derive(Default)]
struct ClientState {
    client_id: Option<String>,
    current_pos: Option<LogPosition>,
    last_ack_pos: Option<LogPosition>,
    authenticated: bool,
    auth_error_count: u32,
}

async fn handle_auth(
    transport: &mut (impl SinkExt<Vec<u8>, Error = CanalError> + Unpin),
    packet: &Packet,
    state: &mut ClientState,
    auth_token: &Option<String>,
    sessions: &SessionManager,
) -> CanalResult<()> {
    let auth = ClientAuth::decode(&packet.body[..])
        .map_err(|e| CanalError::Protocol(format!("failed to decode ClientAuth: {}", e)))?;

    if let Some(ref token) = auth_token {
        let pass = String::from_utf8_lossy(&auth.password);
        if pass.as_ref() != token.as_str() {
            state.auth_error_count += 1;
            warn!("Auth failed for client '{}': bad password", auth.client_id);
            send_ack(transport, Some("authentication failed")).await?;
            if state.auth_error_count >= 3 {
                info!("Too many auth failures, disconnecting");
                return Err(CanalError::AuthFailed("too many failures".into()));
            }
            return Ok(());
        }
    }
    state.authenticated = true;

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
    state.client_id = Some(cid.clone());

    if auth.start_timestamp > 0 {
        state.current_pos = Some(LogPosition {
            journal_name: String::new(),
            position: 0,
            timestamp: Some(auth.start_timestamp),
            server_id: None,
            gtid: None,
        });
    }

    info!("Client authenticated: {} (dest={})", cid, auth.destination);
    send_ack(transport, None).await?;
    Ok(())
}

async fn handle_sub(
    transport: &mut (impl SinkExt<Vec<u8>, Error = CanalError> + Unpin),
    packet: &Packet,
    state: &mut ClientState,
    sessions: &SessionManager,
) -> CanalResult<()> {
    let sub = Sub::decode(&packet.body[..])
        .map_err(|e| CanalError::Protocol(format!("failed to decode Sub: {}", e)))?;

    let cid = sub.client_id.clone();
    let black_list = sessions
        .get(&cid)
        .map(|s| s.filter.black_list.clone())
        .unwrap_or_default();

    sessions.register(
        &cid,
        &sub.destination,
        FilterPattern {
            pattern: sub.filter.clone(),
            black_list,
        },
    );
    state.client_id = Some(cid);
    send_ack(transport, None).await?;
    Ok(())
}

async fn handle_get(
    transport: &mut (impl SinkExt<Vec<u8>, Error = CanalError> + Unpin),
    packet: &Packet,
    state: &mut ClientState,
    store: &MemoryEventStore,
    sessions: &SessionManager,
) -> CanalResult<()> {
    let get = Get::decode(&packet.body[..])
        .map_err(|e| CanalError::Protocol(format!("failed to decode Get: {}", e)))?;

    let batch_size = if get.fetch_size > 0 {
        get.fetch_size as usize
    } else {
        100
    };

    let cid = state
        .client_id
        .clone()
        .unwrap_or_else(|| "anonymous".to_string());
    let start = state
        .current_pos
        .clone()
        .unwrap_or_else(|| LogPosition::new("mysql-bin.000001", 4));

    let events: Events = store.get_batch(&start, batch_size).await?;

    let mut msgs = Messages::default();
    if !events.is_empty() {
        state.current_pos = Some(events.position_range.end.clone());
        if sessions.get(&cid).is_some() {
            if let Some(ref pos) = state.current_pos {
                sessions.update_position(&cid, pos.clone());
            }
        }

        msgs = Messages {
            batch_id: events.batch_id,
            ..Default::default()
        };
        for event in &events.events {
            let entry = canal_event_to_entry(event);
            msgs.messages.push(entry.encode_to_vec());
        }
    }

    let resp_packet = Packet {
        r#type: PacketType::Messages as i32,
        body: msgs.encode_to_vec(),
        ..Default::default()
    };
    transport.send(resp_packet.encode_to_vec()).await?;
    Ok(())
}

fn handle_client_ack(packet: &Packet, state: &mut ClientState, sessions: &SessionManager) {
    if let Ok(client_ack) = ClientAck::decode(&packet.body[..]) {
        if let Some(ref pos) = state.current_pos {
            state.last_ack_pos = Some(pos.clone());
            sessions.update_ack(&client_ack.client_id, pos.clone());
        }
    }
}

fn handle_client_rollback(packet: &Packet, state: &mut ClientState) {
    if ClientRollback::decode(&packet.body[..]).is_ok() {
        if let Some(ref ack_pos) = state.last_ack_pos {
            state.current_pos = Some(ack_pos.clone());
        } else {
            state.current_pos = None;
        }
    }
}

async fn handle_heartbeat(
    transport: &mut (impl SinkExt<Vec<u8>, Error = CanalError> + Unpin),
    state: &ClientState,
    sessions: &SessionManager,
) -> CanalResult<()> {
    if let Some(ref cid) = state.client_id {
        sessions.heartbeat(cid);
    }
    send_ack(transport, None).await?;
    Ok(())
}

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
        EventType::Unknown(v) => {
            warn!("Unknown event type {} mapped to Insert", v);
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
        event_length: 0,
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
                rd.after_columns
                    .push(column_value_to_proto(col, col.updated));
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

    // Set event_length from store_value if present, otherwise from raw_bytes
    if let Some(ref mut hdr) = entry.header {
        hdr.event_length = if !entry.store_value.is_empty() {
            entry.store_value.len() as i64
        } else {
            event.raw_bytes.len() as i64
        };
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
