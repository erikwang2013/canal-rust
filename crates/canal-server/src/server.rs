use std::net::SocketAddr;
use std::sync::Arc;

use crate::conversion::canal_event_to_entry;
use canal_common::lifecycle::CanalLifecycle;
use canal_common::{CanalError, CanalResult, Events, FilterPattern, LogPosition};
use canal_proto::{
    Ack, ClientAck, ClientAuth, ClientRollback, Get, Messages, Packet, PacketType, Sub,
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

use canal_common::MutexLockExt;

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
    idle_timeout_secs: u64,
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
            idle_timeout_secs: 600,
        }
    }

    pub fn with_auth(mut self, token: String) -> Self {
        self.auth_token = Some(token);
        self
    }

    pub fn with_idle_timeout(mut self, secs: u64) -> Self {
        self.idle_timeout_secs = secs;
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
            let idle_timeout = self.idle_timeout_secs;

            self.client_tasks.lock().await.spawn(async move {
                let _permit = permit;
                let transport = Framed::new(socket, CanalCodec::new());
                if let Err(e) = handle_client(transport, store, sessions, auth_token, idle_timeout).await {
                    error!("Client {} error: {}", peer_addr, e);
                }
                info!("Client {} disconnected", peer_addr);
            });
        }

        // Await all client tasks to finish gracefully, with a timeout
        let mut tasks = self.client_tasks.lock().await;
        info!("Waiting for {} client tasks to complete...", tasks.len());

        let drain_fut = async {
            while let Some(result) = tasks.join_next().await {
                if let Err(e) = result {
                    error!("Client task panicked: {}", e);
                }
            }
        };

        match tokio::time::timeout(std::time::Duration::from_secs(30), drain_fut).await {
            Ok(()) => info!("All client tasks completed gracefully"),
            Err(_) => {
                warn!("Shutdown timeout reached — some client tasks may be forcibly aborted");
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
    idle_timeout_secs: u64,
) -> CanalResult<()> {
    let mut state = ClientState::default();

    loop {
        let frame_bytes = match tokio::time::timeout(
            std::time::Duration::from_secs(idle_timeout_secs),
            transport.next(),
        )
        .await
        {
            Ok(Some(bytes)) => bytes,
            Ok(None) => break, // stream ended
            Err(_) => {
                warn!(
                    "Client idle timeout after {}s, disconnecting",
                    idle_timeout_secs
                );
                break;
            }
        };
        let frame_bytes = frame_bytes?;

        // Skip zero-length keepalive frames
        if frame_bytes.is_empty() {
            continue;
        }

        let packet = Packet::decode(&frame_bytes[..])
            .map_err(|e| CanalError::Protocol(format!("failed to decode Packet: {}", e)))?;

        let ptype = packet.r#type;

        if ptype == PacketType::Clientauthentication as i32 {
            handle_auth(&mut transport, &packet, &mut state, &auth_token, &sessions).await?;
        } else if !state.authenticated {
            send_ack_err(&mut transport, "not authenticated").await?;
            continue;
        } else if ptype == PacketType::Subscription as i32 {
            state.unknown_packet_count = 0;
            handle_sub(&mut transport, &packet, &mut state, &sessions).await?;
        } else if ptype == PacketType::Get as i32 {
            state.unknown_packet_count = 0;
            handle_get(&mut transport, &packet, &mut state, &store, &sessions).await?;
        } else if ptype == PacketType::Clientack as i32 {
            state.unknown_packet_count = 0;
            handle_client_ack(&packet, &mut state, &sessions);
        } else if ptype == PacketType::Clientrollback as i32 {
            state.unknown_packet_count = 0;
            handle_client_rollback(&packet, &mut state);
        } else if ptype == PacketType::Heartbeat as i32 {
            state.unknown_packet_count = 0;
            handle_heartbeat(&mut transport, &state, &sessions).await?;
        } else {
            state.unknown_packet_count += 1;
            if state.unknown_packet_count >= 10 {
                warn!(
                    "Too many unknown packets ({}), disconnecting client",
                    state.unknown_packet_count
                );
                return Err(CanalError::Protocol("too many unknown packets".into()));
            }
            warn!(
                "Unknown packet type: {} (count={})",
                ptype, state.unknown_packet_count
            );
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
    last_get_batch_id: i64,
    last_get_end_pos: Option<LogPosition>,
    authenticated: bool,
    auth_error_count: u32,
    unknown_packet_count: u32,
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
        let pass_bytes = &auth.password;
        let token_bytes = token.as_bytes();
        // Constant-time comparison to prevent timing side-channel
        if pass_bytes.len() != token_bytes.len()
            || pass_bytes
                .iter()
                .zip(token_bytes.iter())
                .fold(0, |acc, (a, b)| acc | (a ^ b))
                != 0
        {
            state.auth_error_count += 1;
            warn!("Auth failed for client '{}': bad password", auth.client_id);
            send_ack_err(transport, "authentication failed").await?;
            if state.auth_error_count >= 3 {
                info!("Too many auth failures, disconnecting");
                return Err(CanalError::AuthFailed("too many failures".into()));
            }
            // Rate-limit auth failures to slow brute-force attacks
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            return Ok(());
        }
    }
    state.authenticated = true;

    let cid = if auth.client_id.is_empty() {
        "anonymous".to_string()
    } else {
        auth.client_id.clone()
    };

    const MAX_FILTER_PATTERN_LEN: usize = 256;

    let filter = if auth.filter.is_empty() {
        FilterPattern::default()
    } else {
        if auth.filter.len() > MAX_FILTER_PATTERN_LEN {
            let msg = format!(
                "filter pattern too long: {} > {}",
                auth.filter.len(),
                MAX_FILTER_PATTERN_LEN
            );
            return send_ack_err(transport, &msg).await;
        }
        let fp = FilterPattern {
            pattern: auth.filter.clone(),
            black_list: String::new(),
        };
        if let Err(e) = fp.validate() {
            let msg = format!("invalid filter pattern: {}", e);
            return send_ack_err(transport, &msg).await;
        }
        fp
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
    send_ack_ok(transport).await?;
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

    // Validate client_id matches authenticated identity
    if let Some(ref auth_cid) = state.client_id {
        if sub.client_id != *auth_cid {
            return send_ack_err(
                transport,
                &format!(
                    "client_id mismatch: sub has '{}' but authenticated as '{}'",
                    sub.client_id, auth_cid
                ),
            )
            .await;
        }
    }

    const MAX_FILTER_PATTERN_LEN: usize = 256;
    if sub.filter.len() > MAX_FILTER_PATTERN_LEN {
        return send_ack_err(
            transport,
            &format!("filter pattern too long: {} > {}", sub.filter.len(), MAX_FILTER_PATTERN_LEN),
        )
        .await;
    }

    let black_list = sessions
        .get(&sub.client_id)
        .map(|s| s.filter.black_list.clone())
        .unwrap_or_default();

    let fp = FilterPattern {
        pattern: sub.filter.clone(),
        black_list,
    };
    if let Err(e) = fp.validate() {
        let msg = format!("invalid filter pattern: {}", e);
        return send_ack_err(transport, &msg).await;
    }

    sessions.register(&sub.client_id, &sub.destination, fp);
    state.client_id = Some(sub.client_id);
    send_ack_ok(transport).await?;
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

    let batch_size = usize::try_from(get.fetch_size)
        .unwrap_or(100)
        .clamp(1, MAX_FETCH_SIZE);
    if get.fetch_size > MAX_FETCH_SIZE as i32 {
        warn!(
            "Client requested fetch_size {} exceeding max {}, clamped",
            get.fetch_size, MAX_FETCH_SIZE
        );
    }

    const DEFAULT_START_JOURNAL: &str = "mysql-bin.000001";
    const DEFAULT_START_POSITION: u64 = 4;
    const MAX_FETCH_SIZE: usize = 10_000;

    let cid = state
        .client_id
        .clone()
        .ok_or_else(|| CanalError::Protocol("not authenticated — client_id missing".into()))?;
    let start = state.current_pos.clone().unwrap_or_else(|| {
        sessions
            .get(&cid)
            .and_then(|s| s.last_ack_position.lock_or_recover().clone())
            .unwrap_or_else(|| LogPosition::new(DEFAULT_START_JOURNAL, DEFAULT_START_POSITION))
    });

    let events: Events = store.get_batch(&start, batch_size).await?;

    let mut msgs = Messages::default();
    if !events.is_empty() {
        state.current_pos = Some(events.position_range.end.clone());
        state.last_get_batch_id = events.batch_id;
        state.last_get_end_pos = Some(events.position_range.end.clone());
        if let Some(ref pos) = state.current_pos {
            sessions.update_position(&cid, pos.clone());
        }

        // Apply per-client session filter (use cached compiled regex)
        let session = sessions.get(&cid);
        let pattern_re = session.as_ref().and_then(|s| s.compiled_pattern.as_ref());
        let black_re = session.as_ref().and_then(|s| s.compiled_black_list.as_ref());

        let to_send: Vec<_> = events
            .events
            .iter()
            .filter(|event| {
                let table = format!("{}.{}", event.schema_name, event.table_name);
                let matches = pattern_re.as_ref().map_or(true, |re| re.is_match(&table));
                let blocked = black_re.as_ref().map_or(false, |re| re.is_match(&table));
                matches && !blocked
            })
            .collect();

        if to_send.is_empty() {
            transport.send(
                Packet {
                    r#type: PacketType::Messages as i32,
                    body: msgs.encode_to_vec(),
                    ..Default::default()
                }
                .encode_to_vec(),
            )
            .await?;
            return Ok(());
        }

        msgs = Messages {
            batch_id: events.batch_id,
            ..Default::default()
        };
        for event in to_send {
            let entry = canal_event_to_entry(event)?;
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
        if let (Some(ref cid), Some(ref pos)) =
            (&state.client_id, &state.last_get_end_pos)
        {
            state.last_ack_pos = Some(pos.clone());
            sessions.update_ack(cid, pos.clone());
            if client_ack.batch_id != 0
                && client_ack.batch_id != state.last_get_batch_id
            {
                warn!(
                    "Client {} acked batch {} but last sent was {}",
                    cid, client_ack.batch_id, state.last_get_batch_id,
                );
            }
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
    send_ack_ok(transport).await?;
    Ok(())
}

async fn send_ack_ok(
    transport: &mut (impl SinkExt<Vec<u8>, Error = CanalError> + Unpin),
) -> CanalResult<()> {
    let packet = Packet {
        r#type: PacketType::Ack as i32,
        body: Ack::default().encode_to_vec(),
        ..Default::default()
    };
    transport.send(packet.encode_to_vec()).await?;
    Ok(())
}

async fn send_ack_err(
    transport: &mut (impl SinkExt<Vec<u8>, Error = CanalError> + Unpin),
    message: &str,
) -> CanalResult<()> {
    let ack = Ack {
        error_message: message.to_string(),
        error_code_present: Some(canal_proto::ack::ErrorCodePresent::ErrorCode(1)),
    };
    let packet = Packet {
        r#type: PacketType::Ack as i32,
        body: ack.encode_to_vec(),
        ..Default::default()
    };
    transport.send(packet.encode_to_vec()).await?;
    Ok(())
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
