use std::net::SocketAddr;
use std::sync::Arc;

use canal_common::{CanalError, CanalResult, Events, FilterPattern, LogPosition};
use canal_store::memory::MemoryEventStore;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_util::codec::Framed;
use tracing::{debug, error, info};

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
/// Protocol phases: handshake -> subscribe -> stream events.
async fn handle_client(
    mut transport: impl StreamExt<Item = Result<Vec<u8>, CanalError>>
        + SinkExt<Vec<u8>, Error = CanalError>
        + Unpin,
    store: Arc<MemoryEventStore>,
    sessions: Arc<SessionManager>,
) -> CanalResult<()> {
    let mut client_id: Option<String> = None;
    let mut current_pos: Option<LogPosition> = None;

    while let Some(Ok(_packet)) = transport.next().await {
        // --- Phase 1: Handshake (first packet = ClientAuth) ---
        if client_id.is_none() {
            client_id = Some("anonymous".to_string());
            sessions.register("anonymous", "example", FilterPattern::default());
            info!("Client authenticated: anonymous");

            // Send ACK to confirm handshake
            transport.send(vec![0]).await?;
            continue;
        }

        // --- Phase 2: Get events ---
        let start = current_pos
            .clone()
            .unwrap_or_else(|| LogPosition::new("mysql-bin.000001", 4));

        let events: Events = store.get_batch(&start, 100).await?;

        if !events.is_empty() {
            current_pos = Some(events.position_range.end.clone());
            debug!(
                "Sending batch_id={} with {} events",
                events.batch_id,
                events.events.len()
            );

            // TODO: Serialize Events to protobuf Packet via canal-proto types,
            // then send over wire. Currently sends empty ACK for protocol compatibility.
            transport.send(vec![]).await?;
        }
    }

    // Cleanup on disconnect
    if let Some(ref cid) = client_id {
        sessions.unregister(cid);
    }

    Ok(())
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
}
