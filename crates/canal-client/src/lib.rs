use canal_common::{CanalEvent, CanalResult, FilterPattern, LogPosition};
use tokio::sync::mpsc;

/// Canal client for subscribing to MySQL binlog events from a Canal server.
/// Connects via TCP using the Canal protobuf wire protocol.
#[allow(dead_code)]
pub struct CanalClient {
    host: String,
    port: u16,
    client_id: u64,
    destination: String,
    filter: FilterPattern,
    connected: bool,
}

impl CanalClient {
    /// Create a new Canal client targeting the given server.
    /// Client IDs are auto-generated (globally unique across this process).
    pub fn new(host: &str, port: u16) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT_ID: AtomicU64 = AtomicU64::new(1001);

        Self {
            host: host.to_string(),
            port,
            client_id: NEXT_ID.fetch_add(1, Ordering::SeqCst),
            destination: "example".to_string(),
            filter: FilterPattern::default(),
            connected: false,
        }
    }

    /// Set the destination (Canal instance name to subscribe to)
    pub fn with_destination(mut self, dest: &str) -> Self {
        self.destination = dest.to_string();
        self
    }

    /// Set a table filter pattern for subscription
    pub fn with_filter(mut self, filter: FilterPattern) -> Self {
        self.filter = filter;
        self
    }

    /// Connect to the Canal server.
    /// Performs the handshake (ClientAuth) and negotiates the protocol version.
    pub async fn connect(&mut self) -> CanalResult<()> {
        // TODO: TCP connect + protobuf handshake via canal-proto Packet types.
        //  1. TcpStream::connect(format!("{}:{}", self.host, self.port)).await
        //  2. Construct ClientAuth packet (client_id, destination, filter)
        //  3. Send via Framed<TcpStream, CanalCodec>
        //  4. Read ACK response
        self.connected = true;
        Ok(())
    }

    /// Subscribe to binlog events starting from the given position.
    /// Returns a stream that yields CanalEvent batches as they arrive.
    ///
    /// If position is None, subscribes from the earliest available event.
    pub async fn subscribe(
        &mut self,
        _position: Option<LogPosition>,
    ) -> CanalResult<CanalEventStream> {
        let (_tx, rx) = mpsc::channel(1024);
        // TODO: Spawn a background task that:
        //  1. Sends Sub packet
        //  2. Loops: send Get packet → receive Messages → forward events into tx
        //  3. Sends periodic ClientAck for consumed batch_ids
        Ok(CanalEventStream { rx })
    }

    /// Get the auto-generated client ID
    pub fn client_id(&self) -> u64 {
        self.client_id
    }
}

/// An async stream of Canal binlog events.
/// Returned by `CanalClient::subscribe()`.
pub struct CanalEventStream {
    rx: mpsc::Receiver<CanalResult<CanalEvent>>,
}

impl CanalEventStream {
    /// Receive the next event from the stream.
    /// Returns None when the server connection is closed.
    pub async fn next_event(&mut self) -> Option<CanalResult<CanalEvent>> {
        self.rx.recv().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_id_is_unique() {
        let c1 = CanalClient::new("localhost", 11111);
        let c2 = CanalClient::new("localhost", 11111);
        assert_ne!(c1.client_id(), c2.client_id());
    }

    #[test]
    fn test_builder_pattern() {
        let client = CanalClient::new("db.example.com", 12345)
            .with_destination("production")
            .with_filter(FilterPattern::default());

        assert_eq!(client.host, "db.example.com");
        assert_eq!(client.port, 12345);
        assert_eq!(client.destination, "production");
    }

    #[tokio::test]
    async fn test_connect_sets_connected_flag() {
        let mut client = CanalClient::new("localhost", 11111);
        client.connect().await.unwrap();
        assert!(client.connected);
    }
}
