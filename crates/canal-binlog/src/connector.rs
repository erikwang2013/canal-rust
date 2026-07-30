use async_trait::async_trait;
use canal_common::{CanalEvent, CanalResult, LogPosition};
use tokio::sync::mpsc;
use tracing::info;

/// Trait for MySQL binlog replication connectors.
/// Abstracts the underlying binlog client library so implementations
/// can be swapped (binlog crate, custom protocol, mock for testing).
#[async_trait]
pub trait BinlogConnector: Send {
    /// Connect to MySQL and start replicating from the given position
    async fn connect(&mut self, pos: &LogPosition) -> CanalResult<()>;

    /// Get the receiver end of the event channel.
    /// Events from MySQL binlog are streamed through this channel.
    fn receiver(&mut self) -> mpsc::Receiver<CanalResult<CanalEvent>>;

    /// Gracefully disconnect from MySQL
    async fn disconnect(&mut self) -> CanalResult<()>;

    /// Return the current binlog position, if connected
    fn current_position(&self) -> Option<LogPosition>;
}

/// Default binlog connector implementation.
///
/// When the `binlog` crate (https://crates.io/crates/binlog) is available,
/// the connect() method will use it to establish a binlog replication stream
/// from MySQL and feed CanalEvent instances into the channel.
pub struct DefaultBinlogConnector {
    host: String,
    port: u16,
    #[allow(dead_code)]
    username: String,
    #[allow(dead_code)]
    password: String,
    #[allow(dead_code)]
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
}

#[async_trait]
impl BinlogConnector for DefaultBinlogConnector {
    async fn connect(&mut self, pos: &LogPosition) -> CanalResult<()> {
        self.current_pos = Some(pos.clone());
        self.running = true;
        info!(
            "Connected to MySQL {}:{} at position {}",
            self.host, self.port, pos
        );

        // TODO: Integrate the `binlog` crate when its API is available.
        // The binlog crate (https://crates.io/crates/binlog) provides:
        //   - binlog::Client::new(url, username, password)
        //   - client.replicate_from(journal_name, position)
        //   - Streaming iterator over binlog events (WriteRows, UpdateRows, DeleteRows, etc.)
        //
        // Implementation sketch:
        //   let mut client = binlog::Client::new(
        //       format!("mysql://{}:{}", self.host, self.port),
        //       &self.username, &self.password,
        //   );
        //   let stream = client.replicate_from(&pos.journal_name, pos.position)?;
        //   let tx = self.sender.clone().unwrap();
        //   tokio::spawn(async move {
        //       for event in stream {
        //           let canal_events = convert(event)?;
        //           for e in canal_events { tx.send(Ok(e)).await.ok(); }
        //       }
        //   });

        Ok(())
    }

    fn receiver(&mut self) -> mpsc::Receiver<CanalResult<CanalEvent>> {
        let (tx, rx) = mpsc::channel(4096);
        self.sender = Some(tx);
        rx
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
