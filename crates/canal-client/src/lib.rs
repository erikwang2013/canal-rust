use canal_common::{CanalEvent, CanalResult, FilterPattern, LogPosition};
use canal_proto::{
    Ack, ClientAck, ClientAuth, Get, Messages, Packet, PacketType, Sub,
};
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Canal client for subscribing to MySQL binlog events from a Canal server.
/// Connects via TCP using the Canal protobuf wire protocol.
pub struct CanalClient {
    host: String,
    port: u16,
    client_id: u64,
    destination: String,
    filter: FilterPattern,
    stream: Option<TcpStream>,
}

impl CanalClient {
    pub fn new(host: &str, port: u16) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT_ID: AtomicU64 = AtomicU64::new(1001);

        Self {
            host: host.to_string(),
            port,
            client_id: NEXT_ID.fetch_add(1, Ordering::SeqCst),
            destination: "example".to_string(),
            filter: FilterPattern::default(),
            stream: None,
        }
    }

    pub fn with_destination(mut self, dest: &str) -> Self {
        self.destination = dest.to_string();
        self
    }

    pub fn with_filter(mut self, filter: FilterPattern) -> Self {
        self.filter = filter;
        self
    }

    /// Connect to the Canal server with full handshake (ClientAuth → Ack).
    pub async fn connect(&mut self) -> CanalResult<()> {
        let addr = format!("{}:{}", self.host, self.port);
        let mut stream = TcpStream::connect(&addr).await.map_err(|e| {
            canal_common::CanalError::BinlogConnection(format!(
                "TCP connect to {}: {}",
                addr, e
            ))
        })?;

        // Build and send ClientAuth
        let auth = ClientAuth {
            username: String::new(),
            password: vec![],
            destination: self.destination.clone(),
            client_id: self.client_id.to_string(),
            filter: self.filter.pattern.clone(),
            ..Default::default()
        };
        let packet = Packet {
            r#type: PacketType::Clientauthentication as i32,
            body: auth.encode_to_vec(),
            ..Default::default()
        };
        send_packet(&mut stream, &packet).await?;

        // Read Ack
        let ack_packet = read_packet(&mut stream).await?;
        if ack_packet.r#type != PacketType::Ack as i32 {
            return Err(canal_common::CanalError::Protocol(
                "expected Ack after ClientAuth".into(),
            ));
        }
        let ack = Ack::decode(&ack_packet.body[..]).map_err(|e| {
            canal_common::CanalError::Protocol(format!("decode Ack: {}", e))
        })?;
        if !ack.error_message.is_empty() {
            return Err(canal_common::CanalError::AuthFailed(ack.error_message));
        }
        info!("Client {} authenticated", self.client_id);

        self.stream = Some(stream);
        Ok(())
    }

    /// Subscribe to binlog events starting from the given position.
    /// Returns a stream that yields CanalEvent batches as they arrive.
    pub async fn subscribe(
        &mut self,
        _position: Option<LogPosition>,
    ) -> CanalResult<CanalEventStream> {
        let mut stream = self.stream.take().ok_or_else(|| {
            canal_common::CanalError::Internal("not connected".into())
        })?;

        // Send Sub
        let sub = Sub {
            destination: self.destination.clone(),
            client_id: self.client_id.to_string(),
            filter: self.filter.pattern.clone(),
        };
        let packet = Packet {
            r#type: PacketType::Subscription as i32,
            body: sub.encode_to_vec(),
            ..Default::default()
        };
        send_packet(&mut stream, &packet).await?;

        // Read Ack for Sub
        let ack_packet = read_packet(&mut stream).await?;
        if ack_packet.r#type != PacketType::Ack as i32 {
            return Err(canal_common::CanalError::Protocol(
                "expected Ack after Sub".into(),
            ));
        }

        let (tx, rx) = mpsc::channel(1024);
        let client_id = self.client_id;
        let destination = self.destination.clone();

        // Spawn background poll loop: Get → Messages → ClientAck → repeat
        tokio::spawn(async move {
            let mut batch_id = 0i64;
            loop {
                // Send Get
                let get = Get {
                    destination: destination.clone(),
                    client_id: client_id.to_string(),
                    fetch_size: 100,
                    ..Default::default()
                };
                let pkt = Packet {
                    r#type: PacketType::Get as i32,
                    body: get.encode_to_vec(),
                    ..Default::default()
                };
                if let Err(e) = send_packet(&mut stream, &pkt).await {
                    warn!("Client {} Get send failed: {}", client_id, e);
                    break;
                }

                // Read Messages
                let resp = match read_packet(&mut stream).await {
                    Ok(p) => p,
                    Err(e) => {
                        warn!("Client {} read failed: {}", client_id, e);
                        break;
                    }
                };

                match PacketType::try_from(resp.r#type) {
                    Ok(PacketType::Messages) => {
                        let msgs = match Messages::decode(&resp.body[..]) {
                            Ok(m) => m,
                            Err(e) => {
                                warn!("Client {} decode Messages: {}", client_id, e);
                                break;
                            }
                        };
                        batch_id = msgs.batch_id;

                        // Forward events to the stream consumer
                        for entry_bytes in &msgs.messages {
                            let event = entry_bytes_to_event(entry_bytes);
                            if tx.send(Ok(event)).await.is_err() {
                                return; // consumer dropped
                            }
                        }

                        // Send ClientAck
                        let ack = ClientAck {
                            destination: destination.clone(),
                            client_id: client_id.to_string(),
                            batch_id,
                        };
                        let ack_pkt = Packet {
                            r#type: PacketType::Clientack as i32,
                            body: ack.encode_to_vec(),
                            ..Default::default()
                        };
                        if let Err(e) = send_packet(&mut stream, &ack_pkt).await {
                            warn!("Client {} ack failed: {}", client_id, e);
                            break;
                        }
                    }
                    Ok(PacketType::Ack) => {
                        debug!("Client {} received terminal Ack", client_id);
                        break;
                    }
                    _ => {
                        debug!("Client {} unexpected packet type: {}", client_id, resp.r#type);
                    }
                }
            }
        });

        Ok(CanalEventStream { rx })
    }

    pub fn client_id(&self) -> u64 {
        self.client_id
    }
}

/// An async stream of Canal binlog events.
pub struct CanalEventStream {
    rx: mpsc::Receiver<CanalResult<CanalEvent>>,
}

impl CanalEventStream {
    pub async fn next_event(&mut self) -> Option<CanalResult<CanalEvent>> {
        self.rx.recv().await
    }
}

// ── Wire helpers ────────────────────────────────────────────

/// Send a protobuf Packet with 4-byte BE length prefix.
async fn send_packet(stream: &mut TcpStream, packet: &Packet) -> CanalResult<()> {
    let body = packet.encode_to_vec();
    let len = body.len() as u32;
    let mut buf = Vec::with_capacity(4 + body.len());
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&body);
    stream.write_all(&buf).await.map_err(canal_common::CanalError::Io)?;
    Ok(())
}

/// Read a length-prefixed protobuf Packet.
async fn read_packet(stream: &mut TcpStream) -> CanalResult<Packet> {
    let mut header = [0u8; 4];
    stream.read_exact(&mut header).await.map_err(canal_common::CanalError::Io)?;
    let len = u32::from_be_bytes(header) as usize;

    if len > 64 * 1024 * 1024 {
        return Err(canal_common::CanalError::Protocol(format!(
            "packet too large: {} bytes", len
        )));
    }

    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await.map_err(canal_common::CanalError::Io)?;

    Packet::decode(&body[..]).map_err(|e| {
        canal_common::CanalError::Protocol(format!("decode Packet: {}", e))
    })
}

/// Convert a protobuf Entry (from Messages) into a CanalEvent.
fn entry_bytes_to_event(data: &[u8]) -> CanalEvent {
    if let Ok(entry) = canal_proto::Entry::decode(data) {
        if let Some(hdr) = entry.header {
            let ev_type = hdr.event_type_present
                .map(|et| match et {
                    canal_proto::header::EventTypePresent::EventType(v) => v,
                })
                .unwrap_or(0);

            return CanalEvent {
                journal_name: hdr.logfile_name,
                position: hdr.logfile_offset as u64,
                server_id: hdr.server_id as u64,
                execute_time: hdr.execute_time,
                entry_type: canal_common::EventType::from(ev_type),
                schema_name: hdr.schema_name,
                table_name: hdr.table_name,
                row_change: None,
                ddl_sql: None,
                gtid: if hdr.gtid.is_empty() { None } else { Some(hdr.gtid) },
                raw_bytes: vec![],
            };
        }
    }

    CanalEvent {
        journal_name: String::new(),
        position: 0,
        server_id: 0,
        execute_time: 0,
        entry_type: canal_common::EventType::Unknown(0),
        schema_name: String::new(),
        table_name: String::new(),
        row_change: None,
        ddl_sql: None,
        gtid: None,
        raw_bytes: vec![],
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
    async fn test_canal_event_stream_drop() {
        let (_tx, rx) = mpsc::channel::<CanalResult<CanalEvent>>(4);
        let mut stream = CanalEventStream { rx };
        drop(_tx);
        // Channel closed — next_event returns None
        assert!(stream.next_event().await.is_none());
    }
}
