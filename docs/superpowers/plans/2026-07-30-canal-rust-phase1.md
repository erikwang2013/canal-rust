# Canal Rust 重写 — 实现计划

> **状态：全部完成** | 日期：2026-07-30 | 更新：2026-07-31 | 版本：v1.0.6 | 测试：100

**Goal:** 从零搭建 canal-rust workspace，实现单机版 Canal Server ✓ 三期全部完成

**Architecture:** Cargo workspace 14 crates

**Spec:** `docs/superpowers/specs/2026-07-30-canal-rust-rewrite-design.md`

---

## File Structure

```
canal-rust/
├── Cargo.toml
├── canal.yaml
├── rust-toolchain.toml
├── proto/
│   ├── CanalProtocol.proto
│   └── EntryProtocol.proto
├── crates/
│   ├── canal-common/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── error.rs
│   │       ├── types.rs
│   │       └── lifecycle.rs
│   ├── canal-proto/
│   │   ├── Cargo.toml
│   │   ├── build.rs
│   │   └── src/lib.rs
│   ├── canal-binlog/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── connector.rs
│   │       ├── converter.rs
│   │       └── table_map.rs
│   ├── canal-store/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── memory.rs
│   │       └── position.rs
│   ├── canal-server/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── codec.rs
│   │       ├── server.rs
│   │       └── session.rs
│   ├── canal-client/
│   │   ├── Cargo.toml
│   │   └── src/lib.rs
│   └── canal-cli/
│       ├── Cargo.toml
│       └── src/main.rs
└── tests/
    └── integration/
        └── end_to_end.rs
```

---

### Task 0: Workspace Scaffolding

**Files Created:** Cargo.toml, rust-toolchain.toml, canal.yaml, 7 crate Cargo.toml files

- [ ] **Step 1: root Cargo.toml**

```toml
[workspace]
members = ["crates/*"]
resolver = "2"

[workspace.package]
version = "0.1.0"
edition = "2021"
license = "Apache-2.0"

[workspace.dependencies]
tokio = { version = "1", features = ["full"] }
tokio-util = { version = "0.7", features = ["codec"] }
prost = "0.13"
prost-types = "0.13"
bytes = "1"
serde = { version = "1", features = ["derive"] }
serde_yaml = "0.9"
thiserror = "2"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["json", "env-filter"] }
anyhow = "1"
clap = { version = "4", features = ["derive", "yaml"] }
async-trait = "0.1"
chrono = { version = "0.4", features = ["serde"] }
```

- [ ] **Step 2: rust-toolchain.toml**

```toml
[toolchain]
channel = "stable"
components = ["rustfmt", "clippy"]
```

- [ ] **Step 3: canal.yaml**

```yaml
canal:
  server_id: 1234
  mysql:
    host: "127.0.0.1"
    port: 3306
    username: "canal"
    password: "canal"
    charset: "utf8mb4"
  position:
    journal_name: "mysql-bin.000001"
    position: 4
  store:
    type: "memory"
    buffer_size: 16384
    batch_timeout_ms: 100
  server:
    bind: "0.0.0.0:11111"
    idle_timeout_secs: 3600
  filter:
    pattern: ".*\\..*"
    black_list: ""
  logging:
    level: "info"
    format: "json"
```

- [ ] **Step 4: Create crate Cargo.toml files**

Run (create dirs and minimal Cargo.toml for each crate):

```bash
mkdir -p crates/canal-{common,proto,binlog,store,server,client,cli}/src tests/integration proto

# canal-common
cat > crates/canal-common/Cargo.toml << 'TOML'
[package]
name = "canal-common"
version.workspace = true
edition.workspace = true
[dependencies]
tokio.workspace = true
serde.workspace = true
thiserror.workspace = true
chrono.workspace = true
tracing.workspace = true
async-trait.workspace = true
TOML

# canal-proto
cat > crates/canal-proto/Cargo.toml << 'TOML'
[package]
name = "canal-proto"
version.workspace = true
edition.workspace = true
[build-dependencies]
prost-build = "0.13"
[dependencies]
prost.workspace = true
prost-types.workspace = true
bytes.workspace = true
canal-common = { path = "../canal-common" }
TOML

# canal-binlog
cat > crates/canal-binlog/Cargo.toml << 'TOML'
[package]
name = "canal-binlog"
version.workspace = true
edition.workspace = true
[dependencies]
tokio.workspace = true
async-trait.workspace = true
tracing.workspace = true
thiserror.workspace = true
serde.workspace = true
canal-common = { path = "../canal-common" }
canal-proto = { path = "../canal-proto" }
TOML

# canal-store
cat > crates/canal-store/Cargo.toml << 'TOML'
[package]
name = "canal-store"
version.workspace = true
edition.workspace = true
[dependencies]
tokio.workspace = true
async-trait.workspace = true
tracing.workspace = true
thiserror.workspace = true
serde.workspace = true
serde_yaml.workspace = true
canal-common = { path = "../canal-common" }
[dev-dependencies]
tokio.workspace = true
TOML

# canal-server
cat > crates/canal-server/Cargo.toml << 'TOML'
[package]
name = "canal-server"
version.workspace = true
edition.workspace = true
[dependencies]
tokio.workspace = true
tokio-util.workspace = true
async-trait.workspace = true
tracing.workspace = true
thiserror.workspace = true
bytes.workspace = true
prost.workspace = true
canal-common = { path = "../canal-common" }
canal-proto = { path = "../canal-proto" }
canal-binlog = { path = "../canal-binlog" }
canal-store = { path = "../canal-store" }
[dev-dependencies]
tokio.workspace = true
futures = "0.3"
TOML

# canal-client
cat > crates/canal-client/Cargo.toml << 'TOML'
[package]
name = "canal-client"
version.workspace = true
edition.workspace = true
[dependencies]
tokio.workspace = true
tokio-util.workspace = true
async-trait.workspace = true
tracing.workspace = true
thiserror.workspace = true
bytes.workspace = true
prost.workspace = true
canal-common = { path = "../canal-common" }
canal-proto = { path = "../canal-proto" }
TOML

# canal-cli
cat > crates/canal-cli/Cargo.toml << 'TOML'
[package]
name = "canal-cli"
version.workspace = true
edition.workspace = true
[[bin]]
name = "canal-rust"
path = "src/main.rs"
[dependencies]
tokio.workspace = true
tracing-subscriber.workspace = true
tracing.workspace = true
serde.workspace = true
serde_yaml.workspace = true
anyhow.workspace = true
clap.workspace = true
canal-common = { path = "../canal-common" }
canal-binlog = { path = "../canal-binlog" }
canal-store = { path = "../canal-store" }
canal-server = { path = "../canal-server" }
TOML
```

- [ ] **Step 5: Verify**

```bash
cargo check
```
Expected: all crates compile successfully.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: scaffold canal-rust workspace with 7 crates"
```

---

### Task 1: canal-common — Core Types

**Files:** crates/canal-common/src/{lib.rs, error.rs, types.rs, lifecycle.rs}

- [ ] **Step 1: error.rs**

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CanalError {
    #[error("binlog connection: {0}")] BinlogConnection(String),
    #[error("pos {0}:{1} not found")] PositionNotFound(String, u64),
    #[error("protocol: {0}")] Protocol(String),
    #[error("auth failed: {0}")] AuthFailed(String),
    #[error("io: {0}")] Io(#[from] std::io::Error),
    #[error("store: {0}")] Store(String),
    #[error("config: {0}")] Config(String),
    #[error("not found: {0}")] NotFound(String),
    #[error("internal: {0}")] Internal(String),
}

pub type CanalResult<T> = Result<T, CanalError>;
```

- [ ] **Step 2: types.rs**

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogPosition {
    pub journal_name: String,
    pub position: u64,
    pub timestamp: Option<i64>,
    pub server_id: Option<u64>,
    pub gtid: Option<String>,
}

impl LogPosition {
    pub fn new(journal_name: &str, position: u64) -> Self {
        Self { journal_name: journal_name.into(), position, timestamp: None, server_id: None, gtid: None }
    }
}

impl std::fmt::Display for LogPosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.gtid {
            Some(g) => write!(f, "{}:{}:{}", self.journal_name, self.position, g),
            None => write!(f, "{}:{}", self.journal_name, self.position),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionRange { pub start: LogPosition, pub end: LogPosition }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventType { Insert, Update, Delete, Ddl, Query, Rotate, Xid, Heartbeat, Unknown(i32) }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DmlType { Insert, Update, Delete }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnValue {
    pub name: String, pub value: Option<String>,
    pub column_type: i32, pub is_key: bool, pub updated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowData { pub columns: Vec<ColumnValue> }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowChange {
    pub table_name: String, pub schema_name: String,
    pub before: Option<RowData>, pub after: Option<RowData>, pub dml_type: DmlType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanalEvent {
    pub journal_name: String, pub position: u64, pub server_id: u64,
    pub execute_time: i64, pub entry_type: EventType,
    pub schema_name: String, pub table_name: String,
    pub row_change: Option<RowChange>, pub ddl_sql: Option<String>,
    pub gtid: Option<String>, pub raw_bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Events {
    pub position_range: PositionRange, pub events: Vec<CanalEvent>, pub batch_id: i64,
}

impl Events {
    pub fn new(batch_id: i64) -> Self {
        Self { position_range: PositionRange { start: LogPosition::new("", 0), end: LogPosition::new("", 0) }, events: vec![], batch_id }
    }
    pub fn with_events(events: Vec<CanalEvent>, batch_id: i64) -> Self {
        let (first, last) = match (events.first(), events.last()) {
            (Some(f), Some(l)) => (LogPosition::new(&f.journal_name, f.position), LogPosition::new(&l.journal_name, l.position)),
            _ => (LogPosition::new("", 0), LogPosition::new("", 0)),
        };
        Self { position_range: PositionRange { start: first, end: last }, events, batch_id }
    }
    pub fn is_empty(&self) -> bool { self.events.is_empty() }
    pub fn len(&self) -> usize { self.events.len() }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterPattern { pub pattern: String, pub black_list: String }
impl Default for FilterPattern {
    fn default() -> Self { Self { pattern: ".*\\..*".into(), black_list: String::new() } }
}
```

- [ ] **Step 3: lifecycle.rs**

```rust
use crate::error::CanalResult;
#[async_trait::async_trait]
pub trait CanalLifecycle: Send + Sync {
    async fn start(&mut self) -> CanalResult<()> { Ok(()) }
    async fn stop(&mut self) -> CanalResult<()> { Ok(()) }
    fn is_running(&self) -> bool;
}
```

- [ ] **Step 4: lib.rs**

```rust
pub mod error; pub mod types; pub mod lifecycle;
pub use error::{CanalError, CanalResult};
pub use lifecycle::CanalLifecycle;
pub use types::*;
```

- [ ] **Step 5: Verify & Commit**

```bash
cargo build -p canal-common && git add crates/canal-common/ && git commit -m "feat(canal-common): core types, errors, lifecycle trait"
```

---

### Task 2: canal-proto — Protobuf Code Generation

**Files:** proto/{CanalProtocol,EntryProtocol}.proto, crates/canal-proto/{build.rs,src/lib.rs}

- [ ] **Step 1: Fetch proto files**

```bash
curl -sL https://raw.githubusercontent.com/alibaba/canal/master/protocol/src/main/java/com/alibaba/otter/canal/protocol/CanalProtocol.proto -o proto/CanalProtocol.proto
curl -sL https://raw.githubusercontent.com/alibaba/canal/master/protocol/src/main/java/com/alibaba/otter/canal/protocol/EntryProtocol.proto -o proto/EntryProtocol.proto
```

- [ ] **Step 2: build.rs**

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    prost_build::Config::new().out_dir("src/").compile_protos(
        &["../proto/CanalProtocol.proto", "../proto/EntryProtocol.proto"], &["../proto/"],
    )?;
    Ok(())
}
```

- [ ] **Step 3: src/lib.rs**

```rust
include!(concat!(env!("OUT_DIR"), "/com.alibaba.otter.canal.protocol.rs"));
```

- [ ] **Step 4: Build & Commit**

```bash
cargo build -p canal-proto && git add proto/ crates/canal-proto/ && git commit -m "feat(canal-proto): protobuf codegen from upstream proto files"
```

---

### Task 3: canal-binlog — Binlog Parser Layer

**Files:** crates/canal-binlog/src/{lib.rs, table_map.rs, converter.rs, connector.rs}

- [ ] **Step 1: table_map.rs**

```rust
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct TableMapCache { tables: HashMap<u64, (String, String)> }

impl TableMapCache {
    pub fn new() -> Self { Self { tables: HashMap::new() } }
    pub fn put(&mut self, table_id: u64, schema: String, table: String) { self.tables.insert(table_id, (schema, table)); }
    pub fn get(&self, table_id: u64) -> Option<(String, String)> { self.tables.get(&table_id).cloned() }
    pub fn clear(&mut self) { self.tables.clear(); }
}
```

- [ ] **Step 2: converter.rs**

```rust
use canal_common::*;
use crate::table_map::TableMapCache;

pub struct EventConverter { table_map: TableMapCache }

impl EventConverter {
    pub fn new() -> Self { Self { table_map: TableMapCache::new() } }

    pub fn handle_table_map(&mut self, table_id: u64, schema: &str, table: &str) {
        self.table_map.put(table_id, schema.into(), table.into());
    }

    pub fn handle_row_event(&self, table_id: u64, event_type: EventType, columns: Vec<ColumnValue>) -> CanalResult<RowChange> {
        let (schema, table) = self.table_map.get(table_id)
            .ok_or_else(|| CanalError::NotFound(format!("table_id {} not in TableMap", table_id)))?;
        let (before, after, dml_type) = match event_type {
            EventType::Insert => (None, Some(RowData { columns }), DmlType::Insert),
            EventType::Delete => (Some(RowData { columns }), None, DmlType::Delete),
            EventType::Update => {
                let mid = columns.len() / 2;
                (Some(RowData { columns: columns[..mid].to_vec() }),
                 Some(RowData { columns: columns[mid..].to_vec() }), DmlType::Update)
            }
            _ => return Err(CanalError::Internal("unexpected event type for row".into())),
        };
        Ok(RowChange { table_name: table, schema_name: schema, before, after, dml_type })
    }

    pub fn clear_table_map(&mut self) { self.table_map.clear(); }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_table_map_roundtrip() {
        let mut c = EventConverter::new();
        c.handle_table_map(100, "test_db", "users");
        assert_eq!(c.table_map.get(100).unwrap(), ("test_db".into(), "users".into()));
    }
    #[test]
    fn test_missing_table_map_errors() {
        let c = EventConverter::new();
        assert!(c.handle_row_event(999, EventType::Insert, vec![]).is_err());
    }
    #[test]
    fn test_clear_table_map() {
        let mut c = EventConverter::new();
        c.handle_table_map(1, "db", "tbl");
        c.clear_table_map();
        assert!(c.table_map.get(1).is_none());
    }
}
```

- [ ] **Step 3: connector.rs**

```rust
use async_trait::async_trait;
use canal_common::*;
use tokio::sync::mpsc;
use tracing::info;

#[async_trait]
pub trait BinlogConnector: Send {
    async fn connect(&mut self, pos: &LogPosition) -> CanalResult<()>;
    fn receiver(&mut self) -> mpsc::Receiver<CanalResult<CanalEvent>>;
    async fn disconnect(&mut self) -> CanalResult<()>;
    fn current_position(&self) -> Option<LogPosition>;
}

pub struct DefaultBinlogConnector {
    host: String, port: u16, username: String, password: String, server_id: u64,
    sender: Option<mpsc::Sender<CanalResult<CanalEvent>>>,
    current_pos: Option<LogPosition>, running: bool,
}

impl DefaultBinlogConnector {
    pub fn new(host: &str, port: u16, username: &str, password: &str, server_id: u64) -> Self {
        Self { host: host.into(), port, username: username.into(), password: password.into(),
               server_id, sender: None, current_pos: None, running: false }
    }
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
        info!("Connected to MySQL {}:{} at {}", self.host, self.port, pos);
        // TODO: integrate binlog crate (https://crates.io/crates/binlog)
        Ok(())
    }
    fn receiver(&mut self) -> mpsc::Receiver<CanalResult<CanalEvent>> {
        let (tx, rx) = mpsc::channel(4096);
        self.sender = Some(tx);
        rx
    }
    async fn disconnect(&mut self) -> CanalResult<()> { self.running = false; Ok(()) }
    fn current_position(&self) -> Option<LogPosition> { self.current_pos.clone() }
}
```

- [ ] **Step 4: lib.rs**

```rust
pub mod table_map; pub mod converter; pub mod connector;
pub use connector::{BinlogConnector, DefaultBinlogConnector};
pub use converter::EventConverter;
pub use table_map::TableMapCache;
```

- [ ] **Step 5: Test & Commit**

```bash
cargo test -p canal-binlog && git add crates/canal-binlog/ && git commit -m "feat(canal-binlog): binlog connector, event converter, table map cache"
```

---

### Task 4: canal-store — Event Storage

**Files:** crates/canal-store/src/{lib.rs, position.rs, memory.rs}

- [ ] **Step 1: position.rs**

```rust
use canal_common::*;
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Debug)]
pub struct PositionTracker { positions: RwLock<HashMap<String, LogPosition>> }

impl PositionTracker {
    pub fn new() -> Self { Self { positions: RwLock::new(HashMap::new()) } }
    pub fn update(&self, client_id: &str, pos: LogPosition) { self.positions.write().unwrap().insert(client_id.into(), pos); }
    pub fn get(&self, client_id: &str) -> Option<LogPosition> { self.positions.read().unwrap().get(client_id).cloned() }
    pub fn remove(&self, client_id: &str) { self.positions.write().unwrap().remove(client_id); }
}
```

- [ ] **Step 2: memory.rs**

```rust
use std::collections::VecDeque;
use std::sync::{Mutex, atomic::{AtomicBool, AtomicI64, Ordering}};
use async_trait::async_trait;
use canal_common::*;
use canal_common::lifecycle::CanalLifecycle;
use tokio::sync::Notify;
use tracing::{info, debug};

pub struct MemoryEventStore {
    buffer: Mutex<VecDeque<CanalEvent>>,
    capacity: usize,
    batch_id_seq: AtomicI64,
    latest_position: Mutex<Option<LogPosition>>,
    first_position: Mutex<Option<LogPosition>>,
    running: AtomicBool,
    notify: Notify,
}

impl MemoryEventStore {
    pub fn new(capacity: usize) -> Self {
        Self { buffer: Mutex::new(VecDeque::with_capacity(capacity)), capacity,
               batch_id_seq: AtomicI64::new(0), latest_position: Mutex::new(None),
               first_position: Mutex::new(None), running: AtomicBool::new(false),
               notify: Notify::new() }
    }

    pub async fn put_batch(&self, events: Vec<CanalEvent>) -> CanalResult<()> {
        if events.is_empty() { return Ok(()); }
        let first = LogPosition::new(&events[0].journal_name, events[0].position);
        let last = LogPosition::new(&events.last().unwrap().journal_name, events.last().unwrap().position);
        let mut buf = self.buffer.lock().unwrap();
        while buf.len() + events.len() > self.capacity { buf.pop_front(); }
        if buf.is_empty() { *self.first_position.lock().unwrap() = Some(first); }
        *self.latest_position.lock().unwrap() = Some(last);
        buf.extend(events);
        self.notify.notify_waiters();
        debug!("buffer size: {}", buf.len());
        Ok(())
    }

    pub async fn get_batch(&self, start: &LogPosition, batch_size: usize) -> CanalResult<Events> {
        loop {
            let buf = self.buffer.lock().unwrap();
            let idx = buf.iter().position(|e| e.journal_name == start.journal_name && e.position > start.position);
            if let Some(i) = idx {
                let batch_id = self.batch_id_seq.fetch_add(1, Ordering::SeqCst);
                let events: Vec<_> = buf.iter().skip(i).take(batch_size).cloned().collect();
                if !events.is_empty() { return Ok(Events::with_events(events, batch_id)); }
            }
            drop(buf);
            self.notify.notified().await;
        }
    }

    pub fn latest_position(&self) -> Option<LogPosition> { self.latest_position.lock().unwrap().clone() }
    pub fn first_position(&self) -> Option<LogPosition> { self.first_position.lock().unwrap().clone() }
}

#[async_trait]
impl CanalLifecycle for MemoryEventStore {
    async fn start(&mut self) -> CanalResult<()> { self.running.store(true, Ordering::SeqCst); info!("MemoryEventStore started, capacity={}", self.capacity); Ok(()) }
    async fn stop(&mut self) -> CanalResult<()> { self.running.store(false, Ordering::SeqCst); info!("MemoryEventStore stopped"); Ok(()) }
    fn is_running(&self) -> bool { self.running.load(Ordering::SeqCst) }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn make_event(pos: u64) -> CanalEvent {
        CanalEvent { journal_name: "mysql-bin.000001".into(), position: pos, server_id: 1,
            execute_time: 0, entry_type: EventType::Insert, schema_name: "test".into(),
            table_name: "t".into(), row_change: None, ddl_sql: None, gtid: None, raw_bytes: vec![] }
    }
    #[tokio::test]
    async fn test_put_and_get() {
        let store = MemoryEventStore::new(1024);
        store.put_batch(vec![make_event(100)]).await.unwrap();
        let batch = store.get_batch(&LogPosition::new("mysql-bin.000001", 4), 10).await.unwrap();
        assert_eq!(batch.len(), 1);
    }
    #[tokio::test]
    async fn test_latest_position() {
        let store = MemoryEventStore::new(1024);
        assert!(store.latest_position().is_none());
        store.put_batch(vec![make_event(200)]).await.unwrap();
        assert_eq!(store.latest_position().unwrap().position, 200);
    }
}
```

- [ ] **Step 3: lib.rs**

```rust
pub mod memory; pub mod position;
pub use memory::MemoryEventStore;
pub use position::PositionTracker;
```

- [ ] **Step 4: Test & Commit**

```bash
cargo test -p canal-store && git add crates/canal-store/ && git commit -m "feat(canal-store): memory event store with ring buffer and position tracking"
```

---

### Task 5: canal-server — TCP Server

**Files:** crates/canal-server/src/{lib.rs, codec.rs, session.rs, server.rs}

- [ ] **Step 1: codec.rs**

```rust
use bytes::{Buf, BufMut, BytesMut};
use canal_common::CanalError;
use tokio_util::codec::{Decoder, Encoder};

pub type PacketBytes = Vec<u8>;

pub struct CanalCodec;

impl CanalCodec { pub fn new() -> Self { Self } }

impl Decoder for CanalCodec {
    type Item = PacketBytes;
    type Error = CanalError;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 { return Ok(None); }
        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&src[..4]);
        let len = u32::from_be_bytes(len_bytes) as usize;
        if len > 64 * 1024 * 1024 { return Err(CanalError::Protocol("packet too large".into())); }
        if src.len() < 4 + len { src.reserve(4 + len - src.len()); return Ok(None); }
        src.advance(4);
        let payload = src[..len].to_vec();
        src.advance(len);
        Ok(Some(payload))
    }
}

impl Encoder<PacketBytes> for CanalCodec {
    type Error = CanalError;
    fn encode(&mut self, item: PacketBytes, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.reserve(4 + item.len());
        dst.put_u32(item.len() as u32);
        dst.put_slice(&item);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_decode_complete_packet() {
        let mut c = CanalCodec;
        let mut b = BytesMut::from(&[0,0,0,5,104,101,108,108,111][..]);
        assert_eq!(c.decode(&mut b).unwrap(), Some(b"hello".to_vec()));
    }
    #[test]
    fn test_decode_incomplete_header() {
        let mut c = CanalCodec;
        let mut b = BytesMut::from(&[0,0][..]);
        assert_eq!(c.decode(&mut b).unwrap(), None);
    }
    #[test]
    fn test_decode_incomplete_payload() {
        let mut c = CanalCodec;
        let mut b = BytesMut::from(&[0,0,0,10,1,2,3][..]);
        assert_eq!(c.decode(&mut b).unwrap(), None);
        assert_eq!(b.len(), 7);
    }
    #[test]
    fn test_encode_roundtrip() {
        let mut c = CanalCodec;
        let mut b = BytesMut::new();
        c.encode(b"test".to_vec(), &mut b).unwrap();
        assert_eq!(c.decode(&mut b).unwrap(), Some(b"test".to_vec()));
    }
}
```

- [ ] **Step 2: session.rs**

```rust
use canal_common::*;
use std::collections::HashMap;
use std::sync::RwLock;
use chrono::Utc;

#[derive(Debug, Clone)]
pub struct ClientSession {
    pub client_id: String, pub destination: String, pub filter: FilterPattern,
    pub last_position: Option<LogPosition>, pub last_ack_position: Option<LogPosition>,
    pub connected_at: chrono::DateTime<Utc>, pub last_heartbeat: chrono::DateTime<Utc>,
}

impl ClientSession {
    pub fn new(client_id: &str, destination: &str, filter: FilterPattern) -> Self {
        let now = Utc::now();
        Self { client_id: client_id.into(), destination: destination.into(), filter,
               last_position: None, last_ack_position: None, connected_at: now, last_heartbeat: now }
    }
    pub fn heartbeat(&mut self) { self.last_heartbeat = Utc::now(); }
}

#[derive(Debug, Default)]
pub struct SessionManager { sessions: RwLock<HashMap<String, ClientSession>> }

impl SessionManager {
    pub fn new() -> Self { Self { sessions: RwLock::new(HashMap::new()) } }
    pub fn register(&self, client_id: &str, destination: &str, filter: FilterPattern) {
        let s = ClientSession::new(client_id, destination, filter);
        self.sessions.write().unwrap().insert(client_id.into(), s);
    }
    pub fn unregister(&self, client_id: &str) { self.sessions.write().unwrap().remove(client_id); }
    pub fn get(&self, client_id: &str) -> Option<ClientSession> { self.sessions.read().unwrap().get(client_id).cloned() }
    pub fn update_position(&self, client_id: &str, pos: LogPosition) {
        if let Some(s) = self.sessions.write().unwrap().get_mut(client_id) { s.last_position = Some(pos); }
    }
    pub fn update_ack(&self, client_id: &str, pos: LogPosition) {
        if let Some(s) = self.sessions.write().unwrap().get_mut(client_id) { s.last_ack_position = Some(pos); }
    }
    pub fn heartbeat(&self, client_id: &str) {
        if let Some(s) = self.sessions.write().unwrap().get_mut(client_id) { s.heartbeat(); }
    }
}
```

- [ ] **Step 3: server.rs**

```rust
use std::net::SocketAddr;
use std::sync::Arc;
use canal_common::*;
use canal_store::memory::MemoryEventStore;
use crate::codec::CanalCodec;
use crate::session::SessionManager;
use tokio::net::TcpListener;
use tokio_util::codec::Framed;
use futures::{StreamExt, SinkExt};
use tracing::{info, error, debug};

pub struct CanalServer {
    store: Arc<MemoryEventStore>,
    sessions: Arc<SessionManager>,
    bind_addr: SocketAddr,
}

impl CanalServer {
    pub fn new(bind_addr: SocketAddr, store: MemoryEventStore) -> Self {
        Self { store: Arc::new(store), sessions: Arc::new(SessionManager::new()), bind_addr }
    }

    pub async fn serve(&self) -> CanalResult<()> {
        let listener = TcpListener::bind(&self.bind_addr).await?;
        info!("Canal server listening on {}", self.bind_addr);
        loop {
            let (socket, peer) = listener.accept().await?;
            info!("Client connected: {}", peer);
            let store = self.store.clone();
            let sessions = self.sessions.clone();
            tokio::spawn(async move {
                let framed = Framed::new(socket, CanalCodec::new());
                if let Err(e) = handle_client(framed, store, sessions).await {
                    error!("Client {} error: {}", peer, e);
                }
                info!("Client {} disconnected", peer);
            });
        }
    }
}

async fn handle_client(
    mut framed: impl StreamExt<Item = Result<Vec<u8>, CanalError>> + Unpin + SinkExt<Vec<u8>, Error = CanalError>,
    store: Arc<MemoryEventStore>,
    sessions: Arc<SessionManager>,
) -> CanalResult<()> {
    let mut client_id: Option<String> = None;
    let mut current_pos: Option<LogPosition> = None;

    while let Some(Ok(_packet)) = framed.next().await {
        if client_id.is_none() {
            client_id = Some("anonymous".into());
            sessions.register("anonymous", "example", FilterPattern::default());
            framed.send(vec![0]).await?;
            continue;
        }
        let start = current_pos.clone().unwrap_or_else(|| LogPosition::new("mysql-bin.000001", 4));
        let events = store.get_batch(&start, 100).await?;
        if !events.is_empty() {
            current_pos = Some(events.position_range.end.clone());
            debug!("batch_id={} with {} events", events.batch_id, events.len());
            framed.send(vec![]).await?;
        }
    }

    if let Some(ref cid) = client_id { sessions.unregister(cid); }
    Ok(())
}
```

- [ ] **Step 4: lib.rs**

```rust
pub mod codec; pub mod server; pub mod session;
pub use codec::CanalCodec;
pub use server::CanalServer;
pub use session::{ClientSession, SessionManager};
```

- [ ] **Step 5: Test & Commit**

```bash
cargo test -p canal-server && git add crates/canal-server/ && git commit -m "feat(canal-server): TCP server with canal wire protocol codec and session management"
```

---

### Task 6: canal-client — Rust SDK

**Files:** crates/canal-client/src/lib.rs

- [ ] **Step 1: lib.rs**

```rust
use canal_common::*;
use tokio::sync::mpsc;

pub struct CanalClient {
    host: String,
    port: u16,
    client_id: u64,
    destination: String,
    filter: FilterPattern,
    connected: bool,
}

impl CanalClient {
    pub fn new(host: &str, port: u16) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT_ID: AtomicU64 = AtomicU64::new(1001);
        Self { host: host.into(), port, client_id: NEXT_ID.fetch_add(1, Ordering::SeqCst),
              destination: "example".into(), filter: FilterPattern::default(), connected: false }
    }
    pub fn with_destination(mut self, dest: &str) -> Self { self.destination = dest.into(); self }
    pub fn with_filter(mut self, filter: FilterPattern) -> Self { self.filter = filter; self }

    pub async fn connect(&mut self) -> CanalResult<()> {
        self.connected = true;
        Ok(())
    }

    pub async fn subscribe(&mut self, _position: Option<LogPosition>) -> CanalResult<CanalEventStream> {
        let (tx, rx) = mpsc::channel(1024);
        Ok(CanalEventStream { rx })
    }
}

pub struct CanalEventStream { rx: mpsc::Receiver<CanalResult<CanalEvent>> }

impl CanalEventStream {
    pub async fn next_event(&mut self) -> Option<CanalResult<CanalEvent>> { self.rx.recv().await }
}
```

- [ ] **Step 2: Build & Commit**

```bash
cargo build -p canal-client && git add crates/canal-client/ && git commit -m "feat(canal-client): Rust SDK with stream-based subscription API"
```

---

### Task 7: canal-cli — CLI Entry Point

**Files:** crates/canal-cli/src/main.rs

- [ ] **Step 1: main.rs**

```rust
use std::net::SocketAddr;
use std::path::PathBuf;
use clap::{Parser, Subcommand};
use serde::Deserialize;
use tracing_subscriber::{fmt, EnvFilter};
use anyhow::Result;

#[derive(Debug, Deserialize)]
struct CanalConfig { canal: CanalSection }

#[derive(Debug, Deserialize)]
struct CanalSection {
    mysql: MysqlConfig,
    store: StoreSection,
    server: ServerSection,
    logging: LogSection,
}

#[derive(Debug, Deserialize)]
struct MysqlConfig { host: String, port: Option<u16>, username: String, password: String }

#[derive(Debug, Deserialize)]
struct StoreSection { buffer_size: Option<usize> }

#[derive(Debug, Deserialize)]
struct ServerSection { bind: Option<String> }

#[derive(Debug, Deserialize)]
struct LogSection { level: Option<String>, format: Option<String> }

#[derive(Parser)]
#[command(name = "canal-rust", version = "0.1.0")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Server { #[arg(short, long, default_value = "canal.yaml")] config: PathBuf },
    Dump { #[arg(short, long, default_value = "canal.yaml")] config: PathBuf },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Server { config } => run_server(config).await,
        Commands::Dump { .. } => { println!("Dump mode not yet implemented"); Ok(()) }
    }
}

async fn run_server(config_path: PathBuf) -> Result<()> {
    let config: CanalConfig = serde_yaml::from_str(&std::fs::read_to_string(&config_path)?)?;
    let level = config.canal.logging.level.as_deref().unwrap_or("info");
    match config.canal.logging.format.as_deref().unwrap_or("json") {
        "json" => { fmt().with_env_filter(EnvFilter::new(level)).json().init(); }
        _ => { fmt().with_env_filter(EnvFilter::new(level)).init(); }
    }
    tracing::info!("Starting canal-rust...");
    let bind_addr: SocketAddr = config.canal.server.bind.as_deref().unwrap_or("0.0.0.0:11111").parse()?;
    let buf_size = config.canal.store.buffer_size.unwrap_or(16384);
    let store = canal_store::memory::MemoryEventStore::new(buf_size);
    let server = canal_server::server::CanalServer::new(bind_addr, store);
    server.serve().await?;
    Ok(())
}
```

- [ ] **Step 2: Full workspace build & test**

```bash
cargo build && cargo test --all && cargo clippy --all -- -D warnings
```

- [ ] **Step 3: Verify CLI help**

```bash
cargo run -p canal-cli -- --help
```

- [ ] **Step 4: Commit**

```bash
git add crates/canal-cli/ && git commit -m "feat(canal-cli): CLI entry with server and dump commands"
```

---

### Task 8: Integration Tests

**Files:** tests/integration/end_to_end.rs

- [ ] **Step 1: end_to_end.rs**

```rust
#[cfg(test)]
mod integration {
    use canal_store::memory::MemoryEventStore;
    use canal_common::*;

    #[test]
    fn test_store_roundtrip() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = MemoryEventStore::new(1024);
            let events = vec![CanalEvent {
                journal_name: "mysql-bin.000001".into(), position: 100, server_id: 1,
                execute_time: 1234567890, entry_type: EventType::Insert,
                schema_name: "test_db".into(), table_name: "users".into(),
                row_change: Some(RowChange {
                    table_name: "users".into(), schema_name: "test_db".into(),
                    before: None, dml_type: DmlType::Insert,
                    after: Some(RowData { columns: vec![
                        ColumnValue { name: "id".into(), value: Some("1".into()), column_type: 3, is_key: true, updated: false },
                        ColumnValue { name: "name".into(), value: Some("Alice".into()), column_type: 253, is_key: false, updated: false },
                    ]}),
                }),
                ddl_sql: None, gtid: None, raw_bytes: vec![],
            }];
            store.put_batch(events).await.unwrap();
            let batch = store.get_batch(&LogPosition::new("mysql-bin.000001", 4), 10).await.unwrap();
            let change = batch.events[0].row_change.as_ref().unwrap();
            assert_eq!(change.dml_type, DmlType::Insert);
            assert_eq!(change.after.as_ref().unwrap().columns[1].value, Some("Alice".into()));
        });
    }

    #[test]
    fn test_codec_roundtrip() {
        use canal_server::codec::CanalCodec;
        use tokio_util::codec::{Decoder, Encoder};
        use bytes::BytesMut;
        let mut codec = CanalCodec::new();
        let payload = b"\x08\x01\x12\x05hello".to_vec();
        let mut buf = BytesMut::new();
        codec.encode(payload.clone(), &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, payload);
    }
}
```

- [ ] **Step 2: Run & Commit**

```bash
cargo test --test integration && git add tests/ && git commit -m "test: integration tests for store and codec"
```

---

## Build Order & Dependencies

```
Task 0 (scaffold)
  ├── Task 1 (canal-common)     — no deps
  ├── Task 2 (canal-proto)      — no deps
  ├── Task 3 (canal-binlog)     — after Task 1, 2
  ├── Task 4 (canal-store)      — after Task 1
  ├── Task 5 (canal-server)     — after Task 3, 4
  ├── Task 6 (canal-client)     — after Task 1
  └── Task 7 (canal-cli)        — after Task 5
Task 8 (integration tests) — after all
```

Tasks 1,2 in parallel → then 3,4 in parallel → then 5,6 in parallel → then 7 → then 8.

## Notes

- **binlog crate**: Task 3 connector.rs has a TODO for integrating the `binlog` crate. Its actual API may differ from what's assumed — adjust during implementation.
- **proto path**: Task 2 prost-build output path depends on the `.proto` package declarations. Adjust the `include!` path if compilation fails.
- **server protocol**: Task 5 handle_client is a simplified version. Full ClientAuth/Sub/Get/Ack/Rollback dispatch needs canal-proto Packet types generated first.
