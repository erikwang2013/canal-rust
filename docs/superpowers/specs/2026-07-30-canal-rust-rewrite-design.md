# Canal Rust 重写 — 架构设计文档

> 状态：三期全部完成 | 日期：2026-07-30 | 更新：2026-07-31

## 1. 项目概述

将阿里巴巴 Canal（MySQL binlog 增量订阅 & 消费组件）从 Java 完整重写为 Rust，保持与现有 Canal 客户端生态的协议兼容。

- **源项目**：https://github.com/alibaba/canal
- **源码规模**：~11.2 万行 Java，18 个 Maven 模块
- **许可协议**：Apache License 2.0

## 2. 设计决策

| 决策 | 选择 | 理由 |
|------|------|------|
| 协议兼容性 | 复用 Canal 现有 `.proto` 定义 | 存量客户端零改动接入 |
| 二进制解析 | 基于 `binlog` crate 封装 | 减少 40-50% 底层工作量 |
| 异步运行时 | `tokio` | Rust 生态事实标准 |
| 序列化 | `prost` + `prost-build` | 纯 Rust protobuf 实现，编译时生成 |
| 构建系统 | Cargo workspace | Rust 标准，对应 Maven multi-module |

## 3. 分期规划

| 阶段 | 模块 | 目标 | 预计工作量 |
|------|------|------|-----------|
| **第一期 MVP** | `protocol` → `binlog` → `store` → `server` → `client` → `common` | 单机版 Canal Server：连 MySQL、解析 binlog、客户端订阅消费 | ~50% |
| **第二期 生产级** | `instance` → `filter` → `connector`（Kafka/RocketMQ）→ `sink` → `prometheus` | 多实例管理、消息队列投递、监控 | ~30% |
| **第三期 运维面** | `admin` → `deployer` → `client-adapter` → `meta` → `driver` | Web 管理后台、部署打包、生态适配 | ~20% |

## 4. 第一期架构设计

### 4.1 Crate 结构

```
canal-rust/
├── Cargo.toml                    # workspace
├── canal.yaml                    # 默认配置
├── crates/
│   ├── canal-proto/              # protobuf 生成的 Rust 代码（复用 Canal 的 .proto）
│   ├── canal-common/             # 公共类型：Entry、Position、Error
│   ├── canal-binlog/             # 基于 binlog crate 封装，MySQL binlog 获取与解析
│   ├── canal-store/              # 事件存储：ring buffer、内存/文件持久化、位点管理
│   ├── canal-server/             # protobuf wire protocol、客户端订阅管理、ACK
│   ├── canal-client/             # Rust 客户端 SDK
│   └── canal-cli/                # 二进制入口，启动 server / 管理工具
```

### 4.2 核心数据流

```
MySQL Master
    │  binlog dump protocol
    ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│ canal-binlog │───▶│ canal-store  │───▶│ canal-server │───▶│ canal-client │
│  解析event   │    │ 存ringbuffer │    │ protobuf订阅 │    │ 消费+ack     │
└──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘
```

### 4.3 技术选型对照

| 组件 | Java 版 | Rust 版 |
|------|---------|---------|
| 异步运行时 | (NIO/Netty IO) | `tokio` |
| 网络框架 | Netty | `tokio` + `tokio-util` codec |
| 序列化 | Protobuf 3 (java) | `prost` + `prost-build` |
| Binlog 解析 | `dbsync`/`parse` 自研 | `binlog` crate 封装 |
| 事件存储 | Disruptor ring buffer | tokio channel / 自定义 ring buffer |
| 配置 | Spring + properties | `serde_yaml` |
| 日志 | Logback + SLF4J | `tracing` |
| 构建 | Maven | Cargo |

### 4.4 各 Crate 职责

**canal-proto** — 纯生成代码
- 复制 Canal 上游 `CanalProtocol.proto`、`EntryProtocol.proto`、`AdminProtocol.proto`
- `prost-build` 编译生成 Rust struct/enum
- 零逻辑，仅作为其他 crate 的依赖

**canal-common** — 公共类型
- `CanalEvent`、`LogPosition`、`ClientSession` 等数据模型
- `CanalError` 错误类型（用 `thiserror`）
- `CanalLifecycle` trait（start/stop/is_running）
- 与 `canal-proto` 类型之间的 `From`/`Into` 转换

**canal-binlog** — 核心解析层
- 封装 `binlog` crate，提供高层 API
- MySQL 连接管理（dump 协议握手、认证、SSL）
- 事件流：`impl Stream<Item = Result<CanalEvent, CanalError>>`
- 支持 position/GTID 两种位点模式
- 支持 MySQL 5.1 ~ 8.0、MariaDB
- TableMap 状态管理（table_id → schema 映射）
- DDL 解析用 `sqlparser` crate 替代 Druid SQL parser
- 连接断开自动重连 + 位点续传

**canal-store** — 事件缓冲
- `CanalEventStore` trait（接口抽象）
- 基于 tokio channel 或自定义 ring buffer 的内存实现
- 批量 ACK 与位点落盘
- 接口抽象，为后续文件/磁盘持久化预留

**canal-server** — 网络服务层
- TCP codec：`[4字节BE长度][protobuf payload]`
- `tokio::spawn` 每连接一个 task（对应 Netty pipeline）
- 实现全部 wire protocol 操作：`get`/`getWithoutAck`/`ack`/`rollback`/`subscribe`/`unsubscribe`
- 客户端会话管理（client_id、订阅 filter、位点追踪）
- 单机 HA 心跳（兼容 Canal Client 的 heartbeat 机制）

**canal-client** — Rust SDK
- 与 `canal-server` 通信的 Rust 客户端
- 提供 `Stream` 式的消费 API
- 自动重连、心跳

**canal-cli** — 入口
- `canal-rust server --config canal.yaml` 启动服务
- 配置文件和命令行参数解析（`clap`）
- 信号处理（graceful shutdown）

## 5. Java→Rust 核心映射

### 5.1 interface → trait（CanalEventStore）

对应于 java-to-rust 技能模式 1：Java `interface` 映射为 Rust `trait`。

Java（CanalEventStore.java, 92行）：
```java
public interface CanalEventStore<T> extends CanalLifeCycle, CanalStoreScavenge {
    void put(List<T> data) throws InterruptedException, CanalStoreException;
    Events<T> get(Position start, int batchSize) throws ...;
    void ack(Position position) throws CanalStoreException;
    void rollback() throws CanalStoreException;
}
```

Rust：
```rust
#[async_trait]
pub trait CanalEventStore: CanalLifecycle {
    async fn put(&mut self, data: Vec<CanalEvent>) -> Result<(), CanalStoreError>;
    async fn try_put(&mut self, data: Vec<CanalEvent>) -> Result<bool, CanalStoreError>;
    async fn get(&self, start: &LogPosition, batch_size: usize)
        -> Result<Events, CanalStoreError>;
    async fn ack(&mut self, position: &LogPosition) -> Result<(), CanalStoreError>;
    async fn rollback(&mut self) -> Result<(), CanalStoreError>;
    fn latest_position(&self) -> Result<LogPosition, CanalStoreError>;
    fn first_position(&self) -> Result<Option<LogPosition>, CanalStoreError>;
}
```

关键差异：Java 用 `throws InterruptedException` + `CanalStoreException` 两类异常，Rust 统一为 `Result<T, CanalStoreError>`，阻塞等待通过 `.await` 自然表达。

### 5.2 POJO → struct（Event.java）

对应于 java-to-rust 技能模式：`@Data`/Lombok 替换为 `#[derive()]`。Java 150 行 getter/setter 压缩为 Rust 15 行。

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanalEvent {
    pub journal_name: String,
    pub position: u64,
    pub server_id: u64,
    pub execute_time: u64,
    pub entry_type: EntryType,     // prost 生成的枚举
    pub event_type: EventType,     // prost 生成的枚举
    pub gtid: Option<String>,
    pub raw_entry: Vec<u8>,
    pub rows_count: u32,
}
```

### 5.3 abstract class → trait with default methods（CanalLifeCycle）

对应于 java-to-rust 技能：Java abstract class 映射为 trait with default methods。

```rust
pub trait CanalLifecycle {
    fn start(&mut self) -> Result<(), CanalError> { Ok(()) }
    fn stop(&mut self) -> Result<(), CanalError> { Ok(()) }
    fn is_running(&self) -> bool;
}
```

### 5.4 泛型容器（Events.java）

```java
// Java: public class Events<EVENT> implements Serializable
public class Events<EVENT> {
    private PositionRange positionRange;
    private List<EVENT> events;
}
```

```rust
// Rust: 泛型 struct，无序列化样板
#[derive(Debug, Clone)]
pub struct Events {
    pub position_range: PositionRange,
    pub events: Vec<CanalEvent>,
}
```

## 6. Wire Protocol 实现

### 6.1 TCP 帧格式

```
[4 bytes BE length] [protobuf Packet payload]
```

### 6.2 Codec 实现

```rust
use tokio_util::codec::{Decoder, Encoder};
use bytes::{Buf, BufMut, BytesMut};
use prost::Message;

pub struct CanalCodec;

impl Decoder for CanalCodec {
    type Item = Packet;
    type Error = CanalError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 { return Ok(None); }
        let len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
        if src.len() < 4 + len { return Ok(None); }
        src.advance(4);
        let packet = Packet::decode(&src[..len])?;
        src.advance(len);
        Ok(Some(packet))
    }
}

impl Encoder<Packet> for CanalCodec {
    type Error = CanalError;
    fn encode(&mut self, packet: Packet, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let payload = packet.encode_to_vec();
        dst.reserve(4 + payload.len());
        dst.put_u32(payload.len() as u32);
        dst.put_slice(&payload);
        Ok(())
    }
}
```

### 6.3 Server 主循环

```rust
pub struct CanalServer {
    store: Arc<Mutex<dyn CanalEventStore>>,
    subscribers: Arc<RwLock<HashMap<String, ClientSession>>>,
}

impl CanalServer {
    pub async fn serve(addr: SocketAddr, store: impl CanalEventStore) -> Result<(), CanalError> {
        let listener = TcpListener::bind(addr).await?;
        let server = Arc::new(Self { store: Arc::new(Mutex::new(store)), ... });

        loop {
            let (socket, _) = listener.accept().await?;
            let server = server.clone();
            tokio::spawn(async move {
                let framed = Framed::new(socket, CanalCodec);
                server.handle_client(framed).await;
            });
        }
    }
}
```

### 6.4 客户端交互时序

```
Client                              Server
  │                                   │
  │──── handshake (ClientAuth) ──────▶│  认证 + 协商版本
  │◀──── ack (Packet) ───────────────│
  │                                   │
  │──── subscribe (Sub) ────────────▶│  订阅 database.table
  │◀──── ack ────────────────────────│
  │                                   │
  │──── get (timeout, batch_size) ──▶│  拉取增量事件
  │◀──── Messages (batch_id, entries)─│
  │                                   │
  │──── ack (batch_id) ─────────────▶│  确认消费
```

## 7. 错误处理

采用 java-to-rust 技能的 Mistake 2 策略：`thiserror` 替代 `catch(Exception e)`。

```rust
#[derive(Error, Debug)]
pub enum CanalError {
    #[error("binlog connection failed: {0}")]
    BinlogConnection(#[from] binlog::Error),

    #[error("position {0}:{1} not found in store")]
    PositionNotFound(String, u64),

    #[error("invalid packet: {0}")]
    Protocol(#[from] prost::DecodeError),

    #[error("authentication failed for client {0}")]
    AuthFailed(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("store error: {0}")]
    Store(String),

    #[error("configuration error: {0}")]
    Config(String),
}
```

原则：不做 `Box<dyn Error>` 的类型擦除，保留每个错误变体的语义信息。

## 8. 配置文件

```yaml
# canal.yaml
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

## 9. Cargo.toml

```toml
[workspace]
members = ["crates/*"]
resolver = "2"

[workspace.dependencies]
tokio = { version = "1", features = ["full"] }
tokio-util = { version = "0.7", features = ["codec"] }
prost = "0.13"
tonic = "0.12"
serde = { version = "1", features = ["derive"] }
serde_yaml = "0.9"
thiserror = "2"
tracing = "0.1"
tracing-subscriber = "0.3"
anyhow = "1"
bytes = "1"
clap = { version = "4", features = ["derive", "yaml"] }
async-trait = "0.1"
```

## 10. 依赖关系图

```
canal-cli ──▶ canal-server ──▶ canal-store ──▶ canal-common
                  │
                  ▼
            canal-binlog ──▶ canal-proto ──▶ canal-common
                  │
                  ▼
canal-client ──▶ canal-server (连接远端)
```

## 11. 测试策略

| 策略 | 工具 | 说明 |
|------|------|------|
| 对等测试 | 同 MySQL binlog，Java Canal 和 Rust 分别解析，diff protobuf 输出 | 验证正确性基线 |
| 快照测试 | `insta` crate | 解析结果序列化为快照，回归时自动 diff |
| Fuzz 测试 | `cargo-fuzz` / `libfuzzer` | 随机变异 binlog payload，确保 parser 不 panic |
| 集成测试 | `testcontainers` crate 拉起 MySQL 容器 | 端到端测试完整链路 |
| CI | `cargo test && cargo clippy && cargo fmt --check` | 每次提交自动执行 |

## 12. 待定事项

- [ ] 第二期 instance/filter/connector 详细设计
- [ ] HA/集群方案（ZooKeeper vs etcd vs 自研 raft）
- [ ] Admin Web UI 选型

## 13. 变更记录

| 日期 | 变更 |
|------|------|
| 2026-07-30 | 初始版本：分期规划 + 第一期架构设计 |
| 2026-07-30 | 新增：Java→Rust 核心映射、Wire Protocol 实现、错误处理、配置文件、Cargo.toml、测试策略 |
| 2026-07-31 | 三期全部完成：14 crate，100 测试，版本 v1.1.3。Kafka connector，多实例管理，Prometheus，Admin API，Docker 部署。七轮审查修复（v1~v7）：死锁、竞态、认证、TLS、BLOB hex 编码、auth 测试覆盖、serde_yml→serde_yaml 迁移、服务关闭超时、客户端轮询优化、Kafka spawn_blocking、send_ack 拆分等 60+ 项 |
