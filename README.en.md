# Canal Rust

[中文](README.md)

MySQL binlog incremental subscription & consumption, rewritten in Rust from [Alibaba Canal](https://github.com/alibaba/canal).

## Overview

Canal Rust emulates a MySQL slave, sends dump requests to MySQL master, receives and parses binary log events, then delivers them to downstream consumers via the Canal protobuf protocol.

**Key Features:**

- **Real-time binlog parsing** — Powered by `mysql_cdc`, supports MySQL 5.1~8.0 / MariaDB with position and GTID modes
- **Protocol compatibility** — Reuses upstream Canal `.proto` definitions; existing Java/Go/Python/C#/Node.js clients work without modification
- **Event storage** — In-memory ring buffer with automatic overflow eviction and client ACK tracking
- **Table/schema filtering** — Regex-based include/exclude patterns
- **Message queue** — Kafka connector with JSON flat message serialization
- **Multi-instance** — Run multiple Canal destinations in one process, each with independent binlog/store/filter
- **Observability** — Prometheus counters/gauges + `/metrics` endpoint
- **Admin API** — RESTful API for instance start/stop and status queries
- **Docker** — Multi-stage build + docker-compose

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     Canal Rust                           │
│                                                         │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐            │
│  │  MySQL    │──▶│canal-    │──▶│canal-    │──── TCP ──▶│ Clients
│  │  Master   │   │ binlog   │   │ store    │            │ (Java/Go/
│  └──────────┘   │          │   │(ringbuf) │            │  Python/..)
│                 └────┬─────┘   └────┬─────┘            │
│                      │              │                   │
│                 ┌────▼─────┐   ┌────▼─────┐            │
│                 │canal-    │   │canal-    │            │
│                 │ filter   │   │  sink    │──▶ Kafka   │
│                 │(regex)   │   │(pipeline)│            │
│                 └──────────┘   └──────────┘            │
│                                                         │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐            │
│  │canal-    │   │canal-    │   │canal-    │            │
│  │ instance │   │ admin    │   │prometheus│            │
│  │(manager) │   │(REST API)│   │(metrics) │            │
│  └──────────┘   └──────────┘   └──────────┘            │
└─────────────────────────────────────────────────────────┘
```

**Data Flow:**

```
MySQL Master
    │  binlog dump (mysql_cdc)
    ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│ Event        │───▶│ EventFilter  │───▶│ EventSink    │
│ Converter    │    │ (regex)      │    │ ┌─ store ───▶ Server ──▶ Client
│(TableMap)    │    └──────────────┘    │ └─ connector─▶ Kafka
└──────────────┘                        └──────────────┘
```

## Design

### Key Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Protocol | Reuse Canal `.proto` | Zero-change for existing clients |
| Binlog | `mysql_cdc` crate | Mature CDC library, MySQL/MariaDB |
| Runtime | `tokio` | Rust ecosystem standard |
| Serialization | `prost` + `prost-build` | Pure Rust protobuf |
| Store | Custom ring buffer | Zero external deps, `VecDeque`-based |
| Config | `serde_yaml` | YAML format |
| Logging | `tracing` | Structured, span-based |
| Web | `axum` | Admin API + Prometheus endpoint |
| Build | Cargo workspace | Independent crate compilation |

### Technology Comparison

| Component | Java Canal | Canal Rust |
|-----------|-----------|------------|
| Language | Java 8 | Rust (stable) |
| Runtime | JVM | Native binary (AOT) |
| Networking | Netty 4.x | `tokio` + `tokio-util` codec |
| Serialization | Protobuf 3 (java) | `prost` |
| Binlog | `dbsync` (custom) | `mysql_cdc` |
| Event Store | LMAX Disruptor | Custom ring buffer |
| DI | Spring 5 | Constructor injection |
| Config | Spring properties | `serde_yaml` |
| Logging | Logback + SLF4J | `tracing` |
| Build | Maven | Cargo |
| Expression | Aviator | `regex` |

## Quick Start

### Prerequisites

- Rust 1.80+
- MySQL 5.7+ / 8.0 (binlog enabled, ROW format)
- (Optional) Kafka for message queue output

### Installation

```bash
git clone https://github.com/erikwang2013/canal-rust.git
cd canal-rust
cargo build --release
```

### Configure MySQL

```sql
CREATE USER 'canal'@'%' IDENTIFIED BY 'canal';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%';
FLUSH PRIVILEGES;

SHOW VARIABLES LIKE 'log_bin';
SHOW MASTER STATUS;
```

### Configure canal.yaml

```yaml
canal:
  mysql:
    host: "127.0.0.1"
    port: 3306
    username: "canal"
    password: "canal"
  store:
    buffer_size: 16384
  server:
    bind: "0.0.0.0:11111"
  logging:
    level: "info"
    format: "json"
```

### Start Server

```bash
cargo run --release -- server --config canal.yaml
cargo run --release -- --help
```

### Client Usage

```rust
use canal_client::CanalClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = CanalClient::new("127.0.0.1", 11111)
        .with_destination("example");

    client.connect().await?;
    let mut stream = client.subscribe(None).await?;

    while let Some(Ok(event)) = stream.next_event().await {
        println!("{}: {} @ {}.{}", event.journal_name, event.position,
                 event.schema_name, event.table_name);
    }
    Ok(())
}
```

### Docker

```bash
docker compose -f docker/docker-compose.yml up -d
```

## Project Structure

```
canal-rust/
├── Cargo.toml                     # Cargo workspace, 15 member crates
├── canal.yaml                     # Default configuration
├── rust-toolchain.toml
├── README.md                      # Chinese README
├── README.en.md                   # This file (English)
├── proto/                         # Upstream Canal .proto files
│   ├── CanalProtocol.proto        # Main protocol
│   └── EntryProtocol.proto        # Event definitions
├── docker/                        # Docker deployment
│   ├── Dockerfile                 # Multi-stage build
│   └── docker-compose.yml         # One-command startup
├── crates/
│   ├── canal-common/              # Core types
│   ├── canal-proto/               # Protobuf code generation
│   ├── canal-binlog/              # MySQL binlog parser
│   ├── canal-store/               # Event store (ring buffer)
│   ├── canal-filter/              # Regex table/schema filter
│   ├── canal-sink/                # Event dispatch pipeline
│   ├── canal-connector/           # Kafka connector
│   ├── canal-instance/            # Multi-instance manager
│   ├── canal-server/              # TCP server (wire protocol)
│   ├── canal-client/              # Rust client SDK
│   ├── canal-meta/                # Table schema cache
│   ├── canal-admin/               # REST Admin API
│   ├── canal-prometheus/          # Prometheus metrics endpoint
│   └── canal-cli/                 # CLI entry point
├── docs/superpowers/
│   ├── specs/
│   │   └── 2026-07-30-canal-rust-rewrite-design.md
│   └── plans/
│       └── 2026-07-30-canal-rust-phase1.md
└── tests/
    └── integration/
```

## Stats

| Metric | Value |
|--------|-------|
| Crates | 14 |
| Lines of Rust | ~5,800 |
| Tests | 88 |
| Proto definitions | 2 |
| Clippy warnings | 0 |
| License | Apache-2.0 |

## Usage Guide

### Scenario 1: Real-time sync to Kafka

```rust
use canal_connector::kafka::KafkaConnector;
use canal_instance::{CanalInstance, InstanceConfig};
use canal_common::{FilterPattern, LogPosition};

let kafka = KafkaConnector::new("kafka-sync", "localhost:9092", "mysql-changes");
let config = InstanceConfig {
    destination: "mydb-sync".into(),
    mysql_host: "10.0.0.1".into(),
    mysql_port: 3306,
    mysql_username: "replicator".into(),
    mysql_password: "secret".into(),
    mysql_server_id: 2001,
    start_position: LogPosition::new("mysql-bin.000001", 4),
    filter: FilterPattern { pattern: "mydb\\..*".into(), black_list: "".into() },
    store_buffer_size: 16384,
    connector_names: vec!["kafka-sync".into()],
};
```

### Scenario 2: Multi-instance

```rust
let manager = InstanceManager::new();

manager.register(CanalInstance::new(
    InstanceConfig { destination: "prod".into(), ... }, vec![kafka_prod],
)).await;

manager.register(CanalInstance::new(
    InstanceConfig { destination: "test".into(), ... }, vec![kafka_test],
)).await;

manager.start_all().await?;
```

### Scenario 3: Prometheus Monitoring

```bash
curl http://localhost:9090/metrics
# canal_events_parsed_total 150000
# canal_events_dispatched_total 146800
# canal_instances_active 2
```

## Development

```bash
cargo build --release              # Build
cargo test --all                   # Run tests
cargo clippy --all -- -D warnings  # Lint
cargo fmt --check --all            # Format check
cargo doc --open                   # Docs
```

## Related

- [Alibaba Canal (Java)](https://github.com/alibaba/canal)
- [mysql_cdc crate](https://crates.io/crates/mysql_cdc)
- [Canal Protocol](https://github.com/alibaba/canal/wiki/ClientAPI)

## License

Apache License 2.0
