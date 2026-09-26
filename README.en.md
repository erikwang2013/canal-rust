# Canal Rust

[中文](README.md)

<p align="center">
  <img src="docs/assets/canal-pet.svg" width="620"
       alt="小运 — the Canal Rust mascot: a rust-red crab standing on the lock wall of the data canal, one claw on the paddle wheel, the other carrying a glowing change event, with downstream data packets drifting in the water below" />
</p>

<p align="center"><b>小运 · Canal Crab</b> — the crab keeping the lock on the data canal<br/>
<sub>one claw on the paddle wheel, the other carrying change events downstream</sub></p>

MySQL binlog incremental subscription & consumption, rewritten in Rust from [Alibaba Canal](https://github.com/alibaba/canal).

## Overview

Canal Rust emulates a MySQL slave, sends dump requests to MySQL master, receives and parses binary log events, then delivers them to downstream consumers via the Canal protobuf protocol.

14 crates cover one complete path: **ingest → parse → filter → store → egress → ack**, while keeping the wire protocol byte-compatible with Java Canal.

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

## Project Mascot

**小运 (Canal Crab)** — a rust-red crab, and the **lock keeper** of this data canal.

The design turns the project's job into a character. MySQL keeps producing changes upstream, consumers downstream can fall behind, and something in the middle has to **control the flow**. So 小运 stands on the lock wall: one claw on the paddle wheel (how much data gets through), the other carrying a glowing change event, with released data packets drifting in the water below and a binlog scroll still floating in from upstream.

| In the picture | In the project |
|---|---|
| Rust-red shell | Rust |
| The paddle wheel | `canal-store`'s ring buffer and its capacity/eviction control |
| The event in the right claw | one event fanned out by `canal-sink` |
| Packets drifting below the lock | `canal-server` delivering over TCP, `canal-connector` delivering to Kafka |
| The binlog scroll upstream | raw events `canal-binlog` pulled from MySQL |
| The canal underfoot | the pipeline itself |

- Vector original: [`docs/assets/canal-pet.svg`](docs/assets/canal-pet.svg)
- It also lives in the code: `canal --help` prints the ASCII version (`canal_cli::CANAL_CRAB`), and `canal server` reports for duty once in the startup log.

```
     \    \              /    /
      \    \____________/    /
    ___\_                  _/___
   /     o                o     \
  |               __              |
   \          \________/         /
    '.__________________________.'
   __/   /     |      |     \   \__
  |==================================|
  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
```

## Architecture

![Canal Rust architecture](docs/assets/architecture.svg)

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

**Layer responsibilities:**

| Layer | Crate | Responsibility |
|-------|-------|----------------|
| Ingest | `canal-binlog` | Connect to MySQL, pull binlog, convert to a unified event model |
| Process | `canal-filter` · `canal-sink` | Regex schema/table filtering, fan events out to every sink |
| Egress | `canal-store` · `canal-server` · `canal-connector` | Ring-buffer storage, TCP protocol service, Kafka delivery |
| Orchestration | `canal-instance` | Manage multiple destinations, each owning a full pipeline |
| Operations | `canal-admin` · `canal-prometheus` · `canal-cli` | Start/stop API, metrics, command-line entry point |
| Foundation | `canal-common` · `canal-proto` · `canal-meta` | Types / lifecycle traits, protobuf codegen, schema cache |

## Functional Design

![Canal Rust functional design](docs/assets/functions.svg)

The 14 capabilities map one-to-one onto the 14 crates, grouped into **Ingest**, **Process**, **Egress** and **Operations**. Ingest and Process decide *what you can capture*; Egress and Operations decide *how far it travels and how clearly you can see it*.

## Lifecycle

![Canal Rust lifecycle](docs/assets/lifecycle.svg)

Three independent tracks — a fault in any one of them never blocks a graceful shutdown of the others:

| Track | Owner | States |
|-------|-------|--------|
| **Instance** | `canal-instance` | Created → Starting → Running (`feed()` loop) → Stopping → Stopped |
| **Session** | `canal-server` | accept → Handshake → ClientAuth → Sub → Get → Ack/Rollback → Close |
| **Process** | `canal-cli` | load config → init → register instances → bind port → binlog loop → SIGINT → graceful shutdown |

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

- Rust 1.85+ (`clap` and other dependencies now use edition 2024, which requires rustc ≥ 1.85)
- **`protoc`** (Protocol Buffers compiler) — `canal-proto`'s `build.rs` generates code with
  `prost-build`; without it **every compile fails** with `Could not find 'protoc'`
  - Debian/Ubuntu: `apt-get install protobuf-compiler`
  - macOS: `brew install protobuf`
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
CREATE USER 'canal'@'%' IDENTIFIED WITH mysql_native_password BY 'canal';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%';
FLUSH PRIVILEGES;

SHOW VARIABLES LIKE 'log_bin';
SHOW MASTER STATUS;
```

> **⚠️ The connection is unencrypted.** Upstream `mysql_cdc 0.2.1` implements no TLS at
> all — it calls `unimplemented!()` on any `SslMode` other than `Disabled`, so this project
> can only reach MySQL in plaintext: credentials and binlog data are both unencrypted. Use
> it on a trusted private network only, and tunnel over SSH across untrusted segments.
>
> **Auth plugin limitation:** `mysql_cdc` supports only `mysql_native_password` and
> `caching_sha2_password`. An account using `sha256_password` fails with
> `sha256_password auth plugin is not supported.` — which is why the `CREATE USER` above
> names `mysql_native_password` explicitly.

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
    metrics_bind: "127.0.0.1:9090"
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
├── Cargo.toml                      # Cargo workspace, 14 member crates
├── canal.yaml                      # Default configuration
├── canal.yaml.example              # Annotated configuration template
├── rust-toolchain.toml             # Pinned toolchain (stable + rustfmt + clippy)
├── Makefile                        # build / test / clippy / fmt / check shortcuts
├── README.md                       # Chinese README
├── README.en.md                    # This file (English)
├── proto/                          # Upstream Canal .proto files
│   ├── CanalProtocol.proto         # Main protocol (Packet, Handshake, Messages)
│   └── EntryProtocol.proto         # Event definitions (Entry, RowChange, Column)
├── docker/                         # Docker deployment
│   ├── Dockerfile                  # Multi-stage build
│   └── docker-compose.yml          # One-command startup
├── crates/                         # 14 member crates
│   ├── canal-common/               # Core types
│   │   └── src/ {error, types, lifecycle, utils}.rs
│   ├── canal-proto/                # Protobuf code generation (prost-build)
│   │   ├── build.rs
│   │   └── src/ {lib, com.alibaba.otter.canal.protocol}.rs
│   ├── canal-binlog/               # MySQL binlog parser
│   │   └── src/ {connector, converter, table_map, column_serde}.rs
│   ├── canal-store/                # Event store (ring buffer + position)
│   │   └── src/memory.rs
│   ├── canal-filter/               # Regex table/schema filter
│   │   └── src/lib.rs
│   ├── canal-sink/                 # Event dispatch pipeline (store + connector fan-out)
│   │   └── src/ {sink, connector}.rs
│   ├── canal-connector/            # Kafka connector
│   │   └── src/ {kafka, kafka_tests_extra}.rs
│   ├── canal-instance/             # Multi-instance manager
│   │   └── src/instance.rs
│   ├── canal-server/               # TCP server (wire protocol)
│   │   └── src/ {codec, session, server, conversion}.rs
│   ├── canal-client/               # Rust client SDK
│   │   └── src/lib.rs
│   ├── canal-meta/                 # DDL tracking + table schema cache
│   │   └── src/lib.rs
│   ├── canal-admin/                # REST Admin API (Axum)
│   │   └── src/lib.rs
│   ├── canal-prometheus/           # Prometheus metrics endpoint
│   │   └── src/metrics_server.rs
│   └── canal-cli/                  # CLI entry point (server / dump)
│       └── src/ {main, lib}.rs     # lib.rs holds CLI defs, config loading, mascot
├── docs/
│   ├── assets/                     # Images
│   │   ├── canal-pet.svg           # 小运 · project mascot
│   │   ├── architecture.svg        # Architecture diagram
│   │   ├── functions.svg           # Functional design diagram
│   │   └── lifecycle.svg           # Lifecycle diagram
│   ├── superpowers/
│   │   ├── specs/2026-07-30-canal-rust-rewrite-design.md
│   │   └── plans/2026-07-30-canal-rust-phase1.md
│   └── review-report-*.md          # Historical review / test reports
```

> There is no top-level `tests/` directory: tests live next to the code they cover, inside the same crate.

- Unit tests: `crates/*/src/tests*.rs`, `#[cfg(test)] mod tests`
- Integration tests: `crates/*/tests/*.rs` (e.g. `canal-server/src/tests_e2e.rs`, `canal-client/tests/e2e.rs`)

## Stats

| Metric | Value |
|--------|-------|
| Crates | 14 |
| Lines of Rust (source) | ~8,400 |
| Lines of Rust (tests) | ~3,900 |
| Unit / integration tests | 383 (all passing) |
| Proto definitions | 2 |
| Version | v2.1.0 |
| Clippy warnings | 0 |
| License | Apache-2.0 |

## Usage Guide

### Scenario 1: Real-time sync to Kafka

```rust
use canal_connector::kafka::{KafkaConfig, KafkaConnector};
use canal_instance::{CanalInstance, InstanceConfig};
use canal_common::{FilterPattern, LogPosition};
use std::sync::Arc;

let kafka_config = KafkaConfig::new("localhost:9092", "mysql-changes");
let kafka = Arc::new(KafkaConnector::new("kafka-sync", kafka_config).unwrap());
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

### Scenario 4: Managing instances via the Admin API

Bind rule for the Admin API: by default it is **main port + 1, bound to loopback only** (`bind: 127.0.0.1:11111` → `127.0.0.1:11112`). To make it reachable from outside — from another host, or from the host into a container — you must set `server.admin_bind` explicitly:

```yaml
canal:
  server:
    bind: "0.0.0.0:11111"
    admin_bind: "0.0.0.0:11112"   # omitted => 127.0.0.1:{main port + 1}
```

```bash
curl http://localhost:11112/health
curl http://localhost:11112/api/instances
curl -X POST http://localhost:11112/api/instances/default/stop
```

## Development

```bash
cargo build --release              # Build
cargo test --all                   # Run tests
cargo clippy --all-targets -- -D warnings  # Lint (matches CI)
cargo fmt --check --all            # Format check
cargo doc --open                   # Docs
```

## Related

- [Alibaba Canal (Java)](https://github.com/alibaba/canal)
- [mysql_cdc crate](https://crates.io/crates/mysql_cdc)
- [Canal Protocol](https://github.com/alibaba/canal/wiki/ClientAPI)

## License

Apache License 2.0

---

## Support

If this project helps you, feel free to buy us a coffee ☕

<p align="center">
  <table align="center">
    <tr>
      <td align="center" width="200">
        <img src="docs/weixinpay.png" alt="WeChat Pay" width="130" height="130" /><br/>
        <b>WeChat Pay</b>
      </td>
      <td align="center" width="200">
        <img src="docs/alipay.png" alt="Alipay" width="130" height="130" /><br/>
        <b>Alipay</b>
      </td>
    </tr>
  </table>
</p>
