# Canal Rust

[English](README.en.md)

MySQL binlog 增量订阅 & 消费组件，使用 Rust 重写 [阿里巴巴 Canal](https://github.com/alibaba/canal)。

## 项目简介

Canal Rust 模拟 MySQL slave 的交互协议，向 MySQL master 发送 dump 请求，接收并解析 binary log 事件，通过 protobuf 协议提供给下游消费者。

**核心能力：**

- **binlog 实时解析** — 基于 `mysql_cdc` 连接 MySQL 5.1~8.0 / MariaDB，支持 position 和 GTID 位点
- **协议兼容** — 复用 Canal 上游 `.proto`，现有 Java/Go/Python/C#/Node.js 客户端零改动接入
- **事件存储** — 内存 ring buffer，支持容量溢出自动淘汰和客户端消费 Ack
- **表/库过滤** — 正则表达式过滤，支持 include/exclude 模式
- **消息队列** — Kafka connector（JSON flat message）
- **多实例管理** — 单进程运行多个 Canal destination，各自独立 binlog/存储/过滤
- **可观测性** — Prometheus counter/gauge + `/metrics` 端点
- **管理 API** — RESTful Admin API（实例启停、状态查询）
- **Docker 部署** — 多阶段构建 + docker-compose

## 架构设计

```
┌─────────────────────────────────────────────────────────┐
│                     Canal Rust                           │
│                                                         │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐            │
│  │  MySQL    │──▶│canal-    │──▶│canal-    │──── TCP ──▶│ 客户端
│  │  Master   │   │ binlog   │   │ store    │            │ (Java/Go/
│  └──────────┘   │          │   │(ringbuf) │            │  Python/..)
│                 └────┬─────┘   └────┬─────┘            │
│                      │              │                   │
│                 ┌────▼─────┐   ┌────▼─────┐            │
│                 │canal-    │   │canal-    │            │
│                 │ filter   │   │  sink    │──▶ Kafka   │
│                 │(正则过滤) │   │(分发管道) │            │
│                 └──────────┘   └──────────┘            │
│                                                         │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐            │
│  │canal-    │   │canal-    │   │canal-    │            │
│  │ instance │   │ admin    │   │prometheus│            │
│  │(实例管理) │   │(REST API)│   │(metrics) │            │
│  └──────────┘   └──────────┘   └──────────┘            │
└─────────────────────────────────────────────────────────┘
```

**核心数据流：**

```
MySQL Master
    │  binlog dump (mysql_cdc)
    ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│ Event        │───▶│ EventFilter  │───▶│ EventSink    │
│ Converter    │    │ (regex)      │    │ ┌─ store ───▶ Server ──▶ Client
│(TableMap映射) │    └──────────────┘    │ └─ connector─▶ Kafka
└──────────────┘                        └──────────────┘
```

## 项目设计

### 设计决策

| 决策 | 选择 | 理由 |
|------|------|------|
| 协议兼容 | 复用 Canal `.proto` | 存量客户端零改动 |
| binlog 解析 | `mysql_cdc` crate | 成熟 CDC 库，支持 MySQL/MariaDB |
| 异步运行时 | `tokio` | Rust 生态标准 |
| 序列化 | `prost` + `prost-build` | 纯 Rust protobuf |
| 事件存储 | 自定义 ring buffer | 零外部依赖，基于 `VecDeque` |
| 配置 | `serde_yaml` | YAML，与 Java 版 `properties` 对标 |
| 日志 | `tracing` | 结构化日志，span-based |
| Web 框架 | `axum` | Admin API + Prometheus 端点 |
| 构建 | Cargo workspace | 多 crate 独立编译 |

### Crate 依赖关系

```
canal-common ────────────────────────────────────────────── 基础类型
    │
    ├── canal-proto ─────────────────────────────────────── protobuf 代码生成
    ├── canal-binlog ──▶ canal-proto ────────────────────── binlog 解析
    ├── canal-store ─────────────────────────────────────── 事件存储
    ├── canal-filter ────────────────────────────────────── 表/库过滤
    │
    ├── canal-sink ──▶ canal-store + canal-filter ──────── 事件管道
    ├── canal-connector ──▶ canal-sink ──────────────────── Kafka 输出
    ├── canal-instance ──▶ binlog + store + filter + sink ─ 多实例管理
    ├── canal-server ──▶ store + binlog + proto ─────────── TCP 协议服务
    ├── canal-client ──▶ proto ──────────────────────────── Rust SDK
    ├── canal-meta ──▶ common ───────────────────────────── 表结构缓存
    ├── canal-admin ──▶ instance ────────────────────────── REST API
    ├── canal-prometheus ────────────────────────────────── metrics
    │
    └── canal-cli ──▶ server + store + binlog + common ──── 命令行入口
```

### 技术选型对照

| 组件 | Java Canal | Canal Rust |
|------|-----------|------------|
| 语言 | Java 8 | Rust (stable) |
| 运行时 | JVM | Native binary (AOT) |
| 网络框架 | Netty 4.x | `tokio` + `tokio-util` codec |
| 序列化 | Protobuf 3 (java) | `prost` |
| Binlog | `dbsync` 自研 | `mysql_cdc` |
| 事件存储 | LMAX Disruptor | 自定义 ring buffer |
| DI 容器 | Spring 5 | 构造函数注入 |
| 配置 | Spring properties | `serde_yaml` |
| 日志 | Logback + SLF4J | `tracing` |
| SQL 解析 | Druid | `sqlparser` |
| 构建工具 | Maven | Cargo |
| 表达式引擎 | Aviator | `regex` |

## 快速开始

### 环境要求

- Rust 1.80+
- MySQL 5.7+ / 8.0（开启 binlog，格式 ROW）
- （可选）Kafka 用于消息队列输出

### 安装

```bash
git clone https://github.com/erikwang2013/canal-rust.git
cd canal-rust
cargo build --release
```

### 配置 MySQL 主库

```sql
CREATE USER 'canal'@'%' IDENTIFIED BY 'canal';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%';
FLUSH PRIVILEGES;

SHOW VARIABLES LIKE 'log_bin';
SHOW MASTER STATUS;
```

### 配置 canal.yaml

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

### 启动服务

```bash
cargo run --release -- server --config canal.yaml
cargo run --release -- --help
```

### 使用客户端连接

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

### Docker 部署

```bash
docker compose -f docker/docker-compose.yml up -d
```

## 项目结构

```
canal-rust/
├── Cargo.toml                     # Cargo workspace，14 个成员 crate
├── canal.yaml                     # 默认配置文件
├── rust-toolchain.toml            # Rust 工具链固定
├── README.md                      # 本文件（中文）
├── README.en.md                   # English README
├── proto/                         # Canal 上游 .proto 文件
│   ├── CanalProtocol.proto        # 主协议（Packet, Handshake, Messages）
│   └── EntryProtocol.proto        # 事件定义（Entry, RowChange, Column）
├── docker/                        # Docker 部署
│   ├── Dockerfile                 # 多阶段构建
│   └── docker-compose.yml         # 一键启动
├── crates/
│   ├── canal-common/              # 基础类型
│   │   └── src/ {error, types, lifecycle, utils}.rs
│   ├── canal-proto/               # protobuf 代码生成（prost-build）
│   │   └── src/ {lib, build}.rs
│   ├── canal-binlog/              # MySQL binlog 解析层
│   │   └── src/ {connector, converter, table_map}.rs
│   ├── canal-store/               # 事件存储：ring buffer + 位点管理
│   │   └── src/ {memory, position}.rs
│   ├── canal-filter/              # 正则表/库过滤（include/exclude）
│   │   └── src/lib.rs
│   ├── canal-sink/                # 事件分发管道（store + connector 扇出）
│   │   └── src/ {sink, connector}.rs
│   ├── canal-connector/           # Kafka 连接器
│   │   └── src/kafka.rs
│   ├── canal-instance/            # 多实例管理
│   │   └── src/instance.rs
│   ├── canal-server/              # TCP 服务（Canal wire protocol）
│   │   └── src/ {codec, session, server, tests}.rs
│   ├── canal-client/              # Rust 客户端 SDK
│   │   └── src/lib.rs
│   ├── canal-meta/                # DDL 追踪 + 表结构缓存
│   │   └── src/lib.rs
│   ├── canal-admin/               # REST Admin API（Axum）
│   │   └── src/lib.rs
│   ├── canal-prometheus/          # Prometheus metrics 端点
│   │   └── src/metrics_server.rs
│   └── canal-cli/                 # 命令行入口（server / dump）
│       └── src/main.rs
├── docs/superpowers/
│   ├── specs/
│   │   └── 2026-07-30-canal-rust-rewrite-design.md   # 完整设计文档
│   └── plans/
│       └── 2026-07-30-canal-rust-phase1.md            # 第一期实现计划
└── tests/
    └── integration/               # 集成测试
```

## 项目统计

| 指标 | 数值 |
|------|------|
| Crates | 14 |
| Rust 源码行数 | ~5,900 |
| 单元/集成测试 | 98 |
| Protobuf 定义 | 2 |
| 版本 | v1.1.5 |
| Clippy 警告 | 0 |
| 许可协议 | Apache-2.0 |

## 使用教程

### 场景 1：数据库实时同步到 Kafka

```yaml
canal:
  mysql:
    host: "10.0.0.1"
    username: "replicator"
    password: "secret"
  filter:
    pattern: "mydb\\..*"
```

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

### 场景 2：多实例部署

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

### 场景 3：Prometheus 监控

```bash
curl http://localhost:9090/metrics
# canal_events_parsed_total 150000
# canal_events_dispatched_total 146800
# canal_instances_active 2
```

## 开发

```bash
cargo build --release              # 编译
cargo test --all                   # 运行测试
cargo clippy --all -- -D warnings  # 代码检查
cargo fmt --check --all            # 格式检查
cargo doc --open                   # 文档
```

## 相关资源

- [阿里巴巴 Canal (Java)](https://github.com/alibaba/canal)
- [mysql_cdc crate](https://crates.io/crates/mysql_cdc)
- [Canal Protocol 文档](https://github.com/alibaba/canal/wiki/ClientAPI)

## 许可

Apache License 2.0
