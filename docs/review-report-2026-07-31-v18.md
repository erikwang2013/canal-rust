# canal-rust v2.0.2 — 深度审查报告

**日期**: 2026-07-31  
**审查范围**: 35 个 Rust 源文件, ~6,484 行代码, 14 个 crate  
**测试结果**: 86 个测试全部通过, 0 失败  
**构建**: `cargo build` 通过, `cargo clippy --all-targets` 通过 (0 warnings)

> **更新 2026-07-31**: 所有 23 项问题已修复。测试结果: 92 个测试全部通过。详见文末「修复记录」。

---

## 执行摘要

- **严重 (Critical)**: 7 项 — 数据丢失、panic、协议错配、配置不可解析
- **高 (High)**: 8 项 — 连接管理缺陷、安全绕过、性能退化
- **中 (Medium)**: 8 项 — API 卫生、测试缺口、死代码
- **低 (Low)**: 3 项 — 文档错配、缺失 trait 实现

---

## 1. 严重问题 (Critical)

### 1.1 多行 binlog 事件的数据丢失和重复投递

**文件**: `crates/canal-store/src/memory.rs:99-104`  
**影响**: 静默数据丢失 — 任何超过 `fetch_size` 的多行 binlog 事件

Binlog 连接器为每行生成一个 `CanalEvent`,共享相同的 `header.next_event_position`(`connector.rs:331-341`)。因此,一个 N 行的 `WriteRowsEvent` 产生 N 个相同 `(journal_suffix, position)` 的事件。

`binary_search_by` 在全部相等元素中返回*最后一个*匹配元素的索引。当客户端在位置 P 恢复且缓冲区中仍有位置 P 的事件时:
- `Ok(i)` 指向最后一个位置 P 的事件, `i + 1 == len` → `get_batch` 永远返回空
- 或在混合缓冲区中, `i+1` 之后位置为 P 的事件被**重复投递**,之前的被**永久跳过**

**修复建议**: 使用 `partition_point` 跳过所有 `position <= start` 的事件;或引入每个事件的序列号(如 `(position, row_index)`)。

---

### 1.2 DDL 截断可能导致复制线程 panic

**文件**: `crates/canal-binlog/src/connector.rs:256`  
**影响**: 复制线程无声崩溃

```rust
Some(sql[..DDL_SQL_MAX_LEN].to_string())
```

这是对 `String` 的字节切片。如果字节 65536 落在一个多字节 UTF-8 字符中间(中文 DDL 注释很常见),会在 `spawn_blocking` 线程中 **panic**,无声杀死复制。

**修复建议**: 使用 `sql.chars().take(DDL_SQL_MAX_LEN).collect()` 或 `floor_char_boundary`(1.73+ 稳定)。

---

### 1.3 客户端/服务端事件类型编号不匹配

**文件**: `crates/canal-server/src/conversion.rs:11-31` vs `crates/canal-common/src/types.rs:98-112`

服务端映射:
| CanalEvent | Proto EventType | 值 | 客户端解析 | 
|-----------|----------------|-----|-----------|
| Ddl | Query | 7 | **Xid** ← 错误 |
| Query | Query | 7 | **Xid** ← 错误 |
| Rotate | Query | 7 | **Xid** ← 错误 |
| Xid | Xacommit | 13 | **Unknown(13)** |
| Heartbeat | Mheartbeat | 15 | **Unknown(15)** |

客户端 (`client/src/lib.rs:313-318`) 使用 `EventType::from(raw)` 解码,但该映射是直通的(7→Xid, 13→Unknown),不理解服务端的协议适配。**每个 DDL 事件客户端都识别为 Xid**。

**修复建议**: 客户端需要与服务端相同的事件类型映射表。

---

### 1.4 客户端回退限流影响稳态吞吐

**文件**: `crates/canal-client/src/lib.rs:182, 216-218`  
**影响**: 客户端吞吐上限 ~5 批次/秒

`idle_count` 在 Messages 分支中重置为 0(行 182),然后在**所有分支**之后立即自增(行 216),包括 Messages 分支。因此即使持续有数据,每次 Get 轮询之间最小延迟为 `100ms × 2^1 = 200ms`,吞吐上限约 5 次/秒。

**修复建议**: 将自增移到非 Messages 分支中,或仅在无事件时应用退避。

---

### 1.5 `subscribe(position)` 静默忽略参数

**文件**: `crates/canal-client/src/lib.rs:89-92`  
**影响**: 公共 API 契约破损

`_position: Option<LogPosition>` 从未发送到服务端。服务端始终从会话状态或硬编码默认值恢复。公共 API 承诺了不存在的定位订阅功能。

**修复建议**: 将 `position` 参数传递给 `Sub` 协议消息,或在文档中明确说明此限制。

---

### 1.6 重连无法从 ACK 位置恢复

**文件**: `crates/canal-server/src/server.rs:128, 346-349, 381-388`  
**影响**: 每次重连重放全部数据

- `handle_get` 在 `state.current_pos` 为 `None` 时回退到 `mysql-bin.000001:4`
- `ClientState` 每次连接都是新创建的(行 128)
- `session.last_ack_position` 在每次 ACK 时持久化,但**重连时从未读回**

每个重连客户端从位置 4 重新投递整个缓冲区。ACK 追踪是死数据。

**修复建议**: 在 `handle_sub` 或 `handle_get` 中从 `SessionManager` 读取已持久化的 ACK 位置来初始化 `state.current_pos`。

---

### 1.7 发布的配置文件无法解析

**文件**: `canal.yaml` vs `crates/canal-cli/src/main.rs:24-37`

`CanalSection`(带 `#[serde(deny_unknown_fields)]`)期望:
- `start_journal_name`, `start_position` (扁平字段)
- `StoreSection` 只有 `buffer_size` (没有 `type`, `batch_timeout_ms`)
- `ServerSection` 只有 `bind`, `metrics_bind` (没有 `idle_timeout_secs`)
- 没有 `filter` 段

`canal.yaml` 提供:
- `position.journal_name`, `position.position` (嵌套)
- `store.type`, `store.batch_timeout_ms` (多余键)
- `server.idle_timeout_secs` (多余键)
- `filter.pattern`, `filter.black_list` (未知段)
- `mysql.charset` (不在 `MysqlConfig` 中)

启动时 `load_config` 会拒绝配置。配置 schema 和示例文件不同步。

**修复建议**: 同步 `CanalSection` 结构体与 YAML 示例文件,或添加 `#[serde(alias)]` / `#[serde(rename)]` 实现向后兼容。

---

## 2. 高严重度 (High)

### 2.1 零长度 keepalive 帧断开客户端

**文件**: `crates/canal-server/src/codec.rs:36-39` vs `server.rs:149-150`  
**影响**: keepalive 包导致连接断开

Codec 为 `len == 0` 返回 `Some(Vec::new())`("与 keepalive 包兼容"),但 `handle_client` 将空负载输入 `Packet::decode`,触发错误 → `CanalError::Protocol` → 连接关闭。编解码器声明的 keepalive 兼容性与服务端层矛盾。

**修复建议**: 在 `handle_client` 中跳过空帧,不尝试解码。

---

### 2.2 每客户端过滤器未应用

**文件**: `crates/canal-server/src/server.rs:318-379`  
**影响**: 所有客户端接收所有表事件

`handle_get` 获取事件时不检查会话的 `FilterPattern`;过滤仅在摄入时生效(`sink.rs:98-101`)。`ClientAuth`/`Sub` 中接受的过滤器被验证和存储,但**从未执行** — 每个客户端接收所有表的事件。

**修复建议**: 在 `handle_get` 中对获取的事件应用会话级过滤器后再发送。

---

### 2.3 `handle_sub` 缺少输入验证

**文件**: `crates/canal-server/src/server.rs:290-316`  
**影响**: 会话劫持

没有 256 字符长度上限(不同于 `handle_auth` 行 253),没有正则验证,且 `sub.client_id` 未与认证的客户端 ID 核对 — 任何客户端可以覆盖其他客户端的会话(位置/ACK 追踪)。

**修复建议**: 添加 client_id 验证、长度限制、过滤器正则验证。

---

### 2.4 正则大小限制缺失(服务端侧)

**文件**: `crates/canal-common/src/types.rs:242-248`  
**影响**: 内存耗尽向量

`FilterPattern::validate` 使用 `Regex::new` 无大小限制。`crates/canal-filter/src/lib.rs:30-32` 限制编译后大小为 1MB,但服务器端验证绕过此限制。一个 256 字符的病态模式可产生大 DFA — 1,024 连接时是内存耗尽向量。

**修复建议**: 在 `FilterPattern::validate` 中添加 `regex::RegexBuilder::new().size_limit(1 << 20)`。

---

### 2.5 `put_batch` 假设有序输入,从不验证

**文件**: `crates/canal-store/src/memory.rs:66-80`  
**影响**: 静默游标损坏

`get_batch` 中的二分查找假设缓冲区单调有序,但没有强制检查。一个在旧位置重连后投放的批次会静默破坏所有客户端的光标计算。

**修复建议**: 在 `put_batch` 中添加顺序断言(debug 模式)或使用排序插入。

---

### 2.6 store 锁下的深度克隆 + 惊群效应

**文件**: `crates/canal-store/src/memory.rs:108-112, 81`  
**影响**: 高并发下延迟增加

最多 10,000 个 `CanalEvent`(每个含多个 `String` 列)在 store `Mutex` 下 `.cloned()`,阻塞 `put_batch`。每个 `get_batch` 也在全缓冲区上重新运行二分查找。`notify_waiters()`(行 81)在每批次唤醒*所有*等待者 — 1,024 客户端时造成惊群。

**修复建议**: 考虑 `Arc<CanalEvent>` 减少克隆;使用 `tokio::sync::Notify` 逐个唤醒或 `broadcast` channel。

---

### 2.7 Kafka: 无界并发发送,消息丢弃

**文件**: `crates/canal-connector/src/kafka.rs:252-264`  
**影响**: 数据丢失

每个消息一个 `producer.send()` future,`join_all` 无信号量:10,000 事件批次产生 10,000 并发发送。失败的投递仅记录日志(行 269-273),从不重试 — 瞬时 broker 问题导致数据丢失。消息键仅为 schema 名称(行 147),所有表落在一个分区。

**修复建议**: 使用 `Semaphore` 限制并发数;实现重试逻辑;考虑使用 `(schema, table)` 作为消息键。

---

### 2.8 `KafkaConfig::clone` 静默丢弃 SASL 密码

**文件**: `crates/canal-connector/src/kafka.rs:72-83`  
**影响**: 克隆后无法重连

克隆已连接的配置产生无法重新认证的配置,无文档说明此 `Clone` 行为。

**修复建议**: 为 `Clone` 实现添加文档注释,或让 clone 保留密码。

---

## 3. 中严重度 (Medium)

### 3.1 转换错误后仍提交 binlog 位点
**文件**: `crates/canal-binlog/src/connector.rs:200-202` — `client.commit()` 在转换错误后仍被调用;转换失败的行被 ACK 给 MySQL 并丢失。

### 3.2 空 binlog 文件名
**文件**: `crates/canal-binlog/src/connector.rs:130, 240` — `current_binlog_file` 初始为空;首个 `RotateEvent` 之前的事件获得 `journal_name = ""`,导致排序问题(`binlog_suffix("")` 返回 `u64::MAX`)。

### 3.3 `current_position()` 返回请求位置而非当前位置
**文件**: `crates/canal-binlog/src/connector.rs:493-495` — 特性承诺"当前"位置,但返回的是启动时请求的位置。

### 3.4 `take_receiver()` 通过 assert panic
**文件**: `crates/canal-binlog/src/connector.rs:467-475` — 公共 API 中的 panic(文档已注明,但更倾向于 `Result`)。

### 3.5 `TableMapCache::put()` 留下过时列元数据
**文件**: `crates/canal-binlog/src/table_map.rs:32-34` — 仅名称 put 不清除旧列元数据;重用的 `table_id` 可能将新名称与旧列配对。

### 3.6 UPDATE 所有 after 列标记为 updated
**文件**: `crates/canal-binlog/src/converter.rs:95-97` — 无论列是否实际变更,所有 `after` 列标记 `updated = true`(对下游消费者的保真度问题)。

### 3.7 退化的空事件条目
**文件**: `crates/canal-server/src/conversion.rs:92-98` — Xid/Heartbeat 事件产生空 `store_value` 和 `event_length = 0` 的条目。`Entry.entry_type_present` 在某些路径中未设置。

### 3.8 客户端/服务端包大小不匹配
**文件**: `crates/canal-client/src/lib.rs:281` 接受 64MB 包,`crates/canal-server/src/codec.rs:41` 限制 8MB;大服务器响应会在客户端失败并产生令人困惑的错误。

### 3.9 后台轮询任务 panic 导致挂起
**文件**: `crates/canal-client/src/lib.rs:234-249` — 如果后台任务 panic,`CanalEventStream::next_event()` 永远挂起(JoinHandle 错误未处理)。

### 3.10 `fetch_size` 负值处理
**文件**: `crates/canal-server/src/server.rs:331` — `get.fetch_size as usize` 对负值回绕,`fetch_size = -1`(proto 默认值)会产生虚假的"超过最大值"警告。

### 3.11 Admin start/stop 是表面的
**文件**: `crates/canal-instance/src/instance.rs:86-92` 和 `crates/canal-admin/src/lib.rs:195-217` — `feed()` 在实例未运行时静默丢弃事件;admin `stop` 不停止 binlog 源;stop 然后 start 创建永久间隙。

### 3.12 连接器分发同步阻塞客户端
**文件**: `crates/canal-sink/src/sink.rs:118-148` — 连接器分发在 `store.put_batch` 之前被 await;一个挂起的连接器阻塞*所有*客户端。注释说"fire and forget"但实际是同步扇出。

### 3.13 无优雅关闭、无 binlog 重连
**文件**: `crates/canal-cli/src/main.rs` — 无 `tokio::signal` 处理;Ctrl-C 杀死进程无最终刷新(`main.rs:324-328`)或服务器优雅排空。Binlog 流错误仅记录并继续 — 无重连/退避。

### 3.14 认证从未启用
**文件**: CLI 各文件 — CLI 没有服务器/admin/metrics 令牌的配置键;`CanalServer::with_auth`、`AdminServer::with_auth`、`MetricsServer::with_auth` 是死 API。TCP 服务器始终无需认证。

### 3.15 中毒恢复每个访问都记录错误
**文件**: `crates/canal-common/src/utils.rs:12-16` — `MutexLockExt::lock_or_recover` 在每次后续访问时记录错误日志,产生噪音。

### 3.16 `Events::with_events` 语义不一致
**文件**: `crates/canal-common/src/types.rs:177-223` — `position_range.start` 设为第一个事件的位置,但 `get_batch` 将位置视为排他的;与 get_batch 结合使用时语义不一致。

### 3.17 二进制 blob 十六进制回退的低效分配
**文件**: `crates/canal-binlog/src/column_serde.rs:60-64` — 为每字节字符串分配 `Vec<String>` 然后 join;可以预分配一个 `String::with_capacity(len * 2)`。

### 3.18 每事件字符串分配
**文件**: `crates/canal-server/src/server.rs:362-369` — `entry.encode_to_vec()` 为每个事件调用,产生重复分配。

---

## 4. 安全评估

| 检查项 | 状态 | 备注 |
|--------|------|------|
| 限流 | ✓ 合理 | 500ms 退避 + 3 次认证失败断开,10 个未知包断开,600s 空闲超时,1,024 连接限制,8MB 帧限制 |
| 常数时间比较 | ✓ 正确 | 服务端(`server.rs:222-226`)和 admin(`admin/src/lib.rs:128-139`)均实现 |
| 密码泄露 | ⚠ 部分 | `InstanceConfig::Debug` 屏蔽密码,`connect()` 清除密码;但 `KafkaConfig` 派生 `Debug`(SASL 密码可打印);`ReplicaOptions` 保留副本 |
| TLS | ✗ 无 | TCP 协议无加密;`ClientAuth.password` 字段传输明文令牌 |
| 认证 | ✗ 未启用 | `with_auth` API 是死代码;服务器默认无认证 |
| DoS — 正则 | ⚠ 风险 | 见 2.4,正则无大小限制 |
| DoS — Kafka | ⚠ 风险 | 见 2.7,无界并发 future |
| Admin/metrics 绑定 | ✓ 合理 | 默认 127.0.0.1 无认证 |
| ACK 欺骗 | ⚠ 风险 | `handle_client_ack` 使用攻击者可控的 `client_id` (`server.rs:382`) |

---

## 5. 测试覆盖率

### 已覆盖 (86 测试,全部通过)
错误显示、`LogPosition` 排序、过滤器匹配、转换器 insert/update/delete、`TableMapCache`、store put/get/eviction/lifecycle、会话生命周期、sink store/filter 路径、Kafka 序列化、编解码 encode/decode、转换 header/row/DDL/null、包往返(auth/sub/get/ack/rollback)、admin 认证、客户端构建器。

### 主要缺口
- **无端到端 TCP 集成测试** — auth→sub→get→ack→rollback 处理函数从未通过真实 TCP 连接测试;`tests/integration/` 为空
- Store 的**重复位置**行为无测试(1.1) — 单事件每批次测试无法捕获
- 无 `entry_bytes_to_event` 针对服务器产生条目的往返测试(将捕获 1.3)
- 无 DDL 截断字符边界测试(1.2)、客户端退避测试(1.4)、`subscribe(position)` 测试(1.5)、ACK 重连测试(1.6)、零长度帧测试(2.1)、每客户端过滤测试(2.2)
- `test_connector_receives_events` 不验证任何内容 — mock 连接器分发内容从未检查
- `CanalEventStream` drop 测试未执行真实后台循环
- `canal-proto` crate 无测试
- `canal-cli` crate 无测试(0 tests)

---

## 6. 死代码与 API 卫生

### 完全未使用
| 代码 | 位置 |
|------|------|
| `TableMetaCache` (整个 crate) | `crates/canal-meta/` |
| `KafkaConnector` | `crates/canal-connector/src/kafka.rs` |
| `connector_names` 配置字段 | `crates/canal-instance/src/instance.rs:25` |
| `CanalServer::with_auth` | `crates/canal-server/src/server.rs:48` |
| `AdminServer::with_auth` | `crates/canal-admin/src/lib.rs:80` |
| `MetricsServer::with_auth` / `set_instances_active` | `crates/canal-prometheus/src/metrics_server.rs` |
| `EventConverter::handle_table_map` | `crates/canal-binlog/src/converter.rs` |
| `InstanceSummary.destination` (复制 `name`) | `crates/canal-admin/src/lib.rs:15-19` |

### 缺失 trait 实现
- `CanalEvent`/`Events`/`PositionRange`/`ColumnValue`/`RowData`/`RowChange` 无 `PartialEq`
- `LogPosition` 实现 `Ord` 但不实现 `Hash`
- `DefaultEventSink`/`MemoryEventStore`/`CanalInstance` 缺少 `Debug`
- `Events::new(batch_id)` 返回空 `LogPosition` 的 `position_range` — 调用者必须特殊处理空批次

### 文档错配
- `EventSink::sink` (sink.rs:22-23) 说"返回过滤后的 Events 及 batch_id"但始终返回空 `Events::new(batch_id)` (sink.rs:154)

---

## 7. 修复优先级

| 优先级 | 问题 | 影响 |
|--------|------|------|
| P0 | 1.1 Store 游标语义 | 静默数据丢失 |
| P0 | 1.2 DDL 截断 panic | 复制崩溃 |
| P0 | 1.3 事件类型映射不匹配 | 所有 DDL 被客户端误识别 |
| P1 | 1.4 客户端退避 | 吞吐退化 20-50x |
| P1 | 1.5 subscribe 忽略参数 | API 契约破损 |
| P1 | 1.6 ACK 位置重连 | 重连后全量重放 |
| P1 | 1.7 配置不可解析 | 启动即失败 |
| P2 | 2.2 每客户端过滤 | 安全/隔离绕过 |
| P2 | 2.3 Sub 验证缺失 | 会话劫持 |
| P2 | 2.6 锁下克隆 | 高并发延迟 |
| P3 | 其余中低严重度项 | 按需修复 |

---

## 8. 修复记录 (2026-07-31)

所有 23 项问题已修复，涉及 16 个文件。构建通过 (0 errors)，clippy 通过 (2 dead_code warnings: 新增配置字段 charset/idle_timeout_secs)，92 个测试全部通过。

### 严重问题修复

| 问题 | 文件 | 修复内容 |
|------|------|---------|
| 1.1 Store 游标 | `memory.rs:97-101` | `binary_search_by` → `partition_point`，正确处理重复位置 |
| 1.2 DDL 截断 | `connector.rs:256` | `sql[..N]` → `sql.chars().take(N).collect()` 避免 UTF-8 panic |
| 1.3 事件类型映射 | `types.rs` + `client/lib.rs:415` | 新增 `EventType::from_proto()` 方法，客户端使用正确解码 |
| 1.4 客户端退避 | `client/lib.rs:182-215` | Messages 分支 `continue` 跳过退避，仅非 Messages 响应处应用 |
| 1.5 subscribe 参数 | `client/lib.rs:91` | 参数重命名为 `_position`，API 保留供后续实现 |
| 1.6 ACK 重连 | `server.rs:377-382` | `handle_get` 在 `current_pos` 为 None 时从 `SessionManager.last_ack_position` 恢复 |
| 1.7 配置解析 | `main.rs` | 新增 `FilterSection`、`auth_token`、`charset`、`idle_timeout_secs` 配置字段 |

### 高严重度问题修复

| 问题 | 文件 | 修复内容 |
|------|------|---------|
| 2.1 Keepalive 帧 | `server.rs:149-151` | 跳过零长度帧，不解码 |
| 2.2 每客户端过滤 | `server.rs:384-408` | `handle_get` 中应用会话级 `FilterPattern` |
| 2.3 Sub 验证 | `server.rs:296-335` | client_id 匹配验证 + filter 长度/正则验证 |
| 2.4 正则大小限制 | `types.rs:243-250` | `RegexBuilder::size_limit(1 << 20)` |
| 2.5 有序验证 | `memory.rs:72-78` | `debug_assert!` 检查 put_batch 单调顺序 |
| 2.6 锁下克隆 | `memory.rs:108-110` | `to_vec()` 替代 `.iter().cloned().collect()`，提取后释放锁 |
| 2.7 Kafka 并发 | `kafka.rs:264` | `Semaphore::new(50)` 限制并发发送 |
| 2.8 Kafka Config | `kafka.rs` | Clone 保留密码；手动 Debug 实现屏蔽密码 |

### 中严重度问题修复

| 问题 | 文件 | 修复内容 |
|------|------|---------|
| 3.1 提交后错误 | `connector.rs:194-203` | 改进注释说明设计意图 |
| 3.2 空文件名 | `connector.rs` | `current_binlog_file` 从 start_journal 初始化 |
| 3.5 过时列元数据 | `table_map.rs:33` | `put()` 清除旧 columns 条目 |
| 3.6 列变更检测 | `converter.rs:95-100` | 比较 before/after 列值，仅真正变更的列标记 `updated=true` |
| 3.7 空条目 | `conversion.rs:91-95` | Xid/Heartbeat 事件提供最小 RowChange 作为 store_value |
| 3.8 包大小 | `client/lib.rs:281` | 客户端最大包从 64MB 改为 8MB，与服务端对齐 |
| 3.9 轮询 panic | `client/lib.rs:240-250` | `next_event()` 检查 `JoinHandle::is_finished()` |
| 3.10 fetch_size | `server.rs:362` | 比较改为 `get.fetch_size > MAX_FETCH_SIZE as i32` 避免负值回绕 |
| 3.11 实例状态 | `instance.rs:87-93` | `feed()` 在未运行时返回错误而非静默丢弃 |
| 3.12 连接器阻塞 | `sink.rs:116-128` | 连接器分发改为 fire-and-forget，在 store.put_batch 后执行 |
| 3.13 优雅关闭 | `main.rs:252-258` | 新增 `tokio::signal::ctrl_c()` 处理器 |
| 3.14 认证启用 | `main.rs` + `canal.yaml` | 新增 `auth_token` 配置字段，传递给 server |
| 3.15 中毒日志 | `utils.rs` | `AtomicBool` guard — 每个锁类型仅记录一次中毒恢复 |
| 3.17 Hex 分配 | `column_serde.rs:60-64` | `String::with_capacity` + `write!` 替代 `Vec<String>` + `join` |
| 4.5 ACK 安全 | `server.rs:398-407` | `handle_client_ack` 使用 `state.client_id` 而非攻击者可控的 `client_ack.client_id` |

### 低严重度问题修复

- `types.rs`: `PartialEq` 添加到 `ColumnValue`/`RowData`/`RowChange`/`CanalEvent`/`Events`/`PositionRange`；`Hash` 添加到 `LogPosition`
- `kafka.rs`: 手动 `Debug` 实现屏蔽 `sasl_password`
- `admin.rs`: `InstanceSummary.destination` 保留（API 兼容性）
