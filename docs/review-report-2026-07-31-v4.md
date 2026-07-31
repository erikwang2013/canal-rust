# Canal 项目审查报告 v4

**日期**: 2026-07-31
**测试结果**: 91 个测试全部通过，0 失败
**构建**: `cargo build` 成功，`cargo clippy` 零警告
**版本**: v1.0.8

---

## 总览

经过上一轮 27 项修复后，项目质量显著提升。本轮审查发现 20 个新问题：5 个严重、9 个重要、6 个建议。

### 评分

| 维度 | 评分 | 说明 |
|------|------|------|
| 正确性 | 中 | 事件存储唤醒竞争、Kafka 潜在 panic、server_id 截断 |
| 安全性 | 良 | 认证已实现，密码已脱敏，默认凭据已移除 |
| 性能 | 中 | Kafka 批量、事件克隆、双重编码可优化 |
| 可维护性 | 良 | 架构清晰，API 设计合理 |
| 测试覆盖 | 中 | 核心模块已加强，错误路径测试仍不足 |

---

## 严重问题 (5)

### C1. MemoryEventStore::get_batch 存在唤醒丢失竞争

**文件**: `crates/canal-store/src/memory.rs:82-123`

在释放 `MutexGuard`（第 109 行）和进入 `tokio::select!`（第 117 行）之间，并发的 `put_batch` 可以插入事件并调用 `notify.notify_waiters()`。由于尚未进入 `Notify::notified()`，此唤醒会悄然丢失。低负载时客户端需要等待 5 秒超时才能发现已有事件。

**修复**: 在释放锁之前注册 `Notified` future。

### C2. KafkaConnector::dispatch 在未连接时 panic

**文件**: `crates/canal-connector/src/kafka.rs:171`

`.expect("KafkaConnector: not connected")` 在 `connect()` 之前或 `close()` 之后调用 `dispatch()` 时会 panic。这是一个 DoS 向量。

**修复**: 返回 `CanalError::Internal(...)` 而非 panic。

### C3. Kafka security.protocol 配置错误

**文件**: `crates/canal-connector/src/kafka.rs:120-135`

当仅配置 TLS（设置 `ssl_ca_location` 但未设置 `sasl_username`）时，`security.protocol` 被错误设置为 `SASL_SSL`。纯 TLS 无 SASL 时应使用 `SSL`。Kafka broker 会拒绝错误的握手。

**修复**: 三种场景分别设置：TLS+SASL -> SASL_SSL，仅TLS -> SSL，仅SASL -> SASL_PLAINTEXT。

### C4. server_id 从 u64 截断为 u32

**文件**: `crates/canal-binlog/src/connector.rs:91`

`self.server_id as u32` -- MySQL server_id 理论上可超过 2^32。`server_id` 在配置和类型中存储为 `u64`，截断到 `u32` 是静默的数据丢失。

**修复**: 构造时验证 `server_id <= u32::MAX`，溢出时返回错误。

### C5. start_all/stop_all 绕过 CanalLifecycle trait 方法

**文件**: `crates/canal-instance/src/instance.rs:171-189`

`start_all()` 直接设置 `instance.running.store(true, ...)` 而非调用 `instance.start().await`。如果 `CanalLifecycle::start()` 将来增加额外逻辑，会被悄然跳过。

**修复**: 通过 trait 方法 `instance.start().await` / `instance.stop().await` 操作。

---

## 重要问题 (9)

### I1. Admin API 静默丢弃 start/stop 错误

**文件**: `crates/canal-admin/src/lib.rs:158,178`

操作失败时 HTTP 响应仍返回 `status: "ok"`。修复：将错误传播到 HTTP 响应，返回 500 状态码。

### I2. Kafka dispatch 每个连接器克隆整个事件批次

**文件**: `crates/canal-sink/src/sink.rs:103`

`conn.dispatch((*events).clone()).await` -- 整个 `Vec<CanalEvent>` 为每个连接器克隆一次。修复：`SinkConnector::dispatch` 接受 `Arc<Vec<CanalEvent>>` 或 `&[CanalEvent]`。

### I3. Kafka FutureProducer 每条消息克隆一次

**文件**: `crates/canal-connector/src/kafka.rs:169-172`

1000 条消息 = 1000 次不必要的克隆。修复：在循环外克隆一次。

### I4. canal_event_to_entry 双重编码 RowChange

**文件**: `crates/canal-server/src/server.rs:377-451, 278-281`

RowChange 先编码到 store_value，然后整个 Entry 重新编码。修复：考虑在存储插入时预计算 entry 字节。

### I5. Messages 响应无聚合大小限制

**文件**: `crates/canal-server/src/server.rs:274-282`

编码的 entry 被追加而不检查聚合大小，可能超过 64MB 编码器限制。修复：跟踪累计字节数，超出时分批。

### I6. binlog_handle 失败不会触发服务器关闭

**文件**: `crates/canal-cli/src/main.rs:169-218, 223-224`

binlog 连接器任务退出时，服务器继续运行并提供过期数据。修复：触发 shutdown_token 实现级联关闭。

### I7. 关闭时剩余批次错误被静默忽略

**文件**: `crates/canal-cli/src/main.rs:216`

`let _ = store_for_binlog.put_batch(batch).await;` 刷新剩余事件时错误被丢弃。修复：记录错误日志。

### I8. 服务器关闭时丢弃客户端任务未等待完成

**文件**: `crates/canal-server/src/server.rs:87-118`

shutdown_token 触发时服务器立即返回，客户端任务被取消但未等待完成。修复：shutdown().await。

### I9. event_length 始终报告为 0

**文件**: `crates/canal-server/src/server.rs:404`

`raw_bytes` 始终为 `vec![]`，因为 binlog 连接器从不填充它。修复：设置为 `entry.store_value.len()`。

---

## 建议 (6)

- S1. TOCTOU: 配置文件大小检查 (`crates/canal-cli/src/main.rs:123-132`)
- S2. Client subscribe 忽略请求的 position (`crates/canal-client/src/lib.rs:95-97`)
- S3. test_server_binds_to_port 未实际测试 CanalServer (`crates/canal-server/src/tests.rs:37-47`)
- S4. ColumnValue.updated 在 INSERT/DELETE 中无意义 (`crates/canal-common/src/types.rs:105-112`)
- S5. Kafka 无批量发送 (`crates/canal-connector/src/kafka.rs:58-103`)
- S6. Client GET 不使用重连位置 (`crates/canal-client/src/lib.rs:132-137`)

---

## 已修复（v3 -> v4）

27 项修复已完成：6 个严重正确性 bug（排序、取消、防护、竞争、零长度检查）、5 个高优先级安全漏洞（认证、TLS、密码脱敏、凭据移除、serde_yml）、9 个中等问题（UPDATE 分割、server_id 配置、过滤器错误、Kafka 分区键、Admin 认证、默认绑定、连接限制等）、7 个低优先级改进（suffix、Bit、DashMap、FxHashMap、Debug 派生、行处理器去重等）。

---

## 测试统计

| Crate | 测试数 | 变化 |
|-------|--------|------|
| canal-common | 13 | +2 (Ord, suffix) |
| canal-binlog | 9 | +1 (put_with_columns) |
| canal-instance | 7 | +1 (invalid_filter) |
| 其他 11 个 crate | 62 | — |
| **合计** | **91** | **+3** |

---

## 修复优先级

**第一轮**: C2 (panic), C3 (Kafka 协议), C1 (唤醒竞争), C4 (server_id 截断)

**第二轮**: C5 (trait 方法), I1 (admin 错误), I6 (binlog 失败传播), I8 (优雅关闭)

**第三轮**: I2+I3 (Kafka 性能), I7 (错误日志), I9 (event_length), 建议项
