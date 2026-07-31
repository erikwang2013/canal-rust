# canal-rust v2.0.2 — 第二轮审查报告

**日期**: 2026-07-31  
**审查范围**: 全量修复后的剩余问题  
**测试结果**: 92 个测试全部通过, 0 失败  
**构建**: `cargo build` 通过, `cargo clippy --all-targets` 通过 (14 warnings, 均为 style/dead_code)

---

## 执行摘要

上一轮 23 项问题已全部修复。本轮发现 **7 项剩余问题**:

| 严重度 | 数量 |
|--------|------|
| 高 | 3 |
| 中 | 3 |
| 低 | 1 |

---

## 高严重度

### 1. ACK 位置跳跃 — 崩溃窗口内事件丢失

**文件**: `crates/canal-server/src/server.rs:388-391, 448-455`  
**影响**: 客户端 Get→Ack 之间崩溃时事件丢失

`handle_get` 在返回 `Messages` 时就更新了 `state.current_pos`（行 388-391），而不是等客户端 ACK。`handle_client_ack` 使用 `state.current_pos` 而非 `batch_id` 来确定 ACK 位置。如果客户端连续 Get 两次（batch_1, batch_2）再 Ack batch_1，ACK 位置会跳到 batch_2 的末尾，而非 batch_1 的末尾。一旦崩溃，batch_2 中的事件会被跳过。

**修复建议**: ACK 应使用 batch_id 定位实际已确认的位置，或在 Get 时暂存当前位点待 ACK 确认后才推进。

### 2. 重连密码清空 — 静默空密码认证

**文件**: `crates/canal-binlog/src/connector.rs:421-422`  
**影响**: `disconnect()` + `connect()` 使用空密码重连,认证静默失败

`connect()` 在构建 options 后清空 `self.password`（行 421-422），但 `disconnect()` 不恢复密码。超时路径（`connect():453-460`）设置了 `running=true` 但不发送 `started` 信号，导致 `connect()` 超时后 connector 卡在 "已连接" 状态，阻塞重试。

**修复建议**: 在 `disconnect()` 中恢复密码；`connect()` 超时时设置 `running=false` 或传播错误。

### 3. 不可解码事件导致重启活锁

**文件**: `crates/canal-binlog/src/connector.rs:201-203`  
**影响**: 永久坏事件使复制在每次重启时卡在同一位置

提交+发送错误意味着坏事件的位置被提交给 MySQL，但消费端的存储位置未前进。下次重启时，MySQL 从此位置继续，再次遇到同一事件。如果事件是*永久*不可解码的（而非瞬态），形成重启活锁。

**修复建议**: 对反复失败的事件设置跳过阈值，或使用 `gtid` 跳过已知坏事件。

---

## 中严重度

### 4. Sink fire-and-forget 破坏 Kafka 批次顺序

**文件**: `crates/canal-sink/src/sink.rs:119-131`  
**影响**: 批次 N+1 可能在 N 之前到达 Kafka

`tokio::spawn` 每批次不保证顺序。并发任务 + 重试可能重排批次。关闭时正在进行的 dispatches 无等待/排空 — 进程退出时静默丢失。

**修复建议**: 使用 FIFO 有序执行（如 `tokio::sync::mpsc` + 单 worker）或添加关闭排空。

### 5. 每 Get 重新编译正则

**文件**: `crates/canal-server/src/server.rs:394-403`  
**影响**: 每客户端每轮询周期重复编译过滤器正则

`sessions.get(&cid)` 被调用 3 次（行 377, 394, 398）。每个 Get 都重新编译 pattern 和 black_list 正则。对典型小正则成本低，但高频轮询下可避免。

**修复建议**: 在 `ClientSession` 中缓存编译后的 `Regex`，或使用 `once_cell::sync::OnceCell`。

### 6. 管理端口溢出

**文件**: `crates/canal-cli/src/main.rs:269`  
**影响**: 配置端口 65535 时 `bind_addr.port() + 1` panic（debug 模式下 u16 溢出）

`admin_bind = format!("127.0.0.1:{}", bind_addr.port() + 1)` — 端口 65535 时溢出。

**修复建议**: 使用 `bind_addr.port().saturating_add(1)` 并检查是否为 0（回绕）。

---

## 低严重度

### 7. `from_proto` 映射覆盖

**文件**: `crates/canal-common/src/types.rs:103-109`  
**影响**: 服务端发送的 Query(7)/Rotate(7) 在客户端都解码为 Ddl

这是协议设计的固有限制 — 原始 Canal 协议将 Ddl/Query/Rotate 都映射到同一个 proto 枚举值。`from_proto` 的行为正确且与 v18 报告中的修复一致。`From<i32>` 的直通映射（6→Rotate, 7→Xid）与新 `from_proto`（7→Ddl）不同，但 `From<i32>` 已不再用于客户端解码。

**状态**: 无需修复，是协议限制。文档可补充说明。

---

## 测试缺口（本轮新发现）

- `memory.rs`: 无重复位置事件（多行 binlog）返回正确切片的测试
- `memory.rs`: 无 drain + oversized-batch 组合测试
- `types.rs`: 无 `from_proto` 往返测试（server 编码 → client 解码）
- `sink.rs`: 无 fire-and-forget 排序验证测试

---

## 对比 v18

| 指标 | v18 | v19 |
|------|-----|-----|
| 严重问题 | 7 | 0 (已修复) |
| 高严重度 | 8 | 3 |
| 中严重度 | 8 | 3 |
| 低严重度 | 3 | 1 |
| **总计** | **23** | **7** |

核心架构问题（数据丢失、DDL panic、协议错配、配置不可解析）均已解决。剩余问题为边界条件（重连密码、ACK 位点竞态、Kafka 顺序）和优化项（正则缓存、端口溢出）。

---

## 修复记录 (2026-07-31, 第二轮)

| 问题 | 文件 | 修复 |
|------|------|------|
| 1. ACK 位置跳跃 | `server.rs` | ClientState 新增 `last_get_batch_id`/`last_get_end_pos`；ACK 使用 Get 返回的准确位置，batch_id 不匹配时 warn |
| 2. 重连密码清空 | `connector.rs` | 新增 `original_password` 字段；`disconnect()` 中恢复密码；超时路径重置 `running=false` |
| 3. 重启活锁 | `connector.rs` | 追踪 `consecutive_errors` 计数器；同位置连续失败 ≥3 次时跳过并记录 error |
| 4. Sink 排序 | `sink.rs` | FIFO 后台 worker + unbounded channel；新增 `test_fifo_ordering_preserved` 测试 |
| 5. 正则缓存 | `session.rs` + `server.rs` | ClientSession 预编译 `compiled_pattern`/`compiled_black_list`；handle_get 使用缓存 |
| 6. 端口溢出 | `main.rs` | `saturating_add(1)` + 溢出检查 |
| 7. from_proto 文档 | `types.rs` | 补充协议限制说明 |

构建通过，92 测试全部通过，0 clippy 错误。
