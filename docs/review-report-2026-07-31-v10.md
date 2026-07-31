# Canal 项目审查报告 v10

**日期:** 2026-07-31  
**分支:** main (v1.1.5 — 含本轮修复)  
**测试:** 98 passed, 0 failed  
**Clippy:** 0 warnings  
**格式化:** 通过  
**target/ 清理:** 释放 25GB  

---

## 本轮修复清单（v8 → v10）

| 编号 | 类别 | 文件 | 说明 |
|------|------|------|------|
| B1 | Bug | `types.rs`, `lib.rs`, `memory.rs` | `binlog_suffix` 函数去重，提升为公共 API |
| B2 | Bug | `memory.rs` | `expect()` 替换为 `unwrap()`（已受 `is_empty()` 守卫） |
| P1 | 性能 | `sink.rs` | `Arc<Vec<CanalEvent>>` 共享事件，克隆从 N+2 次降为 1 次 |
| C1 | 代码质量 | `main.rs`, `sink.rs`, 两个 `Cargo.toml` | CanalMetrics 集成到主程序和 sink 管道 |
| C2 | 代码质量 | `connector.rs` | `DefaultBinlogConnector` 添加 `Drop` 实现取消 token |
| C4 | 代码质量 | `server.rs` | 未知 packet 类型累加计数，超过 10 次断开连接 |
| S1 | 安全 | `kafka.rs` | `KafkaConfig` 手动 `Clone` 清除 `sasl_password` |
| — | 清理 | `prometheus/Cargo.toml` | 移除未使用的 `canal-instance` 依赖（修复循环依赖） |
| — | 文档 | `README.md`, `README.en.md` | 更新版本号、配置示例、依赖关系图 |

---

## 测试结果

```
canal_admin      10 passed
canal_binlog      9 passed
canal_client      3 passed
canal_common     13 passed
canal_connector   4 passed
canal_filter      6 passed
canal_instance    7 passed
canal_meta        6 passed
canal_prometheus  3 passed
canal_server     25 passed
canal_sink        3 passed
canal_store       9 passed
                        ——
Total            98 passed, 0 failed
```

---

## 静态分析

- **Clippy:** 0 warnings（`--all-targets --all-features -D warnings`）
- **格式化:** 全部通过 `cargo fmt --check`
- **unsafe 代码:** 无
- **循环依赖:** 已修复（移除 canal-prometheus → canal-instance 未使用依赖）

---

## 剩余关注点（优先级低）

### R1 — `inc_dispatched` 语义

**文件:** `crates/canal-sink/src/sink.rs:143`  
**严重程度:** 低

`inc_dispatched(1)` 在每个 connector 成功后递增 1，代表「dispatch 操作次数」而非「dispatch 事件数」。如需更精确的指标，可改为 `inc_dispatched(batch.len() as u64)`。

### R2 — `CanalMetrics::with_metrics` 未被外部使用

**文件:** `crates/canal-sink/src/sink.rs:61-73`  
**严重程度:** 低

新增的 `with_metrics` 构造函数允许外部传入共享的 `CanalMetrics`，但目前仅在 `canal-cli` 的 `main.rs` 中使用独立的实例。`canal-instance`/`canal-admin` 路径仍然自建实例。这是 API 预留，可择机对接。

### R3 — `run_dump` 命令无 metrics 集成

**文件:** `crates/canal-cli/src/main.rs:288`  
**严重程度:** 低

`run_dump` 是诊断命令，故意不启动 metrics server。如果未来需要监控 dump 模式，可添加。

### R4 — 缺少集成测试

98 个测试全为单元测试。完整链路（binlog → store → server → client）缺少端到端覆盖。

---

## 项目健康度

| 维度 | 状态 |
|------|------|
| 构建 | 通过 |
| 单元测试 | 98/98 通过 |
| Clippy | 0 warnings |
| 格式化 | 一致 |
| 循环依赖 | 无 |
| 死代码 | 无 |
| 资源泄漏 | Drop 实现已覆盖 |
| 安全 | 密码 Clone 已清除 |
| 可观测性 | metrics 已集成 |
| 文档 | README 已更新 |

---

## 总结

本轮修复了 v8 报告中发现的全部 8 个问题，无新增回归。项目处于健康状态。
