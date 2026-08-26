# Canal 测试报告

**日期**: 2026-08-27
**分支**: main
**提交**: 90255b3 + 本轮新增
**版本**: v2.0.6
**结果**: 全部通过 (334 passed / 0 failed / 0 ignored)

---

## 概览

| 指标 | 数值 |
|------|------|
| 总测试数 | 334（基线 92 → 新增 242） |
| 通过 | 334 |
| 失败 | 0 |
| Clippy 警告 | 0（workspace --all-targets） |
| 修复的 bug | 6（含 3 个认证绕过/安全问题） |

测试团队：3 名资深 Rust 测试工程师（tester-core / tester-runtime / tester-app），覆盖全部 14 个 crate。全部为纯本地单元测试（无网络/数据库/kafka/mysql 外部依赖）。

---

## 按 Crate 分布

| crate | 新增 | 总计 | 覆盖要点 |
|-------|------|------|----------|
| canal-common | +31 | 44 | binlog_suffix 边界、LogPosition Ord 全分支、EventType 全映射、serde 往返、FilterPattern 校验错误路径、CanalError display、Mutex/RwLock 中毒恢复 |
| canal-binlog | +33 | 42 | MySqlValue 18 类型转字符串（Blob 退化 hex）、build_column_infos 边界、converter 变化检测、table_map 覆盖语义、connector 校验 |
| canal-proto | +24 | 24 | 全部 18 个 message encode/decode 往返（含 oneof）、截断字节报错、5 个枚举线格式数值校验 |
| canal-store | +10 | 16 | 游标独占性、batch_size=0→1、重复 get 不消费、驱逐后跟踪、跨 journal 读取、超时空批次 |
| canal-filter | +9 | 15 | 超长 pattern 报错、黑名单优先、子串语义、大小写敏感 |
| canal-meta | +12 | 18 | cache 缺失/覆盖/克隆隔离/多主键/零主键、serde 往返 |
| canal-server | +53 | 78 | codec 边界（8MB 限制）、session 脱敏/覆盖、conversion 映射、handler 全路径（auth/sub/get/ack/rollback）、真实 TCP 端到端流程 |
| canal-instance | +10 | 17 | lifecycle 幂等、非法 blacklist、Debug 脱敏、manager 增删、黑名单 filter 构建 |
| canal-sink | +8 | 12 | 空事件/全过滤/黑名单、connector 失败隔离、多 connector 扇出、batch_id 单调 |
| canal-connector | +10 | 14 | 配置默认/脱敏、JSON 序列化形状、gtid 省略/携带、close 幂等 |
| canal-admin | +14 | 23 | check_auth 全路径、handler 直接调用（无端口）、token 永不泄漏 |
| canal-cli | +21 | 21 | YAML 解析全路径、load_config、Debug 脱敏、clap 解析 |
| canal-client | +21 | 24 | entry 解码、packet framing（8MB 限制）、进程内 mock server 完整握手 e2e |
| canal-prometheus | +11 | 14 | Render 断言、check_metrics_auth、MetricsServer 绑定 |

---

## 修复的 Bug（6 个）

| # | 位置 | 问题 | 修复 |
|---|------|------|------|
| 1 | canal-server/src/server.rs handle_auth | `authenticated = true` 在 filter 校验**之前**设置，认证失败后仍可订阅拉取事件（**认证绕过**） | 校验通过后才置认证标志 |
| 2 | canal-admin/src/lib.rs:110 | 空配置 token 匹配缺失/空 Authorization 头，静默禁用认证（**认证绕过**） | 前置空 token 拒绝 |
| 3 | canal-prometheus/src/metrics_server.rs:145 | 同上，影响 /metrics（**认证绕过**） | 前置空 token 拒绝 |
| 4 | canal-prometheus/src/metrics_server.rs:11-35 | describe_*! 在 install_recorder() 之前调用，exporter 丢弃 HELP 描述 | 移至 install 之后 |
| 5 | canal-cli/src/main.rs:251 | admin port 溢出检查死代码（saturating_add 永不产生 0） | 改用 checked_add(1) |
| 6 | canal-cli/src/lib.rs FilterSection | 派生 Default 产生 pattern `""`，与 serde 默认 `".*\\..*"` 不一致，缺 filter 配置时静默丢弃所有事件 | 手动实现 Default 与 serde 一致 |

---

## 遗留说明

- Kafka connect()/dispatch() 真实投递、MySQL 真实连接（connector/run_server/run_dump）需外部服务，未测（仅测连接前校验与错误路径）
- store 超时路径测试耗时约 5s（GET_BATCH_TIMEOUT_MS 固定值），为全套最慢用例
- canal-cli 拆分为 lib.rs（bin 无法被集成测试导入）；canal-client 公开 send_packet/read_packet/entry_bytes_to_event 以便测试
- 3 个 Cargo.toml dev-dependencies 新增 serde_json/prost（版本号未动）
