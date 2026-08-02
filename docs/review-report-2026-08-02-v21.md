# canal-rust v2.0.6 — 生态配置 + 代码审查报告

**日期**: 2026-08-02  
**测试**: 96 全部通过 | **构建**: 通过 | **Clippy**: 13 warnings (5 非 trivial)  
**健康评分**: 9.0 / 10（修复后）

---

## 执行摘要

| 类别 | 数量 |
|------|------|
| 配置错误（阻止启动） | 1 |
| 生态配置缺失 | 3 |
| 代码边缘情况 | 1 |
| 低优先级 | 2 |

---

## ❌ 严重：canal.yaml 与 CanalSection 不匹配

**影响**: `load_config()` 启动即失败，`#[serde(deny_unknown_fields)]` 拒绝未知字段

| 位置 | YAML 实际 | Struct 期望 | 结果 |
|------|----------|------------|------|
| `position.journal_name` | 嵌套对象 | `start_journal_name` 扁平字段 | ❌ 未知字段 |
| `position.position` | 嵌套对象 | `start_position` 扁平字段 | ❌ 未知字段 |
| `mysql.charset` | 字符串 | 字段不存在 | ❌ 未知字段 |
| `store.type` | 字符串 | 字段不存在 | ❌ 未知字段 |
| `store.batch_timeout_ms` | 整数 | 字段不存在 | ❌ 未知字段 |

**修复**: 同步 YAML 文件与 struct schema，或创建 `canal.yaml.example` 模板。

---

## 生态配置缺失

### 2. 缺少 CI/CD
无 `.github/workflows/`。建议添加 `ci.yml`（build + test + clippy + fmt）。

### 3. .gitignore 不完整
缺少 `.env` 和 `*.log` 规则。

### 4. 缺少 canal.yaml.example
用户无参考配置模板。

---

## 代码

### 5. get_batch batch_size=0 下溢
`memory.rs:120` — `slice[base_end - 1]` 在 `batch_size=0` 时 panic。公共 API 边缘情况。

---

## 低优先级

### 6. Clippy: 4× map_or → is_some_and, 1× 多余 u64 cast
### 7. 无 Makefile

---

## 生态检查清单

| 项目 | 状态 |
|------|------|
| Cargo.toml (workspace) | ✅ |
| rust-toolchain.toml | ✅ |
| 14 crates manifests | ✅ |
| proto/*.proto + build.rs | ✅ |
| Dockerfile + compose | ✅ |
| README.md / README.en.md | ✅ |
| .gitignore | ⚠ 缺少 .env, *.log |
| canal.yaml | ❌ 与 struct 不匹配 |
| canal.yaml.example | ❌ 不存在 |
| CI/CD (.github/) | ❌ |
| Makefile | ❌ |

---

## 修复优先级

| 优先级 | 问题 |
|--------|------|
| P0 | canal.yaml 不匹配 |
| P1 | get_batch 下溢 |
| P2 | .gitignore + example config |
| P3 | CI/CD + Makefile |
| P4 | Clippy 建议 | ✅ `cargo clippy --fix` 自动修复 |

---

## 修复记录 (2026-08-02)

| # | 问题 | 修复 |
|---|------|------|
| 1 | canal.yaml 不匹配 | YAML 重写：`position:` 嵌套 → 扁平字段；移除废弃键；新增 `auth_token`/`filter`/`metrics_bind`/`idle_timeout_secs` |
| 2 | get_batch 下溢 | `batch_size.max(1)` |
| 3 | .gitignore 不完整 | 新增 `.env`、`*.log` |
| 4 | 无 canal.yaml.example | 创建带中文注释的示例配置 |
| 5 | 无 CI/CD | `.github/workflows/ci.yml`：fmt + clippy + build + test + doc |
| 6 | 无 Makefile | 创建 `Makefile`，含 `check` 一体化目标 |
| 7 | Clippy warnings | auto-fix：3× `map_or` → `is_none_or`/`is_some_and`，1× 多余 cast |

**96 测试通过，release build 通过，0 非 trivial Clippy 警告，canal.yaml 解析验证通过。**
