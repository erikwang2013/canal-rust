# Canal 测试报告

**日期**: 2026-07-31  
**分支**: main  
**提交**: 164e81d  
**版本**: v1.0.6  
**结果**: 全部通过 (100 passed / 0 failed / 0 ignored)

---

## 概览

| 指标 | 数值 |
|------|------|
| 总测试数 | 100 |
| 通过 | 100 |
| 失败 | 0 |
| 忽略 | 0 |
| 编译警告 | 0 |
| Clippy 警告 | 0 |

---

## 按 Crate 分布

### canal-admin (10 tests)
| 测试 | 结果 |
|------|------|
| test_check_auth_no_token_required | passed |
| test_check_auth_missing_header | passed |
| test_check_auth_bearer_token_valid | passed |
| test_check_auth_raw_token_valid | passed |
| test_check_auth_wrong_token | passed |
| test_check_auth_empty_header_value | passed |
| test_admin_server_with_auth | passed |
| test_admin_state_debug_masks_token | passed |
| test_register_and_list_instances | passed |
| test_health_endpoint | passed |

### canal-binlog (9 tests)
| 测试 | 结果 |
|------|------|
| converter::test_clear_after_rotate | passed |
| converter::test_insert_event | passed |
| converter::test_delete_event | passed |
| converter::test_missing_table_map_errors | passed |
| converter::test_update_event_separate_before_after | passed |
| table_map::test_clear_removes_all | passed |
| table_map::test_missing_table_id | passed |
| table_map::test_put_and_get | passed |
| table_map::test_put_with_columns | passed |

### canal-client (3 tests)
| 测试 | 结果 |
|------|------|
| test_builder_pattern | passed |
| test_client_id_is_unique | passed |
| test_canal_event_stream_drop | passed |

### canal-common (13 tests)
| 测试 | 结果 |
|------|------|
| error::test_error_from_io | passed |
| error::test_error_display | passed |
| types::test_events_new_is_empty | passed |
| types::test_column_value_key_detection | passed |
| types::test_event_type_from_i32 | passed |
| types::test_events_with_events_populates_range | passed |
| types::test_log_position_display_with_gtid | passed |
| types::test_log_position_new | passed |
| types::test_filter_pattern_default | passed |
| types::test_log_position_display | passed |
| types::test_log_position_ord | passed |
| types::test_log_position_ord_suffix_fallback | passed |
| types::test_row_change_roundtrip | passed |

### canal-connector (4 tests)
| 测试 | 结果 |
|------|------|
| test_connector_name | passed |
| test_empty_events_produces_empty_messages | passed |
| test_serialize_multiple_events | passed |
| test_serialize_insert_event | passed |

### canal-filter (6 tests)
| 测试 | 结果 |
|------|------|
| test_invalid_regex_returns_error | passed |
| test_specific_table | passed |
| test_empty_blacklist_passes_all | passed |
| test_match_all_pattern | passed |
| test_blacklist_excludes | passed |
| test_wildcard_schema | passed |

### canal-instance (7 tests)
| 测试 | 结果 |
|------|------|
| test_config_clone | passed |
| test_invalid_filter_returns_error | passed |
| test_manager_remove | passed |
| test_manager_register_and_lookup | passed |
| test_feed_events_to_instance | passed |
| test_instance_creation_and_lifecycle | passed |
| test_manager_list_instances | passed |

### canal-meta (6 tests)
| 测试 | 结果 |
|------|------|
| test_clear | passed |
| test_contains | passed |
| test_get_column_by_name | passed |
| test_primary_keys | passed |
| test_put_and_get | passed |
| test_remove | passed |

### canal-prometheus (5 tests)
| 测试 | 结果 |
|------|------|
| test_snapshot_is_cloneable | passed |
| test_gauge_updates | passed |
| test_counter_increments | passed |
| test_multiple_counters | passed |
| test_metrics_server_starts_on_random_port | passed |

### canal-server (25 tests)
| 测试 | 结果 |
|------|------|
| codec::test_decode_incomplete_header | passed |
| codec::test_decode_incomplete_payload | passed |
| codec::test_decode_complete_packet | passed |
| codec::test_decode_multiple_packets | passed |
| codec::test_encode_header_is_big_endian | passed |
| codec::test_encode_roundtrip | passed |
| server::test_canal_event_to_entry_ddl | passed |
| server::test_canal_event_to_entry_delete | passed |
| server::test_canal_event_to_entry_update_with_before_and_after | passed |
| server::test_client_rollback_encoding_roundtrip | passed |
| server::test_canal_event_to_entry_with_row_change | passed |
| server::test_client_auth_encoding_roundtrip | passed |
| server::test_get_encoding_roundtrip | passed |
| server::test_messages_packet_roundtrip | passed |
| server::test_send_ack_packet_structure | passed |
| server::test_canal_event_to_entry_header_fields | passed |
| server::test_send_ack_packet_with_error | passed |
| server::test_client_ack_encoding_roundtrip | passed |
| session::test_position_tracking | passed |
| session::test_session_lifecycle | passed |
| server::test_column_value_to_proto_with_null | passed |
| server::test_sub_encoding_roundtrip | passed |
| server::test_handle_client_registers_and_sends_events | passed |
| server::test_server_binds_to_port | passed |
| session::test_heartbeat_updates_timestamp | passed |

### canal-sink (3 tests)
| 测试 | 结果 |
|------|------|
| test_filter_excludes_non_matching | passed |
| test_sink_stores_and_returns_events | passed |
| test_connector_receives_events | passed |

### canal-store (9 tests)
| 测试 | 结果 |
|------|------|
| position::test_missing_client | passed |
| memory::test_buffer_overflow_evicts_oldest | passed |
| memory::test_lifecycle_start_stop | passed |
| position::test_update_and_get | passed |
| memory::test_empty_put_is_noop | passed |
| memory::test_oversized_batch_truncated | passed |
| position::test_remove | passed |
| memory::test_latest_position_tracks_head | passed |
| memory::test_put_and_get_batch | passed |

---

## 测试覆盖分析

| 层级 | Crate | 测试数 | 覆盖领域 |
|------|-------|--------|---------|
| 协议层 | canal-server | 25 | 编解码、握手、订阅、消息收发 |
| 数据模型 | canal-common | 13 | 类型、序列化、位点排序 |
| 数据流 | canal-binlog | 9 | binlog 转换、表映射 |
| 存储层 | canal-store | 9 | 内存缓冲、位点追踪、溢出、超大batch |
| 过滤 | canal-filter | 6 | 正则匹配、黑名单 |
| 实例管理 | canal-instance | 7 | 注册、查询、生命周期、无效过滤器 |
| 元数据 | canal-meta | 6 | 表信息缓存、主键 |
| 监控 | canal-prometheus | 5 | 计数器、Gauge、HTTP 端点 |
| 连接器 | canal-connector | 4 | Kafka 序列化 |
| API | canal-admin | 10 | auth、REST 健康检查、实例管理 |
| 分发 | canal-sink | 3 | 过滤、存储、连接器分发 |
| 客户端 | canal-client | 3 | 构建器模式、ID 唯一性 |
| CLI | canal-cli | 0 | — |
| Proto | canal-proto | 0 | 生成代码 |

---

## 本日新增测试 (v5→v6)

| Crate | 新增 | 内容 |
|-------|------|------|
| canal-admin | +7 | check_auth (6 cases)、admin_server_with_auth、debug_masks_token |
| canal-binlog | +1 | test_put_with_columns |
| canal-common | +2 | test_log_position_ord、test_log_position_ord_suffix_fallback |
| canal-instance | +1 | test_invalid_filter_returns_error |
| canal-store | +1 | test_oversized_batch_truncated |

---

## 测试质量评估

- **二进制协议覆盖完整**: canal-server 的 25 个测试覆盖了编解码边界情况、协议 round-trip 以及 CanalEvent→Entry 转换的各种 DML 类型
- **存储层正确性验证**: 包含 buffer 溢出驱逐、超大 batch 截断、生命周期启停、空批处理等边界场景
- **Admin auth 全覆盖**: 10 个测试覆盖无 token、有效/无效 Bearer、裸 token、空 header 等场景
- **过滤器边界测试**: 覆盖无效正则、具体表、通配符、黑名单等场景
- **缺失覆盖**: canal-cli (0 tests)、canal-proto (生成代码，0 tests)
