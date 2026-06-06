# Flare Storage 存储服务

> **版本**: v1.0.0  
> **更新日期**: 2025-01-XX  
> **状态**: ✅ 核心功能已实现

---

## 📋 模块概述

`flare-storage` 包含两个核心服务：

### 1. flare-storage/writer（消息持久化服务）

**职责**：
- 订阅 JetStream `storage-messages` Topic
- 消息持久化到 PostgreSQL/TimescaleDB
- 消息缓存到 Redis（热数据）
- 消息存储到 MongoDB（实时数据）
- 会话状态更新（Redis）
- 用户游标更新（Redis）
- ACK 确认发布（JetStream）

**核心功能**：
- ✅ 消息去重（基于 message_id，Redis SETNX）
- ✅ 消息持久化（PostgreSQL/TimescaleDB）
- ✅ 消息缓存（Redis，TTL: 1小时）
- ✅ 会话状态更新（Redis Hash）
- ✅ 用户游标更新（Redis Hash）
- ✅ ACK 确认发布（JetStream）

**数据流**：
```
JetStream (storage-messages) 
  → Writer Consumer
    → 幂等性检查 (Redis)
    → 热缓存 (Redis)
    → 实时存储 (MongoDB)
    → 归档存储 (PostgreSQL/TimescaleDB)
    → 会话状态更新 (Redis)
    → 用户游标更新 (Redis)
    → ACK 发布 (JetStream)
```

---

### 2. flare-storage/reader（消息查询服务）

**职责**：
- 提供消息查询接口（gRPC）
- Redis 缓存查询（优先）
- 数据库回源查询（缓存未命中）
- 消息搜索和导出任务登记

**已实现的接口**：
- ✅ `QueryMessages` - 查询消息列表（支持分页、时间范围、游标）
- ✅ `GetMessage` - 获取单条消息
- ✅ `DeleteMessage` - 删除消息（软删除）
- ✅ `RecallMessage` - 撤回消息（支持时间限制）
- ✅ `ClearConversation` - 清理会话消息
- ✅ `MarkMessageRead` - 标记消息已读（支持阅后即焚）
- ✅ `ExportMessages` - 创建消息导出任务（持久化 pending task，文件生成由后续 worker 处理）

**待实现的接口**：
- ⏳ `DeleteMessageForUser` - 为用户删除消息（软删除，只对特定用户隐藏）
- ⏳ `SearchMessages` - 全文搜索消息
- ⏳ `SetMessageAttributes` - 设置消息属性
- ⏳ `ListMessageTags` - 列出消息标签

---

## 🏗️ 目录结构

### Writer 结构

```
flare-storage/writer/
├── cmd/
│   └── main.rs                    # 应用入口
├── src/
│   ├── lib.rs                     # 库入口
│   ├── config.rs                  # 配置管理
│   ├── application/
│   │   └── commands/
│   │       └── process_store_message.rs  # 消息处理命令
│   ├── domain/
│   │   ├── events.rs              # 领域事件
│   │   ├── message_persistence.rs # 消息持久化模型
│   │   └── repositories.rs       # 仓储接口
│   ├── infrastructure/
│   │   ├── persistence/
│   │   │   ├── redis_cache.rs     # Redis 热缓存
│   │   │   ├── redis_idempotency.rs # Redis 幂等性
│   │   │   ├── mongo_store.rs     # MongoDB 实时存储
│   │   │   ├── postgres_store.rs  # PostgreSQL 归档存储
│   │   │   ├── conversation_state.rs   # 会话状态更新
│   │   │   └── user_cursor.rs     # 用户游标更新
│   │   └── messaging/
│   │       └── ack_publisher.rs   # ACK 发布器
│   ├── interface/
│   │   └── messaging/
│   │       └── consumer.rs        # JetStream 消费者
│   └── service/
│       ├── bootstrap.rs            # 应用启动器
│       └── registry.rs             # 服务注册
└── tests/
    ├── integration_test.rs        # 集成测试
    └── storage_test.rs             # 存储测试
```

### Reader 结构

```
flare-storage/reader/
├── cmd/
│   └── main.rs                    # 应用入口
├── src/
│   ├── lib.rs                     # 库入口
│   ├── config.rs                  # 配置管理
│   ├── application/
│   │   ├── queries/
│   │   │   ├── query_messages.rs  # 查询消息服务
│   │   │   └── get_message.rs     # 获取单条消息服务
│   │   └── commands/
│   │       ├── delete_message.rs   # 删除消息服务
│   │       ├── recall_message.rs   # 撤回消息服务
│   │       ├── clear_conversation.rs    # 清理会话服务
│   │       └── mark_read.rs        # 标记已读服务
│   ├── domain/
│   │   └── mod.rs                 # 领域接口定义
│   ├── infrastructure/
│   │   └── persistence/
│   │       └── mongo.rs            # MongoDB 存储实现
│   ├── interface/
│   │   └── grpc/
│   │       ├── server.rs           # gRPC 服务器
│   │       └── handler.rs         # 请求处理器
│   └── service/
│       ├── bootstrap.rs           # 应用启动器
│       └── registry.rs            # 服务注册
└── tests/
    └── reader_test.rs              # Reader 测试
```

---

## 🔧 配置说明

### Writer 配置

环境变量：
- `JETSTREAM_BOOTSTRAP_SERVERS` - JetStream 服务器地址
- `JETSTREAM_TOPIC` - 消息 Topic（默认: `storage-messages`）
- `JETSTREAM_GROUP_ID` - 消费者组 ID
- `JETSTREAM_ACK_TOPIC` - ACK Topic（可选）
- `REDIS_URL` - Redis 连接地址
- `REDIS_HOT_TTL_SECONDS` - 热缓存 TTL（默认: 3600秒）
- `REDIS_IDEMPOTENCY_TTL_SECONDS` - 幂等性 TTL（默认: 86400秒）
- `MONGO_URL` - MongoDB 连接地址（可选）
- `MONGO_DATABASE` - MongoDB 数据库名
- `MONGO_COLLECTION` - MongoDB 集合名
- `POSTGRES_URL` - PostgreSQL 连接地址（可选）
- `WAL_HASH_KEY` - WAL Hash Key（可选）

### Reader 配置

环境变量：
- `REDIS_URL` - Redis 连接地址（可选）
- `MONGO_URL` - MongoDB 连接地址（可选）
- `MONGO_DATABASE` - MongoDB 数据库名（默认: `flare_im`）
- `MONGO_COLLECTION` - MongoDB 集合名（默认: `messages`）
- `POSTGRES_URL` - PostgreSQL 连接地址（可选）
- `STORAGE_READER_DEFAULT_RANGE_SECONDS` - 默认查询时间范围（默认: 7天）
- `STORAGE_READER_MAX_PAGE_SIZE` - 最大分页大小（默认: 200）

---

## 📊 数据库表结构

### PostgreSQL/TimescaleDB

数据库初始化以 `deploy/init.sql` 为唯一入口，不在 storage 模块 README 中复制 DDL，避免 schema 漂移。

当前核心约定：

- `messages` 是消息聚合根，按 `created_at` 建 TimescaleDB Hypertable。
- `timestamp` 是业务消息时间，用于时间线、筛选和展示，不作为分区键。
- `message_write_ledger` 是普通表，负责 `tenant_id + server_id` 的最终幂等和写链路状态诊断。
- 同步查询优先使用 `(tenant_id, conversation_id, seq)`。
- 管理端检索优先使用租户 + 维度 + `timestamp DESC` 组合索引。

完整字段、索引、压缩策略和触发器见 `deploy/init.sql` 与 `deploy/TIMESCALEDB_GUIDE.md`。

---

## 🧪 测试

### 运行测试

```bash
# Writer 测试
cargo test --package flare-storage-writer

# Reader 测试
cargo test --package flare-storage-reader

# 所有测试
cargo test --workspace
```

### 测试覆盖

- ✅ 消息持久化流程测试
- ✅ 消息去重测试
- ✅ 消息查询测试
- ✅ 消息删除测试
- ✅ 消息撤回测试
- ✅ 消息已读标记测试

---

## 🚀 部署

### Writer 部署

```bash
# 启动 Writer
cargo run --bin flare-storage-writer

# 或使用 Docker
docker run flare-storage-writer
```

### Reader 部署

```bash
# 启动 Reader
cargo run --bin flare-storage-reader

# 或使用 Docker
docker run flare-storage-reader
```

---

## 📈 性能指标

### Writer 性能目标

- **消息写入 TPS**: 50,000+/实例
- **消息写入延迟**: P99 < 50ms
- **缓存命中率**: > 80%

### Reader 性能目标

- **消息查询 TPS**: 100,000+/实例
- **消息查询延迟**: P99 < 100ms
- **缓存命中率**: > 80%

---

## 🔍 监控指标

### Writer 指标

- `storage_writer_messages_processed_total` - 处理的消息总数
- `storage_writer_messages_duplicated_total` - 重复消息数
- `storage_writer_write_latency_seconds` - 写入延迟
- `storage_writer_cache_hit_rate` - 缓存命中率

### Reader 指标

- `storage_reader_queries_total` - 查询总数
- `storage_reader_query_latency_seconds` - 查询延迟
- `storage_reader_cache_hit_rate` - 缓存命中率

---

## 📚 参考文档

- [架构设计总览](../doc/架构设计总览.md)
- [消息系统设计](../doc/消息系统设计.md)
- [存储层实施计划](../doc/plan/03-存储层-消息存储实施.md)
- [StorageService Proto定义](../../flare-proto/proto/storage.proto)

---

**文档维护**: Flare IM Architecture Team  
**最后更新**: 2025-01-XX  
**版本**: v1.0.0
