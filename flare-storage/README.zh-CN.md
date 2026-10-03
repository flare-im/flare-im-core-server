# Flare Storage 存储服务

[English](README.md) · 中文

> **状态**: 核心链路以 PostgreSQL / TimescaleDB 为唯一持久化数据库，Redis 仅承载热缓存、WAL、短期状态和幂等辅助。

`flare-storage` 包含两个核心服务：

- `flare-storage/writer`：消费 MQ 存储事件，完成消息归档、事件流、ledger、热缓存和 ACK。
- `flare-storage/reader`：提供历史消息、事件流、审计和导出查询。

## 设计边界

- 不使用 MongoDB 存储消息体或实时数据。
- 不做同一份消息数据的跨数据库同步双写。
- PostgreSQL / TimescaleDB 是消息、事件、ledger、审计查询的唯一持久化数据库族。
- Redis 只用于热缓存、发送/写入 WAL、幂等窗口、在线/短期状态和读路径加速。
- MQ 是写路径异步边界；writer 消费失败进入 retry / DLQ，而不是让接入层同步等待数据库成功。

## Writer

`flare-storage/writer` 的职责：

- 消费 `flare.im.message.storage` 和 `flare.im.message.events` 等存储 topic。
- 使用 Redis 幂等窗口和 PostgreSQL `message_write_ledger` 保证重复投递可重入。
- 将消息写入 `messages`，将操作/同步事件写入 `events`。
- 维护热缓存和写入阶段状态。
- 发布 ACK 或失败状态，供客户端 ack/sync 收敛和运维排障。

持久消息写入流：

```text
flare-message-ingest
  -> MQ main
  -> flare-orchestrator fanout
  -> storage topic
  -> flare-storage/writer
  -> PostgreSQL / TimescaleDB
  -> ACK / retry / DLQ
```

失败处理：

- 可重试错误进入 `STORAGE_MESSAGE_RETRY_TOPIC`。
- 超过重试预算或不可恢复错误进入 `STORAGE_MESSAGE_DLQ_TOPIC`。
- 重投按同一 `(tenant_id, conversation_id, seq)` 上已有的 `events` 行判重：同一事件（完全相同、map 条目顺序不同的重新编码、修复前 Writer 落的旧类型号，或只差元数据的同一条消息）按重复接受并保留已有行；另一个事件按冲突处理，经重试进 DLQ。
- `message_write_ledger` 记录 archive/event/ack 等阶段，便于按 `tenant_id + server_id` 排障。

## Reader

`flare-storage/reader` 的职责：

- `QueryMessages`：按会话、seq、时间范围、分页游标查询消息。
- `GetMessage`：按消息 ID 查询详情。
- `QueryMessageEvents`：查询消息相关事件流。
- `QueryMessageWriteLedger`：查询写入阶段和失败原因。
- `ExportMessages`：登记导出任务，文件生成由后续 worker 执行。
- 消息操作查询和审计读取统一回源 PostgreSQL / TimescaleDB，Redis 只作为缓存。

## 配置

Writer 常用环境变量：

- `STORAGE_JETSTREAM_URL`
- `STORAGE_JETSTREAM_GROUP`
- `STORAGE_JETSTREAM_ACK_SUBJECT`
- `STORAGE_MESSAGE_RETRY_TOPIC`
- `STORAGE_MESSAGE_DLQ_TOPIC`
- `STORAGE_REDIS_URL`
- `STORAGE_REDIS_HOT_TTL_SECONDS`
- `STORAGE_REDIS_IDEMPOTENCY_TTL_SECONDS`
- `STORAGE_POSTGRES_URL`
- `STORAGE_POSTGRES_MAX_CONNECTIONS`
- `STORAGE_POSTGRES_MIN_CONNECTIONS`
- `STORAGE_POSTGRES_ACQUIRE_TIMEOUT_SECONDS`
- `STORAGE_WAL_HASH_KEY`

Reader 常用环境变量：

- `STORAGE_REDIS_URL`
- `STORAGE_POSTGRES_URL`（未设时取 `POSTGRES_URL`）
- `STORAGE_READER_DEFAULT_RANGE_SECONDS`
- `STORAGE_READER_MAX_PAGE_SIZE`

两个进程各自共享一条 Redis 连接，上限如下：

- `STORAGE_REDIS_CONNECT_TIMEOUT_MS`：每次建连尝试（TCP 加握手）的上限，默认 2000。建连失败后这段时间内不再发起连接，直接失败。
- `STORAGE_REDIS_RESPONSE_TIMEOUT_MS`：每条命令或流水线等待应答的上限，默认 3000。
- `STORAGE_REDIS_CONNECT_RETRIES`：建连失败后的重试次数，0 到 5，默认 1（两次尝试间隔约 1 到 2 s）。

按默认值，Redis 不可达时一次调用约 6 s 内失败。Writer 对写热缓存失败只记 WARN 与 `storage_writer_persist_total{path="hot_cache",result="error"}`，照常确认已持久的写入；Reader 回落到 PostgreSQL。取值超出范围时进程启动即失败。

实际配置优先来自 `config/services/*.toml` 和 `FlareAppConfig`，环境变量用于部署覆盖。

## PostgreSQL / TimescaleDB

权威的 schema 历史是 `migrations/*.sql`，由 `flare-migrate` 执行并校验 checksum（`make db-migrate`，见 `tools/flare-migrate/README.md`）。`deploy/init.sql` 只是同一 schema 的破坏性开发快照（`make dev-reset`），不能用来升级已有的库。本 README 不复制 DDL，避免 schema 漂移。

当前核心约定：

- `messages` 是消息聚合根，按 `created_at` 建 TimescaleDB hypertable。
- `timestamp` 是业务消息时间，用于时间线、筛选和展示，不作为分区键。
- `events` 是 durable event stream，按 `tenant_id + conversation_id + seq` 保证事件幂等。
- `message_write_ledger` 是普通表，负责 `tenant_id + server_id` 的最终幂等和写链路状态诊断。
- 同步查询优先使用 `(tenant_id, conversation_id, seq)`。
- 管理端检索优先使用租户 + 维度 + `timestamp DESC` 组合索引。

完整字段、索引、压缩策略和触发器见 `migrations/`（或 `deploy/init.sql` 中的快照）与 `deploy/TIMESCALEDB_GUIDE.md`。

## 验证

```bash
cargo test --package flare-storage-writer
cargo test --package flare-storage-reader
```
