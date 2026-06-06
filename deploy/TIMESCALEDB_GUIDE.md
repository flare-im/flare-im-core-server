# TimescaleDB 使用指南

> 版本: 0.1.0
> 用途: 说明 `deploy/init.sql` 中消息主存储的 TimescaleDB 设计和维护方式

---

## 概述

Flare IM Core 使用 PostgreSQL + TimescaleDB 承载消息主存储。当前部署脚本以 [init.sql](./init.sql) 为唯一 schema 入口，`messages` 表被转换为 TimescaleDB Hypertable，用于优化按时间写入、会话消息查询、管理端检索和历史数据压缩。

---

## 当前设计

### Hypertable

`messages` 表按 `created_at` 分区，而不是按业务消息时间 `timestamp` 分区：

```sql
SELECT create_hypertable(
    'messages',
    'created_at',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);
```

这样做的原因是 `created_at` 是数据库入库时间，适合作为稳定、单调、可控的写入分区键；`timestamp` 保留为业务时间，仍用于会话时间线、管理端筛选和展示。

### 主键与唯一性

TimescaleDB 对 Hypertable 的唯一索引要求包含分区列，因此 `messages` 使用：

```sql
PRIMARY KEY (created_at, server_id)
```

消息全局幂等不依赖 Hypertable 唯一约束，而是由普通表 `message_write_ledger` 承载：

```sql
PRIMARY KEY (tenant_id, server_id)
```

这能避免 TimescaleDB 分区唯一索引限制影响消息写入链路，同时让恢复、补偿和管理端诊断都有明确账本。

---

## 核心表结构

`messages` 是消息聚合根，字段与 `common/message.proto` 和存储写入路径对齐，关键字段包括：

- `tenant_id`: 租户隔离键
- `server_id`: 服务端消息 ID
- `conversation_id`: 会话 ID
- `sender_id`: 发送者
- `channel_id`: 路由频道
- `seq`: 会话内主序
- `timestamp`: 业务消息时间
- `created_at`: 入库时间，也是 Hypertable 分区键
- `message_type`: 消息类型
- `status`: 消息状态
- `content`: protobuf 编码后的消息体
- `extra` / `extensions`: 扩展字段

完整字段以 [init.sql](./init.sql) 为准。

---

## 查询索引

当前 `init.sql` 为写入、同步、管理端查询和媒体生命周期保留了核心索引：

```sql
CREATE INDEX IF NOT EXISTS idx_messages_tenant_conv_seq
    ON messages(tenant_id, conversation_id, seq);

CREATE INDEX IF NOT EXISTS idx_messages_conversation_ts
    ON messages(tenant_id, conversation_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_messages_sender_client
    ON messages(tenant_id, sender_id, client_msg_id)
    WHERE client_msg_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_messages_tenant_timestamp
    ON messages(tenant_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_messages_tenant_sender_timestamp
    ON messages(tenant_id, sender_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_messages_tenant_message_type_timestamp
    ON messages(tenant_id, message_type, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_messages_tenant_status_timestamp
    ON messages(tenant_id, status, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_messages_tenant_source_timestamp
    ON messages(tenant_id, source, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_messages_tenant_channel_timestamp
    ON messages(tenant_id, channel_id, timestamp DESC)
    WHERE channel_id IS NOT NULL;
```

索引设计原则：

- IM 同步优先走 `(tenant_id, conversation_id, seq)`。
- 会话时间线优先走 `(tenant_id, conversation_id, timestamp DESC)`。
- 管理端多维检索优先走租户 + 筛选维度 + 时间倒序索引。
- 写入幂等优先走 `message_write_ledger`，不把复杂唯一性压到 Hypertable。

---

## Columnstore / 压缩

当前脚本启用 TimescaleDB columnstore，并按租户与会话分段：

```sql
ALTER TABLE messages SET (
    timescaledb.enable_columnstore = true,
    timescaledb.segmentby = 'tenant_id, conversation_id',
    timescaledb.orderby = 'created_at DESC, server_id'
);
```

同时会尝试为 30 天前的消息增加 columnstore policy：

```sql
CALL add_columnstore_policy('messages', after => INTERVAL '30 days');
```

`init.sql` 已兼容不支持 `add_columnstore_policy` 的 TimescaleDB 版本：如果函数不可用，会打印 notice 并跳过，不影响本地初始化。

---

## 常用查询

### 查询会话消息

```sql
SELECT *
FROM messages
WHERE tenant_id = '0'
  AND conversation_id = 'conversation_123'
  AND seq > 100
ORDER BY seq ASC
LIMIT 100;
```

### 管理端按时间查询

```sql
SELECT *
FROM messages
WHERE tenant_id = '0'
  AND timestamp >= NOW() - INTERVAL '7 days'
ORDER BY timestamp DESC
LIMIT 100;
```

### 管理端按发送者查询

```sql
SELECT *
FROM messages
WHERE tenant_id = '0'
  AND sender_id = 'user_123'
  AND timestamp >= NOW() - INTERVAL '7 days'
ORDER BY timestamp DESC
LIMIT 100;
```

### 查询写入账本异常

```sql
SELECT *
FROM message_write_ledger
WHERE tenant_id = '0'
  AND failed_at IS NOT NULL
ORDER BY updated_at DESC
LIMIT 100;
```

---

## 维护操作

### 查看 Hypertable

```sql
SELECT *
FROM timescaledb_information.hypertables
WHERE hypertable_name = 'messages';
```

### 查看 Chunk

```sql
SELECT *
FROM timescaledb_information.chunks
WHERE hypertable_name = 'messages'
ORDER BY range_start DESC;
```

### 调整分区间隔

默认分区间隔为 1 天。高写入量场景可压到 6 小时或 1 小时，低写入量场景可扩大到 7 天：

```sql
SELECT set_chunk_time_interval('messages', INTERVAL '6 hours');
```

调整前应结合写入量、查询时间窗口、chunk 数量和压缩策略一起评估。

### 手动压缩历史 Chunk

```sql
SELECT compress_chunk(chunk)
FROM timescaledb_information.chunks
WHERE hypertable_name = 'messages'
  AND range_start < NOW() - INTERVAL '30 days';
```

不同 TimescaleDB 版本对 compression / columnstore API 命名不同，本地以 `init.sql` 兼容逻辑为准。

---

## 后续扩展

连续聚合视图目前未在 `init.sql` 中默认创建。原因是当前核心优先保证消息写入、同步、管理端检索和压缩策略清晰；统计型视图建议在管理分析服务或独立迁移中按真实运营指标追加，例如按租户、会话类型、消息类型、小时级流量聚合。

如需增加连续聚合，建议单独评估：

- 聚合粒度是否会影响写入性能
- 是否需要 tenant_id 维度隔离
- 是否由管理端服务维护，而不是 IM Core 写入链路承担
- 是否需要与 Prometheus 指标区分职责

---

## 相关文件

- [init.sql](./init.sql): 唯一数据库初始化入口
- [docker-compose.yml](./docker-compose.yml): 本地 TimescaleDB 容器配置
- [README.md](./README.md): deploy 目录整体说明
