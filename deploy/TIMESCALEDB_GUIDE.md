# TimescaleDB 使用指南

> **版本**: 0.1.0  
> **用途**: TimescaleDB 消息存储配置和使用说明

---

## 📋 概述

Flare IM 使用 **TimescaleDB** 作为消息存储数据库。TimescaleDB 是基于 PostgreSQL 的时序数据库，专门优化了时序数据的存储和查询性能。

---

## 🎯 为什么使用 TimescaleDB

### 优势

1. **时序数据优化**: 专为时序数据设计，查询性能优异
2. **自动分区**: 按时间自动分区，管理简单
3. **压缩存储**: 自动压缩历史数据，节省存储空间
4. **连续聚合**: 支持预聚合视图，加速统计查询
5. **数据保留策略**: 自动清理过期数据
6. **PostgreSQL 兼容**: 完全兼容 PostgreSQL，生态丰富

### 适用场景

- ✅ 消息存储（按时间查询）
- ✅ 日志存储
- ✅ 指标存储
- ✅ 事件流数据

---

## 🏗️ 架构设计

### 超表（Hypertable）

消息表 `messages` 被转换为 TimescaleDB 超表：

```sql
-- 按时间分区，每个分区 1 天
SELECT create_hypertable('messages', 'timestamp', 
    chunk_time_interval => INTERVAL '1 day'
);
```

### 分区策略

- **分区键**: `timestamp` (消息时间戳)
- **分区间隔**: 1 天
- **自动管理**: TimescaleDB 自动创建和管理分区

---

## 📊 数据模型

### 消息表结构

```sql
CREATE TABLE messages (
    id VARCHAR(255) PRIMARY KEY,
    conversation_id VARCHAR(255) NOT NULL,
    sender_id VARCHAR(255) NOT NULL,
    receiver_ids JSONB,
    content BYTEA,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    extra JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

### 索引

- `idx_messages_conversation_id`: 会话ID索引
- `idx_messages_timestamp`: 时间戳索引
- `idx_messages_sender_id`: 发送者ID索引
- `idx_messages_conversation_timestamp`: 会话+时间复合索引

---

## 🔍 常用查询

### 1. 查询会话消息（按时间范围）

```sql
-- 查询最近 7 天的消息
SELECT * FROM messages
WHERE conversation_id = 'conversation_123'
  AND timestamp >= NOW() - INTERVAL '7 days'
ORDER BY timestamp DESC
LIMIT 100;
```

### 2. 统计消息数量

```sql
-- 按小时统计消息数量
SELECT 
    time_bucket('1 hour', timestamp) AS hour,
    COUNT(*) AS message_count
FROM messages
WHERE timestamp >= NOW() - INTERVAL '24 hours'
GROUP BY hour
ORDER BY hour;
```

### 3. 使用连续聚合视图

```sql
-- 查询预聚合的统计数据
SELECT * FROM messages_hourly_stats
WHERE hour >= NOW() - INTERVAL '24 hours'
ORDER BY hour DESC;
```

---

## ⚙️ 配置说明

### 分区间隔

当前配置为 **1 天**，适合中等规模的消息量。可根据实际需求调整：

```sql
-- 调整为 1 小时（适合高并发场景）
SELECT set_chunk_time_interval('messages', INTERVAL '1 hour');

-- 调整为 7 天（适合低并发场景）
SELECT set_chunk_time_interval('messages', INTERVAL '7 days');
```

### 数据保留策略

启用数据保留策略，自动清理过期数据：

```sql
-- 保留最近 90 天的数据
SELECT add_retention_policy('messages', INTERVAL '90 days');
```

### 压缩策略

启用压缩策略，压缩历史数据：

```sql
-- 压缩 7 天前的数据
SELECT add_compression_policy('messages', INTERVAL '7 days');
```

---

## 📈 性能优化

### 1. 查询优化

- ✅ 使用时间范围查询（利用分区裁剪）
- ✅ 使用复合索引（conversation_id + timestamp）
- ✅ 使用连续聚合视图（统计查询）

### 2. 写入优化

- ✅ 批量插入（减少事务开销）
- ✅ 异步写入（通过 JetStream 削峰）
- ✅ 连接池（复用数据库连接）

### 3. 存储优化

- ✅ 启用压缩（节省存储空间）
- ✅ 设置保留策略（自动清理）
- ✅ 定期 VACUUM（清理碎片）

---

## 🔧 维护操作

### 查看超表信息

```sql
-- 查看所有超表
SELECT * FROM timescaledb_information.hypertables;

-- 查看分区信息
SELECT * FROM timescaledb_information.chunks
WHERE hypertable_name = 'messages';
```

### 手动压缩

```sql
-- 压缩指定时间范围的数据
SELECT compress_chunk(chunk)
FROM timescaledb_information.chunks
WHERE hypertable_name = 'messages'
  AND range_start < NOW() - INTERVAL '7 days';
```

### 手动删除分区

```sql
-- 删除 90 天前的分区
SELECT drop_chunks('messages', INTERVAL '90 days');
```

---

## 📚 相关文档

- [TimescaleDB 官方文档](https://docs.timescale.com/)
- [TimescaleDB 最佳实践](https://docs.timescale.com/timescaledb/latest/how-to-guides/)
- [PostgreSQL 文档](https://www.postgresql.org/docs/)

---

**文档维护**: Flare IM Architecture Team  
**最后更新**: 2025-01-XX  
**版本**: 0.1.0

