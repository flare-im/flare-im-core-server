//! 优化的 PostgreSQL 消息存储实现
//!
//! 提供高性能的查询、批处理和缓存功能

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::Row;
use tokio::time::Instant;
use tracing::instrument;

use crate::convert::{event_from_proto, message_from_proto, message_to_proto};
use crate::domain::model::{ConversationMessageHead, Event, EventType, FilterExpression, Message, MessageUpdate, VisibilityStatus};
use crate::domain::repository::message_storage::MessageStorage;
use crate::infrastructure::persistence::event_stream_row::proto_event_from_events_row;
use crate::infrastructure::persistence::helpers::*;
use crate::infrastructure::persistence::postgres_base::PostgresBaseStorage;
use crate::infrastructure::persistence::redis_cache::RedisMessageCache;
use flare_server_core::context::Ctx;

// TODO: 暂时使用占位符类型，等 monitoring 模块实现后再替换
// use crate::infrastructure::monitoring::performance_metrics::PerformanceMetrics;
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {}

impl PerformanceMetrics {
    pub fn record_cache_hit(&self, _cache_type: &str) {}
    pub fn record_cache_miss(&self, _cache_type: &str) {}
    pub fn record_query(&self, _query_type: &str, _duration_ms: u64) {}
}

/// 优化的 PostgreSQL 消息存储实现
#[derive(Clone)]
pub struct OptimizedPostgresMessageStorageImpl {
    pub base: PostgresBaseStorage,
    pub cache: Option<Arc<RedisMessageCache>>,
    pub metrics: Option<Arc<PerformanceMetrics>>,
}

impl OptimizedPostgresMessageStorageImpl {
    pub fn new(
        base: PostgresBaseStorage, 
        cache: Option<Arc<RedisMessageCache>>, 
        metrics: Option<Arc<PerformanceMetrics>>
    ) -> Self {
        Self { base, cache, metrics }
    }
}

#[async_trait]
impl MessageStorage for OptimizedPostgresMessageStorageImpl {
    #[instrument(skip(self, _message), fields(message_id = %_message.server_id))]
    async fn store_message(&self, ctx: &Ctx, _message: &Message, _conversation_id: &str) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        // 读侧存储通常不需要实现 store_message
        // 但为了兼容性，可以提供一个空实现或委托给 Writer
        tracing::warn!(
            message_id = %_message.server_id,
            "store_message called on read-only storage, this should be handled by Storage Writer"
        );
        Ok(())
    }

    #[instrument(skip(self), fields(conversation_id))]
    async fn query_messages(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        user_id: Option<&str>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
        limit: i32,
    ) -> Result<Vec<Message>> {
        let _ = ctx; // 上下文用于日志追踪
        let start = Instant::now();
        let start_ts = start_time.unwrap_or_else(|| Utc::now() - chrono::Duration::days(7));
        let end_ts = end_time.unwrap_or(Utc::now());
        let limit = limit.min(1000).max(1); // 限制范围 1-1000

        // L2 缓存策略：先查 Redis，未命中再查 TimescaleDB
        if let Some(cache) = &self.cache {
            if let Ok(Some(cached_messages)) = cache
                .get_session_messages(conversation_id, start_ts, end_ts, limit)
                .await
            {
                tracing::debug!(
                    conversation_id = %conversation_id,
                    cached_count = cached_messages.len(),
                    "Cache hit: retrieved messages from Redis"
                );

                // 转换 proto 类型的消息为领域模型类型
                let domain_messages: Vec<Message> = cached_messages
                    .into_iter()
                    .map(|msg| message_from_proto(&msg))
                    .collect();

                tracing::debug!(
                    conversation_id = %conversation_id,
                    cached_count = domain_messages.len(),
                    "Cache hit: retrieved messages from Redis"
                );

                // 记录缓存命中指标
                if let Some(ref metrics) = self.metrics {
                    metrics.record_cache_hit("redis");
                }

                return Ok(domain_messages);
            }
        }

        // 记录缓存未命中
        if let Some(ref metrics) = self.metrics {
            metrics.record_cache_miss("redis");
        }

        // init_v2: messages 列为 INT + channel_id, offline_push_info, extensions；visibility_status 为 INT（1=HIDDEN, 2=DELETED）
        let result = if let Some(uid) = user_id {
            sqlx::query(
                r#"
                SELECT 
                    m.tenant_id, m.server_id, m.conversation_id, m.client_msg_id, m.sender_id, m.sender_name, m.sender_avatar,
                    m.channel_id, m.source, m.seq, m.timestamp, m.conversation_type, m.message_type,
                    m.content, m.status, m.offline_push_info, m.extra, m.extensions, m.created_at, m.persisted_at, m.delivered_at
                FROM messages m
                WHERE m.conversation_id = $1 AND m.timestamp >= $2 AND m.timestamp <= $3
                  AND NOT EXISTS (
                      SELECT 1 FROM message_visibility mv
                      WHERE mv.tenant_id = m.tenant_id AND mv.message_id = m.server_id AND mv.user_id = $4
                        AND mv.visibility_status IN (1, 2)
                  )
                ORDER BY m.timestamp DESC, m.seq DESC NULLS LAST
                LIMIT $5
                "#,
            )
            .bind(conversation_id)
            .bind(start_ts)
            .bind(end_ts)
            .bind(uid)
            .bind(limit)
            .fetch_all(&self.base.pool)
            .await
        } else {
            sqlx::query(
                r#"
                SELECT 
                    tenant_id, server_id, conversation_id, client_msg_id, sender_id, sender_name, sender_avatar,
                    channel_id, source, seq, timestamp, conversation_type, message_type, content,
                    status, offline_push_info, extra, extensions, created_at, persisted_at, delivered_at
                FROM messages
                WHERE conversation_id = $1 AND timestamp >= $2 AND timestamp <= $3
                ORDER BY timestamp DESC, seq DESC NULLS LAST
                LIMIT $4
                "#,
            )
            .bind(conversation_id)
            .bind(start_ts)
            .bind(end_ts)
            .bind(limit)
            .fetch_all(&self.base.pool)
            .await
        }
        .context("Failed to query messages");

        // 记录查询性能指标
        let duration = start.elapsed();
        if let Some(ref metrics) = self.metrics {
            metrics.record_query("query_messages", duration.as_millis() as u64);
        }

        let rows = result?;

        let mut messages = Vec::with_capacity(rows.len());
        for row in rows {
            messages.push(self.base.row_to_message(&row)?);
        }

        // 反转顺序，使最旧的消息在前（符合历史消息查询习惯）
        messages.reverse();

        // 回填缓存（异步，不阻塞）
        if let Some(cache) = &self.cache {
            let cache_clone = std::sync::Arc::clone(cache);
            let messages_clone = messages.clone();
            let conversation_id_clone = conversation_id.to_string();
            tokio::spawn(async move {
                // 转换领域模型消息为 proto 类型
                let proto_messages: Vec<flare_proto::Message> = messages_clone
                    .iter()
                    .map(|msg| message_to_proto(msg))
                    .collect();

                if let Err(e) = cache_clone
                    .cache_session_messages(&conversation_id_clone, start_ts, end_ts, &proto_messages)
                    .await
                {
                    tracing::warn!(
                        error = %e,
                        "Failed to cache messages to Redis (non-blocking)"
                    );
                }
            });
        }

        Ok(messages)
    }

    #[instrument(skip(self), fields(conversation_id))]
    async fn query_messages_by_seq(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        user_id: Option<&str>,
        after_seq: i64,
        before_seq: Option<i64>,
        limit: i32,
    ) -> Result<Vec<Message>> {
        let _ = ctx; // 上下文用于日志追踪
        let limit = limit.min(1000).max(1);

        // 构建查询：基于 seq 查询（性能更好），支持多租户
        // 优化：使用预编译查询和索引优化
        let rows = if let Some(uid) = user_id {
            sqlx::query(
                r#"
                SELECT 
                    m.tenant_id, m.server_id, m.conversation_id, m.client_msg_id, m.sender_id, m.sender_name, m.sender_avatar,
                    m.channel_id, m.source, m.seq, m.timestamp, m.conversation_type, m.message_type,
                    m.content, m.status, m.offline_push_info, m.extra, m.extensions, m.created_at, m.persisted_at, m.delivered_at
                FROM messages m
                WHERE m.conversation_id = $1 AND m.seq > $2 AND ($3::BIGINT IS NULL OR m.seq < $3)
                  AND NOT EXISTS (
                      SELECT 1 FROM message_visibility mv
                      WHERE mv.tenant_id = m.tenant_id AND mv.message_id = m.server_id AND mv.user_id = $4
                        AND mv.visibility_status IN (1, 2)
                  )
                ORDER BY m.seq ASC
                LIMIT $5
                "#,
            )
            .bind(conversation_id)
            .bind(after_seq)
            .bind(before_seq)
            .bind(uid)
            .bind(limit)
            .fetch_all(&self.base.pool)
            .await
        } else {
            sqlx::query(
                r#"
                SELECT tenant_id, server_id, conversation_id, client_msg_id, sender_id, sender_name, sender_avatar,
                    channel_id, source, seq, timestamp, conversation_type, message_type, content,
                    status, offline_push_info, extra, extensions, created_at, persisted_at, delivered_at
                FROM messages
                WHERE conversation_id = $1 AND seq > $2 AND ($3::BIGINT IS NULL OR seq < $3)
                ORDER BY seq ASC
                LIMIT $4
                "#,
            )
            .bind(conversation_id)
            .bind(after_seq)
            .bind(before_seq)
            .bind(limit)
            .fetch_all(&self.base.pool)
            .await
        }
        .context("Failed to query messages by seq")?;

        let mut messages = Vec::with_capacity(rows.len());
        for row in rows {
            messages.push(self.base.row_to_message(&row)?);
        }

        Ok(messages)
    }

    #[instrument(skip(self), fields(message_id))]
    async fn get_message(&self, ctx: &Ctx, message_id: &str) -> Result<Option<Message>> {
        let _ = ctx; // 上下文用于日志追踪
        // 1. Query Database
        // Support querying by server_id or client_msg_id
        let row = sqlx::query(
            r#"
            SELECT 
                tenant_id, server_id, conversation_id, client_msg_id, sender_id, sender_name, sender_avatar,
                channel_id, source, seq, timestamp, conversation_type, message_type, content,
                status, offline_push_info, extra, extensions, created_at, persisted_at, delivered_at
            FROM messages
            WHERE server_id = $1 OR client_msg_id = $1
            LIMIT 1
            "#,
        )
        .bind(message_id)
        .fetch_optional(&self.base.pool)
        .await
        .context("Failed to get message")?;

        match row {
            Some(row) => {
                let message = self.base.row_to_message(&row)?;

                // 回填缓存（异步，不阻塞）
                if let Some(cache) = &self.cache {
                    let cache_clone = std::sync::Arc::clone(cache);
                    let message_clone = message.clone();
                    tokio::spawn(async move {
                        // 转换领域模型消息为 proto 类型
                        let proto_msg = message_to_proto(&message_clone);

                        if let Err(e) = cache_clone.cache_message(&proto_msg).await {
                            tracing::warn!(
                                error = %e,
                                "Failed to cache message to Redis (non-blocking)"
                            );
                        }
                    });
                }

                Ok(Some(message))
            }
            None => Ok(None),
        }
    }

    #[instrument(skip(self), fields(message_id))]
    async fn get_message_timestamp(&self, ctx: &Ctx, message_id: &str) -> Result<Option<DateTime<Utc>>> {
        let _ = ctx; // 上下文用于日志追踪
        // 直接查询消息的时间戳，避免加载完整的消息内容
        let row = sqlx::query(
            r#"
            SELECT timestamp
            FROM messages
            WHERE server_id = $1
            LIMIT 1
            "#,
        )
        .bind(message_id)
        .fetch_optional(&self.base.pool)
        .await
        .context("Failed to get message timestamp")?;

        match row {
            Some(row) => {
                let timestamp: DateTime<Utc> = row.get("timestamp");
                Ok(Some(timestamp))
            }
            None => Ok(None),
        }
    }

    #[instrument(skip(self, updates), fields(message_id))]
    async fn update_message(&self, ctx: &Ctx, message_id: &str, updates: MessageUpdate) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        // 使用 QueryBuilder 构建动态 UPDATE 语句
        let mut query = sqlx::QueryBuilder::new("UPDATE messages SET ");
        let mut has_updates = false;

        // 使用 separated 来添加逗号分隔的 SET 子句
        let mut separated = query.separated(", ");

        if let Some(is_recalled) = updates.is_recalled {
            if is_recalled {
                separated.push("status = ");
                separated.push_bind(6i32); // MessageStatus::Recalled
                has_updates = true;
            }
        }
        if updates.recalled_at.is_some() {
            // init_v2 messages 无 recalled_at 列，忽略
        }
        if let Some(_read_by) = updates.read_by {
            // read_by moved to message_read_records table
            // separated.push("read_by = ");
            // ...
            // has_updates = true;
        }
        if let Some(_operations) = updates.operations {
            // operations moved to message_operation_history table
            // separated.push("operations = ");
            // ...
            // has_updates = true;
        }
        if let Some(_visibility) = updates.visibility {
            // visibility moved to message_visibility table
            // separated.push(r#"visibility = COALESCE(visibility, '{}'::jsonb) || "#);
            // ...
            // has_updates = true;
        }
        if let Some(attributes) = updates.attributes {
            // 更新 extra 中的 attributes（需要合并到 extra JSONB）
            separated.push(r#"extra = COALESCE(extra, '{}'::jsonb) || "#);
            let attrs_json: HashMap<String, Value> = attributes
                .into_iter()
                .map(|(k, v)| (k, Value::String(v)))
                .collect();
            separated.push_bind(serde_json::to_value(&attrs_json)?);
            separated.push("::jsonb");
            has_updates = true;
        }
        if let Some(tags) = updates.tags {
            // 更新 extra 中的 tags
            separated
                .push(r#"extra = COALESCE(extra, '{}'::jsonb) || jsonb_build_object('tags', "#);
            separated.push_bind(serde_json::to_value(&tags)?);
            separated.push(")");
            has_updates = true;
        }
        if let Some(status) = updates.status {
            separated.push("status = ");
            separated.push_bind(status);
            has_updates = true;
        }
        if updates.reactions.is_some() {
            // init_v2: reactions 在 message_reactions 表，不更新 messages 列
        }

        if !has_updates {
            return Ok(());
        }

        // 添加 WHERE 子句
        query.push(" WHERE server_id = ");
        query.push_bind(message_id);

        query
            .build()
            .execute(&self.base.pool)
            .await
            .context("Failed to update message")?;

        // 更新后清除缓存
        // 注意：需要 conversation_id 才能清除缓存，但这里只有 message_id
        // 实际生产环境可以维护 message_id -> conversation_id 的映射，或通过查询获取
        // 这里暂时不实现缓存失效，因为需要额外的查询开销
        if self.cache.is_some() {
            tracing::debug!(
                message_id = %message_id,
                "Message updated, cache invalidation skipped (requires conversation_id query)"
            );
        }

        Ok(())
    }

    #[instrument(skip(self, message_ids), fields(user_id, visibility))]
    async fn batch_update_visibility(
        &self,
        ctx: &Ctx,
        message_ids: &[String],
        user_id: &str,
        visibility: VisibilityStatus,
    ) -> Result<usize> {
        let _ = ctx; // 上下文用于日志追踪
        if message_ids.is_empty() {
            return Ok(0);
        }

        let vis_int = visibility as i32;

        let result = sqlx::query(
            r#"
            INSERT INTO message_visibility (tenant_id, message_id, user_id, visibility_status, changed_at)
            SELECT m.tenant_id, m.server_id, $1, $2, CURRENT_TIMESTAMP
            FROM messages m
            WHERE m.server_id = ANY($3)
            ON CONFLICT (tenant_id, message_id, user_id)
            DO UPDATE SET visibility_status = $2, changed_at = CURRENT_TIMESTAMP
            "#,
        )
        .bind(user_id)
        .bind(vis_int)
        .bind(message_ids)
        .execute(&self.base.pool)
        .await
        .context("Failed to batch update visibility")?;

        Ok(result.rows_affected() as usize)
    }

    #[instrument(skip(self), fields(conversation_id))]
    async fn count_messages(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        user_id: Option<&str>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<i64> {
        let _ = ctx; // 上下文用于日志追踪
        let start_ts = start_time.unwrap_or_else(|| Utc::now() - chrono::Duration::days(7));
        let end_ts = end_time.unwrap_or(Utc::now());

        // 修复：使用独立的查询构建器，避免参数绑定冲突
        let query_builder = sqlx::QueryBuilder::new(
            "SELECT COUNT(*) FROM messages WHERE conversation_id = "
        );
        let mut query = query_builder;
        query.push_bind(conversation_id);
        query.push(" AND timestamp >= ");
        query.push_bind(start_ts);
        query.push(" AND timestamp <= ");
        query.push_bind(end_ts);

        if let Some(uid) = user_id {
            // Filter out hidden/deleted messages for the user
            query.push(" AND NOT EXISTS (");
            query.push("SELECT 1 FROM message_visibility mv ");
            query.push("WHERE mv.tenant_id = messages.tenant_id ");
            query.push("AND mv.message_id = messages.server_id ");
            query.push("AND mv.user_id = ");
            query.push_bind(uid);
            query.push(" AND mv.visibility_status IN (1, 2))");
        }

        let count: i64 = query
            .build()
            .fetch_one(&self.base.pool)
            .await
            .and_then(|row| Ok(row.get::<i64, _>(0)))
            .context("Failed to count messages")?;

        Ok(count)
    }

    #[instrument(skip(self, filters), fields(filter_count = filters.len()))]
    async fn search_messages(
        &self,
        ctx: &Ctx,
        filters: &[FilterExpression],
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
        limit: i32,
    ) -> Result<Vec<Message>> {
        let _ = ctx; // 上下文用于日志追踪
        let start_ts = start_time.unwrap_or_else(|| Utc::now() - chrono::Duration::days(7));
        let end_ts = end_time.unwrap_or(Utc::now());
        let limit = limit.min(1000).max(1);

        // 构建基础查询
        let mut query = sqlx::QueryBuilder::new(
            r#"
            SELECT 
                tenant_id, server_id, conversation_id, client_msg_id, sender_id, sender_name, sender_avatar,
                channel_id, source, seq, timestamp, conversation_type, message_type, content,
                status, offline_push_info, extra, extensions, created_at, persisted_at, delivered_at
            FROM messages
            WHERE timestamp >= $1 AND timestamp <= $2
            "#,
        );
        query.push_bind(start_ts);
        query.push_bind(end_ts);

        let mut param_index = 3u32;
        for filter in filters {
            if filter.field.is_empty() || filter.value.is_empty() {
                continue;
            }
            match filter.field.as_str() {
                "conversation_id" => {
                    query.push(format!(" AND conversation_id = ${}", param_index));
                    query.push_bind(&filter.value);
                    param_index += 1;
                }
                "sender_id" => {
                    query.push(format!(" AND sender_id = ${}", param_index));
                    query.push_bind(&filter.value);
                    param_index += 1;
                }
                "message_type" => {
                    query.push(format!(" AND message_type = ${}", param_index));
                    let v: i32 = filter.value.parse().unwrap_or(0);
                    query.push_bind(v);
                    param_index += 1;
                }
                "status" => {
                    query.push(format!(" AND status = ${}", param_index));
                    let v: i32 = filter.value.parse().unwrap_or(0);
                    query.push_bind(v);
                    param_index += 1;
                }
                "is_recalled" => {
                    let val = filter.value.parse::<bool>().unwrap_or(false);
                    if val {
                        query.push(format!(" AND status = ${}", param_index));
                        query.push_bind(6i32);
                    } else {
                        query.push(format!(" AND status != ${}", param_index));
                        query.push_bind(6i32);
                    }
                    param_index += 1;
                }
                _ => {
                    // 其他字段暂不支持，忽略
                }
            }
        }

        query.push(" ORDER BY timestamp DESC, seq DESC NULLS LAST LIMIT ");
        query.push_bind(limit);

        let rows = query
            .build()
            .fetch_all(&self.base.pool)
            .await
            .context("Failed to search messages")?;

        let mut messages = Vec::with_capacity(rows.len());
        for row in rows {
            messages.push(self.base.row_to_message(&row)?);
        }

        Ok(messages)
    }

    #[instrument(skip(self, attributes, tags), fields(message_id))]
    async fn update_message_attributes(
        &self,
        ctx: &Ctx,
        message_id: &str,
        attributes: HashMap<String, String>,
        tags: Vec<String>,
    ) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        // 更新 extra JSONB 中的 attributes 和 tags
        let mut extra_updates = serde_json::Map::new();

        // 添加 attributes
        for (k, v) in &attributes {
            extra_updates.insert(k.clone(), serde_json::Value::String(v.clone()));
        }

        // 添加 tags
        if !tags.is_empty() {
            extra_updates.insert(
                "tags".to_string(),
                serde_json::Value::Array(
                    tags.iter()
                        .map(|t| serde_json::Value::String(t.clone()))
                        .collect(),
                ),
            );
        }

        sqlx::query(
            r#"
            UPDATE messages SET extra = COALESCE(extra, '{}'::jsonb) || $1::jsonb WHERE server_id = $2
            "#,
        )
        .bind(serde_json::to_value(&extra_updates)?)
        .bind(message_id)
        .execute(&self.base.pool)
        .await
        .context("Failed to update message attributes")?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn list_all_tags(&self, ctx: &Ctx) -> Result<Vec<String>> {
        let _ = ctx; // 上下文用于日志追踪
        // 从 extra JSONB 中提取所有 tags
        let rows = sqlx::query(
            r#"
            SELECT DISTINCT jsonb_array_elements_text(extra->'tags') as tag
            FROM messages
            WHERE extra->'tags' IS NOT NULL
            "#,
        )
        .fetch_all(&self.base.pool)
        .await
        .context("Failed to list tags")?;

        let mut tags = Vec::new();
        for row in rows {
            if let Ok(tag) = row.try_get::<String, _>("tag") {
                tags.push(tag);
            }
        }

        Ok(tags)
    }

    #[instrument(skip(self), fields(message_id))]
    async fn query_message_operations(
        &self,
        ctx: &Ctx,
        _message_id: &str,
    ) -> Result<Vec<crate::domain::model::Event>> {
        let _ = ctx; // 上下文用于日志追踪
        // TODO: 从 message_operation_history 或事件表构建 Event 列表；当前与 proto 对齐返回 Event，先返回空
        Ok(vec![])
    }

    #[instrument(skip(self), fields(message_id))]
    async fn query_message_edit_history(
        &self,
        ctx: &Ctx,
        message_id: &str,
    ) -> Result<Vec<crate::domain::model::EditHistoryEntry>> {
        let _ = ctx; // 上下文用于日志追踪
        // TODO: 实现编辑历史查询
        Ok(vec![])
    }

    #[instrument(skip(self), fields(message_id))]
    async fn query_message_read_records(
        &self,
        ctx: &Ctx,
        message_id: &str,
    ) -> Result<Vec<crate::domain::model::ReadListEntry>> {
        let _ = ctx; // 上下文用于日志追踪
        // TODO: 实现已读记录查询
        Ok(vec![])
    }

    #[instrument(skip(self), fields(message_id, user_id))]
    async fn query_message_visibility(
        &self,
        ctx: &Ctx,
        message_id: &str,
        user_id: &str,
    ) -> Result<Option<crate::domain::model::VisibilityStatus>> {
        let _ = ctx; // 上下文用于日志追踪
        // TODO: 实现可见性查询
        Ok(None)
    }

    #[instrument(skip(self), fields(message_id))]
    async fn query_message_reactions(
        &self,
        ctx: &Ctx,
        message_id: &str,
    ) -> Result<Vec<crate::domain::model::ReactionItem>> {
        let _ = ctx; // 上下文用于日志追踪
        // TODO: 实现反应查询
        Ok(vec![])
    }

    #[instrument(skip(self), fields(conversation_id))]
    async fn query_pinned_messages(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
    ) -> Result<Vec<crate::domain::model::PinnedMessageInfo>> {
        let _ = ctx; // 上下文用于日志追踪
        // TODO: 实现置顶消息查询
        Ok(vec![])
    }

    #[instrument(skip(self, _event_types), fields(message_id, limit = _limit))]
    async fn query_message_events(
        &self,
        ctx: &Ctx,
        _message_id: &str,
        _event_types: Option<&[EventType]>,
        _limit: i32,
        _offset: i64,
    ) -> Result<(Vec<Event>, bool)> {
        let _ = ctx; // 上下文用于日志追踪
        // TODO: 从事件表按消息 ID 查询事件，支持类型过滤与分页
        Ok((vec![], false))
    }

    #[instrument(skip(self, event_type_filter), fields(tenant_id, conversation_id, after_seq, before_seq, limit))]
    async fn query_events(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        conversation_id: &str,
        after_seq: i64,
        before_seq: i64,
        limit: i32,
        event_type_filter: Vec<i32>,
    ) -> Result<Vec<Event>> {
        let _ = ctx;
        let limit = limit.clamp(1, 500);

        let mut b = sqlx::QueryBuilder::new(
            "SELECT seq, event_type, created_at, operator_id, request_id, event_seq, payload FROM events WHERE conversation_id = ",
        );
        b.push_bind(conversation_id);
        b.push(" AND seq > ");
        b.push_bind(after_seq);
        if before_seq > 0 {
            b.push(" AND seq < ");
            b.push_bind(before_seq);
        }
        if !tenant_id.is_empty() {
            b.push(" AND tenant_id = ");
            b.push_bind(tenant_id);
        }
        if !event_type_filter.is_empty() {
            b.push(" AND event_type = ANY(");
            b.push_bind(event_type_filter);
            b.push(")");
        }
        b.push(" ORDER BY seq ASC LIMIT ");
        b.push_bind(limit);

        let rows = b
            .build()
            .fetch_all(&self.base.pool)
            .await
            .context("query events by conversation seq")?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let seq: i64 = row.try_get("seq").context("row seq")?;
            let event_type: i32 = row.try_get("event_type").context("row event_type")?;
            let created_at: DateTime<Utc> = row.try_get("created_at").context("row created_at")?;
            let operator_id: String = row
                .try_get::<Option<String>, _>("operator_id")
                .context("row operator_id")?
                .unwrap_or_default();
            let request_id: Option<String> = row.try_get("request_id").ok();
            let event_seq: Option<i64> = row.try_get("event_seq").ok();
            let payload: Vec<u8> = row.try_get("payload").unwrap_or_default();

            let proto_ev = match proto_event_from_events_row(
                conversation_id,
                seq,
                event_type,
                created_at,
                operator_id,
                request_id.clone(),
                event_seq,
                &payload,
            ) {
                Ok(ev) => ev,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        conversation_id = %conversation_id,
                        seq,
                        "proto_event_from_events_row failed; returning shell Event"
                    );
                    flare_proto::common::Event {
                        conversation_id: conversation_id.to_string(),
                        seq: seq as u64,
                        r#type: event_type,
                        created_at: crate::convert::datetime_to_timestamp(Some(created_at)),
                        event_id: format!("{conversation_id}:{seq}"),
                        event_seq: event_seq.map(|v| v as u64),
                        request_id,
                        ..Default::default()
                    }
                }
            };
            out.push(event_from_proto(&proto_ev));
        }
        Ok(out)
    }

    #[instrument(skip(self), fields(tenant_id, conversation_id))]
    async fn get_conversation_max_seq(
        &self,
        ctx: &Ctx,
        _tenant_id: &str,
        conversation_id: &str,
    ) -> Result<Option<i64>> {
        let _ = ctx;
        let row = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT MAX(seq) FROM messages WHERE conversation_id = $1",
        )
        .bind(conversation_id)
        .fetch_one(&self.base.pool)
        .await
        .context("get_conversation_max_seq")?;
        Ok(row)
    }

    #[instrument(skip(self), fields(conversation_id))]
    async fn get_conversation_message_head(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
    ) -> Result<Option<ConversationMessageHead>> {
        let _ = ctx;
        let row = sqlx::query(
            r#"
            SELECT seq, server_id, timestamp
            FROM messages
            WHERE conversation_id = $1
            ORDER BY seq DESC NULLS LAST
            LIMIT 1
            "#,
        )
        .bind(conversation_id)
        .fetch_optional(&self.base.pool)
        .await
        .context("get_conversation_message_head")?;

        let Some(row) = row else {
            return Ok(None);
        };
        let seq: i64 = row.try_get("seq").context("head seq")?;
        let last_message_id: String = row.try_get("server_id").context("head server_id")?;
        let last_at: Option<DateTime<Utc>> = row.try_get("timestamp").ok();
        Ok(Some(ConversationMessageHead {
            max_seq: seq,
            last_message_id,
            last_at,
        }))
    }

    #[instrument(skip(self), fields(tenant_id, user_id, conversation_id))]
    async fn get_sync_cursor(
        &self,
        ctx: &Ctx,
        _tenant_id: &str,
        _user_id: &str,
        _conversation_id: &str,
    ) -> Result<Option<crate::domain::model::SyncCursor>> {
        let _ = ctx; // 上下文用于日志追踪
        // TODO: 获取用户在某会话的同步游标
        Ok(None)
    }

    #[instrument(skip(self), fields(tenant_id, user_id))]
    async fn get_sync_snapshot(
        &self,
        ctx: &Ctx,
        _tenant_id: &str,
        _user_id: &str,
        conversation_ids: &[String],
        messages_per_conversation: i32,
    ) -> Result<Vec<(String, Vec<Message>, i64)>> {
        let _ = ctx; // 上下文用于日志追踪
        
        let limit = messages_per_conversation.clamp(1, 100); // 限制范围 1-100
        let mut results = Vec::new();

        // conversation_ids 为空时，按最近活跃会话回填快照（避免冷启动永远空列表）
        let target_conversation_ids: Vec<String> = if conversation_ids.is_empty() {
            let rows = sqlx::query(
                r#"
                SELECT conversation_id
                FROM messages
                GROUP BY conversation_id
                ORDER BY MAX(seq) DESC
                LIMIT $1
                "#,
            )
            .bind(limit)
            .fetch_all(&self.base.pool)
            .await
            .context("Failed to query recent conversations for sync snapshot")?;

            rows.into_iter()
                .filter_map(|row| row.try_get::<String, _>("conversation_id").ok())
                .collect()
        } else {
            conversation_ids.to_vec()
        };
        
        // 对每个会话查询最新的消息
        for conversation_id in &target_conversation_ids {
            // 查询会话内最新的消息
            let rows = sqlx::query(
                r#"
                SELECT 
                    tenant_id, server_id, conversation_id, client_msg_id, sender_id, sender_name, sender_avatar,
                    channel_id, source, seq, timestamp, conversation_type, message_type, content,
                    status, offline_push_info, extra, extensions, created_at, persisted_at, delivered_at
                FROM messages
                WHERE conversation_id = $1
                ORDER BY seq DESC
                LIMIT $2
                "#,
            )
            .bind(conversation_id)
            .bind(limit)
            .fetch_all(&self.base.pool)
            .await
            .context("Failed to query sync snapshot messages")?;
            
            let mut messages = Vec::with_capacity(rows.len());
            let mut max_seq = 0;
            
            for row in rows {
                let message = self.base.row_to_message(&row)?;
                let seq_i64 = message.seq as i64;
                max_seq = max_seq.max(seq_i64);
                messages.push(message);
            }
            
            // 反转顺序，使最旧的消息在前
            messages.reverse();
            
            results.push((conversation_id.clone(), messages, max_seq));
        }
        
        Ok(results)
    }

    #[instrument(skip(self), fields(tenant_id, user_id, conversation_id))]
    async fn update_sync_cursor(
        &self,
        ctx: &Ctx,
        _tenant_id: &str,
        _user_id: &str,
        _conversation_id: &str,
        _last_synced_seq: i64,
        _last_synced_ts: i64,
        _device_id: Option<&str>,
    ) -> Result<()> {
        // TODO: 更新用户在某会话的同步游标
        Ok(())
    }
}