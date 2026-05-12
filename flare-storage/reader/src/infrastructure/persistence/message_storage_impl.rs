//! 消息存储仓储实现
//!
//! 基于 PostgreSQL/TimescaleDB 实现消息的查询、更新、搜索等功能

use std::collections::HashMap;

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use flare_im_core::utils::{datetime_to_timestamp, timestamp_to_datetime};
use flare_im_core::Ctx;
use prost_types;
use serde_json::{Value, from_value};
use sqlx::{Pool, Postgres, Row, postgres::PgRow};
use tracing::instrument;

use crate::domain::model::{
    EditHistoryEntry, Event, EventType, FilterExpression, Message, MessageUpdate, 
    PinnedMessageInfo, ReadListEntry, ReactionItem, VisibilityStatus,
};
use crate::domain::repository::message_storage::MessageStorage;
use crate::infrastructure::persistence::postgres_base::PostgresBaseStorage;

/// PostgreSQL 消息存储实现
pub struct PostgresMessageStorageImpl {
    pub base: PostgresBaseStorage,
}

impl PostgresMessageStorageImpl {
    pub fn new(base: PostgresBaseStorage) -> Self {
        Self { base }
    }
}

#[async_trait]
impl MessageStorage for PostgresMessageStorageImpl {
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

    #[instrument(skip(self), fields(conversation_id, user_id, limit))]
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
        let start_ts = start_time.unwrap_or_else(|| Utc::now() - chrono::Duration::days(7));
        let end_ts = end_time.unwrap_or(Utc::now());
        let limit = limit.min(1000).max(1); // 限制范围 1-1000

        // L2 缓存策略：先查 Redis，未命中再查 TimescaleDB
        if let Some(cache) = &self.base.cache {
            if let Ok(Some(cached_messages)) = cache
                .get_session_messages(conversation_id, start_ts, end_ts, limit)
                .await
            {
                tracing::trace!(
                    conversation_id = %conversation_id,
                    cached_count = cached_messages.len(),
                    "Cache hit: retrieved messages from Redis"
                );
                return Ok(cached_messages);
            }
        }

        // 缓存未命中，查询 TimescaleDB
        // 构建查询：利用 TimescaleDB 的时间分区裁剪优化
        // TimescaleDB 会自动裁剪不相关的分区，提高查询性能
        // 修复：使用独立的查询构建器，避免参数绑定冲突
        let query_builder = sqlx::QueryBuilder::new(
            r#"
            SELECT 
                server_id, conversation_id, client_msg_id, sender_id, content, timestamp,
                extra, created_at, message_type, content_type, business_type,
                status, fsm_state_changed_at, is_burn_after_read, burn_after_seconds,
                seq, updated_at, tenant_id
            FROM messages
            WHERE conversation_id = 
            "#,
        );
        let mut query = query_builder;
        query.push_bind(conversation_id);
        // TimescaleDB 时间分区裁剪：使用 timestamp 范围查询，自动裁剪不相关的分区
        query.push(" AND timestamp >= ");
        query.push_bind(start_ts);
        query.push(" AND timestamp <= ");
        query.push_bind(end_ts);

        // 如果提供了 user_id，过滤已删除的消息
        if let Some(uid) = user_id {
            // TODO: Join with message_visibility table to filter hidden/deleted messages
            query.push(" AND NOT EXISTS (");
            query.push("SELECT 1 FROM message_visibility mv ");
            query.push("WHERE mv.message_id = messages.server_id ");
            query.push("AND mv.visibility_status IN ('HIDDEN', 'DELETED') ");
            query.push("AND (mv.scope = 2 OR (mv.scope = 1 AND mv.user_id = ");
            query.push_bind(uid);
            query.push(")))");
        }

        // 使用索引优化：conversation_id + timestamp DESC（复合索引）
        query.push(" ORDER BY timestamp DESC, seq DESC NULLS LAST LIMIT ");
        query.push_bind(limit);

        let rows = query
            .build()
            .fetch_all(&self.base.pool)
            .await
            .context("Failed to query messages")?;

        let mut messages = Vec::with_capacity(rows.len());
        for row in rows {
            messages.push(self.base.row_to_message(&row)?);
        }

        // 反转顺序，使最旧的消息在前（符合历史消息查询习惯）
        messages.reverse();

        // 回填缓存（异步，不阻塞）
        if let Some(cache) = &self.base.cache {
            let cache_clone = cache.clone();
            let messages_clone = messages.clone();
            let conversation_id_clone = conversation_id.to_string();
            tokio::spawn(async move {
                if let Err(e) = cache_clone
                    .cache_session_messages(&conversation_id_clone, start_ts, end_ts, &messages_clone)
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

    #[instrument(skip(self), fields(conversation_id, user_id, after_seq, before_seq, limit))]
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

        // 构建查询：基于 seq 查询（性能更好）
        // 修复：使用独立的查询构建器，避免参数绑定冲突
        let query_builder = sqlx::QueryBuilder::new(
            r#"
            SELECT 
                server_id, conversation_id, client_msg_id, sender_id, content, timestamp,
                extra, created_at, message_type, content_type, business_type,
                status, fsm_state_changed_at, is_burn_after_read, burn_after_seconds,
                seq, updated_at, tenant_id
            FROM messages
            WHERE conversation_id = 
            "#,
        );
        let mut query = query_builder;
        query.push_bind(conversation_id);
        query.push(" AND seq > ");
        query.push_bind(after_seq);

        if let Some(before) = before_seq {
            query.push(" AND seq < ");
            query.push_bind(before);
        }

        // 如果提供了 user_id，过滤已删除的消息
        if let Some(uid) = user_id {
            // TODO: Join with message_visibility table to filter hidden/deleted messages
            query.push(" AND NOT EXISTS (");
            query.push("SELECT 1 FROM message_visibility mv ");
            query.push("WHERE mv.message_id = messages.server_id ");
            query.push("AND mv.visibility_status IN ('HIDDEN', 'DELETED') ");
            query.push("AND (mv.scope = 2 OR (mv.scope = 1 AND mv.user_id = ");
            query.push_bind(uid);
            query.push(")))");
        }

        query.push(" ORDER BY seq ASC LIMIT ");
        query.push_bind(limit);

        let rows = query
            .build()
            .fetch_all(&self.base.pool)
            .await
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
                server_id, conversation_id, client_msg_id, sender_id, content, timestamp,
                extra, created_at, message_type, content_type, business_type,
                status, fsm_state_changed_at, is_burn_after_read, burn_after_seconds,
                seq, updated_at, tenant_id
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
                if let Some(cache) = &self.base.cache {
                    let cache_clone = cache.clone();
                    let message_clone = message.clone();
                    tokio::spawn(async move {
                        if let Err(e) = cache_clone.cache_message(&message_clone).await {
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

    async fn update_message(&self, message_id: &str, updates: MessageUpdate) -> Result<()> {
        // 使用 QueryBuilder 构建动态 UPDATE 语句
        let mut query = sqlx::QueryBuilder::new("UPDATE messages SET ");
        let mut has_updates = false;

        // 使用 separated 来添加逗号分隔的 SET 子句
        let mut separated = query.separated(", ");

        if let Some(is_recalled) = updates.is_recalled {
            if is_recalled {
                separated.push("status = 'recalled'");
                has_updates = true;
            }
        }
        if let Some(recalled_at) = updates.recalled_at {
            separated.push("fsm_state_changed_at = ");
            // timestamp_to_datetime 返回 Option<DateTime<Utc>>，需要 unwrap
            if let Some(dt) = timestamp_to_datetime(&recalled_at) {
                separated.push_bind(dt);
            } else {
                // 如果转换失败，使用 None
                separated.push_bind(Option::<DateTime<Utc>>::None);
            }
            has_updates = true;
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
            // status 在数据库中存储为枚举字符串
            let status_str = MessageStatus::try_from(status)
                .map(|s| match s {
                    MessageStatus::Created => "created",
                    MessageStatus::Sent => "sent",
                    MessageStatus::Delivered => "delivered",
                    MessageStatus::Read => "read",
                    MessageStatus::Failed => "failed",
                    MessageStatus::Recalled => "recalled",
                    
                    _ => "unknown",
                })
                .unwrap_or("unknown");
            separated.push_bind(status_str);
            has_updates = true;
        }
        if let Some(reactions) = updates.reactions {
            separated.push("reactions = ");
            // 将 Reaction 列表序列化为 JSONB
            let reactions_json: Vec<serde_json::Value> = reactions
                .into_iter()
                .map(|reaction| {
                    serde_json::json!({
                        "emoji": reaction.emoji,
                        "user_ids": reaction.user_ids,
                        "count": reaction.count,
                        "last_updated": reaction.last_updated.map(|ts| {
                            serde_json::json!({
                                "seconds": ts.seconds,
                                "nanos": ts.nanos
                            })
                        }),
                        "created_at": reaction.created_at.map(|ts| {
                            serde_json::json!({
                                "seconds": ts.seconds,
                                "nanos": ts.nanos
                            })
                        }),
                    })
                })
                .collect();
            separated.push_bind(serde_json::to_value(&reactions_json)?);
            has_updates = true;
        }

        if !has_updates {
            return Ok(()); // 没有需要更新的字段
        }

        // 添加 updated_at
        separated.push("updated_at = CURRENT_TIMESTAMP");

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
        if self.base.cache.is_some() {
            tracing::trace!(
                message_id = %message_id,
                "Message updated, cache invalidation skipped (requires conversation_id query)"
            );
        }

        Ok(())
    }

    async fn batch_update_visibility(
        &self,
        message_ids: &[String],
        user_id: &str,
        visibility: VisibilityStatus,
    ) -> Result<usize> {
        if message_ids.is_empty() {
            return Ok(0);
        }

        // init_v2: visibility_status 为 INT（0=VISIBLE,1=HIDDEN,2=DELETED）
        let vis_status = match visibility {
            VisibilityStatus::VisibilityVisible => 0i32,
            VisibilityStatus::VisibilityHidden => 1i32,
            VisibilityStatus::VisibilityDeleted => 2i32,
            #[allow(unreachable_patterns)]
            _ => 0i32, // 默认为可见
        };

        // 使用 INSERT ... ON CONFLICT DO UPDATE 语法更新或插入可见性记录
        let result = sqlx::query(
            r#"
            INSERT INTO message_visibility (tenant_id, message_id, user_id, scope, visibility_status, changed_at, created_at, updated_at)
            SELECT m.tenant_id, m.server_id, $1, 1, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            FROM messages m
            WHERE m.server_id = ANY($3)
            ON CONFLICT (tenant_id, message_id, user_id, scope)
            DO UPDATE SET 
                visibility_status = EXCLUDED.visibility_status,
                changed_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            "#,
        )
        .bind(user_id)
        .bind(vis_status)
        .bind(message_ids)
        .execute(&self.base.pool)
        .await
        .context("Failed to batch update visibility")?;

        Ok(result.rows_affected() as usize)
    }

    async fn count_messages(
        &self,
        conversation_id: &str,
        user_id: Option<&str>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<i64> {
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
            query.push("WHERE mv.message_id = messages.server_id ");
            query.push("AND mv.visibility_status IN ('HIDDEN', 'DELETED') ");
            query.push("AND (mv.scope = 2 OR (mv.scope = 1 AND mv.user_id = ");
            query.push_bind(uid);
            query.push(")))");
        }

        let count: i64 = query
            .build()
            .fetch_one(&self.base.pool)
            .await
            .and_then(|row| Ok(row.get::<i64, _>(0)))
            .context("Failed to count messages")?;

        Ok(count)
    }

    async fn search_messages(
        &self,
        filters: &[flare_proto::common::FilterExpression],
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
        limit: i32,
    ) -> Result<Vec<Message>> {
        let start_ts = start_time.unwrap_or_else(|| Utc::now() - chrono::Duration::days(7));
        let end_ts = end_time.unwrap_or(Utc::now());
        let limit = limit.min(1000).max(1);

        // 修复：使用独立的查询构建器，避免参数绑定冲突
        let query_builder = sqlx::QueryBuilder::new(
            r#"
            SELECT 
                server_id, conversation_id, client_msg_id, sender_id, content, timestamp,
                extra, created_at, message_type, content_type, business_type,
                status, fsm_state_changed_at, is_burn_after_read, burn_after_seconds,
                seq, updated_at, tenant_id
            FROM messages
            WHERE timestamp >= 
            "#,
        );
        let mut query = query_builder;
        query.push_bind(start_ts);
        query.push(" AND timestamp <= ");
        query.push_bind(end_ts);

        // 应用过滤器
        for filter in filters {
            if filter.field.is_empty() || filter.values.is_empty() {
                continue;
            }

            match filter.field.as_str() {
                "conversation_id" => {
                    query.push(" AND conversation_id = ");
                    query.push_bind(&filter.values[0]);
                }
                "sender_id" => {
                    query.push(" AND sender_id = ");
                    query.push_bind(&filter.values[0]);
                }
                "message_type" => {
                    query.push(" AND message_type = ");
                    query.push_bind(&filter.values[0]);
                }
                "status" => {
                    query.push(" AND status = ");
                    query.push_bind(&filter.values[0]);
                }
                "is_recalled" => {
                    let val = filter.values[0].parse::<bool>().unwrap_or(false);
                    if val {
                        query.push(" AND status = 'recalled'");
                    } else {
                        query.push(" AND status != 'recalled'");
                    }
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

    async fn update_message_attributes(
        &self,
        message_id: &str,
        attributes: HashMap<String, String>,
        tags: Vec<String>,
    ) -> Result<()> {
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
            UPDATE messages
            SET 
                extra = COALESCE(extra, '{}'::jsonb) || $1::jsonb,
                updated_at = CURRENT_TIMESTAMP
            WHERE server_id = $2
            "#,
        )
        .bind(serde_json::to_value(&extra_updates)?)
        .bind(message_id)
        .execute(&self.base.pool)
        .await
        .context("Failed to update message attributes")?;

        Ok(())
    }

    async fn list_all_tags(&self) -> Result<Vec<String>> {
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

    async fn query_message_operations(
        &self,
        message_id: &str,
    ) -> Result<Vec<flare_proto::common::MessageOperation>> {
        // 从 message_operation_history 表中查询操作历史
        let rows = sqlx::query(
            r#"
            SELECT 
                operation_type, operator_id, target_user_id, operation_data, 
                show_notice, notice_text, timestamp, metadata, tenant_id
            FROM message_operation_history
            WHERE message_id = $1
            ORDER BY timestamp ASC
            "#,
        )
        .bind(message_id)
        .fetch_all(&self.base.pool)
        .await
        .context("Failed to query message operations")?;

        let mut operations = Vec::with_capacity(rows.len());
        for row in rows {
            // 解析 operation_type
            let operation_type_str: String = row.get("operation_type");
            let operation_type = match operation_type_str.as_str() {
                "OPERATION_TYPE_RECALL" => flare_proto::common::OperationType::Recall as i32,
                "OPERATION_TYPE_EDIT" => flare_proto::common::OperationType::Edit as i32,
                "OPERATION_TYPE_DELETE" => flare_proto::common::OperationType::Delete as i32,
                "OPERATION_TYPE_READ" => flare_proto::common::OperationType::Read as i32,
                "OPERATION_TYPE_REACTION_ADD" => flare_proto::common::OperationType::ReactionAdd as i32,
                "OPERATION_TYPE_REACTION_REMOVE" => flare_proto::common::OperationType::ReactionRemove as i32,
                "OPERATION_TYPE_PIN" => flare_proto::common::OperationType::Pin as i32,
                "OPERATION_TYPE_UNPIN" => flare_proto::common::OperationType::Unpin as i32,
                "OPERATION_TYPE_MARK" => flare_proto::common::OperationType::Mark as i32,
                "OPERATION_TYPE_UNMARK" => flare_proto::common::OperationType::Unmark as i32,
                _ => flare_proto::common::OperationType::Unspecified as i32,
            };

            // 解析时间戳
            let timestamp_chrono: DateTime<Utc> = row.get("timestamp");
            let timestamp = Some(prost_types::Timestamp {
                seconds: timestamp_chrono.timestamp(),
                nanos: timestamp_chrono.timestamp_subsec_nanos() as i32,
            });

            // 解析操作数据
            let operation_data_json: Option<serde_json::Value> = row.get("operation_data");
            let operation_data = if let Some(json_val) = operation_data_json {
                // 根据操作类型解析 operation_data
                match operation_type_str.as_str() {
                    "OPERATION_TYPE_RECALL" => {
                        // RecallOperationData
                        Some(flare_proto::common::message_operation::OperationData::Recall(
                            flare_proto::common::RecallOperationData {
                                reason: json_val.get("reason").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
                                time_limit_seconds: json_val.get("time_limit_seconds").and_then(|v| v.as_i64()).unwrap_or_default() as i32,
                                allow_admin_recall: json_val.get("allow_admin_recall").and_then(|v| v.as_bool()).unwrap_or_default(),
                            },
                        ))
                    },
                    "OPERATION_TYPE_EDIT" => {
                        // EditOperationData
                        Some(flare_proto::common::message_operation::OperationData::Edit(
                            flare_proto::common::EditOperationData {
                                new_content: json_val.get("new_content").and_then(|v| v.as_str()).unwrap_or_default().as_bytes().to_vec(),
                                edit_version: json_val.get("edit_version").and_then(|v| v.as_i64()).unwrap_or_default() as i32,
                                reason: json_val.get("reason").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
                                show_edited_mark: json_val.get("show_edited_mark").and_then(|v| v.as_bool()).unwrap_or_default(),
                            },
                        ))
                    },
                    "OPERATION_TYPE_REACTION_ADD" | "OPERATION_TYPE_REACTION_REMOVE" => {
                        // ReactionOperationData
                        let action = if operation_type_str.as_str() == "OPERATION_TYPE_REACTION_ADD" {
                            flare_proto::common::ReactionAction::Add as i32
                        } else {
                            flare_proto::common::ReactionAction::Remove as i32
                        };
                        Some(flare_proto::common::message_operation::OperationData::Reaction(
                            flare_proto::common::ReactionOperationData {
                                emoji: json_val.get("emoji").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
                                action,
                                count: json_val.get("count").and_then(|v| v.as_i64()).unwrap_or_default() as i32,
                            },
                        ))
                    },
                    "OPERATION_TYPE_PIN" => {
                        // PinOperationData
                        Some(flare_proto::common::message_operation::OperationData::Pin(
                            flare_proto::common::PinOperationData {
                                reason: json_val.get("reason").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
                                expire_at: json_val.get("expire_at").and_then(|v| v.as_str()).map(|s| {
                                    // 尝试解析时间戳
                                    DateTime::parse_from_rfc3339(s).ok().map(|dt| {
                                        prost_types::Timestamp {
                                            seconds: dt.timestamp(),
                                            nanos: dt.timestamp_subsec_nanos() as i32,
                                        }
                                    })
                                }).flatten(),
                            },
                        ))
                    },
                    "OPERATION_TYPE_UNPIN" => {
                        // PinOperationData for unpin
                        Some(flare_proto::common::message_operation::OperationData::Pin(
                            flare_proto::common::PinOperationData {
                                reason: json_val.get("reason").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
                                expire_at: None, // Unpin doesn't typically have an expiration
                            },
                        ))
                    },
                    "OPERATION_TYPE_MARK" => {
                        // MarkOperationData
                        let mark_type_str = json_val.get("mark_type").and_then(|v| v.as_str()).unwrap_or_default();
                        let mark_type = match mark_type_str {
                            "MARK_TYPE_IMPORTANT" => flare_proto::common::MarkType::Important as i32,
                            "MARK_TYPE_TODO" => flare_proto::common::MarkType::Todo as i32,
                            "MARK_TYPE_DONE" => flare_proto::common::MarkType::Done as i32,
                            "MARK_TYPE_CUSTOM" => flare_proto::common::MarkType::Custom as i32,
                            _ => flare_proto::common::MarkType::Important as i32,
                        };
                        Some(flare_proto::common::message_operation::OperationData::Mark(
                            flare_proto::common::MarkOperationData {
                                mark_type,
                                color: json_val.get("color").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
                            },
                        ))
                    },
                    "OPERATION_TYPE_UNMARK" => {
                        // UnmarkOperationData
                        let mark_type_str = json_val.get("mark_type").and_then(|v| v.as_str()).unwrap_or_default();
                        let mark_type = match mark_type_str {
                            "MARK_TYPE_IMPORTANT" => flare_proto::common::MarkType::Important as i32,
                            "MARK_TYPE_TODO" => flare_proto::common::MarkType::Todo as i32,
                            "MARK_TYPE_DONE" => flare_proto::common::MarkType::Done as i32,
                            "MARK_TYPE_CUSTOM" => flare_proto::common::MarkType::Custom as i32,
                            _ => flare_proto::common::MarkType::Important as i32,
                        };
                        Some(flare_proto::common::message_operation::OperationData::Unmark(
                            flare_proto::common::UnmarkOperationData {
                                mark_type,
                            },
                        ))
                    },
                    _ => None,
                }
            } else {
                None
            };

            // 解析元数据
            let metadata_json: Option<serde_json::Value> = row.get("metadata");
            let mut metadata = std::collections::HashMap::new();
            if let Some(json_val) = metadata_json {
                if let Some(obj) = json_val.as_object() {
                    for (key, value) in obj {
                        metadata.insert(key.clone(), value.as_str().unwrap_or_default().to_string());
                    }
                }
            }

            let operation = flare_proto::common::MessageOperation {
                operation_type,
                target_message_id: message_id.to_string(),
                operator_id: row.get("operator_id"),
                timestamp,
                show_notice: row.get("show_notice"),
                notice_text: row.get::<Option<String>, _>("notice_text").unwrap_or_else(|| "".to_string()),
                target_user_id: row.get::<Option<String>, _>("target_user_id").unwrap_or_else(|| "".to_string()),
                operation_data,
                metadata,
            };

            operations.push(operation);
        }

        Ok(operations)
    }

    async fn query_message_edit_history(
        &self,
        message_id: &str,
    ) -> Result<Vec<flare_proto::common::EditHistory>> {
        // 从 message_edit_history 表中查询编辑历史
        let rows = sqlx::query(
            r#"
            SELECT 
                edit_version, content, editor_id, edited_at, reason, show_edited_mark, tenant_id
            FROM message_edit_history
            WHERE message_id = $1
            ORDER BY edit_version ASC
            "#,
        )
        .bind(message_id)
        .fetch_all(&self.base.pool)
        .await
        .context("Failed to query message edit history")?;

        let mut histories = Vec::with_capacity(rows.len());
        for row in rows {
            // 解析时间戳
            let edited_at_chrono: DateTime<Utc> = row.get("edited_at");
            let edited_at = Some(prost_types::Timestamp {
                seconds: edited_at_chrono.timestamp(),
                nanos: edited_at_chrono.timestamp_subsec_nanos() as i32,
            });

            // 解析内容
            let content_bytes: Vec<u8> = row.get("content");
            let content = flare_proto::decode_message_content(&content_bytes[..]).ok();

            let history = flare_proto::common::EditHistory {
                edit_version: row.get("edit_version"),
                content,
                edited_at,
                editor_id: row.get::<String, _>("editor_id"),
                reason: row.get::<Option<String>, _>("reason").unwrap_or_else(|| "".to_string()),
                show_edited_mark: row.get("show_edited_mark"),
            };

            histories.push(history);
        }

        Ok(histories)
    }

    async fn query_message_read_records(
        &self,
        message_id: &str,
    ) -> Result<Vec<flare_proto::common::MessageReadRecord>> {
        // 从 message_read_records 表中查询已读记录
        let rows = sqlx::query(
            r#"
            SELECT 
                user_id, read_at, burned_at, tenant_id
            FROM message_read_records
            WHERE message_id = $1
            ORDER BY read_at ASC
            "#,
        )
        .bind(message_id)
        .fetch_all(&self.base.pool)
        .await
        .context("Failed to query message read records")?;

        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            // 解析 read_at
            let read_at_chrono: DateTime<Utc> = row.get("read_at");
            let read_at = Some(prost_types::Timestamp {
                seconds: read_at_chrono.timestamp(),
                nanos: read_at_chrono.timestamp_subsec_nanos() as i32,
            });

            // 解析 burned_at
            let burned_at_chrono: Option<DateTime<Utc>> = row.get("burned_at");
            let burned_at = if let Some(burned_at_dt) = burned_at_chrono {
                Some(prost_types::Timestamp {
                    seconds: burned_at_dt.timestamp(),
                    nanos: burned_at_dt.timestamp_subsec_nanos() as i32,
                })
            } else {
                None
            };

            let record = flare_proto::common::MessageReadRecord {
                user_id: row.get("user_id"),
                read_at,
                burned_at,
            };

            records.push(record);
        }

        Ok(records)
    }

    async fn query_message_visibility(
        &self,
        message_id: &str,
        user_id: &str,
    ) -> Result<Option<flare_grpc_proto::VisibilityStatus>> {
        // 全局作用域(scope=2)优先，其次用户私有作用域(scope=1)。
        let row = sqlx::query(
            r#"
            SELECT 
                visibility_status
            FROM message_visibility
            WHERE message_id = $1
              AND (
                scope = 2
                OR (scope = 1 AND user_id = $2)
              )
            ORDER BY scope DESC, changed_at DESC
            LIMIT 1
            "#,
        )
        .bind(message_id)
        .bind(user_id)
        .fetch_optional(&self.base.pool)
        .await
        .context("Failed to query message visibility")?;

        if let Some(row) = row {
            let status_int: i32 = row.get("visibility_status");
            Ok(Some(
                flare_grpc_proto::VisibilityStatus::try_from(status_int)
                    .unwrap_or(flare_grpc_proto::VisibilityStatus::Visible),
            ))
        } else {
            // 如果没有找到记录，默认为可见
            Ok(Some(flare_grpc_proto::VisibilityStatus::Visible))
        }
    }

    async fn query_message_reactions(
        &self,
        message_id: &str,
    ) -> Result<Vec<flare_proto::common::Reaction>> {
        // 从 message_reactions 表中查询消息反应
        let rows = sqlx::query(
            r#"
            SELECT 
                emoji, user_ids, count, last_updated, created_at, tenant_id
            FROM message_reactions
            WHERE message_id = $1
            "#,
        )
        .bind(message_id)
        .fetch_all(&self.base.pool)
        .await
        .context("Failed to query message reactions")?;

        let mut reactions = Vec::with_capacity(rows.len());
        for row in rows {
            // 解析 last_updated
            let last_updated_chrono: DateTime<Utc> = row.get("last_updated");
            let last_updated = Some(prost_types::Timestamp {
                seconds: last_updated_chrono.timestamp(),
                nanos: last_updated_chrono.timestamp_subsec_nanos() as i32,
            });

            // 解析 created_at
            let created_at_chrono: DateTime<Utc> = row.get("created_at");
            let created_at = Some(prost_types::Timestamp {
                seconds: created_at_chrono.timestamp(),
                nanos: created_at_chrono.timestamp_subsec_nanos() as i32,
            });

            // 解析 user_ids 数组
            let user_ids_db: Vec<String> = row.get("user_ids");

            let reaction = flare_proto::common::Reaction {
                emoji: row.get("emoji"),
                user_ids: user_ids_db,
                count: row.get("count"),
                last_updated,
                created_at,
            };

            reactions.push(reaction);
        }

        Ok(reactions)
    }

    async fn query_pinned_messages(
        &self,
        conversation_id: &str,
    ) -> Result<Vec<flare_proto::common::PinnedMessageInfo>> {
        // 从 pinned_messages 表中查询置顶消息
        let rows = sqlx::query(
            r#"
            SELECT 
                message_id, pinned_by, pinned_at, expire_at, reason, tenant_id
            FROM pinned_messages
            WHERE conversation_id = $1
            ORDER BY pinned_at DESC
            "#,
        )
        .bind(conversation_id)
        .fetch_all(&self.base.pool)
        .await
        .context("Failed to query pinned messages")?;

        let mut pinned_messages = Vec::with_capacity(rows.len());
        for row in rows {
            // 解析 pinned_at
            let pinned_at_chrono: DateTime<Utc> = row.get("pinned_at");
            let pinned_at = Some(prost_types::Timestamp {
                seconds: pinned_at_chrono.timestamp(),
                nanos: pinned_at_chrono.timestamp_subsec_nanos() as i32,
            });

            // 解析 expire_at
            let expire_at_chrono: Option<DateTime<Utc>> = row.get("expire_at");
            let expire_at = if let Some(expire_at_dt) = expire_at_chrono {
                Some(prost_types::Timestamp {
                    seconds: expire_at_dt.timestamp(),
                    nanos: expire_at_dt.timestamp_subsec_nanos() as i32,
                })
            } else {
                None
            };

            let pinned_message = flare_proto::common::PinnedMessageInfo {
                message_id: row.get("message_id"),
                conversation_id: conversation_id.to_string(),
                pinned_by: row.get("pinned_by"),
                pinned_at,
                expire_at,
                reason: row.get::<Option<String>, _>("reason").unwrap_or_else(|| "".to_string()),
            };

            pinned_messages.push(pinned_message);
        }

        Ok(pinned_messages)
    }
}

// 非trait实现的额外方法
impl PostgresMessageStorageImpl {
    /// 批量查询消息（优化性能）
    ///
    /// 使用 TimescaleDB 优化的批量查询策略：
    /// - 小批量（<=10）：直接使用 IN 查询
    /// - 中批量（11-100）：使用 VALUES 表值构造器
    /// - 大批量（>100）：分批处理，每批最多 100 条
    ///
    /// 性能优化：
    /// - 利用 TimescaleDB 的分区裁剪
    /// - 减少网络往返次数
    /// - 优化参数绑定
    pub async fn batch_query_messages(&self, message_ids: &[String]) -> Result<Vec<Message>> {
        if message_ids.is_empty() {
            return Ok(vec![]);
        }

        // 小批量：直接使用 IN 查询
        if message_ids.len() <= 10 {
            let placeholders: Vec<String> = (0..message_ids.len())
                .map(|i| format!("${}", i + 1))
                .collect();
            let query = format!(
                "SELECT server_id, conversation_id, client_msg_id, sender_id, content, timestamp, \
                 extra, created_at, message_type, content_type, business_type, status, \
                 fsm_state_changed_at, is_burn_after_read, burn_after_seconds, seq, updated_at, tenant_id \
                 FROM messages WHERE server_id IN ({})",
                placeholders.join(",")
            );

            let mut query_builder = sqlx::QueryBuilder::new(&query);
            for id in message_ids {
                query_builder.push_bind(id);
            }

            let rows = query_builder
                .build()
                .fetch_all(&self.base.pool)
                .await?;

            let mut messages = Vec::with_capacity(rows.len());
            for row in rows {
                messages.push(self.base.row_to_message(&row)?);
            }

            return Ok(messages);
        }

        // 自适应批量大小：大批量时分批处理
        let batch_size = if message_ids.len() > 100 {
            100
        } else {
            message_ids.len()
        };

        let mut all_messages = Vec::new();
        for chunk in message_ids.chunks(batch_size) {
            // 使用 VALUES 表值构造器进行批量查询
            let mut query_builder = sqlx::QueryBuilder::new(
                "SELECT server_id, conversation_id, client_msg_id, sender_id, content, timestamp, \
                 extra, created_at, message_type, content_type, business_type, status, \
                 fsm_state_changed_at, is_burn_after_read, burn_after_seconds, seq, updated_at, tenant_id \
                 FROM messages WHERE server_id IN (",
            );

            // 构建 VALUES 子句
            let mut separated = query_builder.separated(", ");
            for (i, id) in chunk.iter().enumerate() {
                separated.push_bind(id);
            }
            separated.push_unseparated(")");

            let rows = query_builder
                .build()
                .fetch_all(&self.base.pool)
                .await?;

            for row in rows {
                all_messages.push(self.base.row_to_message(&row)?);
            }
        }

        Ok(all_messages)
    }
}
