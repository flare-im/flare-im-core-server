//! 优化的 PostgreSQL 消息存储实现
//!
//! 提供高性能的查询、批处理和缓存功能

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use flare_server_core::error::{AnyhowContext, Result};
use serde_json::Value;
use sqlx::Row;
use tokio::time::Instant;
use tracing::{instrument, warn};

use crate::convert::{
    event_from_proto, event_type_to_proto_i32, message_from_proto, message_to_proto,
};
use crate::domain::model::{
    ConversationMessageHead, Event, EventType, FilterExpression, MarkEntry, Message,
    MessageExportTaskDraft, MessageUpdate, MessageWriteLedgerEntry, MessageWriteLedgerQuery,
    PinnedMessageInfo, ReactionItem, ReadListEntry, VisibilityStatus,
};
use crate::domain::repository::message_storage::MessageStorage;
use crate::infrastructure::persistence::event_stream_row::proto_event_from_events_row;
use crate::infrastructure::persistence::postgres_base::PostgresBaseStorage;
use crate::infrastructure::persistence::redis_cache::RedisMessageCache;
use flare_im_contracts::Ctx;
use flare_proto::common::ContentVisibility;

// TODO: 暂时使用占位符类型，等 monitoring 模块实现后再替换
// use crate::infrastructure::monitoring::performance_metrics::PerformanceMetrics;
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {}

impl PerformanceMetrics {
    pub fn record_cache_hit(&self, _cache_type: &str) {}
    pub fn record_cache_miss(&self, _cache_type: &str) {}
    pub fn record_query(&self, _query_type: &str, _duration_ms: u64) {}
}

fn tenant_id_from_ctx(ctx: &Ctx) -> &str {
    ctx.tenant_id().unwrap_or("0")
}

const MESSAGE_PIN_SCOPE_CONVERSATION: i32 = 0;
const MESSAGE_PIN_SCOPE_SELF: i32 = 1;

fn user_id_from_ctx(ctx: &Ctx) -> &str {
    ctx.user_id().unwrap_or("")
}

fn timestamp_from_datetime(dt: Option<DateTime<Utc>>) -> Option<prost_types::Timestamp> {
    dt.map(|dt| prost_types::Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    })
}

fn event_type_from_operation_type(operation_type: &str) -> EventType {
    match operation_type {
        "OPERATION_TYPE_RECALL" | "recall" => EventType::MessageRecall,
        "OPERATION_TYPE_EDIT" | "edit" => EventType::MessageEdit,
        "OPERATION_TYPE_DELETE" | "delete" => EventType::MessageDelete,
        "OPERATION_TYPE_READ" | "read" => EventType::ReadReceipt,
        "OPERATION_TYPE_REACTION_ADD" | "OPERATION_TYPE_REACTION_REMOVE" | "reaction" => {
            EventType::Reaction
        }
        "OPERATION_TYPE_PIN" | "pin" => EventType::Pin,
        "OPERATION_TYPE_UNPIN" | "unpin" => EventType::Unpin,
        "OPERATION_TYPE_MARK" | "mark" => EventType::Mark,
        "OPERATION_TYPE_UNMARK" | "unmark" => EventType::Unmark,
        _ => EventType::Custom,
    }
}

fn domain_event_from_events_row(
    row: &sqlx::postgres::PgRow,
    conversation_id: &str,
) -> Result<Event> {
    let seq: i64 = row.try_get("seq").context("row seq")?;
    let event_type: i32 = row.try_get("event_type").context("row event_type")?;
    let created_at: DateTime<Utc> = row.try_get("created_at").context("row created_at")?;
    let operator_id: String = row
        .try_get::<Option<String>, _>("operator_id")
        .context("row operator_id")?
        .unwrap_or_default();
    let request_id: Option<String> = row.try_get("request_id").ok();
    let event_seq: Option<i64> = row.try_get("event_seq").ok();
    let payload: Vec<u8> = row
        .try_get::<Option<Vec<u8>>, _>("payload")
        .unwrap_or_default()
        .unwrap_or_default();

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
                conversation_seq: seq as u64,
                r#type: event_type,
                created_at: created_at.timestamp_millis(),
                event_id: format!("{conversation_id}:{seq}"),
                request_id,
                ..Default::default()
            }
        }
    };
    Ok(event_from_proto(&proto_ev))
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
        metrics: Option<Arc<PerformanceMetrics>>,
    ) -> Self {
        Self {
            base,
            cache,
            metrics,
        }
    }

    async fn apply_current_pin_state(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        user_id: &str,
        messages: &mut [Message],
    ) -> Result<()> {
        let message_ids: Vec<String> = messages
            .iter()
            .filter_map(|message| {
                let message_id = message.server_id.trim();
                (!message_id.is_empty()).then(|| message_id.to_string())
            })
            .collect();
        if message_ids.is_empty() {
            return Ok(());
        }

        let rows = sqlx::query(
            r#"
            SELECT message_id
            FROM pinned_messages
            WHERE tenant_id = $1
              AND conversation_id = $2
              AND message_id = ANY($3)
              AND (scope = $4 OR (scope = $5 AND owner_user_id = $6))
              AND (expire_at IS NULL OR expire_at > CURRENT_TIMESTAMP)
            "#,
        )
        .bind(tenant_id)
        .bind(conversation_id)
        .bind(&message_ids)
        .bind(MESSAGE_PIN_SCOPE_CONVERSATION)
        .bind(MESSAGE_PIN_SCOPE_SELF)
        .bind(user_id)
        .fetch_all(&self.base.pool)
        .await
        .context("query current pin state for messages")?;

        let pinned: HashSet<String> = rows
            .into_iter()
            .filter_map(|row| row.try_get::<String, _>("message_id").ok())
            .collect();
        for message in messages {
            if pinned.contains(&message.server_id) {
                message
                    .extra
                    .insert("pinned".to_string(), "true".to_string());
            } else {
                message.extra.remove("pinned");
            }
        }
        Ok(())
    }
}

fn apply_burn_query_visibility(message: &mut Message, include_placeholder: bool) -> bool {
    match message.content_visibility() {
        ContentVisibility::Hidden | ContentVisibility::Redacted | ContentVisibility::Purged => {
            if !include_placeholder {
                return false;
            }
            message.content = None;
            message.offline_push_info = None;
            message.extensions.clear();
            message.extra.insert(
                "retention_placeholder".to_string(),
                "该消息已不可见".to_string(),
            );
            true
        }
        _ => true,
    }
}

#[async_trait]
impl MessageStorage for OptimizedPostgresMessageStorageImpl {
    #[instrument(skip(self, _message), fields(message_id = %_message.server_id))]
    async fn store_message(
        &self,
        ctx: &Ctx,
        _message: &Message,
        _conversation_id: &str,
    ) -> Result<()> {
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
        include_burned_placeholder: bool,
    ) -> Result<Vec<Message>> {
        let tenant_id = tenant_id_from_ctx(ctx).to_string();
        let start = Instant::now();
        let start_ts = start_time.unwrap_or_else(|| Utc::now() - chrono::Duration::days(7));
        let end_ts = end_time.unwrap_or(Utc::now());
        let limit = limit.clamp(1, 1000);

        // L2 缓存策略：先查 Redis，未命中再查 TimescaleDB
        if let Some(cache) = &self.cache
            && let Ok(Some(cached_messages)) = cache
                .get_session_messages(&tenant_id, conversation_id, start_ts, end_ts, limit)
                .await
        {
            tracing::trace!(
                conversation_id = %conversation_id,
                cached_count = cached_messages.len(),
                "Cache hit: retrieved messages from Redis"
            );

            // 转换 proto 类型的消息为领域模型类型
            let mut domain_messages: Vec<Message> = cached_messages
                .into_iter()
                .map(|msg| message_from_proto(&msg))
                .collect();
            self.apply_current_pin_state(
                &tenant_id,
                conversation_id,
                user_id_from_ctx(ctx),
                &mut domain_messages,
            )
            .await?;

            tracing::trace!(
                conversation_id = %conversation_id,
                cached_count = domain_messages.len(),
                "Cache hit: retrieved messages from Redis"
            );

            // 记录缓存命中指标
            if let Some(ref metrics) = self.metrics {
                metrics.record_cache_hit("redis");
            }

            let mut visible = Vec::with_capacity(domain_messages.len());
            for mut message in domain_messages {
                if apply_burn_query_visibility(&mut message, include_burned_placeholder) {
                    visible.push(message);
                }
            }
            return Ok(visible);
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
                    m.tenant_id, m.server_id, m.conversation_id,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM message_visibility mv
                        WHERE mv.tenant_id = m.tenant_id AND mv.message_id = m.server_id
                          AND mv.visibility_status IN (1, 2)
                          AND (mv.scope = 2 OR (mv.scope = 1 AND mv.user_id = $5))
                    ) THEN '' ELSE m.client_msg_id END AS client_msg_id,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM message_visibility mv
                        WHERE mv.tenant_id = m.tenant_id AND mv.message_id = m.server_id
                          AND mv.visibility_status IN (1, 2)
                          AND (mv.scope = 2 OR (mv.scope = 1 AND mv.user_id = $5))
                    ) THEN '' ELSE m.sender_id END AS sender_id,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM message_visibility mv
                        WHERE mv.tenant_id = m.tenant_id AND mv.message_id = m.server_id
                          AND mv.visibility_status IN (1, 2)
                          AND (mv.scope = 2 OR (mv.scope = 1 AND mv.user_id = $5))
                    ) THEN '' ELSE m.sender_name END AS sender_name,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM message_visibility mv
                        WHERE mv.tenant_id = m.tenant_id AND mv.message_id = m.server_id
                          AND mv.visibility_status IN (1, 2)
                          AND (mv.scope = 2 OR (mv.scope = 1 AND mv.user_id = $5))
                    ) THEN '' ELSE m.sender_avatar END AS sender_avatar,
                    m.channel_id, m.source, m.seq, m.timestamp, m.conversation_type,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM message_visibility mv
                        WHERE mv.tenant_id = m.tenant_id AND mv.message_id = m.server_id
                          AND mv.visibility_status IN (1, 2)
                          AND (mv.scope = 2 OR (mv.scope = 1 AND mv.user_id = $5))
                    ) THEN 0 ELSE m.message_type END AS message_type,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM message_visibility mv
                        WHERE mv.tenant_id = m.tenant_id AND mv.message_id = m.server_id
                          AND mv.visibility_status IN (1, 2)
                          AND (mv.scope = 2 OR (mv.scope = 1 AND mv.user_id = $5))
                    ) THEN '\x'::bytea ELSE m.content END AS content,
                    m.status,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM message_visibility mv
                        WHERE mv.tenant_id = m.tenant_id AND mv.message_id = m.server_id
                          AND mv.visibility_status IN (1, 2)
                          AND (mv.scope = 2 OR (mv.scope = 1 AND mv.user_id = $5))
                    ) THEN NULL ELSE m.offline_push_info END AS offline_push_info,
                    CASE
                        WHEN EXISTS (
                            SELECT 1 FROM message_visibility mv
                            WHERE mv.tenant_id = m.tenant_id AND mv.message_id = m.server_id
                              AND mv.visibility_status IN (1, 2)
                              AND (mv.scope = 2 OR (mv.scope = 1 AND mv.user_id = $5))
                        )
                        THEN jsonb_set(COALESCE(m.extra, '{}'::jsonb), '{__sync_skip}', '"visibility_filtered"'::jsonb, true)
                        ELSE m.extra
                    END AS extra,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM message_visibility mv
                        WHERE mv.tenant_id = m.tenant_id AND mv.message_id = m.server_id
                          AND mv.visibility_status IN (1, 2)
                          AND (mv.scope = 2 OR (mv.scope = 1 AND mv.user_id = $5))
                    ) THEN NULL ELSE m.extensions END AS extensions,
                    m.created_at, m.persisted_at, m.delivered_at,
                    COALESCE((
                        SELECT jsonb_agg(
                            jsonb_build_object(
                                'emoji', mr.emoji,
                                'user_ids', mr.user_ids,
                                'count', mr.count
                            )
                        )
                        FROM message_reactions mr
                        WHERE mr.tenant_id = m.tenant_id AND mr.message_id = m.server_id
                    ), '[]'::jsonb) AS reactions_json,
                    EXISTS (
                        SELECT 1 FROM pinned_messages pm
                        WHERE pm.tenant_id = m.tenant_id
                          AND pm.conversation_id = m.conversation_id
                          AND pm.message_id = m.server_id
                          AND (pm.expire_at IS NULL OR pm.expire_at > CURRENT_TIMESTAMP)
                    ) AS is_pinned
	                FROM messages m
	                WHERE m.tenant_id = $1 AND m.conversation_id = $2 AND m.timestamp >= $3 AND m.timestamp <= $4
	                  AND NOT EXISTS (
	                      SELECT 1 FROM message_visibility mv
	                      WHERE mv.tenant_id = m.tenant_id AND mv.message_id = m.server_id
	                        AND mv.visibility_status IN (1, 2)
	                        AND (mv.scope = 2 OR (mv.scope = 1 AND mv.user_id = $5))
	                  )
	                ORDER BY m.timestamp DESC, m.seq DESC NULLS LAST
	                LIMIT $6
	                "#,
            )
            .bind(&tenant_id)
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
                    status, offline_push_info, extra, extensions, created_at, persisted_at, delivered_at,
                    COALESCE((
                        SELECT jsonb_agg(
                            jsonb_build_object(
                                'emoji', mr.emoji,
                                'user_ids', mr.user_ids,
                                'count', mr.count
                            )
                        )
                        FROM message_reactions mr
                        WHERE mr.tenant_id = messages.tenant_id AND mr.message_id = messages.server_id
                    ), '[]'::jsonb) AS reactions_json,
                    EXISTS (
                        SELECT 1 FROM pinned_messages pm
                        WHERE pm.tenant_id = messages.tenant_id
                          AND pm.conversation_id = messages.conversation_id
                          AND pm.message_id = messages.server_id
                          AND (pm.expire_at IS NULL OR pm.expire_at > CURRENT_TIMESTAMP)
                    ) AS is_pinned
	                FROM messages
	                WHERE tenant_id = $1 AND conversation_id = $2 AND timestamp >= $3 AND timestamp <= $4
	                ORDER BY timestamp DESC, seq DESC NULLS LAST
	                LIMIT $5
	                "#,
            )
            .bind(&tenant_id)
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
            let mut message = self.base.row_to_message(&row)?;
            if apply_burn_query_visibility(&mut message, include_burned_placeholder) {
                messages.push(message);
            }
        }
        self.apply_current_pin_state(
            &tenant_id,
            conversation_id,
            user_id_from_ctx(ctx),
            &mut messages,
        )
        .await?;

        // 反转顺序，使最旧的消息在前（符合历史消息查询习惯）
        messages.reverse();

        // 回填缓存（异步，不阻塞）
        if let Some(cache) = &self.cache {
            let cache_clone = std::sync::Arc::clone(cache);
            let messages_clone = messages.clone();
            let tenant_id_clone = tenant_id.clone();
            let conversation_id_clone = conversation_id.to_string();
            tokio::spawn(async move {
                // 转换领域模型消息为 proto 类型
                let proto_messages: Vec<flare_proto::Message> =
                    messages_clone.iter().map(message_to_proto).collect();

                if let Err(e) = cache_clone
                    .cache_session_messages(
                        &tenant_id_clone,
                        &conversation_id_clone,
                        start_ts,
                        end_ts,
                        &proto_messages,
                    )
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
        include_burned_placeholder: bool,
    ) -> Result<Vec<Message>> {
        let tenant_id = tenant_id_from_ctx(ctx).to_string();
        let limit = limit.clamp(1, 1000);

        if user_id.is_none()
            && let Some(cache) = &self.cache
        {
            match cache
                .get_tail_messages_by_seq(&tenant_id, conversation_id, after_seq, before_seq, limit)
                .await
            {
                Ok(Some(cached_messages)) => {
                    if let Some(ref metrics) = self.metrics {
                        metrics.record_cache_hit("redis_tail");
                    }

                    let mut messages: Vec<Message> = cached_messages
                        .into_iter()
                        .map(|message| message_from_proto(&message))
                        .collect();
                    self.apply_current_pin_state(
                        &tenant_id,
                        conversation_id,
                        user_id_from_ctx(ctx),
                        &mut messages,
                    )
                    .await?;

                    let mut visible = Vec::with_capacity(messages.len());
                    for mut message in messages {
                        if apply_burn_query_visibility(&mut message, include_burned_placeholder) {
                            visible.push(message);
                        }
                    }
                    return Ok(visible);
                }
                Ok(None) => {
                    if let Some(ref metrics) = self.metrics {
                        metrics.record_cache_miss("redis_tail");
                    }
                }
                Err(err) => {
                    warn!(
                        error = %err,
                        conversation_id = %conversation_id,
                        "Redis tail cache query failed; falling back to PostgreSQL"
                    );
                    if let Some(ref metrics) = self.metrics {
                        metrics.record_cache_miss("redis_tail");
                    }
                }
            }
        }

        // 构建查询：基于 seq 查询（性能更好），支持多租户
        // 优化：使用预编译查询和索引优化
        let rows = if let Some(uid) = user_id {
            sqlx::query(
                r#"
                SELECT
                    m.tenant_id, m.server_id, m.conversation_id,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM message_visibility mv
                        WHERE mv.tenant_id = m.tenant_id AND mv.message_id = m.server_id
                          AND mv.visibility_status IN (1, 2)
                          AND (mv.scope = 2 OR (mv.scope = 1 AND mv.user_id = $5))
                    ) THEN '' ELSE m.client_msg_id END AS client_msg_id,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM message_visibility mv
                        WHERE mv.tenant_id = m.tenant_id AND mv.message_id = m.server_id
                          AND mv.visibility_status IN (1, 2)
                          AND (mv.scope = 2 OR (mv.scope = 1 AND mv.user_id = $5))
                    ) THEN '' ELSE m.sender_id END AS sender_id,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM message_visibility mv
                        WHERE mv.tenant_id = m.tenant_id AND mv.message_id = m.server_id
                          AND mv.visibility_status IN (1, 2)
                          AND (mv.scope = 2 OR (mv.scope = 1 AND mv.user_id = $5))
                    ) THEN '' ELSE m.sender_name END AS sender_name,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM message_visibility mv
                        WHERE mv.tenant_id = m.tenant_id AND mv.message_id = m.server_id
                          AND mv.visibility_status IN (1, 2)
                          AND (mv.scope = 2 OR (mv.scope = 1 AND mv.user_id = $5))
                    ) THEN '' ELSE m.sender_avatar END AS sender_avatar,
                    m.channel_id, m.source, m.seq, m.timestamp, m.conversation_type,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM message_visibility mv
                        WHERE mv.tenant_id = m.tenant_id AND mv.message_id = m.server_id
                          AND mv.visibility_status IN (1, 2)
                          AND (mv.scope = 2 OR (mv.scope = 1 AND mv.user_id = $5))
                    ) THEN 0 ELSE m.message_type END AS message_type,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM message_visibility mv
                        WHERE mv.tenant_id = m.tenant_id AND mv.message_id = m.server_id
                          AND mv.visibility_status IN (1, 2)
                          AND (mv.scope = 2 OR (mv.scope = 1 AND mv.user_id = $5))
                    ) THEN '\x'::bytea ELSE m.content END AS content,
                    m.status,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM message_visibility mv
                        WHERE mv.tenant_id = m.tenant_id AND mv.message_id = m.server_id
                          AND mv.visibility_status IN (1, 2)
                          AND (mv.scope = 2 OR (mv.scope = 1 AND mv.user_id = $5))
                    ) THEN NULL ELSE m.offline_push_info END AS offline_push_info,
                    CASE
                        WHEN EXISTS (
                            SELECT 1 FROM message_visibility mv
                            WHERE mv.tenant_id = m.tenant_id AND mv.message_id = m.server_id
                              AND mv.visibility_status IN (1, 2)
                              AND (mv.scope = 2 OR (mv.scope = 1 AND mv.user_id = $5))
                        )
                        THEN jsonb_set(COALESCE(m.extra, '{}'::jsonb), '{__sync_skip}', '"visibility_filtered"'::jsonb, true)
                        ELSE m.extra
                    END AS extra,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM message_visibility mv
                        WHERE mv.tenant_id = m.tenant_id AND mv.message_id = m.server_id
                          AND mv.visibility_status IN (1, 2)
                          AND (mv.scope = 2 OR (mv.scope = 1 AND mv.user_id = $5))
                    ) THEN NULL ELSE m.extensions END AS extensions,
                    m.created_at, m.persisted_at, m.delivered_at,
                    COALESCE((
                        SELECT jsonb_agg(
                            jsonb_build_object(
                                'emoji', mr.emoji,
                                'user_ids', mr.user_ids,
                                'count', mr.count
                            )
                        )
                        FROM message_reactions mr
                        WHERE mr.tenant_id = m.tenant_id AND mr.message_id = m.server_id
                    ), '[]'::jsonb) AS reactions_json,
                    EXISTS (
                        SELECT 1 FROM pinned_messages pm
                        WHERE pm.tenant_id = m.tenant_id
                          AND pm.conversation_id = m.conversation_id
                          AND pm.message_id = m.server_id
                          AND (pm.expire_at IS NULL OR pm.expire_at > CURRENT_TIMESTAMP)
                    ) AS is_pinned
	                FROM messages m
	                WHERE m.tenant_id = $1 AND m.conversation_id = $2 AND m.seq > $3 AND ($4::BIGINT IS NULL OR m.seq < $4)
	                ORDER BY m.seq ASC
	                LIMIT $6
	                "#,
            )
            .bind(&tenant_id)
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
                    status, offline_push_info, extra, extensions, created_at, persisted_at, delivered_at,
                    COALESCE((
                        SELECT jsonb_agg(
                            jsonb_build_object(
                                'emoji', mr.emoji,
                                'user_ids', mr.user_ids,
                                'count', mr.count
                            )
                        )
                        FROM message_reactions mr
                        WHERE mr.tenant_id = messages.tenant_id AND mr.message_id = messages.server_id
                    ), '[]'::jsonb) AS reactions_json,
                    EXISTS (
                        SELECT 1 FROM pinned_messages pm
                        WHERE pm.tenant_id = messages.tenant_id
                          AND pm.conversation_id = messages.conversation_id
                          AND pm.message_id = messages.server_id
                          AND (pm.expire_at IS NULL OR pm.expire_at > CURRENT_TIMESTAMP)
                    ) AS is_pinned
	                FROM messages
	                WHERE tenant_id = $1 AND conversation_id = $2 AND seq > $3 AND ($4::BIGINT IS NULL OR seq < $4)
	                ORDER BY seq ASC
	                LIMIT $5
	                "#,
            )
            .bind(&tenant_id)
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
            let mut message = self.base.row_to_message(&row)?;
            if apply_burn_query_visibility(&mut message, include_burned_placeholder) {
                messages.push(message);
            }
        }
        self.apply_current_pin_state(
            &tenant_id,
            conversation_id,
            user_id_from_ctx(ctx),
            &mut messages,
        )
        .await?;

        Ok(messages)
    }

    #[instrument(skip(self), fields(message_id))]
    async fn get_message(&self, ctx: &Ctx, message_id: &str) -> Result<Option<Message>> {
        let tenant_id = tenant_id_from_ctx(ctx).to_string();
        // 1. Query Database
        // Support querying by server_id or client_msg_id
        let row = sqlx::query(
            r#"
            SELECT
                tenant_id, server_id, conversation_id, client_msg_id, sender_id, sender_name, sender_avatar,
                channel_id, source, seq, timestamp, conversation_type, message_type, content,
                status, offline_push_info, extra, extensions, created_at, persisted_at, delivered_at,
                COALESCE((
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'emoji', mr.emoji,
                            'user_ids', mr.user_ids,
                            'count', mr.count
                        )
                    )
                    FROM message_reactions mr
                    WHERE mr.tenant_id = messages.tenant_id AND mr.message_id = messages.server_id
                ), '[]'::jsonb) AS reactions_json,
                EXISTS (
                    SELECT 1 FROM pinned_messages pm
                    WHERE pm.tenant_id = messages.tenant_id
                      AND pm.conversation_id = messages.conversation_id
                      AND pm.message_id = messages.server_id
                      AND (pm.expire_at IS NULL OR pm.expire_at > CURRENT_TIMESTAMP)
                ) AS is_pinned
	            FROM messages
	            WHERE tenant_id = $1 AND (server_id = $2 OR client_msg_id = $2)
	            LIMIT 1
	            "#,
        )
        .bind(&tenant_id)
        .bind(message_id)
        .fetch_optional(&self.base.pool)
        .await
        .context("Failed to get message")?;

        match row {
            Some(row) => {
                let mut message = self.base.row_to_message(&row)?;
                let conversation_id = message.conversation_id.clone();
                self.apply_current_pin_state(
                    &tenant_id,
                    &conversation_id,
                    user_id_from_ctx(ctx),
                    std::slice::from_mut(&mut message),
                )
                .await?;

                // 回填缓存（异步，不阻塞）
                if let Some(cache) = &self.cache {
                    let cache_clone = std::sync::Arc::clone(cache);
                    let message_clone = message.clone();
                    let tenant_id_clone = tenant_id.clone();
                    tokio::spawn(async move {
                        // 转换领域模型消息为 proto 类型
                        let proto_msg = message_to_proto(&message_clone);

                        if let Err(e) = cache_clone
                            .cache_message(&tenant_id_clone, &proto_msg)
                            .await
                        {
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
    async fn get_message_timestamp(
        &self,
        ctx: &Ctx,
        message_id: &str,
    ) -> Result<Option<DateTime<Utc>>> {
        let tenant_id = tenant_id_from_ctx(ctx);
        // 直接查询消息的时间戳，避免加载完整的消息内容
        let row = sqlx::query(
            r#"
	            SELECT timestamp
	            FROM messages
	            WHERE tenant_id = $1 AND server_id = $2
	            LIMIT 1
	            "#,
        )
        .bind(tenant_id)
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
    async fn update_message(
        &self,
        ctx: &Ctx,
        message_id: &str,
        updates: MessageUpdate,
    ) -> Result<()> {
        let tenant_id = tenant_id_from_ctx(ctx);
        // 使用 QueryBuilder 构建动态 UPDATE 语句
        let mut query = sqlx::QueryBuilder::new("UPDATE messages SET ");
        let mut has_updates = false;

        // 使用 separated 来添加逗号分隔的 SET 子句
        let mut separated = query.separated(", ");

        if let Some(is_recalled) = updates.is_recalled
            && is_recalled
        {
            separated.push("status = ");
            separated.push_bind(6i32); // MessageStatus::Recalled
            has_updates = true;
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
        query.push(" WHERE tenant_id = ");
        query.push_bind(tenant_id);
        query.push(" AND server_id = ");
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
            tracing::trace!(
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
        let tenant_id = tenant_id_from_ctx(ctx);
        if message_ids.is_empty() {
            return Ok(0);
        }

        let vis_int = visibility as i32;

        let result = sqlx::query(
            r#"
	            INSERT INTO message_visibility (tenant_id, message_id, user_id, scope, visibility_status, changed_at)
	            SELECT m.tenant_id, m.server_id, $1, 1, $2, CURRENT_TIMESTAMP
	            FROM messages m
	            WHERE m.tenant_id = $3 AND m.server_id = ANY($4)
	            ON CONFLICT (tenant_id, message_id, user_id, scope)
	            DO UPDATE SET visibility_status = EXCLUDED.visibility_status, changed_at = CURRENT_TIMESTAMP
	            "#,
        )
        .bind(user_id)
        .bind(vis_int)
        .bind(tenant_id)
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
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let start_ts = start_time.unwrap_or_else(|| Utc::now() - chrono::Duration::days(7));
        let end_ts = end_time.unwrap_or(Utc::now());

        // 修复：使用独立的查询构建器，避免参数绑定冲突
        let query_builder =
            sqlx::QueryBuilder::new("SELECT COUNT(*) FROM messages WHERE tenant_id = ");
        let mut query = query_builder;
        query.push_bind(tenant_id);
        query.push(" AND conversation_id = ");
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
            query.push("AND mv.visibility_status IN (1, 2) ");
            query.push("AND (mv.scope = 2 OR (mv.scope = 1 AND mv.user_id = ");
            query.push_bind(uid);
            query.push(")))");
        }

        let count: i64 = query
            .build()
            .fetch_one(&self.base.pool)
            .await
            .map(|row| row.get::<i64, _>(0))
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
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let start_ts = start_time.unwrap_or_else(|| Utc::now() - chrono::Duration::days(7));
        let end_ts = end_time.unwrap_or(Utc::now());
        let limit = limit.clamp(1, 1000);

        let mut query = sqlx::QueryBuilder::new(
            r#"
            SELECT
                tenant_id, server_id, conversation_id, client_msg_id, sender_id, sender_name, sender_avatar,
                channel_id, source, seq, timestamp, conversation_type, message_type, content,
                status, offline_push_info, extra, extensions, created_at, persisted_at, delivered_at,
                COALESCE((
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'emoji', mr.emoji,
                            'user_ids', mr.user_ids,
                            'count', mr.count
                        )
                        ORDER BY mr.last_updated ASC
                    )
                    FROM message_reactions mr
                    WHERE mr.tenant_id = messages.tenant_id
                      AND mr.message_id = messages.server_id
                ), '[]'::jsonb) AS reactions_json,
                EXISTS (
                    SELECT 1 FROM pinned_messages pm
                    WHERE pm.tenant_id = messages.tenant_id
                      AND pm.conversation_id = messages.conversation_id
                      AND pm.message_id = messages.server_id
                      AND (pm.expire_at IS NULL OR pm.expire_at > CURRENT_TIMESTAMP)
                ) AS is_pinned
            FROM messages
            WHERE tenant_id =
            "#,
        );
        query.push_bind(tenant_id);
        query.push(" AND timestamp >= ");
        query.push_bind(start_ts);
        query.push(" AND timestamp <= ");
        query.push_bind(end_ts);

        for filter in filters {
            let field = filter.field.trim();
            let value = filter.value.trim();
            if field.is_empty() || value.is_empty() {
                continue;
            }
            match field {
                "tenant_id" => {
                    if value != tenant_id {
                        query.push(" AND 1 = 0");
                    }
                }
                "conversation_id" => {
                    query.push(" AND conversation_id = ");
                    query.push_bind(value);
                }
                "sender_id" => {
                    query.push(" AND sender_id = ");
                    query.push_bind(value);
                }
                "channel_id" => {
                    query.push(" AND channel_id = ");
                    query.push_bind(value);
                }
                "client_msg_id" => {
                    query.push(" AND client_msg_id = ");
                    query.push_bind(value);
                }
                "server_id" | "message_id" => {
                    query.push(" AND server_id = ");
                    query.push_bind(value);
                }
                "message_type" => {
                    if let Ok(v) = value.parse::<i32>() {
                        query.push(" AND message_type = ");
                        query.push_bind(v);
                    }
                }
                "conversation_type" => {
                    if let Ok(v) = value.parse::<i32>() {
                        query.push(" AND conversation_type = ");
                        query.push_bind(v);
                    }
                }
                "source" => {
                    if let Ok(v) = value.parse::<i32>() {
                        query.push(" AND source = ");
                        query.push_bind(v);
                    }
                }
                "status" => {
                    if let Ok(v) = value.parse::<i32>() {
                        query.push(" AND status = ");
                        query.push_bind(v);
                    }
                }
                "is_recalled" => {
                    let val = matches!(
                        value.to_ascii_lowercase().as_str(),
                        "true" | "1" | "yes" | "y"
                    );
                    if val {
                        query.push(" AND status = ");
                        query.push_bind(6i32);
                    } else {
                        query.push(" AND status != ");
                        query.push_bind(6i32);
                    }
                }
                "seq_after" | "after_seq" | "conversation_seq_after" => {
                    if let Ok(v) = value.parse::<i64>() {
                        query.push(" AND seq > ");
                        query.push_bind(v);
                    }
                }
                "seq_from" | "seq_ge" | "conversation_seq_from" => {
                    if let Ok(v) = value.parse::<i64>() {
                        query.push(" AND seq >= ");
                        query.push_bind(v);
                    }
                }
                "seq_before" | "before_seq" | "conversation_seq_before" => {
                    if let Ok(v) = value.parse::<i64>() {
                        query.push(" AND seq < ");
                        query.push_bind(v);
                    }
                }
                "seq_to" | "seq_le" | "conversation_seq_to" => {
                    if let Ok(v) = value.parse::<i64>() {
                        query.push(" AND seq <= ");
                        query.push_bind(v);
                    }
                }
                _ => {
                    tracing::trace!(field, "忽略未支持的消息搜索过滤字段");
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
        let mut indexes_by_conversation: HashMap<String, Vec<usize>> = HashMap::new();
        for (index, message) in messages.iter().enumerate() {
            indexes_by_conversation
                .entry(message.conversation_id.clone())
                .or_default()
                .push(index);
        }
        for (conversation_id, indexes) in indexes_by_conversation {
            let mut subset: Vec<Message> = indexes
                .iter()
                .map(|index| messages[*index].clone())
                .collect();
            self.apply_current_pin_state(
                &tenant_id,
                &conversation_id,
                user_id_from_ctx(ctx),
                &mut subset,
            )
            .await?;
            for (index, message) in indexes.into_iter().zip(subset.into_iter()) {
                messages[index].extra = message.extra;
            }
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
        let tenant_id = tenant_id_from_ctx(ctx);
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
	            UPDATE messages SET extra = COALESCE(extra, '{}'::jsonb) || $1::jsonb
	            WHERE tenant_id = $2 AND server_id = $3
	            "#,
        )
        .bind(serde_json::to_value(&extra_updates)?)
        .bind(tenant_id)
        .bind(message_id)
        .execute(&self.base.pool)
        .await
        .context("Failed to update message attributes")?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn list_all_tags(&self, ctx: &Ctx) -> Result<Vec<String>> {
        let tenant_id = tenant_id_from_ctx(ctx);
        // 从 extra JSONB 中提取所有 tags
        let rows = sqlx::query(
            r#"
	            SELECT DISTINCT jsonb_array_elements_text(extra->'tags') as tag
	            FROM messages
	            WHERE tenant_id = $1 AND extra->'tags' IS NOT NULL
	            "#,
        )
        .bind(tenant_id)
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
        message_id: &str,
    ) -> Result<Vec<crate::domain::model::Event>> {
        let tenant_id = tenant_id_from_ctx(ctx);
        let rows = sqlx::query(
            r#"
            SELECT id, tenant_id, operation_type, operator_id, target_user_id,
                   operation_data, timestamp, metadata
            FROM message_operation_history
            WHERE tenant_id = $1 AND message_id = $2
            ORDER BY timestamp ASC, id ASC
            "#,
        )
        .bind(tenant_id)
        .bind(message_id)
        .fetch_all(&self.base.pool)
        .await
        .context("query message operation history")?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let id: i64 = row.try_get("id").context("operation id")?;
            let tenant_id: String = row.try_get("tenant_id").context("operation tenant_id")?;
            let operation_type: String = row.try_get("operation_type").context("operation type")?;
            let operator_id: String = row
                .try_get::<Option<String>, _>("operator_id")
                .context("operation operator_id")?
                .unwrap_or_default();
            let timestamp: DateTime<Utc> =
                row.try_get("timestamp").context("operation timestamp")?;
            let operation_data: Option<Value> = row.try_get("operation_data").ok();
            let metadata: Option<Value> = row.try_get("metadata").ok();
            let payload = serde_json::json!({
                "operation_type": operation_type,
                "target_user_id": row.try_get::<Option<String>, _>("target_user_id").ok().flatten(),
                "operation_data": operation_data,
                "metadata": metadata,
            });
            out.push(Event {
                tenant_id,
                conversation_id: String::new(),
                seq: id.max(0) as u64,
                r#type: event_type_from_operation_type(&operation_type),
                created_at: timestamp_from_datetime(Some(timestamp)),
                operator_id,
                event_seq: None,
                request_id: None,
                payload_bytes: Some(serde_json::to_vec(&payload)?),
            });
        }
        Ok(out)
    }

    #[instrument(skip(self), fields(message_id))]
    async fn query_message_edit_history(
        &self,
        ctx: &Ctx,
        message_id: &str,
    ) -> Result<Vec<crate::domain::model::EditHistoryEntry>> {
        let tenant_id = tenant_id_from_ctx(ctx);
        let rows = sqlx::query(
            r#"
            SELECT edit_version, content, editor_id, edited_at, reason, show_edited_mark
            FROM message_edit_history
            WHERE tenant_id = $1 AND message_id = $2
            ORDER BY edit_version ASC
            "#,
        )
        .bind(tenant_id)
        .bind(message_id)
        .fetch_all(&self.base.pool)
        .await
        .context("query message edit history")?;

        rows.into_iter()
            .map(|row| {
                Ok(crate::domain::model::EditHistoryEntry {
                    edit_version: row.try_get("edit_version").context("edit_version")?,
                    content_bytes: row.try_get("content").context("edit content")?,
                    edited_at: row.try_get("edited_at").ok(),
                    editor_id: row.try_get("editor_id").context("editor_id")?,
                    reason: row.try_get("reason").ok(),
                    show_edited_mark: row.try_get("show_edited_mark").unwrap_or(true),
                })
            })
            .collect()
    }

    #[instrument(skip(self), fields(message_id))]
    async fn query_message_read_records(
        &self,
        ctx: &Ctx,
        message_id: &str,
    ) -> Result<Vec<crate::domain::model::ReadListEntry>> {
        let tenant_id = tenant_id_from_ctx(ctx);
        let rows = sqlx::query(
            r#"
            SELECT user_id, read_at, burned_at
            FROM message_read_records
            WHERE tenant_id = $1 AND message_id = $2
            ORDER BY read_at ASC
            "#,
        )
        .bind(tenant_id)
        .bind(message_id)
        .fetch_all(&self.base.pool)
        .await
        .context("query message read records")?;

        rows.into_iter()
            .map(|row| {
                Ok(ReadListEntry {
                    user_id: row.try_get("user_id").context("read user_id")?,
                    read_at: row.try_get("read_at").ok(),
                    burned_at: row.try_get("burned_at").ok(),
                })
            })
            .collect()
    }

    #[instrument(skip(self), fields(message_id, user_id))]
    async fn query_message_visibility(
        &self,
        ctx: &Ctx,
        message_id: &str,
        user_id: &str,
    ) -> Result<Option<crate::domain::model::VisibilityStatus>> {
        let tenant_id = tenant_id_from_ctx(ctx);
        let row = sqlx::query(
            r#"
            SELECT visibility_status
            FROM message_visibility
            WHERE tenant_id = $1
              AND message_id = $2
              AND (scope = 2 OR (scope = 1 AND user_id = $3))
            ORDER BY scope DESC, changed_at DESC
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(message_id)
        .bind(user_id)
        .fetch_optional(&self.base.pool)
        .await
        .context("query message visibility")?;

        Ok(row.and_then(
            |row| match row.try_get::<i32, _>("visibility_status").ok()? {
                0 => Some(VisibilityStatus::Visible),
                1 => Some(VisibilityStatus::Hidden),
                2 => Some(VisibilityStatus::Deleted),
                _ => None,
            },
        ))
    }

    #[instrument(skip(self), fields(message_id))]
    async fn query_message_reactions(
        &self,
        ctx: &Ctx,
        message_id: &str,
    ) -> Result<Vec<crate::domain::model::ReactionItem>> {
        let tenant_id = tenant_id_from_ctx(ctx);
        let rows = sqlx::query(
            r#"
            SELECT emoji, user_ids, count, last_updated
            FROM message_reactions
            WHERE tenant_id = $1 AND message_id = $2
            ORDER BY last_updated DESC
            "#,
        )
        .bind(tenant_id)
        .bind(message_id)
        .fetch_all(&self.base.pool)
        .await
        .context("Failed to query message reactions")?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let emoji: String = row.try_get("emoji").context("reaction emoji")?;
            let user_ids: Vec<String> = row.try_get("user_ids").unwrap_or_default();
            let count: i32 = row.try_get("count").unwrap_or(0);
            let last_updated: Option<DateTime<Utc>> = row.try_get("last_updated").ok();
            out.push(ReactionItem {
                emoji,
                user_ids,
                count,
                last_updated,
            });
        }
        Ok(out)
    }

    #[instrument(skip(self), fields(message_id))]
    async fn query_message_marks(&self, ctx: &Ctx, message_id: &str) -> Result<Vec<MarkEntry>> {
        let tenant_id = tenant_id_from_ctx(ctx);
        let rows = sqlx::query(
            r#"
            SELECT user_id, mark_type, color, marked_at
            FROM marked_messages
            WHERE tenant_id = $1 AND message_id = $2
            ORDER BY marked_at DESC
            "#,
        )
        .bind(tenant_id)
        .bind(message_id)
        .fetch_all(&self.base.pool)
        .await
        .context("query message marks")?;

        rows.into_iter()
            .map(|row| {
                Ok(MarkEntry {
                    user_id: row.try_get("user_id").context("mark user_id")?,
                    mark_type: row.try_get("mark_type").context("mark_type")?,
                    color: row.try_get("color").ok(),
                    marked_at: row.try_get("marked_at").ok(),
                })
            })
            .collect()
    }

    #[instrument(skip(self, draft), fields(task_id = %draft.task_id, conversation_id = %draft.conversation_id))]
    async fn create_message_export_task(
        &self,
        ctx: &Ctx,
        draft: MessageExportTaskDraft,
    ) -> Result<String> {
        let _ = ctx;
        sqlx::query(
            r#"
            INSERT INTO message_export_tasks (
                tenant_id, task_id, conversation_id, start_time, end_time, filters,
                requested_by, request_id, trace_id, status, created_at, updated_at
            )
            VALUES ($1, $2, $3, to_timestamp($4::double precision / 1000.0), to_timestamp($5::double precision / 1000.0), $6,
                    $7, $8, $9, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (tenant_id, task_id) DO NOTHING
            "#,
        )
        .bind(&draft.tenant_id)
        .bind(&draft.task_id)
        .bind(&draft.conversation_id)
        .bind(draft.start_time)
        .bind(draft.end_time)
        .bind(&draft.filters)
        .bind(&draft.requested_by)
        .bind(&draft.request_id)
        .bind(&draft.trace_id)
        .execute(&self.base.pool)
        .await
        .context("create message export task")?;

        Ok(draft.task_id)
    }

    #[instrument(skip(self, ctx, query), fields(tenant_id = %query.tenant_id))]
    async fn query_message_write_ledger(
        &self,
        ctx: &Ctx,
        query: MessageWriteLedgerQuery,
    ) -> Result<(Vec<MessageWriteLedgerEntry>, bool)> {
        let _ = ctx;
        self.base.query_message_write_ledger(query).await
    }

    #[instrument(skip(self), fields(conversation_id))]
    async fn query_pinned_messages(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
    ) -> Result<Vec<crate::domain::model::PinnedMessageInfo>> {
        let tenant_id = tenant_id_from_ctx(ctx);
        let user_id = user_id_from_ctx(ctx);
        let rows = sqlx::query(
            r#"
            SELECT message_id, pinned_by, scope, owner_user_id, pinned_at, expire_at, reason
            FROM pinned_messages
            WHERE tenant_id = $1
              AND conversation_id = $2
              AND (scope = $3 OR (scope = $4 AND owner_user_id = $5))
              AND (expire_at IS NULL OR expire_at > CURRENT_TIMESTAMP)
            ORDER BY pinned_at DESC
            "#,
        )
        .bind(tenant_id)
        .bind(conversation_id)
        .bind(MESSAGE_PIN_SCOPE_CONVERSATION)
        .bind(MESSAGE_PIN_SCOPE_SELF)
        .bind(user_id)
        .fetch_all(&self.base.pool)
        .await
        .context("query pinned messages")?;

        rows.into_iter()
            .map(|row| {
                Ok(PinnedMessageInfo {
                    message_id: row.try_get("message_id").context("pinned message_id")?,
                    user_id: row.try_get("pinned_by").context("pinned_by")?,
                    scope: row.try_get("scope").context("pin scope")?,
                    owner_user_id: row.try_get("owner_user_id").context("pin owner_user_id")?,
                    pinned_at: row.try_get("pinned_at").ok(),
                    expire_at: row.try_get("expire_at").ok(),
                    reason: row.try_get("reason").ok(),
                })
            })
            .collect()
    }

    #[instrument(skip(self, event_types), fields(message_id, limit))]
    async fn query_message_events(
        &self,
        ctx: &Ctx,
        message_id: &str,
        event_types: Option<&[EventType]>,
        limit: i32,
        offset: i64,
    ) -> Result<(Vec<Event>, bool)> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let limit = limit.clamp(1, 500);
        let offset = offset.max(0);

        let message_row = sqlx::query(
            r#"
            SELECT tenant_id, conversation_id, seq
            FROM messages
            WHERE tenant_id = $1
              AND (server_id = $2 OR client_msg_id = $2)
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(message_id)
        .fetch_optional(&self.base.pool)
        .await
        .context("query message coordinate for event chain")?;

        let Some(message_row) = message_row else {
            return Ok((Vec::new(), false));
        };
        let message_tenant_id: String = message_row
            .try_get("tenant_id")
            .context("message tenant_id")?;
        let conversation_id: String = message_row
            .try_get("conversation_id")
            .context("message conversation_id")?;
        let message_seq: Option<i64> = message_row.try_get("seq").context("message seq")?;
        let Some(message_seq) = message_seq.filter(|seq| *seq > 0) else {
            return Ok((Vec::new(), false));
        };

        let event_type_filter: Vec<i32> = event_types
            .unwrap_or_default()
            .iter()
            .map(|event_type| event_type_to_proto_i32(*event_type))
            .filter(|event_type| *event_type != 0)
            .collect();
        if event_types.is_some() && event_type_filter.is_empty() {
            return Ok((Vec::new(), false));
        }

        let mut b = sqlx::QueryBuilder::new(
            "SELECT seq, event_type, created_at, operator_id, request_id, event_seq, payload FROM events WHERE tenant_id = ",
        );
        b.push_bind(&message_tenant_id);
        b.push(" AND conversation_id = ");
        b.push_bind(&conversation_id);
        b.push(" AND (seq = ");
        b.push_bind(message_seq);
        b.push(" OR event_seq = ");
        b.push_bind(message_seq);
        b.push(")");
        if !event_type_filter.is_empty() {
            b.push(" AND event_type = ANY(");
            b.push_bind(event_type_filter);
            b.push(")");
        }
        b.push(" ORDER BY seq ASC LIMIT ");
        b.push_bind(limit + 1);
        b.push(" OFFSET ");
        b.push_bind(offset);

        let rows = b
            .build()
            .fetch_all(&self.base.pool)
            .await
            .context("query message event chain")?;

        let has_more = rows.len() as i32 > limit;
        let mut events = Vec::with_capacity(rows.len().min(limit as usize));
        for row in rows.into_iter().take(limit as usize) {
            events.push(domain_event_from_events_row(&row, &conversation_id)?);
        }

        Ok((events, has_more))
    }

    #[instrument(
        skip(self, event_type_filter),
        fields(tenant_id, conversation_id, after_seq, before_seq, limit)
    )]
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
            "SELECT seq, event_type, created_at, operator_id, request_id, event_seq, payload FROM events WHERE tenant_id = ",
        );
        b.push_bind(tenant_id);
        b.push(" AND conversation_id = ");
        b.push_bind(conversation_id);
        b.push(" AND seq > ");
        b.push_bind(after_seq);
        if before_seq > 0 {
            b.push(" AND seq < ");
            b.push_bind(before_seq);
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
                        conversation_seq: seq as u64,
                        r#type: event_type,
                        created_at: created_at.timestamp_millis(),
                        event_id: format!("{conversation_id}:{seq}"),
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
        tenant_id: &str,
        conversation_id: &str,
    ) -> Result<Option<i64>> {
        let _ = ctx;
        let row = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT MAX(seq) FROM messages WHERE tenant_id = $1 AND conversation_id = $2",
        )
        .bind(tenant_id)
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
        let tenant_id = tenant_id_from_ctx(ctx);
        let row = sqlx::query(
            r#"
            SELECT seq, server_id, timestamp
            FROM messages
            WHERE tenant_id = $1 AND conversation_id = $2
            ORDER BY seq DESC NULLS LAST
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
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
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<Option<crate::domain::model::SyncCursor>> {
        let _ = ctx;
        let row = sqlx::query(
            r#"
            SELECT user_id, conversation_id, last_synced_seq, last_synced_ts
            FROM user_sync_cursor
            WHERE tenant_id = $1 AND user_id = $2 AND conversation_id = $3
            "#,
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(conversation_id)
        .fetch_optional(&self.base.pool)
        .await
        .context("get sync cursor")?;

        Ok(row.map(|row| crate::domain::model::SyncCursor {
            user_id: row
                .try_get("user_id")
                .unwrap_or_else(|_| user_id.to_string()),
            conversation_id: row
                .try_get("conversation_id")
                .unwrap_or_else(|_| conversation_id.to_string()),
            last_seq: row.try_get("last_synced_seq").unwrap_or_default(),
            last_message_id: 0,
            last_timestamp: row.try_get("last_synced_ts").unwrap_or_default(),
        }))
    }

    #[instrument(skip(self), fields(tenant_id, user_id))]
    async fn get_sync_snapshot(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
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
	                WHERE tenant_id = $1
	                GROUP BY conversation_id
	                ORDER BY MAX(seq) DESC
	                LIMIT $2
	                "#,
            )
            .bind(tenant_id)
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
                    status, offline_push_info, extra, extensions, created_at, persisted_at, delivered_at,
                    COALESCE((
                        SELECT jsonb_agg(
                            jsonb_build_object(
                                'emoji', mr.emoji,
                                'user_ids', mr.user_ids,
                                'count', mr.count
                            )
                        )
                        FROM message_reactions mr
                        WHERE mr.tenant_id = messages.tenant_id AND mr.message_id = messages.server_id
                    ), '[]'::jsonb) AS reactions_json,
                    EXISTS (
                        SELECT 1 FROM pinned_messages pm
                        WHERE pm.tenant_id = messages.tenant_id
                          AND pm.conversation_id = messages.conversation_id
                          AND pm.message_id = messages.server_id
                          AND (pm.expire_at IS NULL OR pm.expire_at > CURRENT_TIMESTAMP)
                    ) AS is_pinned
	                FROM messages
	                WHERE tenant_id = $1 AND conversation_id = $2
	                ORDER BY seq DESC
	                LIMIT $3
	                "#,
            )
            .bind(tenant_id)
            .bind(conversation_id)
            .bind(limit)
            .fetch_all(&self.base.pool)
            .await
            .context("Failed to query sync snapshot messages")?;

            let mut messages = Vec::with_capacity(rows.len());
            let mut max_seq = 0;

            for row in rows {
                let mut message = self.base.row_to_message(&row)?;
                if !apply_burn_query_visibility(&mut message, false) {
                    continue;
                }
                let seq_i64 = message.conversation_seq as i64;
                max_seq = max_seq.max(seq_i64);
                messages.push(message);
            }
            self.apply_current_pin_state(
                tenant_id,
                conversation_id,
                user_id_from_ctx(ctx),
                &mut messages,
            )
            .await?;

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
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        last_synced_seq: i64,
        last_synced_ts: i64,
        device_id: Option<&str>,
    ) -> Result<()> {
        let _ = ctx;
        sqlx::query(
            r#"
            INSERT INTO user_sync_cursor (
                tenant_id, user_id, conversation_id, last_synced_seq,
                last_synced_ts, device_id, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)
            ON CONFLICT (tenant_id, user_id, conversation_id)
            DO UPDATE SET
                last_synced_seq = EXCLUDED.last_synced_seq,
                last_synced_ts = EXCLUDED.last_synced_ts,
                device_id = EXCLUDED.device_id,
                updated_at = CURRENT_TIMESTAMP
            "#,
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(conversation_id)
        .bind(last_synced_seq)
        .bind(last_synced_ts)
        .bind(device_id)
        .execute(&self.base.pool)
        .await
        .context("update sync cursor")?;
        Ok(())
    }
}
