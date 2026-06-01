use anyhow::Result;
use base64::Engine;
use flare_im_core::Ctx;
use flare_im_core::message::BurnStatus;
use flare_im_core::utils::normalize_tenant_id;
use serde_json::to_value;
use sqlx::{Pool, Postgres, Row, postgres::PgPoolOptions};
use tracing::instrument;

use super::operation_store;
use crate::convert;

use crate::config::StorageWriterConfig;
use crate::domain::model::{Event, Message};
use crate::domain::repository::ArchiveStoreRepository;

/// 与 init_v2.sql messages 表结构对齐（无 receiver_id，与 common/message.proto 一致）
#[derive(sqlx::FromRow)]
#[allow(dead_code)]
struct MessageRow {
    tenant_id: String,
    server_id: String,
    conversation_id: String,
    client_msg_id: Option<String>,
    sender_id: String,
    sender_name: Option<String>,
    sender_avatar: Option<String>,
    channel_id: Option<String>,
    source: i32,
    seq: i64,
    timestamp: chrono::DateTime<chrono::Utc>,
    conversation_type: i32,
    message_type: i32,
    content: Option<Vec<u8>>,
    status: i32,
    burn_enabled: bool,
    burn_after_read_seconds: Option<i64>,
    burn_status: i32,
    first_read_at: Option<i64>,
    burn_at: Option<i64>,
    burned_at: Option<i64>,
    offline_push_info: Option<serde_json::Value>,
    extra: Option<serde_json::Value>,
    extensions: Option<serde_json::Value>,
    created_at: chrono::DateTime<chrono::Utc>,
    persisted_at: Option<chrono::DateTime<chrono::Utc>>,
    delivered_at: Option<chrono::DateTime<chrono::Utc>>,
}

pub struct PostgresMessageStore {
    pool: Pool<Postgres>,
    operation_store: operation_store::OperationStore,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DueBurnMessageRow {
    pub tenant_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub burn_at: i64,
}

impl PostgresMessageStore {
    pub async fn new(config: &StorageWriterConfig) -> Result<Option<Self>> {
        let url = match &config.postgres_url {
            Some(url) => url,
            None => return Ok(None),
        };

        // 优化连接池配置（根据配置参数）
        let pool = PgPoolOptions::new()
            .max_connections(config.postgres_max_connections)
            .min_connections(config.postgres_min_connections)
            .acquire_timeout(std::time::Duration::from_secs(
                config.postgres_acquire_timeout_seconds,
            ))
            .idle_timeout(Some(std::time::Duration::from_secs(
                config.postgres_idle_timeout_seconds,
            )))
            .max_lifetime(Some(std::time::Duration::from_secs(
                config.postgres_max_lifetime_seconds,
            )))
            .test_before_acquire(true) // 获取连接前测试连接是否有效
            .connect(url)
            .await?;

        Self::ensure_messages_schema(&pool).await?;

        let operation_store = operation_store::OperationStore::new(pool.clone());

        let store = Self {
            pool,
            operation_store,
        };
        Ok(Some(store))
    }

    async fn ensure_messages_schema(pool: &Pool<Postgres>) -> Result<()> {
        // Keep writer startup tolerant of existing dev/prod databases created
        // before init_v2.sql gained burn-after-read and delivery timeline fields.
        let statements = [
            "ALTER TABLE messages ADD COLUMN IF NOT EXISTS burn_enabled BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE messages ADD COLUMN IF NOT EXISTS burn_after_read_seconds BIGINT",
            "ALTER TABLE messages ADD COLUMN IF NOT EXISTS burn_status SMALLINT NOT NULL DEFAULT 0",
            "ALTER TABLE messages ADD COLUMN IF NOT EXISTS first_read_at BIGINT",
            "ALTER TABLE messages ADD COLUMN IF NOT EXISTS burn_at BIGINT",
            "ALTER TABLE messages ADD COLUMN IF NOT EXISTS burned_at BIGINT",
            "ALTER TABLE messages ADD COLUMN IF NOT EXISTS delivered_at TIMESTAMPTZ",
            "ALTER TABLE messages ADD COLUMN IF NOT EXISTS persisted_at TIMESTAMPTZ",
            r#"
            CREATE INDEX IF NOT EXISTS idx_messages_burn_due
                ON messages(tenant_id, burn_status, burn_at)
                WHERE burn_status = 3 AND burn_at IS NOT NULL
            "#,
        ];

        for statement in statements {
            sqlx::query(statement).execute(pool).await.map_err(|err| {
                anyhow::anyhow!("failed to ensure messages schema: {err}; sql={statement}")
            })?;
        }

        Ok(())
    }
}

impl PostgresMessageStore {
    /// 获取数据库连接池（用于创建 ConversationRepository）
    pub fn pool(&self) -> &Pool<Postgres> {
        &self.pool
    }

    pub async fn scan_due_burn_messages(
        &self,
        tenant_id: &str,
        now: i64,
        limit: i64,
    ) -> Result<Vec<DueBurnMessageRow>> {
        let limit = limit.clamp(1, 1000);
        let rows = sqlx::query_as::<_, DueBurnMessageRow>(
            r#"
            SELECT tenant_id, conversation_id, server_id AS message_id, burn_at
            FROM messages
            WHERE tenant_id = $1
              AND burn_status = $2
              AND burn_at IS NOT NULL
              AND burn_at <= $3
            ORDER BY burn_at ASC
            LIMIT $4
            "#,
        )
        .bind(tenant_id)
        .bind(BurnStatus::BurnPending.as_i32())
        .bind(now)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// 根据消息ID查询消息（内部辅助方法），返回领域模型（init_v2 列 + common/message.proto）
    async fn get_message_by_id(&self, message_id: &str) -> Result<Option<Message>> {
        use serde_json::Value as JsonValue;

        let row = sqlx::query_as::<_, MessageRow>(
            r#"
            SELECT tenant_id, server_id, conversation_id, client_msg_id, sender_id, sender_name, sender_avatar,
                channel_id, source, seq, timestamp, conversation_type, message_type, content,
                status, burn_enabled, burn_after_read_seconds, burn_status, first_read_at, burn_at,
                burned_at, offline_push_info, extra, extensions, created_at, persisted_at, delivered_at
            FROM messages WHERE server_id = $1 LIMIT 1
            "#,
        )
        .bind(message_id)
        .fetch_optional(&self.pool)
        .await?;

        let row = match row {
            Some(r) => r,
            None => return Ok(None),
        };

        let extra_value: JsonValue = row.extra.unwrap_or_else(|| serde_json::json!({}));
        let extra_map = extra_value
            .as_object()
            .cloned()
            .unwrap_or_else(serde_json::Map::new);

        let content: Vec<u8> = row.content.clone().unwrap_or_default();

        let timestamp = Some(prost_types::Timestamp {
            seconds: row.timestamp.timestamp(),
            nanos: row.timestamp.timestamp_subsec_nanos() as i32,
        });

        let offline_push_info = row.offline_push_info.as_ref().and_then(|v| {
            use flare_proto::common::OfflinePushInfo;
            let o = v.as_object()?;
            Some(OfflinePushInfo {
                title: o
                    .get("title")
                    .and_then(|t| t.as_str())
                    .unwrap_or("")
                    .to_string(),
                body: o
                    .get("body")
                    .and_then(|t| t.as_str())
                    .unwrap_or("")
                    .to_string(),
                sound: o
                    .get("sound")
                    .and_then(|t| t.as_str())
                    .unwrap_or("")
                    .to_string(),
                badge: o.get("badge").and_then(|b| b.as_bool()).unwrap_or(false),
                payload: o
                    .get("payload")
                    .and_then(|p| p.as_str())
                    .unwrap_or("")
                    .to_string(),
                ..Default::default()
            })
        });

        let extensions: std::collections::HashMap<String, Vec<u8>> = row
            .extensions
            .as_ref()
            .and_then(|v| v.as_object())
            .map(|o| {
                o.iter()
                    .filter_map(|(k, v)| {
                        let s = v.as_str()?;
                        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s)
                            .ok()
                            .map(|bytes| (k.clone(), bytes))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let mut extra: std::collections::HashMap<String, String> = extra_map
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
            .collect();
        extra.insert("tenant_id".to_string(), row.tenant_id.clone());

        let msg = Message {
            server_id: row.server_id,
            conversation_id: row.conversation_id,
            client_msg_id: row.client_msg_id.unwrap_or_default(),
            sender_id: row.sender_id,
            sender_name: row.sender_name.unwrap_or_default(),
            sender_avatar: row.sender_avatar.unwrap_or_default(),
            source: row.source,
            seq: row.seq as u64,
            timestamp: timestamp.clone(),
            conversation_type: row.conversation_type,
            message_type: row.message_type,
            channel_id: row.channel_id.unwrap_or_default(),
            content,
            status: row.status,
            burn_enabled: row.burn_enabled,
            burn_after_read_seconds: row.burn_after_read_seconds,
            burn_status: row.burn_status,
            first_read_at: row.first_read_at,
            burn_at: row.burn_at,
            burned_at: row.burned_at,
            offline_push_info,
            extra,
            extensions,
            ..Default::default()
        };
        Ok(Some(msg))
    }
}

fn offline_push_info_to_json(
    opt: Option<&flare_proto::common::OfflinePushInfo>,
) -> serde_json::Value {
    let o = match opt {
        Some(o) => o,
        None => return serde_json::json!({}),
    };
    serde_json::json!({
        "title": o.title,
        "body": o.body,
        "sound": o.sound,
        "badge": o.badge,
        "payload": o.payload,
    })
}

fn extensions_to_json(map: &std::collections::HashMap<String, Vec<u8>>) -> serde_json::Value {
    use serde_json::Value as J;
    let o: serde_json::Map<String, J> = map
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                J::String(base64::engine::general_purpose::STANDARD.encode(v)),
            )
        })
        .collect();
    J::Object(o)
}

fn resolve_tenant_id(
    ctx: &Ctx,
    message_id: &str,
    extra: &serde_json::Map<String, serde_json::Value>,
) -> Result<String> {
    if let Some(tenant_id) = ctx
        .tenant_id()
        .filter(|tenant_id| !tenant_id.trim().is_empty())
    {
        return Ok(normalize_tenant_id(tenant_id));
    }

    extra
        .get("tenant_id")
        .and_then(|value| value.as_str())
        .filter(|tenant_id| !tenant_id.trim().is_empty())
        .map(normalize_tenant_id)
        .ok_or_else(|| anyhow::anyhow!("tenant_id is required for message {}", message_id))
}

impl ArchiveStoreRepository for PostgresMessageStore {
    #[instrument(skip(self, message), fields(message_id = %message.server_id, conversation_id = %message.conversation_id))]
    async fn store_archive(&self, ctx: &Ctx, message: &Message) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        use crate::infrastructure::persistence::helpers::*;

        let proto_msg = convert::message_to_proto(message);
        let created_at_dt = get_message_timestamp(&proto_msg);
        let content_bytes = proto_msg.content.clone();
        let extra_value = build_extra_value(&proto_msg)?;
        let seq = i64::try_from(proto_msg.seq).map_err(|_| {
            anyhow::anyhow!("invalid seq overflow for message {}", proto_msg.server_id)
        })?;
        if seq <= 0 {
            return Err(anyhow::anyhow!(
                "invalid seq={}, message must be assigned in orchestrator: {}",
                seq,
                proto_msg.server_id
            ));
        }

        let tenant_id = resolve_tenant_id(ctx, &proto_msg.server_id, &extra_value)?;

        sqlx::query(
            r#"
            INSERT INTO messages (
                tenant_id, server_id, conversation_id, client_msg_id, sender_id, sender_name, sender_avatar,
                channel_id, source, seq, timestamp, conversation_type, message_type, content,
                status, burn_enabled, burn_after_read_seconds, burn_status, first_read_at, burn_at,
                burned_at, offline_push_info, extra, extensions, created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25)
            ON CONFLICT (tenant_id, server_id, created_at) DO NOTHING
            "#,
        )
        .bind(&tenant_id)
        .bind(&proto_msg.server_id)
        .bind(&proto_msg.conversation_id)
        .bind(if proto_msg.client_msg_id.is_empty() { None::<String> } else { Some(proto_msg.client_msg_id.clone()) })
        .bind(&proto_msg.sender_id)
        .bind(if proto_msg.sender_name.is_empty() { None::<String> } else { Some(proto_msg.sender_name.clone()) })
        .bind(if proto_msg.sender_avatar.is_empty() { None::<String> } else { Some(proto_msg.sender_avatar.clone()) })
        .bind(if proto_msg.channel_id.is_empty() { None::<String> } else { Some(proto_msg.channel_id.clone()) })
        .bind(proto_msg.source)
        .bind(seq)
        .bind(created_at_dt)
        .bind(proto_msg.conversation_type)
        .bind(proto_msg.message_type)
        .bind(content_bytes)
        .bind(proto_msg.status)
        .bind(proto_msg.burn_enabled)
        .bind(proto_msg.burn_after_read_seconds)
        .bind(proto_msg.burn_status)
        .bind(proto_msg.first_read_at)
        .bind(proto_msg.burn_at)
        .bind(proto_msg.burned_at)
        .bind(offline_push_info_to_json(proto_msg.offline_push_info.as_ref()))
        .bind(to_value(&extra_value)?)
        .bind(extensions_to_json(&proto_msg.extensions))
        .bind(created_at_dt)
        .execute(&self.pool)
        .await
        .map_err(|err| {
            anyhow::anyhow!(
                "insert message archive failed message_id={} conversation_id={} seq={}: {err}",
                proto_msg.server_id,
                proto_msg.conversation_id,
                seq
            )
        })?;

        Ok(())
    }

    #[instrument(skip(self), fields(tenant_id, message_id, fsm_state))]
    async fn update_message_fsm_state(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        fsm_state: &str,
        recall_reason: Option<&str>,
    ) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        self.operation_store
            .update_message_fsm_state(tenant_id, message_id, fsm_state, recall_reason)
            .await
    }

    #[instrument(skip(self), fields(tenant_id, message_id, editor_id))]
    async fn update_message_content(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        new_content: &[u8],
        edit_version: i32,
        editor_id: &str,
        reason: Option<&str>,
        content_text_for_extra: Option<&str>,
    ) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        self.operation_store
            .update_message_content(
                tenant_id,
                message_id,
                new_content,
                edit_version,
                editor_id,
                reason,
                content_text_for_extra,
            )
            .await
    }

    #[instrument(skip(self), fields(tenant_id, message_id, user_id, visibility_status))]
    async fn update_message_visibility(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        user_id: &str,
        scope: i32,
        visibility_status: &str,
    ) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        self.operation_store
            .update_message_visibility(tenant_id, message_id, user_id, scope, visibility_status)
            .await
    }

    #[instrument(skip(self), fields(tenant_id, message_id, user_id))]
    async fn record_message_read(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        user_id: &str,
    ) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        self.operation_store
            .record_message_read(tenant_id, message_id, user_id)
            .await
    }

    #[instrument(skip(self), fields(tenant_id, message_id, reader_id = ?reader_id, burn_at))]
    async fn schedule_message_burn(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        reader_id: Option<&str>,
        first_read_at: i64,
        burn_at: i64,
    ) -> Result<bool> {
        let _ = ctx;
        let result = sqlx::query(
            r#"
            UPDATE messages
            SET first_read_at = COALESCE(first_read_at, $3),
                burn_at = COALESCE(burn_at, $4),
                burn_status = $5
            WHERE tenant_id = $1
              AND server_id = $2
              AND burn_enabled = TRUE
              AND burn_status IN ($6, $7)
              AND burned_at IS NULL
            "#,
        )
        .bind(tenant_id)
        .bind(message_id)
        .bind(first_read_at)
        .bind(burn_at)
        .bind(BurnStatus::BurnPending.as_i32())
        .bind(BurnStatus::Init.as_i32())
        .bind(BurnStatus::Read.as_i32())
        .execute(&self.pool)
        .await?;

        if result.rows_affected() > 0 {
            if let Some(reader_id) = reader_id {
                let read_at = chrono::DateTime::from_timestamp(first_read_at, 0)
                    .unwrap_or_else(chrono::Utc::now);
                let _ = sqlx::query(
                    r#"
                    INSERT INTO message_read_records (tenant_id, message_id, user_id, read_at)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (tenant_id, message_id, user_id)
                    DO UPDATE SET read_at = LEAST(message_read_records.read_at, EXCLUDED.read_at)
                    "#,
                )
                .bind(tenant_id)
                .bind(message_id)
                .bind(reader_id)
                .bind(read_at)
                .execute(&self.pool)
                .await;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    #[instrument(skip(self), fields(tenant_id, message_id, reader_id = ?reader_id))]
    async fn schedule_message_burn_after_read(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        reader_id: Option<&str>,
        first_read_at: i64,
    ) -> Result<Option<i64>> {
        let _ = ctx;
        let row = sqlx::query(
            r#"
            UPDATE messages
            SET first_read_at = COALESCE(first_read_at, $3),
                burn_at = COALESCE(
                    burn_at,
                    $3 + GREATEST(COALESCE(burn_after_read_seconds, 0), 1)
                ),
                burn_status = $4
            WHERE tenant_id = $1
              AND server_id = $2
              AND burn_enabled = TRUE
              AND COALESCE(burn_after_read_seconds, 0) > 0
              AND burn_status IN ($5, $6)
              AND burned_at IS NULL
            RETURNING burn_at
            "#,
        )
        .bind(tenant_id)
        .bind(message_id)
        .bind(first_read_at)
        .bind(BurnStatus::BurnPending.as_i32())
        .bind(BurnStatus::Init.as_i32())
        .bind(BurnStatus::Read.as_i32())
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };
        let burn_at = row.try_get::<i64, _>("burn_at").unwrap_or(first_read_at);
        if let Some(reader_id) = reader_id {
            let read_at =
                chrono::DateTime::from_timestamp(first_read_at, 0).unwrap_or_else(chrono::Utc::now);
            let _ = sqlx::query(
                r#"
                INSERT INTO message_read_records (tenant_id, message_id, user_id, read_at)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (tenant_id, message_id, user_id)
                DO UPDATE SET read_at = LEAST(message_read_records.read_at, EXCLUDED.read_at)
                "#,
            )
            .bind(tenant_id)
            .bind(message_id)
            .bind(reader_id)
            .bind(read_at)
            .execute(&self.pool)
            .await;
        }
        Ok(Some(burn_at))
    }

    #[instrument(skip(self), fields(tenant_id, message_id, burned_at))]
    async fn mark_message_burned(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        burned_at: i64,
    ) -> Result<bool> {
        let _ = ctx;
        let result = sqlx::query(
            r#"
            UPDATE messages
            SET burn_status = $3,
                burned_at = COALESCE(burned_at, $4),
                content = '\x'::bytea,
                offline_push_info = NULL,
                extensions = '{}'::jsonb
            WHERE tenant_id = $1
              AND server_id = $2
              AND burn_enabled = TRUE
              AND burn_status = $5
            "#,
        )
        .bind(tenant_id)
        .bind(message_id)
        .bind(BurnStatus::Burned.as_i32())
        .bind(burned_at)
        .bind(BurnStatus::BurnPending.as_i32())
        .execute(&self.pool)
        .await?;

        if result.rows_affected() > 0 {
            let burned_at_dt =
                chrono::DateTime::from_timestamp(burned_at, 0).unwrap_or_else(chrono::Utc::now);
            let _ = sqlx::query(
                r#"
                UPDATE message_read_records
                SET burned_at = COALESCE(burned_at, $3)
                WHERE tenant_id = $1 AND message_id = $2
                "#,
            )
            .bind(tenant_id)
            .bind(message_id)
            .bind(burned_at_dt)
            .execute(&self.pool)
            .await;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    #[instrument(skip(self), fields(tenant_id, message_id, hard_deleted_at))]
    async fn mark_message_hard_deleted(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        hard_deleted_at: i64,
    ) -> Result<bool> {
        let _ = (ctx, hard_deleted_at);
        let result = sqlx::query(
            r#"
            UPDATE messages
            SET burn_status = $3,
                content = '\x'::bytea,
                offline_push_info = NULL,
                extensions = '{}'::jsonb
            WHERE tenant_id = $1
              AND server_id = $2
              AND burn_enabled = TRUE
              AND burn_status IN ($4, $5)
            "#,
        )
        .bind(tenant_id)
        .bind(message_id)
        .bind(BurnStatus::HardDeleted.as_i32())
        .bind(BurnStatus::Burned.as_i32())
        .bind(BurnStatus::BurnPending.as_i32())
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    #[instrument(skip(self), fields(tenant_id, message_id, emoji, user_id))]
    async fn upsert_message_reaction(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        emoji: &str,
        user_id: &str,
        add: bool,
    ) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        self.operation_store
            .upsert_message_reaction(tenant_id, message_id, emoji, user_id, add)
            .await
    }

    #[instrument(
        skip(self),
        fields(tenant_id, message_id, conversation_id, user_id, pin)
    )]
    async fn pin_message(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        conversation_id: &str,
        user_id: &str,
        pin: bool,
        expire_at: Option<chrono::DateTime<chrono::Utc>>,
        reason: Option<&str>,
    ) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        self.operation_store
            .pin_message(
                tenant_id,
                message_id,
                conversation_id,
                user_id,
                pin,
                expire_at,
                reason,
            )
            .await
    }

    #[instrument(
        skip(self),
        fields(tenant_id, message_id, conversation_id, user_id, mark_type)
    )]
    async fn mark_message(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        conversation_id: &str,
        user_id: &str,
        mark_type: &str,
        color: Option<&str>,
        add: bool,
    ) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        self.operation_store
            .mark_message(
                tenant_id,
                message_id,
                conversation_id,
                user_id,
                mark_type,
                color,
                add,
            )
            .await
    }

    #[instrument(skip(self, event), fields(tenant_id, message_id, operator_id = %event.operator_id))]
    async fn append_event(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        message_id: &str,
        event: &Event,
    ) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        let proto_event = convert::event_to_proto(event);
        if tenant_id.is_empty() {
            return Err(anyhow::anyhow!("tenant_id is required"));
        }
        self.operation_store
            .append_event(
                tenant_id,
                message_id,
                &proto_event,
                event.operator_id.as_str(),
            )
            .await
    }

    #[instrument(skip(self), fields(message_id))]
    async fn get_message(&self, ctx: &Ctx, message_id: &str) -> Result<Option<Message>> {
        let _ = ctx; // 上下文用于日志追踪
        // 调用内部辅助方法
        self.get_message_by_id(message_id).await
    }

    /// 批量存储消息（优化性能）
    ///
    /// 使用 TimescaleDB 优化的批量插入策略：
    /// - 小批量（<=10）：逐个插入（简单可靠）
    /// - 中批量（11-500）：使用 VALUES 多行插入（单事务，性能较好）
    /// - 大批量（>500）：分批处理，每批最多 500 条（避免单次事务过大）
    ///
    /// 批量大小自适应：
    /// - 根据消息大小动态调整批量大小
    /// - 避免单次事务过大导致超时或内存问题
    #[instrument(skip(self, messages), fields(batch_size = messages.len()))]
    async fn store_archive_batch(&self, ctx: &Ctx, messages: &[Message]) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        if messages.is_empty() {
            return Ok(());
        }

        // 小批量：逐个插入（简单且可靠）
        if messages.len() <= 10 {
            for message in messages {
                self.store_archive(ctx, message).await?;
            }
            return Ok(());
        }

        // 计算平均消息大小（用于自适应批量大小）
        let avg_message_size = messages
            .iter()
            .map(|m| {
                let content_size = m.content.len();
                let extra_size = serde_json::to_string(&m.extra).unwrap_or_default().len();
                content_size + extra_size + 200
            })
            .sum::<usize>()
            / messages.len();

        // 自适应批量大小：
        // - 小消息（<1KB）：每批最多 500 条
        // - 中等消息（1-10KB）：每批最多 200 条
        // - 大消息（>10KB）：每批最多 50 条
        let optimal_batch_size = if avg_message_size < 1024 {
            500
        } else if avg_message_size < 10 * 1024 {
            200
        } else {
            50
        };

        // 如果消息数量超过最优批量大小，分批处理
        if messages.len() > optimal_batch_size {
            let chunks: Vec<_> = messages.chunks(optimal_batch_size).collect();
            tracing::trace!(
                total_messages = messages.len(),
                optimal_batch_size = optimal_batch_size,
                chunks = chunks.len(),
                avg_message_size = avg_message_size,
                "Splitting batch into {} chunks for optimal performance",
                chunks.len()
            );

            for chunk in chunks {
                self.store_archive_batch_values(ctx, chunk).await?;
            }
            return Ok(());
        }

        // 中批量：使用 VALUES 多行插入（单事务，性能较好）
        self.store_archive_batch_values(ctx, messages).await
    }
}

impl PostgresMessageStore {
    /// 使用 VALUES 多行插入进行批量存储（优化版本）
    ///
    /// 此方法使用 sqlx::QueryBuilder 构建批量 INSERT 语句，
    /// 利用 TimescaleDB 的批量插入优化，性能比循环插入提升 10-100 倍
    ///
    /// 错误处理和重试：
    /// - 事务失败时自动重试（最多 3 次）
    /// - 使用指数退避策略
    async fn store_archive_batch_values(&self, ctx: &Ctx, messages: &[Message]) -> Result<()> {
        use sqlx::QueryBuilder;
        use std::time::Duration;

        use crate::infrastructure::persistence::helpers::*;

        let prepared_data: Vec<_> = messages
            .iter()
            .map(|message| -> Result<_> {
                let proto_msg = convert::message_to_proto(message);
                let created_at_dt = get_message_timestamp(&proto_msg);
                let extra_value = build_extra_value(&proto_msg)?;
                let seq = i64::try_from(proto_msg.seq).map_err(|_| {
                    anyhow::anyhow!("invalid seq overflow for message {}", proto_msg.server_id)
                })?;
                if seq <= 0 {
                    return Err(anyhow::anyhow!(
                        "invalid seq={}, message must be assigned in orchestrator: {}",
                        seq,
                        proto_msg.server_id
                    ));
                }
                let tenant_id = resolve_tenant_id(ctx, &proto_msg.server_id, &extra_value)?;

                Ok((
                    tenant_id,
                    proto_msg.server_id.clone(),
                    proto_msg.conversation_id.clone(),
                    if proto_msg.client_msg_id.is_empty() {
                        None
                    } else {
                        Some(proto_msg.client_msg_id.clone())
                    },
                    proto_msg.sender_id.clone(),
                    if proto_msg.sender_name.is_empty() {
                        None
                    } else {
                        Some(proto_msg.sender_name.clone())
                    },
                    if proto_msg.sender_avatar.is_empty() {
                        None
                    } else {
                        Some(proto_msg.sender_avatar.clone())
                    },
                    if proto_msg.channel_id.is_empty() {
                        None
                    } else {
                        Some(proto_msg.channel_id.clone())
                    },
                    proto_msg.source,
                    seq,
                    created_at_dt,
                    proto_msg.conversation_type,
                    proto_msg.message_type,
                    proto_msg.content.clone(),
                    proto_msg.status,
                    proto_msg.burn_enabled,
                    proto_msg.burn_after_read_seconds,
                    proto_msg.burn_status,
                    proto_msg.first_read_at,
                    proto_msg.burn_at,
                    proto_msg.burned_at,
                    offline_push_info_to_json(proto_msg.offline_push_info.as_ref()),
                    to_value(&extra_value).unwrap_or_default(),
                    extensions_to_json(&proto_msg.extensions),
                    created_at_dt,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        // 重试机制（最多 3 次）
        let max_retries = 3;
        let mut last_error: Option<anyhow::Error> = None;

        for attempt in 0..max_retries {
            // 使用事务确保原子性
            let mut tx = match self.pool.begin().await {
                Ok(tx) => tx,
                Err(e) => {
                    last_error = Some(anyhow::Error::from(e));
                    if attempt < max_retries - 1 {
                        // 指数退避：1s, 2s, 4s
                        let backoff = Duration::from_millis(1000 * (1 << attempt));
                        tracing::warn!(
                            attempt = attempt + 1,
                            backoff_ms = backoff.as_millis(),
                            "Failed to begin transaction, retrying after backoff"
                        );
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                    return Err(last_error.unwrap_or_else(|| {
                        anyhow::anyhow!("Failed to begin transaction after retries")
                    }));
                }
            };

            let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
                r#"
                INSERT INTO messages (
                    tenant_id, server_id, conversation_id, client_msg_id, sender_id, sender_name, sender_avatar,
                    channel_id, source, seq, timestamp, conversation_type, message_type, content,
                    status, burn_enabled, burn_after_read_seconds, burn_status, first_read_at, burn_at,
                    burned_at, offline_push_info, extra, extensions, created_at
                )
                "#,
            );

            query_builder.push_values(&prepared_data, |mut b, row| {
                b.push_bind(&row.0); // tenant_id
                b.push_bind(&row.1); // server_id
                b.push_bind(&row.2); // conversation_id
                b.push_bind(&row.3); // client_msg_id
                b.push_bind(&row.4); // sender_id
                b.push_bind(&row.5); // sender_name
                b.push_bind(&row.6); // sender_avatar
                b.push_bind(&row.7); // channel_id
                b.push_bind(row.8); // source
                b.push_bind(row.9); // seq
                b.push_bind(row.10); // timestamp
                b.push_bind(row.11); // conversation_type
                b.push_bind(row.12); // message_type
                b.push_bind(&row.13); // content
                b.push_bind(row.14); // status
                b.push_bind(row.15); // burn_enabled
                b.push_bind(row.16); // burn_after_read_seconds
                b.push_bind(row.17); // burn_status
                b.push_bind(row.18); // first_read_at
                b.push_bind(row.19); // burn_at
                b.push_bind(row.20); // burned_at
                b.push_bind(&row.21); // offline_push_info
                b.push_bind(&row.22); // extra
                b.push_bind(&row.23); // extensions
                b.push_bind(row.24); // created_at
            });

            query_builder.push(" ON CONFLICT (tenant_id, server_id, created_at) DO NOTHING");

            // 执行批量插入
            match query_builder.build().execute(&mut *tx).await {
                Ok(_) => {
                    // 提交事务
                    match tx.commit().await {
                        Ok(_) => {
                            tracing::info!(
                                batch_size = messages.len(),
                                attempt = attempt + 1,
                                "Successfully batch inserted {} messages into TimescaleDB using VALUES",
                                messages.len()
                            );
                            return Ok(());
                        }
                        Err(e) => {
                            last_error = Some(anyhow::Error::from(e));
                            if attempt < max_retries - 1 {
                                let backoff = Duration::from_millis(1000 * (1 << attempt));
                                tracing::warn!(
                                    attempt = attempt + 1,
                                    backoff_ms = backoff.as_millis(),
                                    "Failed to commit transaction, retrying after backoff"
                                );
                                tokio::time::sleep(backoff).await;
                                continue;
                            }
                        }
                    }
                }
                Err(e) => {
                    last_error = Some(anyhow::anyhow!(
                        "batch insert message archive failed batch_size={} attempt={}: {e}",
                        messages.len(),
                        attempt + 1
                    ));
                    // 回滚事务
                    let _ = tx.rollback().await;
                    if attempt < max_retries - 1 {
                        let backoff = Duration::from_millis(1000 * (1 << attempt));
                        tracing::warn!(
                            attempt = attempt + 1,
                            backoff_ms = backoff.as_millis(),
                            "Failed to execute batch insert, retrying after backoff"
                        );
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                }
            }
        }

        // 所有重试都失败
        Err(anyhow::anyhow!(
            "Failed to batch insert {} messages after {} attempts: {}",
            messages.len(),
            max_retries,
            last_error
                .map(|e| e.to_string())
                .unwrap_or_else(|| "Unknown error".to_string())
        ))
    }
}
