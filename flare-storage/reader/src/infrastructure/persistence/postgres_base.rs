//! PostgreSQL 存储基础实现
//!
//! 提供共享的 PostgreSQL 存储功能和辅助方法

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use flare_im_contracts::message::Message;
use flare_im_contracts::utils::datetime_to_timestamp;
use flare_proto::common::{
    ContentVisibility, MessageContent, MessageRetentionLifecycle, MessageRetentionState,
};
use flare_server_core::error::{AnyhowContext, Result};
use prost::Message as ProstMessage;
use serde_json::{Value, from_value};
use sqlx::{Pool, Postgres, Row, postgres::PgPoolOptions};

use crate::config::StorageReaderConfig;
use crate::domain::model::{MessageWriteLedgerEntry, MessageWriteLedgerQuery};
use crate::infrastructure::persistence::redis_cache::RedisMessageCache;

const LEGACY_BURN_STATUS_BURN_PENDING: i32 = 3;
const LEGACY_BURN_STATUS_BURNED: i32 = 4;
const LEGACY_BURN_STATUS_HARD_DELETED: i32 = 5;

#[derive(Clone)]
/// PostgreSQL 消息存储基础结构
pub struct PostgresBaseStorage {
    pub pool: Pool<Postgres>,
    pub cache: Option<std::sync::Arc<RedisMessageCache>>,
}

impl PostgresBaseStorage {
    /// 从已创建好的连接池和缓存构建存储实例（供 wire 使用，便于在 wire 中统一配置如 SQL 日志）
    pub async fn from_pool_and_cache(
        pool: Pool<Postgres>,
        cache: Option<std::sync::Arc<RedisMessageCache>>,
    ) -> Result<Self> {
        let storage = Self { pool, cache };
        storage
            .verify_schema()
            .await
            .context("Failed to verify PostgreSQL schema")?;
        Ok(storage)
    }

    /// 创建新的 PostgreSQL 存储实例（带可选的 Redis 缓存）
    /// 注意：生产环境建议在 wire 中创建 pool 并调用 from_pool_and_cache，以便统一配置 SQL 日志等
    pub async fn new(config: &StorageReaderConfig) -> Result<Option<Self>> {
        let url = match &config.postgres_url {
            Some(url) => url,
            None => return Ok(None),
        };

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
            .test_before_acquire(true)
            .connect(url)
            .await
            .context("Failed to connect to PostgreSQL")?;

        let cache = if let Some(redis_url) = &config.redis_url {
            let client =
                redis::Client::open(redis_url.as_str()).context("Failed to create Redis client")?;
            Some(std::sync::Arc::new(RedisMessageCache::new(
                std::sync::Arc::new(client),
                config,
            )))
        } else {
            None
        };

        Self::from_pool_and_cache(pool, cache).await.map(Some)
    }

    /// 验证运行时 schema 与 `deploy/init.sql` 的当前契约一致。
    pub async fn verify_schema(&self) -> Result<()> {
        self.require_table("messages").await?;
        self.require_columns(
            "messages",
            &[
                "tenant_id",
                "server_id",
                "conversation_id",
                "client_msg_id",
                "sender_id",
                "sender_name",
                "sender_avatar",
                "channel_id",
                "source",
                "seq",
                "timestamp",
                "conversation_type",
                "message_type",
                "content",
                "status",
                "offline_push_info",
                "extra",
                "extensions",
                "created_at",
                "persisted_at",
                "delivered_at",
            ],
        )
        .await?;

        self.require_table("message_operation_history").await?;
        self.require_table("message_edit_history").await?;
        self.require_table("message_read_records").await?;

        self.require_table("message_visibility").await?;
        self.require_columns(
            "message_visibility",
            &[
                "tenant_id",
                "message_id",
                "user_id",
                "scope",
                "visibility_status",
                "changed_at",
            ],
        )
        .await?;

        self.require_table("message_reactions").await?;
        self.require_columns(
            "message_reactions",
            &[
                "tenant_id",
                "message_id",
                "emoji",
                "user_ids",
                "count",
                "last_updated",
            ],
        )
        .await?;

        self.require_table("pinned_messages").await?;
        self.require_columns(
            "pinned_messages",
            &[
                "tenant_id",
                "message_id",
                "conversation_id",
                "pinned_by",
                "scope",
                "owner_user_id",
                "pinned_at",
                "expire_at",
                "reason",
            ],
        )
        .await?;

        Ok(())
    }

    async fn require_table(&self, table_name: &str) -> Result<()> {
        let exists: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = $1
            )
            "#,
        )
        .bind(table_name)
        .fetch_one(&self.pool)
        .await
        .context("Failed to verify required PostgreSQL table")?;

        if !exists {
            return Err(flare_server_core::error::FlareError::system(format!(
                "PostgreSQL schema mismatch: required table `{table_name}` is missing; re-run flare-im-core/deploy/init.sql"
            )));
        }
        Ok(())
    }

    async fn require_columns(&self, table_name: &str, columns: &[&str]) -> Result<()> {
        let expected: Vec<String> = columns.iter().map(|column| (*column).to_string()).collect();
        let actual: HashSet<String> = sqlx::query_scalar::<_, String>(
            r#"
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = $1
              AND column_name = ANY($2)
            "#,
        )
        .bind(table_name)
        .bind(&expected)
        .fetch_all(&self.pool)
        .await
        .context("Failed to verify required PostgreSQL columns")?
        .into_iter()
        .collect();

        let missing: Vec<&str> = columns
            .iter()
            .copied()
            .filter(|column| !actual.contains(*column))
            .collect();
        if !missing.is_empty() {
            return Err(flare_server_core::error::FlareError::system(format!(
                "PostgreSQL schema mismatch: table `{table_name}` missing columns [{}]; re-run flare-im-core/deploy/init.sql",
                missing.join(", ")
            )));
        }
        Ok(())
    }

    /// 健康检查：验证数据库连接和基本查询
    pub async fn health_check(&self) -> Result<()> {
        // 简单的查询测试连接
        let _: i64 = sqlx::query_scalar("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .context("Health check failed: database connection error")?;

        // 检查连接池状态
        let pool_size = self.pool.size();
        let idle_connections = self.pool.num_idle();

        tracing::trace!(
            pool_size = pool_size,
            idle_connections = idle_connections,
            "Database connection pool status"
        );

        Ok(())
    }

    /// 查询消息写入账本。该账本由 Storage Writer 写入，Reader 只提供受限分页查询。
    pub async fn query_message_write_ledger(
        &self,
        query: MessageWriteLedgerQuery,
    ) -> Result<(Vec<MessageWriteLedgerEntry>, bool)> {
        let limit = query.limit.clamp(1, 500);
        let offset = query.offset.max(0);
        let fetch_limit = limit + 1;

        let mut builder = sqlx::QueryBuilder::<Postgres>::new(
            r#"
            SELECT
                tenant_id,
                server_id,
                conversation_id,
                seq,
                write_state,
                archive_persisted_at,
                storage_persisted_at,
                wal_cleaned_at,
                ack_published_at,
                failed_at,
                last_error,
                created_at,
                updated_at
            FROM message_write_ledger
            WHERE tenant_id =
            "#,
        );
        builder.push_bind(&query.tenant_id);

        if let Some(server_id) = query.server_id.as_deref() {
            builder.push(" AND server_id = ");
            builder.push_bind(server_id);
        }
        if let Some(conversation_id) = query.conversation_id.as_deref() {
            builder.push(" AND conversation_id = ");
            builder.push_bind(conversation_id);
        }
        if let Some(write_state) = query.write_state.as_deref() {
            builder.push(" AND write_state = ");
            builder.push_bind(write_state);
        }
        if query.failed_only {
            builder.push(" AND failed_at IS NOT NULL");
        }
        if let Some(updated_after) = query.updated_after {
            builder.push(" AND updated_at >= ");
            builder.push_bind(updated_after);
        }
        if let Some(updated_before) = query.updated_before {
            builder.push(" AND updated_at <= ");
            builder.push_bind(updated_before);
        }

        builder.push(" ORDER BY updated_at DESC, server_id DESC LIMIT ");
        builder.push_bind(fetch_limit);
        builder.push(" OFFSET ");
        builder.push_bind(offset);

        let rows = builder
            .build()
            .fetch_all(&self.pool)
            .await
            .context("Failed to query message write ledger")?;

        let has_more = rows.len() as i64 > limit;
        let entries = rows
            .into_iter()
            .take(limit as usize)
            .map(|row| {
                Ok(MessageWriteLedgerEntry {
                    tenant_id: row.try_get("tenant_id").context("row tenant_id")?,
                    server_id: row.try_get("server_id").context("row server_id")?,
                    conversation_id: row
                        .try_get("conversation_id")
                        .context("row conversation_id")?,
                    seq: row.try_get("seq").context("row seq")?,
                    write_state: row.try_get("write_state").context("row write_state")?,
                    archive_persisted_at: row
                        .try_get("archive_persisted_at")
                        .context("row archive_persisted_at")?,
                    storage_persisted_at: row
                        .try_get("storage_persisted_at")
                        .context("row storage_persisted_at")?,
                    wal_cleaned_at: row
                        .try_get("wal_cleaned_at")
                        .context("row wal_cleaned_at")?,
                    ack_published_at: row
                        .try_get("ack_published_at")
                        .context("row ack_published_at")?,
                    failed_at: row.try_get("failed_at").context("row failed_at")?,
                    last_error: row.try_get("last_error").context("row last_error")?,
                    created_at: row.try_get("created_at").context("row created_at")?,
                    updated_at: row.try_get("updated_at").context("row updated_at")?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok((entries, has_more))
    }

    /// 从 init_v2 messages 行转换为 common/message.proto Message
    pub fn row_to_message(&self, row: &sqlx::postgres::PgRow) -> Result<Message> {
        let server_id: String = row.get("server_id");
        let conversation_id: String = row.get("conversation_id");
        let client_msg_id: Option<String> = row.get("client_msg_id");
        let sender_id: String = row.get("sender_id");
        let sender_name: Option<String> = row.get("sender_name");
        let sender_avatar: Option<String> = row.get("sender_avatar");
        let channel_id: Option<String> = row.get("channel_id");
        let source: i32 = row.get("source");
        let seq: i64 = row.get("seq");
        let timestamp: DateTime<Utc> = row.get("timestamp");
        let conversation_type: i32 = row.get("conversation_type");
        let message_type: i32 = row.get("message_type");
        let content: Option<Vec<u8>> = row.get("content");
        let status: i32 = row.get("status");
        let _burn_enabled: bool = row.try_get("burn_enabled").unwrap_or(false);
        let _burn_after_read_seconds: Option<i64> =
            row.try_get("burn_after_read_seconds").unwrap_or(None);
        let burn_status: i32 = row.try_get("burn_status").unwrap_or(0);
        let first_read_at: Option<i64> = row.try_get("first_read_at").unwrap_or(None);
        let burn_at: Option<i64> = row.try_get("burn_at").unwrap_or(None);
        let burned_at: Option<i64> = row.try_get("burned_at").unwrap_or(None);
        let offline_push_info: Option<Value> = row.get("offline_push_info");
        let extra: Option<Value> = row.get("extra");
        let extensions: Option<Value> = row.get("extensions");
        let reactions_json: Option<Value> = row.try_get("reactions_json").ok();
        let is_pinned: bool = row.try_get("is_pinned").unwrap_or(false);

        let mut extra_map = HashMap::new();
        if let Some(extra_value) = extra
            && let Ok(extra_obj) = from_value::<HashMap<String, Value>>(extra_value)
        {
            for (k, v) in extra_obj {
                extra_map.insert(k, v.to_string().trim_matches('"').to_string());
            }
        }
        if let Some(rx) = reactions_json
            && !rx.is_null()
        {
            let serialized = if let Some(s) = rx.as_str() {
                s.to_string()
            } else {
                rx.to_string()
            };
            extra_map.insert("reactionsJson".to_string(), serialized);
        }
        if is_pinned {
            extra_map.insert("pinned".to_string(), "true".to_string());
        }

        let offline_push_proto = offline_push_info.as_ref().and_then(|v| {
            let o = v.as_object()?;
            Some(flare_proto::common::OfflinePushInfo {
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
            })
        });

        let extensions_map: std::collections::HashMap<String, Vec<u8>> = extensions
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

        let content = content
            .filter(|bytes| !bytes.is_empty())
            .and_then(|bytes| MessageContent::decode(bytes.as_slice()).ok());
        let retention_state =
            legacy_retention_state(burn_status, first_read_at, burn_at, burned_at);

        Ok(Message {
            server_id,
            conversation_id,
            client_msg_id: client_msg_id.unwrap_or_default(),
            sender_id,
            sender_name: sender_name.unwrap_or_default(),
            sender_avatar: sender_avatar.unwrap_or_default(),
            source,
            conversation_seq: seq as u64,
            timestamp: Some(datetime_to_timestamp(timestamp)),
            conversation_type,
            message_type,
            message_seq: None,
            channel_id: channel_id.unwrap_or_default(),
            content,
            status,
            retention_policy: None,
            retention_state,
            offline_push_info: offline_push_proto,
            extra: extra_map,
            extensions: extensions_map,
        })
    }
}

fn legacy_retention_state(
    legacy_status: i32,
    first_read_at: Option<i64>,
    burn_at: Option<i64>,
    burned_at: Option<i64>,
) -> Option<MessageRetentionState> {
    let lifecycle = match legacy_status {
        LEGACY_BURN_STATUS_BURN_PENDING => MessageRetentionLifecycle::Scheduled,
        LEGACY_BURN_STATUS_BURNED => MessageRetentionLifecycle::Expired,
        LEGACY_BURN_STATUS_HARD_DELETED => MessageRetentionLifecycle::Purged,
        _ => return None,
    };
    let content_visibility = match lifecycle {
        MessageRetentionLifecycle::Purged => ContentVisibility::Purged,
        MessageRetentionLifecycle::Expired => ContentVisibility::Redacted,
        _ => ContentVisibility::Available,
    };
    Some(MessageRetentionState {
        lifecycle: lifecycle as i32,
        content_visibility: content_visibility as i32,
        first_triggered_at: first_read_at,
        expire_at: burn_at,
        expired_at: burned_at,
        purged_at: if lifecycle == MessageRetentionLifecycle::Purged {
            burned_at
        } else {
            None
        },
        triggered_by_user_id: None,
    })
}
