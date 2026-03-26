//! PostgreSQL 存储基础实现
//!
//! 提供共享的 PostgreSQL 存储功能和辅助方法

use std::collections::HashMap;

use anyhow::{Context, Result};
use base64::Engine;
use chrono::{DateTime, Utc};
use flare_im_core::utils::{datetime_to_timestamp, timestamp_to_datetime};
use flare_im_core::message::Message;
use prost::Message as ProstMessage;
use prost_types;
use serde_json::{Value, from_value};
use sqlx::{Pool, Postgres, Row, postgres::PgPoolOptions};

use crate::config::StorageReaderConfig;
use crate::infrastructure::persistence::redis_cache::RedisMessageCache;
use crate::infrastructure::persistence::helpers::*;

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
            Some(std::sync::Arc::new(RedisMessageCache::new(std::sync::Arc::new(client), config)))
        } else {
            None
        };

        Self::from_pool_and_cache(pool, cache)
            .await
            .map(Some)
    }

    /// 验证表结构是否存在，并创建必要的索引（如果不存在）
    pub async fn verify_schema(&self) -> Result<()> {
        // 检查 messages 表是否存在
        let exists: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'messages'
            )
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .context("Failed to check if messages table exists")?;

        if !exists {
            return Err(anyhow::anyhow!(
                "messages table does not exist. Please run init.sql or ensure Storage Writer has initialized the schema"
            ));
        }

        // 检查 message_operation_history 表是否存在
        let exists: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'message_operation_history'
            )
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .context("Failed to check if message_operation_history table exists")?;

        if !exists {
            tracing::warn!("message_operation_history table does not exist. Please run init.sql to initialize the schema.");
        }

        // 检查 message_edit_history 表是否存在
        let exists: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'message_edit_history'
            )
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .context("Failed to check if message_edit_history table exists")?;

        if !exists {
            tracing::warn!("message_edit_history table does not exist. Please run init.sql to initialize the schema.");
        }

        // 检查 message_read_records 表是否存在
        let exists: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'message_read_records'
            )
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .context("Failed to check if message_read_records table exists")?;

        if !exists {
            tracing::warn!("message_read_records table does not exist. Please run init.sql to initialize the schema.");
        }

        // 检查 message_visibility 表是否存在
        let exists: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'message_visibility'
            )
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .context("Failed to check if message_visibility table exists")?;

        if !exists {
            tracing::warn!("message_visibility table does not exist. Please run init.sql to initialize the schema.");
        }

        // 检查 message_reactions 表是否存在
        let exists: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'message_reactions'
            )
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .context("Failed to check if message_reactions table exists")?;

        if !exists {
            tracing::warn!("message_reactions table does not exist. Please run init.sql to initialize the schema.");
        }

        // 检查 pinned_messages 表是否存在
        let exists: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'pinned_messages'
            )
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .context("Failed to check if pinned_messages table exists")?;

        if !exists {
            tracing::warn!("pinned_messages table does not exist. Please run init.sql to initialize the schema.");
        }

        // 创建必要的索引（如果不存在）以优化查询性能
        self.ensure_indexes()
            .await
            .context("Failed to create indexes")?;

        Ok(())
    }

    /// 确保必要索引存在（与 init_v2.sql 一致；init_v2 已建主键与唯一索引，此处仅补可选索引）
    pub async fn ensure_indexes(&self) -> Result<()> {
        let indexes: &[(&str, &str)] = &[
            ("idx_message_operation_history_tenant_message", "CREATE INDEX IF NOT EXISTS idx_message_operation_history_tenant_message ON message_operation_history(tenant_id, message_id)"),
            ("idx_message_edit_history_tenant_message", "CREATE INDEX IF NOT EXISTS idx_message_edit_history_tenant_message ON message_edit_history(tenant_id, message_id)"),
            ("idx_message_read_records_tenant_message", "CREATE INDEX IF NOT EXISTS idx_message_read_records_tenant_message ON message_read_records(tenant_id, message_id)"),
            ("idx_message_visibility_tenant_user", "CREATE INDEX IF NOT EXISTS idx_message_visibility_tenant_user ON message_visibility(tenant_id, user_id)"),
            ("idx_message_reactions_tenant_message", "CREATE INDEX IF NOT EXISTS idx_message_reactions_tenant_message ON message_reactions(tenant_id, message_id)"),
            ("idx_pinned_messages_tenant_conversation", "CREATE INDEX IF NOT EXISTS idx_pinned_messages_tenant_conversation ON pinned_messages(tenant_id, conversation_id)"),
            ("idx_marked_messages_tenant_user", "CREATE INDEX IF NOT EXISTS idx_marked_messages_tenant_user ON marked_messages(tenant_id, user_id)"),
        ];

        for (name, sql) in indexes {
            sqlx::query(sql)
                .execute(&self.pool)
                .await
                .with_context(|| format!("Failed to create index: {}", name))?;
        }

        tracing::info!("All indexes verified/created successfully");
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

        tracing::debug!(
            pool_size = pool_size,
            idle_connections = idle_connections,
            "Database connection pool status"
        );

        Ok(())
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
        let offline_push_info: Option<Value> = row.get("offline_push_info");
        let extra: Option<Value> = row.get("extra");
        let extensions: Option<Value> = row.get("extensions");

        let mut extra_map = HashMap::new();
        if let Some(extra_value) = extra {
            if let Ok(extra_obj) = from_value::<HashMap<String, Value>>(extra_value) {
                for (k, v) in extra_obj {
                    extra_map.insert(k, v.to_string().trim_matches('"').to_string());
                }
            }
        }

        let offline_push_proto = offline_push_info.as_ref().and_then(|v| {
            let o = v.as_object()?;
            Some(flare_proto::common::OfflinePushInfo {
                title: o.get("title").and_then(|t| t.as_str()).unwrap_or("").to_string(),
                body: o.get("body").and_then(|t| t.as_str()).unwrap_or("").to_string(),
                sound: o.get("sound").and_then(|t| t.as_str()).unwrap_or("").to_string(),
                badge: o.get("badge").and_then(|b| b.as_bool()).unwrap_or(false),
                payload: o.get("payload").and_then(|p| p.as_str()).unwrap_or("").to_string(),
                ..Default::default()
            })
        });

        let extensions_map: std::collections::HashMap<String, Vec<u8>> = extensions
            .as_ref()
            .and_then(|v| v.as_object())
            .map(|o| {
                o.iter()
                    .filter_map(|(k, v)| {
                        let s = v.as_str()?;
                        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s).ok()
                            .map(|bytes| (k.clone(), bytes))
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(Message {
            server_id,
            conversation_id,
            client_msg_id: client_msg_id.unwrap_or_default(),
            sender_id,
            sender_name: sender_name.unwrap_or_default(),
            sender_avatar: sender_avatar.unwrap_or_default(),
            source,
            seq: seq as u64,
            timestamp: Some(datetime_to_timestamp(timestamp)),
            conversation_type,
            message_type,
            channel_id: channel_id.unwrap_or_default(),
            content: content.unwrap_or_default(),
            status,
            offline_push_info: offline_push_proto,
            extra: extra_map,
            extensions: extensions_map,
        })
    }
}