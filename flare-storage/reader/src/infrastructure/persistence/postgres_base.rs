//! PostgreSQL 存储基础实现
//!
//! 提供共享的 PostgreSQL 存储功能和辅助方法

use std::collections::HashMap;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use flare_im_core::utils::datetime_to_timestamp;
use flare_proto::common::Message;
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
    /// 创建新的 PostgreSQL 存储实例（带可选的 Redis 缓存）
    pub async fn new(config: &StorageReaderConfig) -> Result<Option<Self>> {
        let url = match &config.postgres_url {
            Some(url) => url,
            None => return Ok(None),
        };

        // 使用配置的连接池参数
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
            .test_before_acquire(true) // 连接池健康检查
            .connect(url)
            .await
            .context("Failed to connect to PostgreSQL")?;

        // 初始化 Redis 缓存（可选）
        let cache = if let Some(redis_url) = &config.redis_url {
            let client =
                redis::Client::open(redis_url.as_str()).context("Failed to create Redis client")?;
            Some(std::sync::Arc::new(RedisMessageCache::new(std::sync::Arc::new(client), config)))
        } else {
            None
        };

        let storage = Self { pool, cache };

        // 验证表结构（不创建，由 Writer 或 init.sql 创建）
        storage
            .verify_schema()
            .await
            .context("Failed to verify PostgreSQL schema")?;

        Ok(Some(storage))
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

    /// 确保必要的索引存在（用于优化查询性能）
    /// 注意：索引定义与 init.sql 保持一致
    pub async fn ensure_indexes(&self) -> Result<()> {
        let indexes = vec![
            // messages 表索引
            (
                "idx_messages_server_id_unique",
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_server_id_unique ON messages(server_id)",
            ),
            (
                "idx_messages_conversation_id",
                "CREATE INDEX IF NOT EXISTS idx_messages_conversation_id ON messages(conversation_id)",
            ),
            (
                "idx_messages_sender_id",
                "CREATE INDEX IF NOT EXISTS idx_messages_sender_id ON messages(sender_id)",
            ),
            (
                "idx_messages_conversation_timestamp",
                "CREATE INDEX IF NOT EXISTS idx_messages_conversation_timestamp ON messages(conversation_id, timestamp DESC)",
            ),
            (
                "idx_messages_client_msg_id",
                "CREATE INDEX IF NOT EXISTS idx_messages_client_msg_id ON messages(client_msg_id) WHERE client_msg_id IS NOT NULL",
            ),
            (
                "idx_messages_sender_client_msg_id",
                "CREATE INDEX IF NOT EXISTS idx_messages_sender_client_msg_id ON messages(sender_id, client_msg_id) WHERE client_msg_id IS NOT NULL",
            ),
            (
                "idx_messages_business_type",
                "CREATE INDEX IF NOT EXISTS idx_messages_business_type ON messages(business_type) WHERE business_type IS NOT NULL",
            ),
            (
                "idx_messages_message_type",
                "CREATE INDEX IF NOT EXISTS idx_messages_message_type ON messages(message_type)",
            ),
            (
                "idx_messages_fsm_state",
                "CREATE INDEX IF NOT EXISTS idx_messages_fsm_state ON messages(status)",
            ),
            (
                "idx_messages_fsm_state_changed_at",
                "CREATE INDEX IF NOT EXISTS idx_messages_fsm_state_changed_at ON messages(fsm_state_changed_at) WHERE fsm_state_changed_at IS NOT NULL",
            ),
            (
                "idx_messages_current_edit_version",
                "CREATE INDEX IF NOT EXISTS idx_messages_current_edit_version ON messages(current_edit_version) WHERE current_edit_version > 0",
            ),
            (
                "idx_messages_last_edited_at",
                "CREATE INDEX IF NOT EXISTS idx_messages_last_edited_at ON messages(last_edited_at) WHERE last_edited_at IS NOT NULL",
            ),
            (
                "idx_messages_conversation_seq",
                "CREATE INDEX IF NOT EXISTS idx_messages_conversation_seq ON messages(conversation_id, seq) WHERE seq IS NOT NULL",
            ),
            (
                "idx_messages_seq",
                "CREATE INDEX IF NOT EXISTS idx_messages_seq ON messages(seq) WHERE seq IS NOT NULL",
            ),
            (
                "idx_messages_expire_at",
                "CREATE INDEX IF NOT EXISTS idx_messages_expire_at ON messages(expire_at) WHERE expire_at IS NOT NULL",
            ),
            (
                "idx_messages_source",
                "CREATE INDEX IF NOT EXISTS idx_messages_source ON messages(source)",
            ),
            (
                "idx_messages_tenant_id",
                "CREATE INDEX IF NOT EXISTS idx_messages_tenant_id ON messages(tenant_id) WHERE tenant_id IS NOT NULL",
            ),
            // message_operation_history 表索引
            (
                "idx_message_operation_history_message_id",
                "CREATE INDEX IF NOT EXISTS idx_message_operation_history_message_id ON message_operation_history(message_id)",
            ),
            (
                "idx_message_operation_history_operation_type",
                "CREATE INDEX IF NOT EXISTS idx_message_operation_history_operation_type ON message_operation_history(operation_type)",
            ),
            (
                "idx_message_operation_history_operator_id",
                "CREATE INDEX IF NOT EXISTS idx_message_operation_history_operator_id ON message_operation_history(operator_id)",
            ),
            (
                "idx_message_operation_history_timestamp",
                "CREATE INDEX IF NOT EXISTS idx_message_operation_history_timestamp ON message_operation_history(timestamp DESC)",
            ),
            // message_edit_history 表索引
            (
                "idx_message_edit_history_message_id",
                "CREATE INDEX IF NOT EXISTS idx_message_edit_history_message_id ON message_edit_history(message_id)",
            ),
            (
                "idx_message_edit_history_editor_id",
                "CREATE INDEX IF NOT EXISTS idx_message_edit_history_editor_id ON message_edit_history(editor_id)",
            ),
            (
                "idx_message_edit_history_edited_at",
                "CREATE INDEX IF NOT EXISTS idx_message_edit_history_edited_at ON message_edit_history(edited_at DESC)",
            ),
            // message_read_records 表索引
            (
                "idx_message_read_records_message_id",
                "CREATE INDEX IF NOT EXISTS idx_message_read_records_message_id ON message_read_records(message_id)",
            ),
            (
                "idx_message_read_records_user_id",
                "CREATE INDEX IF NOT EXISTS idx_message_read_records_user_id ON message_read_records(user_id)",
            ),
            (
                "idx_message_read_records_read_at",
                "CREATE INDEX IF NOT EXISTS idx_message_read_records_read_at ON message_read_records(read_at DESC)",
            ),
            // message_visibility 表索引
            (
                "idx_message_visibility_message_id",
                "CREATE INDEX IF NOT EXISTS idx_message_visibility_message_id ON message_visibility(message_id)",
            ),
            (
                "idx_message_visibility_user_id",
                "CREATE INDEX IF NOT EXISTS idx_message_visibility_user_id ON message_visibility(user_id)",
            ),
            (
                "idx_message_visibility_status",
                "CREATE INDEX IF NOT EXISTS idx_message_visibility_status ON message_visibility(visibility_status)",
            ),
            // message_reactions 表索引
            (
                "idx_message_reactions_message_id",
                "CREATE INDEX IF NOT EXISTS idx_message_reactions_message_id ON message_reactions(message_id)",
            ),
            (
                "idx_message_reactions_emoji",
                "CREATE INDEX IF NOT EXISTS idx_message_reactions_emoji ON message_reactions(emoji)",
            ),
            // pinned_messages 表索引
            (
                "idx_pinned_messages_message_id",
                "CREATE INDEX IF NOT EXISTS idx_pinned_messages_message_id ON pinned_messages(message_id)",
            ),
            (
                "idx_pinned_messages_conversation_id",
                "CREATE INDEX IF NOT EXISTS idx_pinned_messages_conversation_id ON pinned_messages(conversation_id)",
            ),
            (
                "idx_pinned_messages_pinned_at",
                "CREATE INDEX IF NOT EXISTS idx_pinned_messages_pinned_at ON pinned_messages(pinned_at DESC)",
            ),
            // 多租户优化索引
            (
                "idx_messages_tenant_conversation_id",
                "CREATE INDEX IF NOT EXISTS idx_messages_tenant_conversation_id ON messages(tenant_id, conversation_id)",
            ),
            (
                "idx_messages_tenant_timestamp",
                "CREATE INDEX IF NOT EXISTS idx_messages_tenant_timestamp ON messages(tenant_id, timestamp DESC)",
            ),
            (
                "idx_message_operation_history_tenant_message_id",
                "CREATE INDEX IF NOT EXISTS idx_message_operation_history_tenant_message_id ON message_operation_history(tenant_id, message_id)",
            ),
            (
                "idx_message_edit_history_tenant_message_id",
                "CREATE INDEX IF NOT EXISTS idx_message_edit_history_tenant_message_id ON message_edit_history(tenant_id, message_id)",
            ),
            (
                "idx_message_read_records_tenant_message_id",
                "CREATE INDEX IF NOT EXISTS idx_message_read_records_tenant_message_id ON message_read_records(tenant_id, message_id)",
            ),
            (
                "idx_message_visibility_tenant_message_id",
                "CREATE INDEX IF NOT EXISTS idx_message_visibility_tenant_message_id ON message_visibility(tenant_id, message_id)",
            ),
            (
                "idx_message_reactions_tenant_message_id",
                "CREATE INDEX IF NOT EXISTS idx_message_reactions_tenant_message_id ON message_reactions(tenant_id, message_id)",
            ),
            (
                "idx_pinned_messages_tenant_conversation_id",
                "CREATE INDEX IF NOT EXISTS idx_pinned_messages_tenant_conversation_id ON pinned_messages(tenant_id, conversation_id)",
            ),
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

    /// 从数据库行转换为 Message protobuf
    pub fn row_to_message(&self, row: &sqlx::postgres::PgRow) -> Result<Message> {
        let server_id: String = row.get("server_id");
        let conversation_id: String = row.get("conversation_id");
        let client_msg_id: Option<String> = row.get("client_msg_id");
        let sender_id: String = row.get("sender_id");
        let content: Option<Vec<u8>> = row.get("content");
        let timestamp: DateTime<Utc> = row.get("timestamp");
        let extra: Option<Value> = row.get("extra");
        let _created_at: Option<DateTime<Utc>> = row.get("created_at");
        let message_type: Option<String> = row.get("message_type");
        let content_type: Option<String> = row.get("content_type");
        let business_type: String = row.get("business_type");
        let status: String = row.get("status");
        let fsm_state_changed_at: Option<DateTime<Utc>> = row.get("fsm_state_changed_at");
        let is_burn_after_read: bool = row.get("is_burn_after_read");
        let burn_after_seconds: i32 = row.get("burn_after_seconds");
        let _seq: Option<i64> = row.get("seq");
        let _updated_at: Option<DateTime<Utc>> = row.get("updated_at");
        let _tenant_id: String = row.get("tenant_id");

        let content_proto = content.and_then(|bytes| flare_proto::decode_message_content(&bytes[..]).ok());

        // 解析 extra JSONB
        let mut extra_map = HashMap::new();
        if let Some(extra_value) = extra {
            if let Ok(extra_obj) = from_value::<HashMap<String, Value>>(extra_value) {
                for (k, v) in extra_obj {
                    extra_map.insert(k, v.to_string().trim_matches('"').to_string());
                }
            }
        }

        // 使用 helpers 模块中的函数解析 extra 字段
        let source = parse_message_source_from_extra(&extra_map);
        let tags = parse_tags_from_extra(&extra_map);
        let attributes = parse_attributes_from_extra(&extra_map);

        // Visibility, ReadBy, Operations now stored in separate tables
        let visibility_map = HashMap::new();
        let read_by_vec = Vec::new();

        // 使用 helpers 模块中的函数转换枚举类型
        let message_type_enum = string_to_message_type(message_type.as_deref());
        let content_type_enum = string_to_content_type(content_type.as_deref());
        // 与 init.sql Message FSM 一致：INIT, SENT, EDITED, RECALLED, DELETED_HARD（大小写不敏感）
        let status_enum = match status.to_uppercase().as_str() {
            "INIT" | "CREATED" => flare_proto::common::MessageStatus::Created as i32,
            "SENT" => flare_proto::common::MessageStatus::Sent as i32,
            "EDITED" => flare_proto::common::MessageStatus::Sent as i32, // 编辑后仍对客户端表现为可读
            "DELIVERED" => flare_proto::common::MessageStatus::Delivered as i32,
            "READ" => flare_proto::common::MessageStatus::Read as i32,
            "FAILED" => flare_proto::common::MessageStatus::Failed as i32,
            "RECALLED" => flare_proto::common::MessageStatus::Recalled as i32,
            "DELETED_HARD" => flare_proto::common::MessageStatus::Failed as i32, // 硬删除对客户端不可见
            _ => flare_proto::common::MessageStatus::Unspecified as i32,
        };

        let is_recalled = status_enum == flare_proto::common::MessageStatus::Recalled as i32;
        let recalled_at = if is_recalled {
            fsm_state_changed_at.map(|dt| datetime_to_timestamp(dt))
        } else {
            None
        };

        // 构建 Message
        Ok(Message {
            server_id,
            conversation_id,
            client_msg_id: client_msg_id.unwrap_or_default(),
            sender_id,
            receiver_id: String::new(), // 从数据库读取：receiver_id 可能为空（旧数据）
            channel_id: String::new(),  // 从数据库读取：channel_id 可能为空（旧数据）
            content: content_proto,
            timestamp: Some(datetime_to_timestamp(timestamp)),
            extra: extra_map,
            source,
            message_type: message_type_enum,
            content_type: content_type_enum,
            business_type,
            status: status_enum,
            is_recalled,
            recalled_at,
            is_burn_after_read,
            burn_after_seconds,
            visibility: visibility_map,
            read_by: read_by_vec,
            tags,
            attributes,
            ..Default::default()
        })
    }
}