use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use flare_im_core::utils::timestamp_to_datetime;
use flare_proto::common::Message;
use flare_proto::MessageContentExt;
use serde_json::to_value;
use sqlx::{Pool, Postgres, postgres::PgPoolOptions};

use crate::infrastructure::persistence::operation_store;

use crate::config::StorageWriterConfig;
use crate::domain::repository::ArchiveStoreRepository;

// 定义用于从数据库查询结果的行结构体
#[derive(sqlx::FromRow)]
struct MessageRow {
    server_id: String,
    conversation_id: String,
    client_msg_id: Option<String>,
    sender_id: String,
    receiver_id: Option<String>,
    channel_id: Option<String>,
    content: Option<Vec<u8>>,
    timestamp: chrono::DateTime<chrono::Utc>,
    extra: Option<serde_json::Value>,
    message_type: Option<String>,
    content_type: Option<String>,
    business_type: Option<String>,
    status: Option<String>,
    is_burn_after_read: Option<bool>,
    burn_after_seconds: Option<i32>,
    seq: Option<i64>,
    tenant_id: String,
    conversation_type: Option<String>,
}

pub struct PostgresMessageStore {
    pool: Pool<Postgres>,
    operation_store: operation_store::OperationStore,
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

        let operation_store = operation_store::OperationStore::new(pool.clone());

        let store = Self {
            pool,
            operation_store,
        };
        Ok(Some(store))
    }
}

impl PostgresMessageStore {
    /// 获取数据库连接池（用于创建 ConversationRepository）
    pub fn pool(&self) -> &Pool<Postgres> {
        &self.pool
    }

    /// 根据消息ID查询消息（内部辅助方法，不需要 tenant_id 作为条件，使用唯一索引）
    async fn get_message_by_id(&self, message_id: &str) -> Result<Option<flare_proto::common::Message>> {
        use serde_json::Value as JsonValue;
        use flare_proto::common::{MessageType, MessageStatus, MessageSource};

        // 查询消息（使用唯一索引 idx_messages_server_id_unique，不需要 tenant_id 作为条件）
        let row = sqlx::query_as::<_, MessageRow>(
            r#"
            SELECT 
                server_id, conversation_id, client_msg_id, sender_id, receiver_id, channel_id,
                content, timestamp, extra, message_type, content_type, business_type, status,
                is_burn_after_read, burn_after_seconds, seq, tenant_id, conversation_type
            FROM messages
            WHERE server_id = $1
            LIMIT 1
            "#,
        )
        .bind(message_id)
        .fetch_optional(&self.pool)
        .await?;

        let row = match row {
            Some(r) => r,
            None => return Ok(None),
        };

        // 解析 extra JSON
        let extra_value: JsonValue = row.extra.unwrap_or_else(|| serde_json::json!({}));
        let extra_map = extra_value.as_object()
            .cloned()
            .unwrap_or_else(|| serde_json::Map::new());

        // 解析 content 为 MessageContent
        let content = if let Some(content_bytes) = row.content {
            flare_proto::decode_message_content(content_bytes.as_slice())
                .ok()
                .map(|c| Some(c))
                .unwrap_or(None)
        } else {
            None
        };

        // 解析 message_type
        let message_type = match row.message_type.as_deref() {
            Some("TEXT") => MessageType::Text as i32,
            Some("IMAGE") => MessageType::Image as i32,
            Some("VIDEO") => MessageType::Video as i32,
            Some("AUDIO") => MessageType::Audio as i32,
            Some("FILE") => MessageType::File as i32,
            Some("LOCATION") => MessageType::Location as i32,
            Some("CARD") => MessageType::Card as i32,
            Some("NOTIFICATION") => MessageType::Notification as i32,
            Some("TYPING") => MessageType::Typing as i32,
            Some("SYSTEM_EVENT") => MessageType::SystemEvent as i32,
            Some("OPERATION") => MessageType::Operation as i32,
            _ => MessageType::Unspecified as i32,
        };

        // 解析 status
        let status = match row.status.as_deref() {
            Some("INIT") => MessageStatus::Created as i32,
            Some("SENT") => MessageStatus::Sent as i32,
            Some("EDITED") => MessageStatus::Sent as i32, // EDITED 状态映射到 Sent
            Some("RECALLED") => MessageStatus::Recalled as i32,
            Some("DELETED_HARD") => MessageStatus::DeletedHard as i32,
            Some("DELETED_SOFT") => MessageStatus::DeletedSoft as i32,
            _ => MessageStatus::Sent as i32,
        };

        // 解析 timestamp
        let timestamp = Some(prost_types::Timestamp {
            seconds: row.timestamp.timestamp(),
            nanos: row.timestamp.timestamp_subsec_nanos() as i32,
        });



        // 解析 conversation_type
        let conversation_type = match row.conversation_type.as_deref() {
            Some("single") => flare_proto::common::ConversationType::Single as i32,
            Some("group") => flare_proto::common::ConversationType::Group as i32,
            Some("channel") => flare_proto::common::ConversationType::Channel as i32,
            _ => flare_proto::common::ConversationType::Unspecified as i32,
        };

        // 解析 content_type
        let content_type = match row.content_type.as_deref() {
            Some("text/plain") => flare_proto::common::ContentType::PlainText as i32,
            Some("text/html") => flare_proto::common::ContentType::Html as i32,
            Some("text/markdown") => flare_proto::common::ContentType::Markdown as i32,
            Some("application/json") => flare_proto::common::ContentType::Json as i32,
            _ => flare_proto::common::ContentType::Unspecified as i32,
        };

        // 构建 Message 对象
        let message = flare_proto::common::Message {
            server_id: row.server_id,
            conversation_id: row.conversation_id,
            client_msg_id: row.client_msg_id.unwrap_or_default(),
            sender_id: row.sender_id,
            source: MessageSource::User as i32,
            seq: row.seq.unwrap_or(0) as u64,
            timestamp,
            conversation_type,
            message_type,
            business_type: row.business_type.unwrap_or_default(),
            receiver_id: row.receiver_id.unwrap_or_default(),
            channel_id: row.channel_id.unwrap_or_default(),
            content,
            content_type,
            attachments: vec![],
            extra: extra_map
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        v.as_str().unwrap_or("").to_string(),
                    )
                })
                .collect(),
            tenant: row.tenant_id.clone(), // 从数据库行中获取租户ID
            offline_push_info: None,
            tags: vec![],
            attributes: std::collections::HashMap::new(),
            status,
            is_recalled: row.status.as_deref() == Some("RECALLED"),
            recalled_at: if row.status.as_deref() == Some("RECALLED") {
                timestamp.clone()
            } else {
                None
            },
            recall_reason: String::new(), // 需要从 extra 或单独字段获取
            is_burn_after_read: row.is_burn_after_read.unwrap_or(false),
            burn_after_seconds: row.burn_after_seconds.unwrap_or(0),
            timeline: None, // 需要从数据库字段构建
            visibility: std::collections::HashMap::new(),
            read_by: vec![],
            reactions: vec![],
            edit_history: vec![],
            current_edit_version: 0,
            last_edited_at: None,
            audit: None,
            extensions: vec![],
            quote: None,
        };

        Ok(Some(message))
    }

    /// 将 MessageRow 转换为 Message 对象
    #[allow(dead_code)]
    fn row_to_message(&self, row: &MessageRow) -> Result<flare_proto::common::Message> {
        use serde_json::Value as JsonValue;
        use flare_proto::common::{MessageType, MessageStatus, MessageSource};
        
        // 解析 extra JSON
        let extra_value: JsonValue = row.extra.clone().unwrap_or_else(|| serde_json::json!({}));
        let extra_map = extra_value.as_object()
            .cloned()
            .unwrap_or_else(|| serde_json::Map::new());

        // 解析 content 为 MessageContent
        let content = if let Some(content_bytes) = &row.content {
            flare_proto::decode_message_content(content_bytes.as_slice())
                .ok()
                .map(|c| Some(c))
                .unwrap_or(None)
        } else {
            None
        };

        // 解析 message_type
        let message_type = match row.message_type.as_deref() {
            Some("TEXT") => MessageType::Text as i32,
            Some("IMAGE") => MessageType::Image as i32,
            Some("VIDEO") => MessageType::Video as i32,
            Some("AUDIO") => MessageType::Audio as i32,
            Some("FILE") => MessageType::File as i32,
            Some("LOCATION") => MessageType::Location as i32,
            Some("CARD") => MessageType::Card as i32,
            Some("NOTIFICATION") => MessageType::Notification as i32,
            Some("TYPING") => MessageType::Typing as i32,
            Some("SYSTEM_EVENT") => MessageType::SystemEvent as i32,
            Some("OPERATION") => MessageType::Operation as i32,
            _ => MessageType::Unspecified as i32,
        };

        // 解析 status
        let status = match row.status.as_deref() {
            Some("INIT") => MessageStatus::Created as i32,
            Some("SENT") => MessageStatus::Sent as i32,
            Some("EDITED") => MessageStatus::Sent as i32, // EDITED 状态映射到 Sent
            Some("RECALLED") => MessageStatus::Recalled as i32,
            Some("DELETED_HARD") => MessageStatus::DeletedHard as i32,
            Some("DELETED_SOFT") => MessageStatus::DeletedSoft as i32,
            _ => MessageStatus::Sent as i32,
        };

        // 解析 timestamp
        let timestamp = Some(prost_types::Timestamp {
            seconds: row.timestamp.timestamp(),
            nanos: row.timestamp.timestamp_subsec_nanos() as i32,
        });

        // 解析 conversation_type
        let conversation_type = match row.conversation_type.as_deref() {
            Some("single") => flare_proto::common::ConversationType::Single as i32,
            Some("group") => flare_proto::common::ConversationType::Group as i32,
            Some("channel") => flare_proto::common::ConversationType::Channel as i32,
            _ => flare_proto::common::ConversationType::Unspecified as i32,
        };

        // 解析 content_type
        let content_type = match row.content_type.as_deref() {
            Some("text/plain") => flare_proto::common::ContentType::PlainText as i32,
            Some("text/html") => flare_proto::common::ContentType::Html as i32,
            Some("text/markdown") => flare_proto::common::ContentType::Markdown as i32,
            Some("application/json") => flare_proto::common::ContentType::Json as i32,
            _ => flare_proto::common::ContentType::Unspecified as i32,
        };

        // 构建 Message 对象
        let message = flare_proto::common::Message {
            server_id: row.server_id.clone(),
            conversation_id: row.conversation_id.clone(),
            client_msg_id: row.client_msg_id.clone().unwrap_or_default(),
            sender_id: row.sender_id.clone(),
            source: MessageSource::User as i32,
            seq: row.seq.unwrap_or(0) as u64,
            timestamp,
            conversation_type,
            message_type,
            business_type: row.business_type.clone().unwrap_or_default(),
            receiver_id: row.receiver_id.clone().unwrap_or_default(),
            channel_id: row.channel_id.clone().unwrap_or_default(),
            content,
            content_type,
            attachments: vec![],
            extra: extra_map
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        v.as_str().unwrap_or("").to_string(),
                    )
                })
                .collect(),
            tenant: row.tenant_id.clone(), // 从数据库行中获取租户ID
            offline_push_info: None,
            tags: vec![],
            attributes: std::collections::HashMap::new(),
            status,
            is_recalled: row.status.as_deref() == Some("RECALLED"),
            recalled_at: if row.status.as_deref() == Some("RECALLED") {
                timestamp.clone()
            } else {
                None
            },
            recall_reason: String::new(), // 需要从 extra 或单独字段获取
            is_burn_after_read: row.is_burn_after_read.unwrap_or(false),
            burn_after_seconds: row.burn_after_seconds.unwrap_or(0),
            timeline: None, // 需要从数据库字段构建
            visibility: std::collections::HashMap::new(),
            read_by: vec![],
            reactions: vec![],
            edit_history: vec![],
            current_edit_version: 0,
            last_edited_at: None,
            audit: None,
            extensions: vec![],
            quote: None,
        };

        Ok(message)
    }
}

#[async_trait]
impl ArchiveStoreRepository for PostgresMessageStore {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn store_archive(&self, message: &Message) -> Result<()> {
        use crate::infrastructure::persistence::helpers::*;

        let timestamp = get_message_timestamp(message);
        let content_type = infer_content_type(message);
        let content_bytes = encode_message_content(message);
        let extra_value = build_extra_value(message)?;
        let message_type_str = message_type_to_string(message.message_type);
        let seq = extract_seq_from_extra(&extra_value);

        // 提取 tenant_id（从 message.tenant 或 extra 中提取）
        use serde_json::Value as JsonValue;
        let tenant_id = extra_value.get("tenant_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "default".to_string());

        // 提取 conversation_type（从 message 或 extra 中）
        let conversation_type_str = if message.conversation_type != 0 {
            match message.conversation_type {
                1 => Some("single"),
                2 => Some("group"),
                3 => Some("channel"),
                _ => None,
            }
        } else if let Some(JsonValue::String(ct)) = extra_value.get("conversation_type") {
            match ct.as_str() {
                "single" => Some("single"),
                "group" => Some("group"),
                "channel" => Some("channel"),
                _ => None,
            }
        } else {
            None
        };

        sqlx::query(
            r#"
            INSERT INTO messages (
                server_id, conversation_id, client_msg_id, sender_id, receiver_id, channel_id,
                content, timestamp, created_at, updated_at, message_type, content_type, business_type,
                source, status, is_burn_after_read, burn_after_seconds, seq, conversation_type,
                tenant_id, extra, attributes, tags
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)
            ON CONFLICT (timestamp, server_id) DO UPDATE
            SET conversation_id = EXCLUDED.conversation_id,
                client_msg_id = EXCLUDED.client_msg_id,
                sender_id = EXCLUDED.sender_id,
                receiver_id = EXCLUDED.receiver_id,
                channel_id = EXCLUDED.channel_id,
                content = EXCLUDED.content,
                content_type = EXCLUDED.content_type,
                status = EXCLUDED.status,
                extra = EXCLUDED.extra,
                attributes = EXCLUDED.attributes,
                tags = EXCLUDED.tags,
                business_type = EXCLUDED.business_type,
                message_type = EXCLUDED.message_type,
                seq = EXCLUDED.seq,
                conversation_type = EXCLUDED.conversation_type,
                tenant_id = EXCLUDED.tenant_id,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(&message.server_id)
        .bind(&message.conversation_id)
        .bind(if message.client_msg_id.is_empty() { None::<String> } else { Some(message.client_msg_id.clone()) })
        .bind(&message.sender_id)
        .bind(if message.receiver_id.is_empty() { None::<String> } else { Some(message.receiver_id.clone()) })
        .bind(if message.channel_id.is_empty() { None::<String> } else { Some(message.channel_id.clone()) })
        .bind(content_bytes)
        .bind(timestamp)
        .bind(timestamp) // created_at 使用 timestamp
        .bind(timestamp) // updated_at 使用 timestamp
        .bind(message_type_str.as_deref())
        .bind(content_type)
        .bind(&message.business_type)
        .bind(match message.source {
            1 => "user",
            2 => "system",
            3 => "bot",
            4 => "admin",
            _ => "user",
        })
        .bind(crate::infrastructure::persistence::helpers::message_status_to_string(message.status))
        .bind(message.is_burn_after_read)
        .bind(message.burn_after_seconds)
        .bind(seq)
        .bind(conversation_type_str)
        .bind(&tenant_id)
        .bind(to_value(&extra_value)?)
        .bind(to_value(&message.attributes)?)
        .bind(&message.tags)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn update_message_fsm_state(
        &self,
        message_id: &str,
        fsm_state: &str,
        recall_reason: Option<&str>,
    ) -> Result<()> {
        let tenant_id = "default".to_string(); // 使用默认租户ID，因为Message结构体已移除tenant字段
        
        self.operation_store
            .update_message_fsm_state(&tenant_id, message_id, fsm_state, recall_reason)
            .await
    }

    async fn update_message_content(
        &self,
        message_id: &str,
        new_content: &flare_proto::common::MessageContent,
        edit_version: i32,
        editor_id: &str,
        reason: Option<&str>,
    ) -> Result<()> {
        // 从消息中查询 tenant_id（查询时需要，UPDATE 时不需要作为条件）
        let tenant_id = self
            .get_message_by_id(message_id)
            .await?
            .and_then(|msg| {
                msg.extra.get("tenant_id").cloned()
            })
            .unwrap_or_else(|| "default".to_string());
        
        self.operation_store
            .update_message_content(&tenant_id, message_id, new_content, edit_version, editor_id, reason)
            .await
    }

    async fn update_message_visibility(
        &self,
        message_id: &str,
        user_id: &str,
        visibility_status: &str,
    ) -> Result<()> {
        // 从消息中查询 tenant_id（查询时需要，UPDATE 时不需要作为条件）
        let tenant_id = self
            .get_message_by_id(message_id)
            .await?
            .and_then(|msg| {
                msg.extra.get("tenant_id").cloned()
            })
            .unwrap_or_else(|| "default".to_string());
        
        self.operation_store
            .update_message_visibility(&tenant_id, message_id, user_id, visibility_status)
            .await
    }

    async fn record_message_read(
        &self,
        message_id: &str,
        user_id: &str,
    ) -> Result<()> {
        // 从消息中查询 tenant_id（INSERT 时需要）
        let tenant_id = self
            .get_message_by_id(message_id)
            .await?
            .and_then(|msg| {
                msg.extra.get("tenant_id").cloned()
            })
            .unwrap_or_else(|| "default".to_string());
        
        self.operation_store
            .record_message_read(&tenant_id, message_id, user_id)
            .await
    }

    async fn upsert_message_reaction(
        &self,
        message_id: &str,
        emoji: &str,
        user_id: &str,
        add: bool,
    ) -> Result<()> {
        // 从消息中查询 tenant_id（INSERT 时需要）
        let tenant_id = self
            .get_message_by_id(message_id)
            .await?
            .and_then(|msg| {
                msg.extra.get("tenant_id").cloned()
            })
            .unwrap_or_else(|| "default".to_string());
        
        self.operation_store
            .upsert_message_reaction(&tenant_id, message_id, emoji, user_id, add)
            .await
    }

    async fn pin_message(
        &self,
        message_id: &str,
        conversation_id: &str,
        user_id: &str,
        pin: bool,
        expire_at: Option<chrono::DateTime<chrono::Utc>>,
        reason: Option<&str>,
    ) -> Result<()> {
        // 从消息中查询 tenant_id（INSERT 时需要）
        let tenant_id = self
            .get_message_by_id(message_id)
            .await?
            .and_then(|msg| {
                msg.extra.get("tenant_id").cloned()
            })
            .unwrap_or_else(|| "default".to_string());
        
        self.operation_store
            .pin_message(&tenant_id, message_id, conversation_id, user_id, pin, expire_at, reason)
            .await
    }

    async fn mark_message(
        &self,
        message_id: &str,
        conversation_id: &str,
        user_id: &str,
        mark_type: &str,
        color: Option<&str>,
        add: bool,
    ) -> Result<()> {
        // 从消息中查询 tenant_id（INSERT 时需要）
        let tenant_id = self
            .get_message_by_id(message_id)
            .await?
            .and_then(|msg| {
                msg.extra.get("tenant_id").cloned()
            })
            .unwrap_or_else(|| "default".to_string());
        
        self.operation_store
            .mark_message(&tenant_id, message_id, conversation_id, user_id, mark_type, color, add)
            .await
    }

    async fn append_operation(
        &self,
        message_id: &str,
        operation: &flare_proto::common::MessageOperation,
    ) -> Result<()> {
        // 从消息中查询 tenant_id（INSERT 时需要）
        let tenant_id = self
            .get_message_by_id(message_id)
            .await?
            .and_then(|msg| {
                msg.extra.get("tenant_id").cloned()
            })
            .unwrap_or_else(|| "default".to_string());
        
        self.operation_store
            .append_operation(&tenant_id, message_id, operation)
            .await
    }

    async fn get_message(&self, message_id: &str) -> Result<Option<Message>> {
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
    async fn store_archive_batch(&self, messages: &[Message]) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        // 小批量：逐个插入（简单且可靠）
        if messages.len() <= 10 {
            for message in messages {
                self.store_archive(message).await?;
            }
            return Ok(());
        }

        // 计算平均消息大小（用于自适应批量大小）
        let avg_message_size = messages
            .iter()
            .map(|m| {
                // 估算消息大小（content + extra + metadata）
                let content_size = m
                    .content
                    .as_ref()
                    .and_then(|c| c.encode_to_bytes().ok())
                    .map(|buf| buf.len())
                    .unwrap_or(0);
                let extra_size = serde_json::to_string(&m.extra).unwrap_or_default().len();
                content_size + extra_size + 200 // 基础元数据约 200 字节
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
            tracing::debug!(
                total_messages = messages.len(),
                optimal_batch_size = optimal_batch_size,
                chunks = chunks.len(),
                avg_message_size = avg_message_size,
                "Splitting batch into {} chunks for optimal performance",
                chunks.len()
            );

            for chunk in chunks {
                self.store_archive_batch_values(chunk).await?;
            }
            return Ok(());
        }

        // 中批量：使用 VALUES 多行插入（单事务，性能较好）
        self.store_archive_batch_values(messages).await
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
    async fn store_archive_batch_values(&self, messages: &[Message]) -> Result<()> {
        use sqlx::QueryBuilder;
        use std::time::Duration;

        // 预先处理所有消息，提取需要的数据（在重试循环外，避免重复计算）
        let prepared_data: Vec<_> = messages
            .iter()
            .map(|message| {
                let timestamp = message
                    .timestamp
                    .as_ref()
                    .and_then(|ts| timestamp_to_datetime(ts))
                    .unwrap_or_else(|| Utc::now());

                use crate::infrastructure::persistence::helpers::*;

                let extra_value = build_extra_value(message).unwrap_or_default();
                let content_type = infer_content_type(message);
                let content_bytes = encode_message_content(message);
                let message_type_str = message_type_to_string(message.message_type);
                let seq = extract_seq_from_extra(&extra_value);
                let status_str = message_status_to_string(message.status);

                (
                    message.server_id.clone(),
                    message.conversation_id.clone(),
                    if message.client_msg_id.is_empty() { None } else { Some(message.client_msg_id.clone()) },
                    message.sender_id.clone(),
                    content_bytes,
                    timestamp,
                    to_value(&extra_value).unwrap_or_default(),
                    message_type_str,
                    content_type.to_string(),
                    message.business_type.clone(),
                    status_str.to_string(),
                    message.is_burn_after_read,
                    message.burn_after_seconds,
                    seq,
                )
            })
            .collect();

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
                    return Err(last_error.unwrap());
                }
            };

            // 构建批量 INSERT 语句
            let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
                r#"
                INSERT INTO messages (
                    server_id, conversation_id, client_msg_id, sender_id, content, timestamp,
                    extra, created_at, message_type, content_type, business_type,
                    status, is_burn_after_read, burn_after_seconds,
                    seq, updated_at
                )
                "#,
            );

            query_builder.push_values(&prepared_data, |mut b, row| {
                b.push_bind(&row.0); // server_id
                b.push_bind(&row.1); // conversation_id
                b.push_bind(&row.2); // client_msg_id (Option<String>)
                b.push_bind(&row.3); // sender_id
                b.push_bind(&row.4); // content_bytes
                b.push_bind(row.5); // timestamp
                b.push_bind(&row.6); // extra
                b.push_bind(row.5); // created_at (same as timestamp)
                b.push_bind(row.7.as_deref()); // message_type_str
                b.push_bind(&row.8); // content_type
                b.push_bind(&row.9); // business_type
                b.push_bind(&row.10); // status_str
                b.push_bind(row.11); // is_burn_after_read
                b.push_bind(row.12); // burn_after_seconds
                b.push_bind(row.13); // seq
                b.push_bind(row.5); // updated_at (same as timestamp)
            });

            query_builder.push(
                r#"
                ON CONFLICT (timestamp, server_id) DO UPDATE
                SET conversation_id = EXCLUDED.conversation_id,
                    client_msg_id = EXCLUDED.client_msg_id,
                    sender_id = EXCLUDED.sender_id,
                    content = EXCLUDED.content,
                    content_type = EXCLUDED.content_type,
                    status = EXCLUDED.status,
                    extra = EXCLUDED.extra,
                    business_type = EXCLUDED.business_type,
                    message_type = EXCLUDED.message_type,
                    seq = EXCLUDED.seq,
                    updated_at = EXCLUDED.updated_at
                "#,
            );

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
                    last_error = Some(anyhow::Error::from(e));
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

impl PostgresMessageStore {
    /// 批量存储消息（内部方法，用于循环插入的旧实现，保留作为备用）
    #[allow(dead_code)]
    async fn store_archive_batch_legacy(&self, messages: &[Message]) -> Result<()> {
        // 大批量：使用事务批量插入（旧实现，保留作为备用）
        let mut tx = self.pool.begin().await?;

        for message in messages {
            let timestamp = message
                .timestamp
                .as_ref()
                .and_then(|ts| timestamp_to_datetime(ts))
                .unwrap_or_else(|| Utc::now());

            use crate::infrastructure::persistence::helpers::*;

            let extra_value = build_extra_value(message).unwrap_or_default();
            let content_type = infer_content_type(message);
            let content_bytes = encode_message_content(message);
            let message_type_str = message_type_to_string(message.message_type);
            let seq = extract_seq_from_extra(&extra_value);
            let status_str = message_status_to_string(message.status);

            sqlx::query(
                r#"
                INSERT INTO messages (
                    server_id, conversation_id, sender_id, content, timestamp,
                    extra, created_at, message_type, content_type, business_type,
                    status, is_burn_after_read, burn_after_seconds,
                    seq, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                ON CONFLICT (timestamp, server_id) DO UPDATE
                SET conversation_id = EXCLUDED.conversation_id,
                    sender_id = EXCLUDED.sender_id,
                    content = EXCLUDED.content,
                    content_type = EXCLUDED.content_type,
                    status = EXCLUDED.status,
                    extra = EXCLUDED.extra,
                    business_type = EXCLUDED.business_type,
                    message_type = EXCLUDED.message_type,
                    seq = EXCLUDED.seq,
                    updated_at = EXCLUDED.updated_at
                "#,
            )
            .bind(&message.server_id)
            .bind(&message.conversation_id)
            .bind(&message.sender_id)
            .bind(&content_bytes)
            .bind(timestamp)
            .bind(to_value(&extra_value)?)
            .bind(timestamp) // created_at
            .bind(message_type_str.as_deref())
            .bind(content_type)
            .bind(&message.business_type)
            .bind(status_str)
            .bind(message.is_burn_after_read)
            .bind(message.burn_after_seconds)
            .bind(seq)
            .bind(timestamp) // updated_at
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        tracing::info!(
            batch_size = messages.len(),
            "Successfully batch inserted {} messages into PostgreSQL",
            messages.len()
        );

        Ok(())
    }

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
    #[allow(dead_code)]
    async fn batch_query_messages(&self, message_ids: &[String]) -> Result<Vec<Message>> {
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
                .build_query_as::<MessageRow>()
                .fetch_all(&self.pool)
                .await?;

            let mut messages = Vec::with_capacity(rows.len());
            for row in rows {
                messages.push(self.row_to_message(&row)?);
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
            for (_i, id) in chunk.iter().enumerate() {
                separated.push_bind(id);
            }
            separated.push_unseparated(")");

            let rows = query_builder
                .build_query_as::<MessageRow>()
                .fetch_all(&self.pool)
                .await?;

            for row in rows {
                all_messages.push(self.row_to_message(&row)?);
            }
        }

        Ok(all_messages)
    }
}

