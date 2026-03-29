//! Redis 缓存层实现
//!
//! 提供消息查询缓存、会话状态缓存等功能
//! 实现 L2 缓存策略：Redis -> TimescaleDB

use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{DateTime, Utc};
use prost::Message as ProstMessage;
use redis::{AsyncCommands, aio::ConnectionManager};
use std::collections::HashMap;
use std::sync::Arc;

use crate::config::StorageReaderConfig;
use flare_proto::common::Message;

/// Redis 缓存配置
#[derive(Debug, Clone)]
pub struct RedisCacheConfig {
    /// 消息缓存 TTL（秒）
    pub message_ttl_seconds: u64,
    /// 会话缓存 TTL（秒）
    pub session_ttl_seconds: u64,
}

impl Default for RedisCacheConfig {
    fn default() -> Self {
        Self {
            message_ttl_seconds: 3600, // 1小时
            session_ttl_seconds: 7200, // 2小时
        }
    }
}

/// Redis 消息缓存仓储
pub struct RedisMessageCache {
    client: Arc<redis::Client>,
    message_ttl_seconds: u64,
    session_ttl_seconds: u64,
}

impl RedisMessageCache {
    pub fn new(client: Arc<redis::Client>, config: &StorageReaderConfig) -> Self {
        Self {
            client,
            message_ttl_seconds: config.redis_message_cache_ttl_seconds,
            session_ttl_seconds: config.redis_session_cache_ttl_seconds,
        }
    }

    /// 使用 RedisCacheConfig 创建实例
    pub fn new_with_config(client: Arc<redis::Client>, cache_config: &RedisCacheConfig) -> Self {
        Self {
            client,
            message_ttl_seconds: cache_config.message_ttl_seconds,
            session_ttl_seconds: cache_config.session_ttl_seconds,
        }
    }

    /// 获取 Redis 连接
    async fn get_connection(&self) -> Result<ConnectionManager> {
        Ok(ConnectionManager::new(self.client.as_ref().clone()).await?)
    }

    /// 缓存单条消息
    pub async fn cache_message(&self, message: &Message) -> Result<()> {
        let mut conn = self.get_connection().await?;

        let message_key = format!(
            "cache:msg:{}:{}",
            message.conversation_id, message.server_id
        );

        // 编码消息为 protobuf bytes，然后 base64 编码
        let mut buf = Vec::new();
        message.encode(&mut buf)?;
        let encoded = BASE64.encode(&buf);

        let _: () = conn.set(&message_key, encoded).await?;

        if self.message_ttl_seconds > 0 {
            let ttl: i64 = self.message_ttl_seconds.try_into()?;
            let _: () = conn.expire(&message_key, ttl).await?;
        }

        Ok(())
    }

    /// 批量缓存消息
    pub async fn cache_messages_batch(&self, messages: &[Message]) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let mut conn = self.get_connection().await?;

        // 使用 Redis Pipeline 批量执行
        let mut pipe = redis::pipe();
        pipe.atomic();

        let ttl: i64 = if self.message_ttl_seconds > 0 {
            self.message_ttl_seconds.try_into()?
        } else {
            0
        };

        for message in messages {
            let message_key = format!(
                "cache:msg:{}:{}",
                message.conversation_id, message.server_id
            );

            let mut buf = Vec::new();
            message.encode(&mut buf)?;
            let encoded = BASE64.encode(&buf);

            pipe.cmd("SET").arg(&message_key).arg(&encoded);
            if ttl > 0 {
                pipe.cmd("EXPIRE").arg(&message_key).arg(ttl);
            }
        }

        let _: Vec<redis::Value> = pipe.query_async(&mut conn).await?;

        tracing::debug!(
            batch_size = messages.len(),
            "Cached {} messages to Redis",
            messages.len()
        );

        Ok(())
    }

    /// 从缓存获取消息
    pub async fn get_message(
        &self,
        conversation_id: &str,
        message_id: &str,
    ) -> Result<Option<Message>> {
        let mut conn = self.get_connection().await?;

        let message_key = format!("cache:msg:{}:{}", conversation_id, message_id);

        let encoded: Option<String> = conn.get(&message_key).await?;

        match encoded {
            Some(encoded) => {
                // 解码 base64，然后反序列化为 Message
                let bytes = BASE64
                    .decode(&encoded)
                    .context("Failed to decode base64 message")?;
                let message =
                    Message::decode(&bytes[..]).context("Failed to decode protobuf message")?;
                Ok(Some(message))
            }
            None => Ok(None),
        }
    }

    /// 批量从缓存获取消息
    pub async fn get_messages_batch(
        &self,
        conversation_id: &str,
        message_ids: &[String],
    ) -> Result<HashMap<String, Message>> {
        if message_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let mut conn = self.get_connection().await?;

        // 构建所有 key
        let keys: Vec<String> = message_ids
            .iter()
            .map(|id| format!("cache:msg:{}:{}", conversation_id, id))
            .collect();

        // 使用 MGET 批量获取
        let encoded_list: Vec<Option<String>> = conn.get(keys).await?;

        let mut result = HashMap::new();
        for (i, encoded_opt) in encoded_list.into_iter().enumerate() {
            if let Some(encoded) = encoded_opt {
                if let Ok(bytes) = BASE64.decode(&encoded) {
                    if let Ok(message) = Message::decode(&bytes[..]) {
                        result.insert(message_ids[i].clone(), message);
                    }
                }
            }
        }

        Ok(result)
    }

    /// 缓存会话消息列表（按时间范围）
    pub async fn cache_session_messages(
        &self,
        conversation_id: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        messages: &[Message],
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        // 缓存消息本身
        self.cache_messages_batch(messages).await?;

        // 缓存查询结果索引（使用 Sorted Set，按 timestamp 排序）
        let index_key = format!(
            "cache:session:{}:query:{}:{}",
            conversation_id,
            start_time.timestamp(),
            end_time.timestamp()
        );

        let mut conn = self.get_connection().await?;

        // 使用 Pipeline 批量添加索引
        let mut pipe = redis::pipe();
        pipe.atomic();

        for message in messages {
            if let Some(ts) = &message.timestamp {
                let score = ts.seconds as f64 + (ts.nanos as f64 / 1_000_000_000.0);
                pipe.cmd("ZADD")
                    .arg(&index_key)
                    .arg(score)
                    .arg(&message.server_id);
            }
        }

        if self.session_ttl_seconds > 0 {
            let ttl: i64 = self.session_ttl_seconds.try_into()?;
            pipe.cmd("EXPIRE").arg(&index_key).arg(ttl);
        }

        let _: Vec<redis::Value> = pipe.query_async(&mut conn).await?;

        Ok(())
    }

    /// 从缓存获取会话消息列表
    pub async fn get_session_messages(
        &self,
        conversation_id: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        limit: i32,
    ) -> Result<Option<Vec<Message>>> {
        let index_key = format!(
            "cache:session:{}:query:{}:{}",
            conversation_id,
            start_time.timestamp(),
            end_time.timestamp()
        );

        let mut conn = self.get_connection().await?;

        // 从 Sorted Set 获取消息 ID 列表
        let message_ids: Vec<String> = conn.zrange(&index_key, 0, (limit - 1) as isize).await?;

        if message_ids.is_empty() {
            return Ok(None);
        }

        // 批量获取消息
        let cached_messages = self
            .get_messages_batch(conversation_id, &message_ids)
            .await?;

        // 按 message_ids 顺序返回
        let mut messages = Vec::new();
        for id in message_ids {
            if let Some(message) = cached_messages.get(&id) {
                messages.push(message.clone());
            }
        }

        Ok(Some(messages))
    }

    /// 清除消息缓存
    pub async fn invalidate_message(&self, conversation_id: &str, message_id: &str) -> Result<()> {
        let mut conn = self.get_connection().await?;

        let message_key = format!("cache:msg:{}:{}", conversation_id, message_id);
        let _: () = conn.del(&message_key).await?;

        Ok(())
    }

    /// 清除会话缓存
    pub async fn invalidate_session(&self, conversation_id: &str) -> Result<()> {
        let mut conn = self.get_connection().await?;

        // 使用 KEYS 命令查找所有相关的 key（注意：KEYS 在生产环境可能阻塞，但用于缓存失效场景可接受）
        // 更好的方案是维护一个会话 key 的 SET，但为了简化实现，这里使用 KEYS
        let pattern = format!("cache:session:{}:*", conversation_id);
        let keys: Vec<String> = conn.keys(&pattern).await?;

        if !keys.is_empty() {
            let _: () = conn.del(&keys).await?;
            tracing::debug!(
                conversation_id = %conversation_id,
                deleted_keys = keys.len(),
                "Invalidated session cache"
            );
        }

        // 同时清除消息缓存（使用消息 key 模式）
        let msg_pattern = format!("cache:msg:{}:*", conversation_id);
        let msg_keys: Vec<String> = conn.keys(&msg_pattern).await?;

        if !msg_keys.is_empty() {
            let _: () = conn.del(&msg_keys).await?;
            tracing::debug!(
                conversation_id = %conversation_id,
                deleted_msg_keys = msg_keys.len(),
                "Invalidated message cache for session"
            );
        }

        Ok(())
    }
}
