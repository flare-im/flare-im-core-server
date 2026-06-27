//! Redis 缓存层实现
//!
//! 提供消息查询缓存、会话状态缓存等功能
//! 实现 L2 缓存策略：Redis -> TimescaleDB

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{DateTime, Utc};
use flare_server_core::error::{AnyhowContext, Result};
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

    fn message_key(tenant_id: &str, conversation_id: &str, message_id: &str) -> String {
        format!("cache:msg:{tenant_id}:{conversation_id}:{message_id}")
    }

    fn session_query_key(
        tenant_id: &str,
        conversation_id: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> String {
        format!(
            "cache:session:{tenant_id}:{conversation_id}:query:{}:{}",
            start_time.timestamp(),
            end_time.timestamp()
        )
    }

    fn tail_key(tenant_id: &str, conversation_id: &str) -> String {
        format!("cache:tail:{tenant_id}:{conversation_id}")
    }

    /// 缓存单条消息
    pub async fn cache_message(&self, tenant_id: &str, message: &Message) -> Result<()> {
        let mut conn = self.get_connection().await?;

        let message_key =
            Self::message_key(tenant_id, &message.conversation_id, &message.server_id);

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
    pub async fn cache_messages_batch(&self, tenant_id: &str, messages: &[Message]) -> Result<()> {
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
            let message_key =
                Self::message_key(tenant_id, &message.conversation_id, &message.server_id);

            let mut buf = Vec::new();
            message.encode(&mut buf)?;
            let encoded = BASE64.encode(&buf);

            pipe.cmd("SET").arg(&message_key).arg(&encoded);
            if ttl > 0 {
                pipe.cmd("EXPIRE").arg(&message_key).arg(ttl);
            }
        }

        let _: Vec<redis::Value> = pipe.query_async(&mut conn).await?;

        tracing::trace!(
            batch_size = messages.len(),
            "Cached {} messages to Redis",
            messages.len()
        );

        Ok(())
    }

    /// 从缓存获取消息
    pub async fn get_message(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        message_id: &str,
    ) -> Result<Option<Message>> {
        let mut conn = self.get_connection().await?;

        let message_key = Self::message_key(tenant_id, conversation_id, message_id);

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
        tenant_id: &str,
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
            .map(|id| Self::message_key(tenant_id, conversation_id, id))
            .collect();

        // 使用 MGET 批量获取
        let encoded_list: Vec<Option<String>> = conn.get(keys).await?;

        let mut result = HashMap::new();
        for (i, encoded_opt) in encoded_list.into_iter().enumerate() {
            if let Some(encoded) = encoded_opt
                && let Ok(bytes) = BASE64.decode(&encoded)
                && let Ok(message) = Message::decode(&bytes[..])
            {
                result.insert(message_ids[i].clone(), message);
            }
        }

        Ok(result)
    }

    /// 从会话尾部热缓存按 seq 范围读取消息。
    pub async fn get_tail_messages_by_seq(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        after_seq: i64,
        before_seq: Option<i64>,
        limit: i32,
    ) -> Result<Option<Vec<Message>>> {
        let limit = limit.clamp(1, 1000);
        let tail_key = Self::tail_key(tenant_id, conversation_id);
        let min = format!("({after_seq}");
        let max = before_seq
            .map(|seq| format!("({seq}"))
            .unwrap_or_else(|| "+inf".to_string());

        let mut conn = self.get_connection().await?;
        let oldest_tail: Vec<(String, f64)> = redis::cmd("ZRANGE")
            .arg(&tail_key)
            .arg(0)
            .arg(0)
            .arg("WITHSCORES")
            .query_async(&mut conn)
            .await?;
        let Some((_, oldest_seq)) = oldest_tail.first() else {
            return Ok(None);
        };
        if after_seq < (*oldest_seq as i64).saturating_sub(1) {
            return Ok(None);
        }

        let message_ids: Vec<String> = redis::cmd("ZRANGEBYSCORE")
            .arg(&tail_key)
            .arg(min)
            .arg(max)
            .arg("LIMIT")
            .arg(0)
            .arg(limit)
            .query_async(&mut conn)
            .await?;

        if message_ids.is_empty() {
            return Ok(None);
        }

        let cached_messages = self
            .get_messages_batch(tenant_id, conversation_id, &message_ids)
            .await?;
        if cached_messages.len() != message_ids.len() {
            return Ok(None);
        }

        let mut messages = Vec::with_capacity(message_ids.len());
        for message_id in message_ids {
            let Some(message) = cached_messages.get(&message_id) else {
                return Ok(None);
            };
            messages.push(message.clone());
        }

        Ok(Some(messages))
    }

    /// 缓存会话消息列表（按时间范围）
    pub async fn cache_session_messages(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        messages: &[Message],
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        // 缓存消息本身
        self.cache_messages_batch(tenant_id, messages).await?;

        // 缓存查询结果索引（使用 Sorted Set，按 timestamp 排序）
        let index_key = Self::session_query_key(tenant_id, conversation_id, start_time, end_time);

        let mut conn = self.get_connection().await?;

        // 使用 Pipeline 批量添加索引
        let mut pipe = redis::pipe();
        pipe.atomic();

        for message in messages {
            if message.created_at > 0 {
                let score = message.created_at as f64 / 1000.0;
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
        tenant_id: &str,
        conversation_id: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        limit: i32,
    ) -> Result<Option<Vec<Message>>> {
        let index_key = Self::session_query_key(tenant_id, conversation_id, start_time, end_time);

        let mut conn = self.get_connection().await?;

        // 从 Sorted Set 获取消息 ID 列表
        let message_ids: Vec<String> = conn.zrange(&index_key, 0, (limit - 1) as isize).await?;

        if message_ids.is_empty() {
            return Ok(None);
        }

        // 批量获取消息
        let cached_messages = self
            .get_messages_batch(tenant_id, conversation_id, &message_ids)
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
    pub async fn invalidate_message(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        message_id: &str,
    ) -> Result<()> {
        let mut conn = self.get_connection().await?;

        let message_key = Self::message_key(tenant_id, conversation_id, message_id);
        let _: () = conn.del(&message_key).await?;

        Ok(())
    }

    /// 清除会话缓存
    pub async fn invalidate_session(&self, tenant_id: &str, conversation_id: &str) -> Result<()> {
        let mut conn = self.get_connection().await?;

        // 使用 KEYS 命令查找所有相关的 key（注意：KEYS 在生产环境可能阻塞，但用于缓存失效场景可接受）
        // 更好的方案是维护一个会话 key 的 SET，但为了简化实现，这里使用 KEYS
        let pattern = format!("cache:session:{tenant_id}:{conversation_id}:*");
        let keys: Vec<String> = conn.keys(&pattern).await?;

        if !keys.is_empty() {
            let _: () = conn.del(&keys).await?;
            tracing::trace!(
                conversation_id = %conversation_id,
                deleted_keys = keys.len(),
                "Invalidated session cache"
            );
        }

        // 同时清除消息缓存（使用消息 key 模式）
        let msg_pattern = format!("cache:msg:{tenant_id}:{conversation_id}:*");
        let msg_keys: Vec<String> = conn.keys(&msg_pattern).await?;

        if !msg_keys.is_empty() {
            let _: () = conn.del(&msg_keys).await?;
            tracing::trace!(
                conversation_id = %conversation_id,
                deleted_msg_keys = msg_keys.len(),
                "Invalidated message cache for session"
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tail_key_is_tenant_scoped() {
        assert_eq!(
            RedisMessageCache::tail_key("tenant-a", "conv-a"),
            "cache:tail:tenant-a:conv-a"
        );
    }
}
