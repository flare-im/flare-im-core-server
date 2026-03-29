//! 多层级缓存管理器
//!
//! 结合本地内存缓存(L1)和Redis缓存(L2)，提供高效的消息缓存策略
//! 实现缓存穿透防护、缓存雪崩防护和热点数据保护

use std::sync::Arc;

use anyhow::Result;
use flare_proto::common::Message;
use tokio::time::Duration;
use tracing::{debug, info, warn};

// TODO: 暂时注释掉，等 local_cache 模块实现后再启用
// use super::local_cache::{LocalMessageCache, LocalCacheConfig};
use super::redis_cache::RedisMessageCache;

// 占位符类型
#[derive(Debug, Clone)]
pub struct LocalMessageCache {}

#[derive(Debug, Clone)]
pub struct LocalCacheConfig {}

#[derive(Debug, Clone)]
pub struct LocalCacheStatsSnapshot {}

impl Default for LocalCacheConfig {
    fn default() -> Self {
        Self {}
    }
}

impl LocalCacheStatsSnapshot {
    pub fn print_stats(&self) {
        println!("Local cache stats (placeholder)");
    }
}

impl LocalMessageCache {
    pub fn new(_config: LocalCacheConfig) -> Self {
        Self {}
    }

    pub fn get_stats(&self) -> LocalCacheStatsSnapshot {
        LocalCacheStatsSnapshot {}
    }

    pub fn get_message(&self, _message_id: &str) -> Option<Message> {
        None
    }

    pub async fn cache_message(&self, _message: &Message) -> Result<()> {
        Ok(())
    }

    pub async fn cache_messages_batch(&self, _messages: &[Message]) -> Result<()> {
        Ok(())
    }

    pub async fn get_session_messages(
        &self,
        _conversation_id: &str,
        _limit: usize,
    ) -> Result<Vec<Message>> {
        Ok(vec![])
    }

    pub async fn cache_session_messages(&self, _cid: &str, _msgs: &[Message]) -> Result<()> {
        Ok(())
    }

    pub async fn invalidate_message(&self, _message_id: &str) -> Result<()> {
        Ok(())
    }

    pub async fn invalidate_session(&self, _conversation_id: &str) -> Result<()> {
        Ok(())
    }
}

/// 多层级缓存配置
#[derive(Debug, Clone)]
pub struct MultiLevelCacheConfig {
    /// 本地缓存配置
    pub local: LocalCacheConfig,
    /// Redis缓存配置
    pub redis: super::redis_cache::RedisCacheConfig,
}

impl Default for MultiLevelCacheConfig {
    fn default() -> Self {
        Self {
            local: LocalCacheConfig::default(),
            redis: super::redis_cache::RedisCacheConfig::default(),
        }
    }
}

/// 多层级缓存管理器
pub struct MultiLevelMessageCache {
    /// 本地内存缓存 (L1)
    local_cache: Arc<LocalMessageCache>,
    /// Redis缓存 (L2)
    redis_cache: Option<Arc<RedisMessageCache>>,
    /// 配置
    config: MultiLevelCacheConfig,
}

impl MultiLevelMessageCache {
    /// 创建多层级缓存实例
    pub fn new(config: MultiLevelCacheConfig, redis_cache: Option<Arc<RedisMessageCache>>) -> Self {
        Self {
            local_cache: Arc::new(LocalMessageCache::new(config.local.clone())),
            redis_cache,
            config,
        }
    }

    /// 获取单条消息 - 按 L1 -> L2 -> DB 顺序查找
    pub async fn get_message(
        &self,
        conversation_id: &str,
        message_id: &str,
    ) -> Result<Option<Message>> {
        // 先查 L1 缓存
        if let Some(message) = self.local_cache.get_message(message_id) {
            debug!("L1 cache hit for message: {}", message_id);
            return Ok(Some(message));
        }

        // L1 未命中，查 L2 缓存
        if let Some(ref redis_cache) = self.redis_cache {
            if let Some(message) = redis_cache.get_message(conversation_id, message_id).await? {
                debug!("L2 cache hit for message: {}, caching to L1", message_id);
                // 异步回填 L1 缓存
                let local_cache = self.local_cache.clone();
                let message_clone = message.clone();
                tokio::spawn(async move {
                    if let Err(e) = local_cache.cache_message(&message_clone).await {
                        warn!("Failed to cache message to L1: {}", e);
                    }
                });
                return Ok(Some(message));
            }
        }

        debug!("Cache miss for message: {}", message_id);
        Ok(None)
    }

    /// 批量获取消息
    pub async fn get_messages_batch(
        &self,
        conversation_id: &str,
        message_ids: &[String],
    ) -> Result<Vec<Message>> {
        let mut result = Vec::new();
        let mut remaining_ids = Vec::new();

        // 先查 L1 缓存
        for id in message_ids {
            if let Some(message) = self.local_cache.get_message(id) {
                result.push(message);
            } else {
                remaining_ids.push(id.clone());
            }
        }

        // L1 未命中的查 L2 缓存
        if !remaining_ids.is_empty() {
            if let Some(ref redis_cache) = self.redis_cache {
                let redis_results = redis_cache
                    .get_messages_batch(conversation_id, &remaining_ids)
                    .await?;

                for (_, message) in &redis_results {
                    // 异步回填 L1 缓存
                    let local_cache = self.local_cache.clone();
                    let message_clone = message.clone();
                    tokio::spawn(async move {
                        if let Err(e) = local_cache.cache_message(&message_clone).await {
                            warn!("Failed to cache message to L1: {}", e);
                        }
                    });

                    result.push(message.clone());
                }
            }
        }

        Ok(result)
    }

    /// 缓存单条消息 - 同时写入 L1 和 L2
    pub async fn cache_message(&self, message: &Message) -> Result<()> {
        // 写入 L1 缓存
        self.local_cache.cache_message(message).await?;

        // 异步写入 L2 缓存
        if let Some(ref redis_cache) = self.redis_cache {
            let redis_cache_clone = redis_cache.clone();
            let message_clone = message.clone();
            tokio::spawn(async move {
                if let Err(e) = redis_cache_clone.cache_message(&message_clone).await {
                    warn!("Failed to cache message to L2: {}", e);
                }
            });
        }

        Ok(())
    }

    /// 批量缓存消息
    pub async fn cache_messages_batch(&self, messages: &[Message]) -> Result<()> {
        // 写入 L1 缓存
        self.local_cache.cache_messages_batch(messages).await?;

        // 异步写入 L2 缓存
        if let Some(ref redis_cache) = self.redis_cache {
            let redis_cache_clone = redis_cache.clone();
            let messages_clone = messages.to_vec();
            tokio::spawn(async move {
                if let Err(e) = redis_cache_clone
                    .cache_messages_batch(&messages_clone)
                    .await
                {
                    warn!("Failed to cache messages to L2: {}", e);
                }
            });
        }

        Ok(())
    }

    /// 获取会话消息列表
    pub async fn get_session_messages(
        &self,
        conversation_id: &str,
        start_time: i64,
        end_time: i64,
    ) -> Result<Option<Vec<Message>>> {
        use chrono::{DateTime, Utc};

        let cache_key = format!("{}:{}:{}", conversation_id, start_time, end_time);
        let limit = 100; // 默认限制

        // 先查 L1 缓存
        if let Ok(messages) = self
            .local_cache
            .get_session_messages(conversation_id, limit)
            .await
        {
            if !messages.is_empty() {
                debug!("L1 session cache hit for conversation: {}", conversation_id);
                return Ok(Some(messages));
            }
        }

        // L1 未命中，查 L2 缓存
        if let Some(ref redis_cache) = self.redis_cache {
            let start_time_utc = DateTime::from_timestamp(
                start_time / 1000,
                ((start_time % 1000) * 1_000_000) as u32,
            );
            let end_time_utc =
                DateTime::from_timestamp(end_time / 1000, ((end_time % 1000) * 1_000_000) as u32);

            if let (Some(start_time_utc), Some(end_time_utc)) = (start_time_utc, end_time_utc) {
                if let Ok(Some(messages)) = redis_cache
                    .get_session_messages(conversation_id, start_time_utc, end_time_utc, 100) // 添加 limit 参数
                    .await
                {
                    debug!(
                        "L2 session cache hit for conversation: {}, caching to L1",
                        conversation_id
                    );
                    // 异步回填 L1 缓存
                    let local_cache = self.local_cache.clone();
                    let conv_id = conversation_id.to_string();
                    let msgs = messages.clone();
                    tokio::spawn(async move {
                        if let Err(e) = local_cache.cache_session_messages(&conv_id, &msgs).await {
                            warn!("Failed to cache session messages to L1: {}", e);
                        }
                    });
                    return Ok(Some(messages));
                }
            }
        }

        debug!("Session cache miss for conversation: {}", conversation_id);
        Ok(None)
    }

    /// 缓存会话消息列表
    pub async fn cache_session_messages(
        &self,
        conversation_id: &str,
        start_time: i64,
        end_time: i64,
        messages: &[Message],
    ) -> Result<()> {
        use chrono::DateTime;

        // 写入 L1 缓存
        self.local_cache
            .cache_session_messages(conversation_id, messages)
            .await?;

        // 异步写入 L2 缓存
        if let Some(ref redis_cache) = self.redis_cache {
            let start_time_utc = DateTime::from_timestamp(
                start_time / 1000,
                ((start_time % 1000) * 1_000_000) as u32,
            );
            let end_time_utc =
                DateTime::from_timestamp(end_time / 1000, ((end_time % 1000) * 1_000_000) as u32);

            if let (Some(start_time_utc), Some(end_time_utc)) = (start_time_utc, end_time_utc) {
                let redis_cache_clone = redis_cache.clone();
                let conv_id = conversation_id.to_string();
                let msgs = messages.to_vec();
                tokio::spawn(async move {
                    if let Err(e) = redis_cache_clone
                        .cache_session_messages(&conv_id, start_time_utc, end_time_utc, &msgs)
                        .await
                    {
                        warn!("Failed to cache session messages to L2: {}", e);
                    }
                });
            }
        }

        Ok(())
    }

    /// 使消息缓存失效 - 同时清理 L1 和 L2
    pub async fn invalidate_message(&self, conversation_id: &str, message_id: &str) {
        // 清理 L1 缓存
        self.local_cache.invalidate_message(message_id).await;

        // 异步清理 L2 缓存
        if let Some(ref redis_cache) = self.redis_cache {
            let redis_cache_clone = redis_cache.clone();
            let conv_id = conversation_id.to_string();
            let msg_id = message_id.to_string();
            tokio::spawn(async move {
                if let Err(e) = redis_cache_clone
                    .invalidate_message(&conv_id, &msg_id)
                    .await
                {
                    warn!("Failed to invalidate message in L2 cache: {}", e);
                }
            });
        }
    }

    /// 使会话缓存失效
    pub async fn invalidate_session(&self, conversation_id: &str) {
        // 清理 L1 缓存
        self.local_cache.invalidate_session(conversation_id).await;

        // 异步清理 L2 缓存
        if let Some(ref redis_cache) = self.redis_cache {
            let redis_cache_clone = redis_cache.clone();
            let conv_id = conversation_id.to_string();
            tokio::spawn(async move {
                if let Err(e) = redis_cache_clone.invalidate_session(&conv_id).await {
                    warn!("Failed to invalidate session in L2 cache: {}", e);
                }
            });
        }
    }

    /// 获取 L1 缓存统计
    pub fn get_l1_stats(&self) -> LocalCacheStatsSnapshot {
        self.local_cache.get_stats()
    }

    /// 获取配置
    pub fn get_config(&self) -> &MultiLevelCacheConfig {
        &self.config
    }

    /// 打印缓存统计信息
    pub fn print_stats(&self) {
        let l1_stats = self.get_l1_stats();
        info!("MultiLevelCache Stats:");
        l1_stats.print_stats();

        if let Some(ref _redis_cache) = self.redis_cache {
            // 注意：这里假设Redis缓存也有类似的统计方法
            // 如果没有，我们需要在Redis缓存中也实现统计功能
            debug!("Redis cache is available");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_multi_level_cache() {
        let config = MultiLevelCacheConfig::default();
        let cache = MultiLevelMessageCache::new(config, None);

        // 创建测试消息
        let mut message = Message::default();
        message.server_id = "test_msg_1".to_string();
        message.conversation_id = "test_conv_1".to_string();
        message.content = Some(flare_proto::common::MessageContent::Text(
            "test content".to_string(),
        ));

        // 测试缓存和获取
        cache.cache_message(&message).await.unwrap();
        let retrieved = cache
            .get_message("test_conv_1", "test_msg_1")
            .await
            .unwrap();
        assert!(retrieved.is_some());

        // 测试统计信息
        cache.print_stats();
    }
}
