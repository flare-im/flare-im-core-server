//! 清除会话历史后，失效 storage-reader 读取的 Redis 消息热缓存。
//!
//! storage-writer 落库后把消息写进 `cache:msg:*` 与会话尾部 ZSET `cache:tail:*`；
//! storage-reader 按 seq 同步时整段命中尾部就直接返回、不再查库。PG 里物理删掉的
//! 消息若还留在这两处，客户端同步会把已清除的旧历史原样拉回来（热缓存 TTL 默认 7 天）。
//! 键约定见 [`flare_im_contracts::message_cache_key`] / [`flare_im_contracts::message_tail_key`]。

use std::sync::Arc;

use async_trait::async_trait;
use flare_im_service_kit::config::FlareAppConfig;
use flare_server_core::error::{ErrorCode, Result, map_infra_error};

/// 一次 pipeline 最多带多少条消息（ZREM 参数 + DEL 键），避免超长命令。
const INVALIDATE_CHUNK: usize = 500;

/// 按消息 ID 失效热缓存的端口；PG 仓储在清除历史的事务里调用。
#[async_trait]
pub trait MessageCacheInvalidator: Send + Sync {
    /// 从会话尾部索引移除这些消息，并删除它们的单条缓存。重复调用幂等。
    async fn invalidate_messages(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        message_ids: &[String],
    ) -> Result<()>;
}

/// storage-reader 实际读取的那一个 Redis 上的实现。
pub struct RedisMessageCacheInvalidator {
    client: Arc<redis::Client>,
}

impl RedisMessageCacheInvalidator {
    pub fn new(client: Arc<redis::Client>) -> Self {
        Self { client }
    }

    /// 按 storage-reader 的同一规则解析它的缓存 Redis：`STORAGE_REDIS_URL` 优先，
    /// 其次 `services.storage_reader.redis` 引用的 profile；都没有表示 reader 不读缓存，
    /// 清除历史也就不需要失效任何东西。
    pub fn storage_reader_redis_url(app: &FlareAppConfig) -> Option<String> {
        std::env::var("STORAGE_REDIS_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())
            .or_else(|| {
                app.storage_reader_service()
                    .redis
                    .as_deref()
                    .and_then(|name| app.redis_profile(name))
                    .map(|profile| profile.url.clone())
            })
    }
}

fn cache_error(error: redis::RedisError) -> flare_server_core::error::FlareError {
    map_infra_error(
        error,
        ErrorCode::NetworkError,
        "Failed to invalidate storage message cache",
    )
}

#[async_trait]
impl MessageCacheInvalidator for RedisMessageCacheInvalidator {
    async fn invalidate_messages(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        message_ids: &[String],
    ) -> Result<()> {
        if message_ids.is_empty() {
            return Ok(());
        }
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(cache_error)?;
        let tail_key = flare_im_contracts::message_tail_key(tenant_id, conversation_id);
        for chunk in message_ids.chunks(INVALIDATE_CHUNK) {
            let mut pipe = redis::pipe();
            pipe.atomic();
            pipe.cmd("ZREM").arg(&tail_key).arg(chunk).ignore();
            let keys: Vec<String> = chunk
                .iter()
                .map(|id| flare_im_contracts::message_cache_key(tenant_id, conversation_id, id))
                .collect();
            pipe.cmd("DEL").arg(&keys).ignore();
            pipe.query_async::<()>(&mut conn)
                .await
                .map_err(cache_error)?;
        }
        Ok(())
    }
}
