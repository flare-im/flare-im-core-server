use std::sync::Arc;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use flare_im_contracts::Ctx;
use flare_server_core::error::Result;
use prost::Message as _;
use redis::{AsyncCommands, aio::ConnectionManager};
use std::convert::TryInto;
use tracing::instrument;

use crate::config::StorageWriterConfig;
use crate::domain::repository::HotCacheRepository;

const DEFAULT_TENANT_ID: &str = "0";

pub struct RedisHotCacheRepository {
    client: Arc<redis::Client>,
    ttl_seconds: u64,
    tail_limit: usize,
    // 注意：redis-rs 的 ConnectionManager 内部已实现连接池，无需手动管理
}

impl RedisHotCacheRepository {
    pub fn new(client: Arc<redis::Client>, config: &StorageWriterConfig) -> Self {
        Self {
            client,
            ttl_seconds: config.redis_hot_ttl_seconds,
            tail_limit: config.redis_hot_tail_limit.max(1),
        }
    }

    /// 获取连接（redis-rs 内部已实现连接池，自动复用）
    async fn get_connection(&self) -> Result<ConnectionManager> {
        // redis-rs 的 ConnectionManager 内部已经实现了连接池
        // 直接创建即可，底层会自动复用连接
        Ok(ConnectionManager::new(self.client.as_ref().clone()).await?)
    }

    fn tenant_id(ctx: &Ctx) -> String {
        ctx.tenant_id()
            .filter(|tenant_id| !tenant_id.trim().is_empty())
            .unwrap_or(DEFAULT_TENANT_ID)
            .to_string()
    }

    fn message_key(tenant_id: &str, conversation_id: &str, message_id: &str) -> String {
        format!("cache:msg:{tenant_id}:{conversation_id}:{message_id}")
    }

    fn tail_key(tenant_id: &str, conversation_id: &str) -> String {
        format!("cache:tail:{tenant_id}:{conversation_id}")
    }
}

impl HotCacheRepository for RedisHotCacheRepository {
    #[instrument(skip(self, ctx, message), fields(message_id = %message.server_id, conversation_id = %message.conversation_id))]
    async fn store_hot(&self, ctx: &Ctx, message: &crate::domain::model::Message) -> Result<()> {
        let tenant_id = Self::tenant_id(ctx);
        let message = crate::convert::message_to_proto(message);
        let mut conn = self.get_connection().await?;

        let message_key =
            Self::message_key(&tenant_id, &message.conversation_id, &message.server_id);
        let tail_key = Self::tail_key(&tenant_id, &message.conversation_id);

        // 将 Message 编码为 protobuf bytes，然后 base64 编码存储
        let mut buf = Vec::new();
        message.encode(&mut buf)?;
        let encoded = BASE64.encode(&buf);
        let _: () = conn.set(&message_key, encoded).await?;
        if self.ttl_seconds > 0 {
            let ttl: i64 = self.ttl_seconds.try_into()?;
            let _: () = conn.expire(&message_key, ttl).await?;
        }

        let score = message.conversation_seq as f64;
        let _: () = conn
            .zadd(tail_key.clone(), message.server_id.clone(), score)
            .await?;
        let trim_stop = -((self.tail_limit as i64) + 1);
        let _: () = redis::cmd("ZREMRANGEBYRANK")
            .arg(&tail_key)
            .arg(0)
            .arg(trim_stop)
            .query_async(&mut conn)
            .await?;
        if self.ttl_seconds > 0 {
            let ttl: i64 = self.ttl_seconds.try_into()?;
            let _: () = conn.expire(tail_key, ttl).await?;
        }

        Ok(())
    }

    /// 批量存储消息到 Redis 热缓存（使用真正的 Pipeline 优化性能）
    ///
    /// 使用 Redis Pipeline 批量执行命令，减少网络往返次数，
    /// 性能比逐个执行提升 10-50 倍（取决于批量大小）
    #[instrument(skip(self, ctx, messages), fields(batch_size = messages.len()))]
    async fn store_hot_batch(
        &self,
        ctx: &Ctx,
        messages: &[crate::domain::model::Message],
    ) -> Result<()> {
        let tenant_id = Self::tenant_id(ctx);
        if messages.is_empty() {
            return Ok(());
        }
        let messages: Vec<flare_proto::common::Message> = messages
            .iter()
            .map(crate::convert::message_to_proto)
            .collect();
        let messages: &[flare_proto::common::Message] = &messages;

        let mut conn = self.get_connection().await?;

        // 使用真正的 Redis Pipeline 批量执行
        // 按会话分组，优化索引更新
        let mut tail_indices: std::collections::HashMap<String, Vec<(String, f64)>> =
            std::collections::HashMap::new();

        // 构建 Pipeline
        let mut pipe = redis::pipe();
        pipe.atomic(); // 原子性执行

        let ttl: i64 = if self.ttl_seconds > 0 {
            self.ttl_seconds.try_into()?
        } else {
            0
        };

        // 准备所有命令
        for message in messages {
            let message_key =
                Self::message_key(&tenant_id, &message.conversation_id, &message.server_id);

            // 编码消息
            let mut buf = Vec::new();
            message.encode(&mut buf)?;
            let encoded = BASE64.encode(&buf);

            // 添加到 Pipeline：SET 命令
            pipe.cmd("SET").arg(&message_key).arg(&encoded);

            // 添加到 Pipeline：EXPIRE 命令（如果有 TTL）
            if ttl > 0 {
                pipe.cmd("EXPIRE").arg(&message_key).arg(ttl);
            }

            let score = message.conversation_seq as f64;
            tail_indices
                .entry(message.conversation_id.clone())
                .or_default()
                .push((message.server_id.clone(), score));
        }

        // 批量执行 Pipeline（一次性发送所有命令）
        let _: Vec<redis::Value> = pipe.query_async(&mut conn).await?;

        // 批量更新索引（按会话分组，使用 Pipeline）
        for (conversation_id, items) in tail_indices {
            let tail_key = Self::tail_key(&tenant_id, &conversation_id);

            // 构建 ZADD Pipeline（支持多成员）
            let mut zadd_pipe = redis::pipe();
            zadd_pipe.atomic();

            // 添加所有成员到 ZADD
            for (message_id, score) in items {
                zadd_pipe
                    .cmd("ZADD")
                    .arg(&tail_key)
                    .arg(score)
                    .arg(&message_id);
            }

            let trim_stop = -((self.tail_limit as i64) + 1);
            zadd_pipe
                .cmd("ZREMRANGEBYRANK")
                .arg(&tail_key)
                .arg(0)
                .arg(trim_stop);

            // 添加 EXPIRE 命令（如果有 TTL）
            if ttl > 0 {
                zadd_pipe.cmd("EXPIRE").arg(&tail_key).arg(ttl);
            }

            // 执行 ZADD Pipeline
            let _: Vec<redis::Value> = zadd_pipe.query_async(&mut conn).await?;
        }

        tracing::trace!(
            batch_size = messages.len(),
            "Successfully batch cached {} messages to Redis using Pipeline",
            messages.len()
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hot_cache_keys_are_tenant_scoped() {
        assert_eq!(
            RedisHotCacheRepository::message_key("tenant-a", "conv-a", "msg-a"),
            "cache:msg:tenant-a:conv-a:msg-a"
        );
        assert_eq!(
            RedisHotCacheRepository::tail_key("tenant-a", "conv-a"),
            "cache:tail:tenant-a:conv-a"
        );
    }
}
