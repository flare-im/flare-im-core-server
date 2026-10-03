//! Redis 缓存层实现
//!
//! 提供消息查询缓存、会话状态缓存等功能
//! 实现 L2 缓存策略：Redis -> TimescaleDB

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{DateTime, Utc};
use flare_im_service_kit::redis_connection::{SharedRedisConnection, redis_error_code};
use flare_server_core::error::{ErrorCode, FlareError, Result, map_infra_error};
use prost::Message as ProstMessage;
use redis::{AsyncCommands, RedisError, aio::ConnectionManager};
use std::collections::HashMap;
use std::sync::Arc;

use crate::config::StorageReaderConfig;
use flare_proto::common::Message;

/// Redis 错误分类（`redis_error_code`）：超时是 `OperationTimeout`；连不上、连接断开、其他 IO 失败与
/// 需要重连的错误，以及 Redis 正在加载、集群不可用、主节点下线，是可重试的 `ServiceUnavailable`；
/// 认证或权限被拒、应答解析失败是 `ConfigurationError`（以前经 redis 的「要重连」判成了暂时错误）；
/// 其余（命令错误、类型不符等）是 `InternalError`。错误文本只进 details，不进 gRPC 状态（见 Storage
/// Reader 的 `storage_failure`）。
fn redis_failure(err: RedisError, context: &'static str) -> FlareError {
    let code = redis_error_code(&err, ErrorCode::OperationTimeout);
    map_infra_error(err, code, context)
}

trait RedisResultExt<T> {
    fn redis_context(self, context: &'static str) -> Result<T>;
}

impl<T> RedisResultExt<T> for std::result::Result<T, RedisError> {
    fn redis_context(self, context: &'static str) -> Result<T> {
        self.map_err(|err| redis_failure(err, context))
    }
}

/// 缓存里的一条消息：base64 编码的 protobuf。解不开是反序列化错误（缓存内容坏了），不是
/// 未分类的系统错误。
fn decode_cached_message(encoded: &str) -> Result<Message> {
    let bytes = BASE64.decode(encoded).map_err(|err| {
        map_infra_error(
            err,
            ErrorCode::DeserializationError,
            "cached message is not valid base64",
        )
    })?;
    Message::decode(&bytes[..]).map_err(|err| {
        map_infra_error(
            err,
            ErrorCode::DeserializationError,
            "cached message is not a valid protobuf message",
        )
    })
}

/// TTL 配置换成 Redis 的秒数；超出 i64 是配置错误。
fn ttl_seconds(ttl: u64) -> Result<i64> {
    ttl.try_into().map_err(|err| {
        map_infra_error(
            err,
            ErrorCode::ConfigurationError,
            "Redis cache TTL is out of range",
        )
    })
}

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
    /// 进程共享的连接（有界建连、应答与重建）。以前每次读写都 `ConnectionManager::new` 新建、用
    /// redis 的默认退避，Redis 不可达时一次读可阻塞数分钟，读路径迟迟回落不到 PostgreSQL。
    redis: Arc<SharedRedisConnection>,
    message_ttl_seconds: u64,
    session_ttl_seconds: u64,
}

impl RedisMessageCache {
    pub fn new(redis: Arc<SharedRedisConnection>, config: &StorageReaderConfig) -> Self {
        Self {
            redis,
            message_ttl_seconds: config.redis_message_cache_ttl_seconds,
            session_ttl_seconds: config.redis_session_cache_ttl_seconds,
        }
    }

    /// 使用 RedisCacheConfig 创建实例
    pub fn new_with_config(
        redis: Arc<SharedRedisConnection>,
        cache_config: &RedisCacheConfig,
    ) -> Self {
        Self {
            redis,
            message_ttl_seconds: cache_config.message_ttl_seconds,
            session_ttl_seconds: cache_config.session_ttl_seconds,
        }
    }

    /// 共享的 Redis 连接（建连、应答与重试都有上限，见 `StorageReaderConfig::redis_limits`）。
    async fn get_connection(&self) -> Result<ConnectionManager> {
        self.redis
            .get()
            .await
            .redis_context("Failed to connect to Redis")
    }

    fn message_key(tenant_id: &str, conversation_id: &str, message_id: &str) -> String {
        flare_im_contracts::message_cache_key(tenant_id, conversation_id, message_id)
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
        flare_im_contracts::message_tail_key(tenant_id, conversation_id)
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

        let _: () = conn
            .set(&message_key, encoded)
            .await
            .redis_context("Failed to cache message")?;

        if self.message_ttl_seconds > 0 {
            let ttl = ttl_seconds(self.message_ttl_seconds)?;
            let _: () = conn
                .expire(&message_key, ttl)
                .await
                .redis_context("Failed to set message cache TTL")?;
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
            ttl_seconds(self.message_ttl_seconds)?
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

        let _: Vec<redis::Value> = pipe
            .query_async(&mut conn)
            .await
            .redis_context("Failed to cache message batch")?;

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

        let encoded: Option<String> = conn
            .get(&message_key)
            .await
            .redis_context("Failed to read cached message")?;

        encoded.as_deref().map(decode_cached_message).transpose()
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
        let encoded_list: Vec<Option<String>> = conn
            .get(keys)
            .await
            .redis_context("Failed to read cached messages")?;

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
            .await
            .redis_context("Failed to read conversation tail cache")?;
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
            .await
            .redis_context("Failed to read conversation tail cache")?;

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
            let ttl = ttl_seconds(self.session_ttl_seconds)?;
            pipe.cmd("EXPIRE").arg(&index_key).arg(ttl);
        }

        let _: Vec<redis::Value> = pipe
            .query_async(&mut conn)
            .await
            .redis_context("Failed to cache conversation messages")?;

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
        let message_ids: Vec<String> = conn
            .zrange(&index_key, 0, (limit - 1) as isize)
            .await
            .redis_context("Failed to read conversation cache index")?;

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
        let _: () = conn
            .del(&message_key)
            .await
            .redis_context("Failed to invalidate cached message")?;

        Ok(())
    }

    /// 清除会话缓存
    pub async fn invalidate_session(&self, tenant_id: &str, conversation_id: &str) -> Result<()> {
        let mut conn = self.get_connection().await?;

        // 使用 KEYS 命令查找所有相关的 key（注意：KEYS 在生产环境可能阻塞，但用于缓存失效场景可接受）
        // 更好的方案是维护一个会话 key 的 SET，但为了简化实现，这里使用 KEYS
        let pattern = format!("cache:session:{tenant_id}:{conversation_id}:*");
        let keys: Vec<String> = conn
            .keys(&pattern)
            .await
            .redis_context("Failed to list conversation cache keys")?;

        if !keys.is_empty() {
            let _: () = conn
                .del(&keys)
                .await
                .redis_context("Failed to invalidate conversation cache")?;
            tracing::trace!(
                conversation_id = %conversation_id,
                deleted_keys = keys.len(),
                "Invalidated session cache"
            );
        }

        // 同时清除消息缓存（使用消息 key 模式）
        let msg_pattern = format!("cache:msg:{tenant_id}:{conversation_id}:*");
        let msg_keys: Vec<String> = conn
            .keys(&msg_pattern)
            .await
            .redis_context("Failed to list cached message keys")?;

        if !msg_keys.is_empty() {
            let _: () = conn
                .del(&msg_keys)
                .await
                .redis_context("Failed to invalidate cached messages")?;
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

    /// Redis 失败按种类分类：超时、连不上与断开（可重试）、其余（内部错误），文本只进 details。
    /// 修复前一律是 `From<RedisError>` 的未分类系统错误，gRPC 层只能回 INTERNAL。
    #[test]
    fn redis_failures_are_classified() {
        let io = |kind| RedisError::from(std::io::Error::from(kind));
        for (err, code) in [
            (
                io(std::io::ErrorKind::TimedOut),
                ErrorCode::OperationTimeout,
            ),
            (
                io(std::io::ErrorKind::ConnectionRefused),
                ErrorCode::ServiceUnavailable,
            ),
            (
                io(std::io::ErrorKind::ConnectionReset),
                ErrorCode::ServiceUnavailable,
            ),
            (
                RedisError::from((redis::ErrorKind::BusyLoadingError, "loading")),
                ErrorCode::ServiceUnavailable,
            ),
            // 认证与权限被拒、应答解析失败是配置错误（W14-R (b)）：修复前前两类经 redis 的「要重连」
            // 判成了 ServiceUnavailable，服务端的 NOAUTH 是 InternalError。
            (
                RedisError::from((redis::ErrorKind::AuthenticationFailed, "leak-marker-78")),
                ErrorCode::ConfigurationError,
            ),
            (
                RedisError::from((redis::ErrorKind::ParseError, "leak-marker-79")),
                ErrorCode::ConfigurationError,
            ),
            (
                redis::make_extension_error("NOAUTH".into(), Some("leak-marker-80".into())),
                ErrorCode::ConfigurationError,
            ),
            (
                RedisError::from((redis::ErrorKind::TypeError, "WRONGTYPE leak-marker-77")),
                ErrorCode::InternalError,
            ),
        ] {
            let mapped = redis_failure(err, "read");
            assert_eq!(mapped.code(), Some(code), "{mapped}");
            assert_eq!(mapped.reason(), "read", "{mapped}");
        }
    }

    /// 缓存内容解不开是反序列化错误，不是未分类的系统错误。
    #[test]
    fn undecodable_cache_entries_are_deserialization_errors() {
        for encoded in [
            "not base64 !!".to_string(),
            BASE64.encode([0xff, 0xff, 0xff]),
        ] {
            let err = decode_cached_message(&encoded).unwrap_err();
            assert_eq!(err.code(), Some(ErrorCode::DeserializationError), "{err}");
        }
        let message = Message {
            server_id: "m1".into(),
            ..Default::default()
        };
        let decoded = decode_cached_message(&BASE64.encode(message.encode_to_vec())).unwrap();
        assert_eq!(decoded.server_id, "m1");
    }

    /// Redis 不可达（关闭的端口）时缓存读在连接上限内失败（W14-R (a)），读路径随即回落到
    /// PostgreSQL。修复前每次读都 `ConnectionManager::new`、用 redis 的默认退避，60 s 仍未返回。
    #[tokio::test]
    async fn a_cache_read_from_an_unreachable_redis_fails_within_the_bound() {
        use flare_im_service_kit::redis_connection::RedisConnectionLimits;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let closed = listener.local_addr().unwrap();
        drop(listener);
        let limits = RedisConnectionLimits::default();
        let cache = RedisMessageCache::new_with_config(
            Arc::new(SharedRedisConnection::new(
                redis::Client::open(format!("redis://{closed}/")).unwrap(),
                limits,
            )),
            &RedisCacheConfig::default(),
        );
        let err = tokio::time::timeout(
            limits.connect_budget() + std::time::Duration::from_secs(2),
            cache.get_tail_messages_by_seq("tenant-a", "conv-a", 0, None, 10),
        )
        .await
        .expect("the cache read must fail within the bound")
        .expect_err("nothing listens on the port");
        assert_eq!(err.code(), Some(ErrorCode::ServiceUnavailable), "{err}");
    }

    #[test]
    fn tail_key_is_tenant_scoped() {
        assert_eq!(
            RedisMessageCache::tail_key("tenant-a", "conv-a"),
            "cache:tail:tenant-a:conv-a"
        );
    }
}
