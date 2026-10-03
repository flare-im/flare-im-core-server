use std::sync::Arc;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use flare_im_contracts::Ctx;
use flare_im_contracts::utils::normalize_optional_tenant_id;
use flare_server_core::error::{ErrorCode, FlareError, Result, map_infra_error};
use prost::Message as _;
use redis::{AsyncCommands, RedisError, aio::ConnectionManager};
use std::convert::TryInto;
use tracing::instrument;

use crate::config::StorageWriterConfig;
use crate::domain::repository::HotCacheRepository;

/// Redis 失败按是否暂时分类，口径同 Storage Reader 的 Redis 缓存（W10-E）：超时，以及连不上、
/// 连接断开、其他 IO 失败、需要重连的错误、Redis 正在加载、集群不可用、主节点下线，都是暂时的、
/// 可重试（超时 `NetworkTimeout`，其余 `ServiceUnavailable`，`is_retryable()` 为真）；命令错误、
/// 类型不符等其余错误是 `InternalError`，不可重试。Redis 的原文只进 details。以前一律经
/// `From<RedisError>` 成为未分类的系统错误，看不出该等 Redis 恢复还是该查数据与配置。
fn redis_failure(err: RedisError, context: &'static str) -> FlareError {
    let code = if err.is_timeout() {
        ErrorCode::NetworkTimeout
    } else if err.is_io_error()
        || err.is_unrecoverable_error()
        || matches!(
            err.kind(),
            redis::ErrorKind::BusyLoadingError
                | redis::ErrorKind::TryAgain
                | redis::ErrorKind::ClusterDown
                | redis::ErrorKind::MasterDown
        )
    {
        ErrorCode::ServiceUnavailable
    } else {
        ErrorCode::InternalError
    };
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

/// TTL 配置换成 Redis 的秒数；超出 i64 是配置错误（非暂时，不可重试）。
fn ttl_seconds(ttl: u64) -> Result<i64> {
    ttl.try_into().map_err(|err| {
        map_infra_error(
            err,
            ErrorCode::ConfigurationError,
            "Redis hot cache TTL is out of range",
        )
    })
}

/// 缓存里的一条消息：base64 编码的 protobuf（与 Storage Reader 的解码对应）。编码写进 `Vec`
/// 不会失败。
fn encode_cached_message(message: &flare_proto::common::Message) -> String {
    BASE64.encode(message.encode_to_vec())
}

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
        ConnectionManager::new(self.client.as_ref().clone())
            .await
            .redis_context("Redis hot cache connection failed")
    }

    /// 热缓存键的租户：缺租户时报错（调用方把投影失败记为告警），不写进租户 "0" 的键。
    fn tenant_id(ctx: &Ctx) -> Result<String> {
        normalize_optional_tenant_id(ctx.tenant_id()).ok_or_else(|| {
            flare_server_core::flare_err!(
                flare_server_core::error::ErrorCode::InvalidParameter,
                "tenant required for hot cache key"
            )
        })
    }

    fn message_key(tenant_id: &str, conversation_id: &str, message_id: &str) -> String {
        flare_im_contracts::message_cache_key(tenant_id, conversation_id, message_id)
    }

    fn tail_key(tenant_id: &str, conversation_id: &str) -> String {
        flare_im_contracts::message_tail_key(tenant_id, conversation_id)
    }
}

impl HotCacheRepository for RedisHotCacheRepository {
    #[instrument(skip(self, ctx, message), fields(message_id = %message.server_id, conversation_id = %message.conversation_id))]
    async fn store_hot(&self, ctx: &Ctx, message: &crate::domain::model::Message) -> Result<()> {
        let tenant_id = Self::tenant_id(ctx)?;
        let message = crate::convert::message_to_proto(message);
        let mut conn = self.get_connection().await?;

        let message_key =
            Self::message_key(&tenant_id, &message.conversation_id, &message.server_id);
        let tail_key = Self::tail_key(&tenant_id, &message.conversation_id);

        // 将 Message 编码为 protobuf bytes，然后 base64 编码存储
        let encoded = encode_cached_message(&message);
        let _: () = conn
            .set(&message_key, encoded)
            .await
            .redis_context("Redis hot cache SET failed")?;
        if self.ttl_seconds > 0 {
            let ttl = ttl_seconds(self.ttl_seconds)?;
            let _: () = conn
                .expire(&message_key, ttl)
                .await
                .redis_context("Redis hot cache EXPIRE failed")?;
        }

        let score = message.conversation_seq as f64;
        let _: () = conn
            .zadd(tail_key.clone(), message.server_id.clone(), score)
            .await
            .redis_context("Redis hot cache ZADD failed")?;
        let trim_stop = -((self.tail_limit as i64) + 1);
        let _: () = redis::cmd("ZREMRANGEBYRANK")
            .arg(&tail_key)
            .arg(0)
            .arg(trim_stop)
            .query_async(&mut conn)
            .await
            .redis_context("Redis hot cache tail trim failed")?;
        if self.ttl_seconds > 0 {
            let ttl = ttl_seconds(self.ttl_seconds)?;
            let _: () = conn
                .expire(tail_key, ttl)
                .await
                .redis_context("Redis hot cache EXPIRE failed")?;
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
        let tenant_id = Self::tenant_id(ctx)?;
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
            ttl_seconds(self.ttl_seconds)?
        } else {
            0
        };

        // 准备所有命令
        for message in messages {
            let message_key =
                Self::message_key(&tenant_id, &message.conversation_id, &message.server_id);

            // 编码消息
            let encoded = encode_cached_message(message);

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
        let _: Vec<redis::Value> = pipe
            .query_async(&mut conn)
            .await
            .redis_context("Redis hot cache batch SET failed")?;

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
            let _: Vec<redis::Value> = zadd_pipe
                .query_async(&mut conn)
                .await
                .redis_context("Redis hot cache batch tail update failed")?;
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

    /// 缺租户的上下文不写进租户 "0" 的缓存键（W11-T）；显式租户 "0" 照常。
    #[test]
    fn hot_cache_tenant_must_come_from_the_context() {
        let missing: Ctx = std::sync::Arc::new(flare_im_contracts::utils::Context::default());
        assert!(RedisHotCacheRepository::tenant_id(&missing).is_err());
        let blank: Ctx =
            std::sync::Arc::new(flare_im_contracts::utils::Context::default().with_tenant_id(" "));
        assert!(RedisHotCacheRepository::tenant_id(&blank).is_err());
        let zero: Ctx =
            std::sync::Arc::new(flare_im_contracts::utils::Context::default().with_tenant_id("0"));
        assert_eq!(RedisHotCacheRepository::tenant_id(&zero).unwrap(), "0");
    }

    /// Redis 失败按是否暂时分类（W13-R2，口径同 Storage Reader 的 W10-E）：超时与连不上、连接断开、
    /// 正在加载是可重试的暂时错误；命令与类型错误是不可重试的内部错误；TTL 越界是配置错误。原文
    /// 只进 details。修复前一律是 `From<RedisError>` 的未分类系统错误（没有错误码、不可重试）。
    #[test]
    fn redis_failures_are_classified_as_transient_or_not() {
        let io = |kind| RedisError::from(std::io::Error::from(kind));
        for (err, code, retryable) in [
            (
                io(std::io::ErrorKind::TimedOut),
                ErrorCode::NetworkTimeout,
                true,
            ),
            (
                io(std::io::ErrorKind::ConnectionRefused),
                ErrorCode::ServiceUnavailable,
                true,
            ),
            (
                io(std::io::ErrorKind::ConnectionReset),
                ErrorCode::ServiceUnavailable,
                true,
            ),
            (
                RedisError::from((redis::ErrorKind::BusyLoadingError, "loading")),
                ErrorCode::ServiceUnavailable,
                true,
            ),
            (
                RedisError::from((redis::ErrorKind::TypeError, "WRONGTYPE leak-marker-41")),
                ErrorCode::InternalError,
                false,
            ),
            (
                RedisError::from((redis::ErrorKind::ResponseError, "ERR leak-marker-42")),
                ErrorCode::InternalError,
                false,
            ),
        ] {
            let mapped = redis_failure(err, "write");
            assert_eq!(mapped.code(), Some(code), "{mapped}");
            assert_eq!(mapped.is_retryable(), retryable, "{mapped}");
            assert_eq!(mapped.reason(), "write", "{mapped}");
        }

        let ttl = ttl_seconds(u64::MAX).unwrap_err();
        assert_eq!(ttl.code(), Some(ErrorCode::ConfigurationError), "{ttl}");
        assert!(!ttl.is_retryable());
        assert_eq!(ttl_seconds(60).unwrap(), 60);
    }

    /// 极简的假 Redis（只为本模块的用例）：逐条读入 RESP 数组命令，建连时的 `CLIENT` 命令回
    /// `+OK`，其余一律回 `error_line`。返回连接地址。
    async fn fake_redis(error_line: &'static str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut pending = Vec::new();
                    let mut chunk = [0u8; 4096];
                    loop {
                        let read = match socket.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(read) => read,
                        };
                        pending.extend_from_slice(&chunk[..read]);
                        while let Some((command, used)) = next_command(&pending) {
                            pending.drain(..used);
                            let reply = if command.eq_ignore_ascii_case("CLIENT") {
                                "+OK\r\n".to_string()
                            } else {
                                format!("-{error_line}\r\n")
                            };
                            if socket.write_all(reply.as_bytes()).await.is_err() {
                                return;
                            }
                        }
                    }
                });
            }
        });
        format!("redis://{address}/")
    }

    /// 缓冲区开头一条完整的 RESP 数组命令：命令名与占用的字节数；不完整时为 `None`。
    fn next_command(buffer: &[u8]) -> Option<(String, usize)> {
        fn line(buffer: &[u8], from: usize) -> Option<(&[u8], usize)> {
            let end = buffer.get(from..)?.windows(2).position(|w| w == b"\r\n")? + from;
            Some((&buffer[from..end], end + 2))
        }
        fn number(text: &[u8], prefix: u8) -> Option<usize> {
            std::str::from_utf8(text.strip_prefix(&[prefix])?)
                .ok()?
                .parse()
                .ok()
        }
        let (header, mut at) = line(buffer, 0)?;
        let mut name = String::new();
        for index in 0..number(header, b'*')? {
            let (length, start) = line(buffer, at)?;
            let end = start + number(length, b'$')?;
            if buffer.len() < end + 2 {
                return None;
            }
            if index == 0 {
                name = String::from_utf8_lossy(&buffer[start..end]).into_owned();
            }
            at = end + 2;
        }
        Some((name, at))
    }

    /// 热缓存写经真实的 Redis 客户端拿到的失败也按是否暂时分类：Redis 正在加载是可重试的
    /// ServiceUnavailable，类型错误是不可重试的 InternalError。修复前两者都是没有错误码的系统错误。
    #[tokio::test]
    async fn hot_cache_writes_classify_redis_replies() {
        let ctx: Ctx =
            Arc::new(flare_im_contracts::utils::Context::default().with_tenant_id("tenant-a"));
        let message = crate::domain::model::Message {
            server_id: "m1".into(),
            conversation_id: "conv-a".into(),
            ..Default::default()
        };
        for (error_line, code, retryable) in [
            (
                "LOADING Redis is loading the dataset in memory",
                ErrorCode::ServiceUnavailable,
                true,
            ),
            (
                "WRONGTYPE Operation against a key holding the wrong kind of value",
                ErrorCode::InternalError,
                false,
            ),
        ] {
            let repository = RedisHotCacheRepository {
                client: Arc::new(redis::Client::open(fake_redis(error_line).await).unwrap()),
                ttl_seconds: 60,
                tail_limit: 10,
            };
            let error = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                repository.store_hot(&ctx, &message),
            )
            .await
            .expect("the fake Redis answers")
            .expect_err("the fake Redis refuses SET");
            assert_eq!(error.code(), Some(code), "{error_line}: {error}");
            assert_eq!(error.is_retryable(), retryable, "{error_line}: {error}");
            assert_eq!(error.reason(), "Redis hot cache SET failed", "{error}");
        }
    }

    /// 写进缓存的编码与 Storage Reader 的解码对应：base64 的 protobuf。
    #[test]
    fn cached_messages_are_base64_protobuf() {
        let message = flare_proto::common::Message {
            server_id: "m1".into(),
            ..Default::default()
        };
        let decoded = flare_proto::common::Message::decode(
            &BASE64
                .decode(encode_cached_message(&message))
                .expect("base64")[..],
        )
        .expect("protobuf");
        assert_eq!(decoded.server_id, "m1");
    }
}
