use async_trait::async_trait;
use redis::aio::ConnectionManager;

use crate::domain::repository::{ReadWatermarkAdvance, ReadWatermarkRepository};
use flare_server_core::error::{FlareError, Result};

/// Raise the watermark only when the offered position is strictly greater, in one
/// round trip, so concurrent acks from several devices admit at most one of them.
/// Returns `{advanced (0|1), previous}`.
const ADVANCE_READ_WATERMARK_SCRIPT: &str = r#"
local current = tonumber(redis.call('GET', KEYS[1]) or '0') or 0
local next = tonumber(ARGV[1])
if next == nil then
  return redis.error_reply('invalid read seq')
end
if next <= current then
  return {0, current}
end
local ttl_seconds = tonumber(ARGV[2])
if ttl_seconds ~= nil and ttl_seconds > 0 then
  redis.call('SET', KEYS[1], ARGV[1], 'EX', ttl_seconds)
else
  redis.call('SET', KEYS[1], ARGV[1])
end
return {1, current}
"#;

/// Restore the previous watermark only while the key still holds the value this
/// claim wrote; a newer position written meanwhile is kept.
const ROLLBACK_READ_WATERMARK_SCRIPT: &str = r#"
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
  return 0
end
local previous = tonumber(ARGV[2]) or 0
if previous <= 0 then
  redis.call('DEL', KEYS[1])
  return 1
end
local ttl_seconds = tonumber(ARGV[3])
if ttl_seconds ~= nil and ttl_seconds > 0 then
  redis.call('SET', KEYS[1], ARGV[2], 'EX', ttl_seconds)
else
  redis.call('SET', KEYS[1], ARGV[2])
end
return 1
"#;

fn advance_script() -> &'static redis::Script {
    static SCRIPT: std::sync::OnceLock<redis::Script> = std::sync::OnceLock::new();
    SCRIPT.get_or_init(|| redis::Script::new(ADVANCE_READ_WATERMARK_SCRIPT))
}

fn rollback_script() -> &'static redis::Script {
    static SCRIPT: std::sync::OnceLock<redis::Script> = std::sync::OnceLock::new();
    SCRIPT.get_or_init(|| redis::Script::new(ROLLBACK_READ_WATERMARK_SCRIPT))
}

/// Redis-backed [`ReadWatermarkRepository`]: one string key per
/// (tenant, conversation, reader).
///
/// The key only has to outlive a burst of repeated acks. When it expires the next ack
/// is admitted once and re-arms it, so the TTL bounds memory without weakening the
/// dedupe in any way that matters.
pub struct RedisReadWatermarkRepository {
    connection: ConnectionManager,
    ttl_seconds: u64,
}

impl RedisReadWatermarkRepository {
    pub const DEFAULT_TTL_SECONDS: u64 = 24 * 60 * 60;

    pub fn new(connection: ConnectionManager, ttl_seconds: u64) -> Self {
        Self {
            connection,
            ttl_seconds,
        }
    }

    fn watermark_key(tenant_id: &str, conversation_id: &str, user_id: &str) -> String {
        format!("read:watermark:{tenant_id}:{conversation_id}:{user_id}")
    }
}

#[async_trait]
impl ReadWatermarkRepository for RedisReadWatermarkRepository {
    async fn advance(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        user_id: &str,
        read_seq: u64,
    ) -> Result<ReadWatermarkAdvance> {
        let mut connection = self.connection.clone();
        let (advanced, previous): (u8, u64) = advance_script()
            .key(Self::watermark_key(tenant_id, conversation_id, user_id))
            .arg(read_seq)
            .arg(self.ttl_seconds)
            .invoke_async(&mut connection)
            .await
            .map_err(|err| FlareError::system(format!("Redis read watermark advance: {err}")))?;
        Ok(if advanced == 1 {
            ReadWatermarkAdvance::Advanced { previous }
        } else {
            ReadWatermarkAdvance::NotAdvanced { current: previous }
        })
    }

    async fn rollback(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        user_id: &str,
        advanced_to: u64,
        previous: u64,
    ) -> Result<()> {
        let mut connection = self.connection.clone();
        let _: u8 = rollback_script()
            .key(Self::watermark_key(tenant_id, conversation_id, user_id))
            .arg(advanced_to)
            .arg(previous)
            .arg(self.ttl_seconds)
            .invoke_async(&mut connection)
            .await
            .map_err(|err| FlareError::system(format!("Redis read watermark rollback: {err}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Runs the scripts against a real Redis. Set `FLARE_TEST_REDIS_URL`
    /// (for example `redis://127.0.0.1:26379/15`) to enable; skipped otherwise.
    async fn live_repository() -> Option<RedisReadWatermarkRepository> {
        let url = std::env::var("FLARE_TEST_REDIS_URL").ok()?;
        let client = redis::Client::open(url).expect("redis url");
        let connection = client
            .get_connection_manager()
            .await
            .expect("redis connection");
        Some(RedisReadWatermarkRepository::new(connection, 60))
    }

    #[test]
    fn keys_are_scoped_per_tenant_conversation_and_reader() {
        let key = RedisReadWatermarkRepository::watermark_key;
        assert_eq!(key("t1", "c1", "u1"), "read:watermark:t1:c1:u1");
        assert_ne!(key("t1", "c1", "u1"), key("t1", "c1", "u2"));
        assert_ne!(key("t1", "c1", "u1"), key("t2", "c1", "u1"));
    }

    #[tokio::test]
    async fn live_redis_advance_is_strict_per_reader_and_rollback_is_fenced() {
        let Some(repo) = live_repository().await else {
            eprintln!("FLARE_TEST_REDIS_URL not set; skipping live Redis watermark test");
            return;
        };
        let tenant = format!("test-{}", uuid::Uuid::new_v4());

        assert_eq!(
            repo.advance(&tenant, "c1", "reader", 3420).await.unwrap(),
            ReadWatermarkAdvance::Advanced { previous: 0 }
        );
        for _ in 0..3 {
            assert_eq!(
                repo.advance(&tenant, "c1", "reader", 3420).await.unwrap(),
                ReadWatermarkAdvance::NotAdvanced { current: 3420 }
            );
        }
        assert_eq!(
            repo.advance(&tenant, "c1", "reader", 3000).await.unwrap(),
            ReadWatermarkAdvance::NotAdvanced { current: 3420 }
        );
        assert_eq!(
            repo.advance(&tenant, "c1", "peer", 3420).await.unwrap(),
            ReadWatermarkAdvance::Advanced { previous: 0 }
        );
        assert_eq!(
            repo.advance(&tenant, "c1", "reader", 3421).await.unwrap(),
            ReadWatermarkAdvance::Advanced { previous: 3420 }
        );

        // Rolling back a claim restores the previous position, so a retry is admitted.
        repo.rollback(&tenant, "c1", "reader", 3421, 3420)
            .await
            .unwrap();
        assert_eq!(
            repo.advance(&tenant, "c1", "reader", 3421).await.unwrap(),
            ReadWatermarkAdvance::Advanced { previous: 3420 }
        );

        // A stale claim never rewinds a newer position.
        repo.advance(&tenant, "c1", "reader", 3500).await.unwrap();
        repo.rollback(&tenant, "c1", "reader", 3421, 3420)
            .await
            .unwrap();
        assert_eq!(
            repo.advance(&tenant, "c1", "reader", 3500).await.unwrap(),
            ReadWatermarkAdvance::NotAdvanced { current: 3500 }
        );

        // Rolling back the first claim of a reader removes the key.
        repo.rollback(&tenant, "c1", "peer", 3420, 0).await.unwrap();
        assert_eq!(
            repo.advance(&tenant, "c1", "peer", 3420).await.unwrap(),
            ReadWatermarkAdvance::Advanced { previous: 0 }
        );
    }
}
