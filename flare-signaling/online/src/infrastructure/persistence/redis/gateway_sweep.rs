//! 网关宕机清扫的 Redis 实现：单例锁、`gw:known` / `gw:lease:*` / `gw:conns:*`
//! （键名约定见 `flare_im_service_kit::gateway::lease`）。与会话哈希同一个 Redis。

use std::time::Duration;

use flare_im_service_kit::gateway::lease::{
    KNOWN_GATEWAYS_KEY, conns_key as gateway_conns_key, lease_key,
};
use flare_im_service_kit::redis_connection::{
    RedisConnectionLimits, SharedRedisConnection, redis_error_code,
};
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError, Result};
use redis::AsyncCommands;
use serde::Deserialize;

use crate::domain::repository::{GatewaySweepRepository, SessionOwner};

/// 清扫器单例锁。
pub const SWEEPER_LOCK_KEY: &str = "gw:sweeper:lock";

/// 持有者续期，否则 `SET NX PX` 抢占。
const HOLD_LOCK_SCRIPT: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then return 1 end
return 0
"#;

const RELEASE_LOCK_SCRIPT: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
"#;

/// 租约仍缺失才退役：清扫途中网关带着同一 ID 回来的话，它的新连接已经写进同一个集合，不能删。
const RETIRE_SCRIPT: &str = r#"
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('DEL', KEYS[2])
redis.call('SREM', KEYS[3], ARGV[1])
return 1
"#;

/// 会话记录里清扫器需要的两个字段。
#[derive(Deserialize)]
struct OwnerRecord {
    #[serde(default)]
    gateway_id: String,
    #[serde(default)]
    created_at: i64,
}

pub struct RedisGatewaySweepRepository {
    connection: SharedRedisConnection,
}

impl RedisGatewaySweepRepository {
    pub fn new(client: redis::Client) -> Self {
        Self {
            connection: SharedRedisConnection::new(client, RedisConnectionLimits::default()),
        }
    }

    async fn connection(&self) -> Result<redis::aio::ConnectionManager> {
        self.connection.get().await.map_err(redis_failure)
    }
}

fn redis_failure(error: redis::RedisError) -> FlareError {
    ErrorBuilder::new(
        redis_error_code(&error, ErrorCode::OperationTimeout),
        "gateway sweep redis operation failed",
    )
    .details(error.to_string())
    .build_error()
}

impl GatewaySweepRepository for RedisGatewaySweepRepository {
    async fn hold_sweeper_lock(&self, token: &str, ttl: Duration) -> Result<bool> {
        let mut connection = self.connection().await?;
        let held: i64 = redis::Script::new(HOLD_LOCK_SCRIPT)
            .key(SWEEPER_LOCK_KEY)
            .arg(token)
            .arg(ttl.as_millis() as u64)
            .invoke_async(&mut connection)
            .await
            .map_err(redis_failure)?;
        Ok(held == 1)
    }

    async fn release_sweeper_lock(&self, token: &str) -> Result<()> {
        let mut connection = self.connection().await?;
        let _: i64 = redis::Script::new(RELEASE_LOCK_SCRIPT)
            .key(SWEEPER_LOCK_KEY)
            .arg(token)
            .invoke_async(&mut connection)
            .await
            .map_err(redis_failure)?;
        Ok(())
    }

    async fn known_gateways(&self) -> Result<Vec<String>> {
        // 集合大小等于网关节点数（加少量待清扫的孤儿），一次读完。
        let mut connection = self.connection().await?;
        connection
            .smembers(KNOWN_GATEWAYS_KEY)
            .await
            .map_err(redis_failure)
    }

    async fn leases_present(&self, gateway_ids: &[String]) -> Result<Vec<bool>> {
        if gateway_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut connection = self.connection().await?;
        let mut pipe = redis::pipe();
        for gateway_id in gateway_ids {
            pipe.exists(lease_key(gateway_id));
        }
        pipe.query_async(&mut connection)
            .await
            .map_err(redis_failure)
    }

    async fn scan_gateway_connections(
        &self,
        gateway_id: &str,
        cursor: u64,
        count: usize,
    ) -> Result<(u64, Vec<String>)> {
        let mut connection = self.connection().await?;
        redis::cmd("SSCAN")
            .arg(gateway_conns_key(gateway_id))
            .arg(cursor)
            .arg("COUNT")
            .arg(count.max(1))
            .query_async(&mut connection)
            .await
            .map_err(redis_failure)
    }

    async fn session_owner(
        &self,
        tenant_id: &str,
        user_id: &str,
        session_field: &str,
    ) -> Result<Option<SessionOwner>> {
        let mut connection = self.connection().await?;
        let raw: Option<String> = connection
            .hget(
                super::RedisConversationRepository::connection_key(tenant_id, user_id),
                session_field,
            )
            .await
            .map_err(redis_failure)?;
        Ok(raw
            .and_then(|raw| serde_json::from_str::<OwnerRecord>(&raw).ok())
            .map(|record| SessionOwner {
                gateway_id: record.gateway_id,
                created_at_ms: record.created_at,
            }))
    }

    async fn forget_gateway_connection(&self, gateway_id: &str, member: &str) -> Result<()> {
        let mut connection = self.connection().await?;
        let _: i64 = connection
            .srem(gateway_conns_key(gateway_id), member)
            .await
            .map_err(redis_failure)?;
        Ok(())
    }

    async fn retire_gateway(&self, gateway_id: &str) -> Result<bool> {
        let mut connection = self.connection().await?;
        let retired: i64 = redis::Script::new(RETIRE_SCRIPT)
            .key(lease_key(gateway_id))
            .key(gateway_conns_key(gateway_id))
            .key(KNOWN_GATEWAYS_KEY)
            .arg(gateway_id)
            .invoke_async(&mut connection)
            .await
            .map_err(redis_failure)?;
        Ok(retired == 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_im_service_kit::gateway::lease::connection_member;

    /// 锁、已知网关、租约、连接索引与退役脚本按约定工作。
    /// Run with `FLARE_TEST_REDIS_URL` pointing at an isolated Redis instance.
    #[tokio::test]
    #[ignore = "requires FLARE_TEST_REDIS_URL"]
    async fn sweep_primitives_follow_the_gateway_lease_contract() {
        let url = std::env::var("FLARE_TEST_REDIS_URL").expect("FLARE_TEST_REDIS_URL");
        let client = redis::Client::open(url.as_str()).unwrap();
        let repository = RedisGatewaySweepRepository::new(client.clone());
        let mut conn = client.get_multiplexed_async_connection().await.unwrap();
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let gateway = format!("gw-sweep-{suffix}");
        let tenant = format!("t-{}", &suffix[..12]);
        let field = "session-1";
        let member = connection_member(&tenant, "u:1", field);

        // 单例锁：持有者可续期，别人拿不到；释放后别人可拿。
        let _: () = conn.del(SWEEPER_LOCK_KEY).await.unwrap();
        let ttl = Duration::from_secs(5);
        assert!(repository.hold_sweeper_lock("a", ttl).await.unwrap());
        assert!(repository.hold_sweeper_lock("a", ttl).await.unwrap());
        assert!(!repository.hold_sweeper_lock("b", ttl).await.unwrap());
        repository.release_sweeper_lock("b").await.unwrap();
        assert!(!repository.hold_sweeper_lock("b", ttl).await.unwrap());
        repository.release_sweeper_lock("a").await.unwrap();
        assert!(repository.hold_sweeper_lock("b", ttl).await.unwrap());
        repository.release_sweeper_lock("b").await.unwrap();

        let session_key = super::super::RedisConversationRepository::connection_key(&tenant, "u:1");
        let record = serde_json::json!({
            "tenant_id": tenant, "conversation_id": field, "gateway_id": gateway,
            "server_id": gateway, "device_id": "d", "device_platform": "ios",
            "connection_id": field, "created_at": 1234, "last_seen": 1,
            "device_priority": 2, "token_version": 0
        });
        let _: () = redis::pipe()
            .hset(&session_key, field, record.to_string())
            .ignore()
            .sadd(gateway_conns_key(&gateway), &member)
            .ignore()
            .sadd(gateway_conns_key(&gateway), "garbage")
            .ignore()
            .sadd(KNOWN_GATEWAYS_KEY, &gateway)
            .ignore()
            .query_async(&mut conn)
            .await
            .unwrap();

        assert!(
            repository
                .known_gateways()
                .await
                .unwrap()
                .contains(&gateway)
        );
        assert_eq!(
            repository
                .leases_present(std::slice::from_ref(&gateway))
                .await
                .unwrap(),
            vec![false]
        );
        let mut members = Vec::new();
        let mut cursor = 0;
        loop {
            let (next, batch) = repository
                .scan_gateway_connections(&gateway, cursor, 1)
                .await
                .unwrap();
            members.extend(batch);
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        members.sort();
        assert_eq!(members, vec!["garbage".to_string(), member.clone()]);
        assert_eq!(
            repository
                .session_owner(&tenant, "u:1", field)
                .await
                .unwrap(),
            Some(SessionOwner {
                gateway_id: gateway.clone(),
                created_at_ms: 1234,
            })
        );
        assert_eq!(
            repository
                .session_owner(&tenant, "u:1", "missing")
                .await
                .unwrap(),
            None
        );
        repository
            .forget_gateway_connection(&gateway, "garbage")
            .await
            .unwrap();

        // 租约回来了：不退役。
        let _: () = conn.set_ex(lease_key(&gateway), "{}", 15).await.unwrap();
        assert!(!repository.retire_gateway(&gateway).await.unwrap());
        assert!(
            conn.exists::<_, bool>(gateway_conns_key(&gateway))
                .await
                .unwrap()
        );
        // 租约缺失：删除索引、移出 gw:known。
        let _: () = conn.del(lease_key(&gateway)).await.unwrap();
        assert!(repository.retire_gateway(&gateway).await.unwrap());
        assert!(
            !conn
                .exists::<_, bool>(gateway_conns_key(&gateway))
                .await
                .unwrap()
        );
        assert!(
            !repository
                .known_gateways()
                .await
                .unwrap()
                .contains(&gateway)
        );

        let _: () = conn.del(&session_key).await.unwrap();
    }
}
