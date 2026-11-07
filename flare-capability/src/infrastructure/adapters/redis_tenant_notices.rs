//! Tenant notices on the Redis channel `{namespace}:notice` of the token store, which every
//! signaling gateway subscribes to (see `flare_im_service_kit::tenant_notice`). The last published
//! version of each tenant and topic sits next to the channel; one script compares, records and
//! publishes, so retries and concurrent replicas announce a version once.
use crate::domain::model::NoticeOutcome;
use crate::domain::repository::tenant_notice::TenantNotices;
use flare_im_service_kit::redis_connection::{RedisConnectionLimits, SharedRedisConnection};
use flare_im_service_kit::tenant_notice::TenantNotice;
use flare_server_core::error::{FlareError, Result};

/// How long the last published version of a topic is remembered.
const LAST_VERSION_TTL_SECS: u64 = 30 * 24 * 3600;

/// KEYS[1] last version; ARGV: version, notice JSON, TTL seconds, channel.
const PUBLISH_IF_NEWER: &str = r#"
local current = redis.call('GET', KEYS[1])
if current and tonumber(current) >= tonumber(ARGV[1]) then
  return {0, current}
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[3])
redis.call('PUBLISH', ARGV[4], ARGV[2])
return {1, ARGV[1]}
"#;

pub struct RedisTenantNotices {
    connection: SharedRedisConnection,
    channel: String,
    script: redis::Script,
}

impl RedisTenantNotices {
    pub fn new(client: redis::Client, channel: String) -> Self {
        Self {
            connection: SharedRedisConnection::new(client, RedisConnectionLimits::default()),
            channel,
            script: redis::Script::new(PUBLISH_IF_NEWER),
        }
    }

    fn last_version_key(&self, tenant_id: &str, topic: &str) -> String {
        format!("{}:last:{tenant_id}:{topic}", self.channel)
    }
}

#[async_trait::async_trait]
impl TenantNotices for RedisTenantNotices {
    async fn publish(&self, tenant_id: &str, topic: &str, version: u64) -> Result<NoticeOutcome> {
        let payload = serde_json::to_string(&TenantNotice::new(tenant_id, topic, version))
            .map_err(|_| FlareError::system("tenant notice not encodable"))?;
        let mut connection = self
            .connection
            .get()
            .await
            .map_err(|_| FlareError::system("tenant notice Redis unavailable"))?;
        let (accepted, current): (i64, String) = self
            .script
            .key(self.last_version_key(tenant_id, topic))
            .arg(version)
            .arg(payload)
            .arg(LAST_VERSION_TTL_SECS)
            .arg(&self.channel)
            .invoke_async(&mut connection)
            .await
            .map_err(|_| FlareError::system("tenant notice not published"))?;
        let outcome = NoticeOutcome {
            accepted: accepted == 1,
            current_version: current.parse().unwrap_or(version),
        };
        tracing::debug!(%tenant_id, %topic, version, accepted = outcome.accepted, "tenant notice");
        Ok(outcome)
    }
}
