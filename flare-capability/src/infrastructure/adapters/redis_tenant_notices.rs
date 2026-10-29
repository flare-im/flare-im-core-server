//! Tenant notices on the Redis channel `{namespace}:notice` of the token store, which every
//! signaling gateway subscribes to (see `flare_im_service_kit::tenant_notice`).
use crate::domain::repository::tenant_notice::TenantNotices;
use flare_im_service_kit::redis_connection::{RedisConnectionLimits, SharedRedisConnection};
use flare_im_service_kit::tenant_notice::TenantNotice;

pub struct RedisTenantNotices {
    connection: SharedRedisConnection,
    channel: String,
}

impl RedisTenantNotices {
    pub fn new(client: redis::Client, channel: String) -> Self {
        Self {
            connection: SharedRedisConnection::new(client, RedisConnectionLimits::default()),
            channel,
        }
    }

    async fn publish(&self, payload: String) -> redis::RedisResult<()> {
        let mut connection = self.connection.get().await?;
        redis::cmd("PUBLISH")
            .arg(&self.channel)
            .arg(payload)
            .query_async::<i64>(&mut connection)
            .await
            .map(|_| ())
    }
}

#[async_trait::async_trait]
impl TenantNotices for RedisTenantNotices {
    async fn plugin_catalog_changed(&self, tenant_id: &str, version: u64) {
        let payload = match serde_json::to_string(&TenantNotice::plugin_catalog_changed(
            tenant_id, version,
        )) {
            Ok(payload) => payload,
            Err(error) => {
                tracing::warn!(%error, %tenant_id, "plugin catalog change notice not encodable");
                return;
            }
        };
        match self.publish(payload).await {
            Ok(()) => {
                tracing::debug!(%tenant_id, version, "plugin catalog change notice published")
            }
            Err(error) => tracing::warn!(
                %error,
                %tenant_id,
                version,
                "plugin catalog change notice not published; clients refresh on reconnect"
            ),
        }
    }
}
