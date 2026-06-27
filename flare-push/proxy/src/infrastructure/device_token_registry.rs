//! Redis-backed device push token registry writer.

use std::sync::Arc;

use flare_im_contracts::{
    DevicePushToken, device_push_token_registry_field, device_push_token_registry_key,
};
use flare_server_core::error::Result;
use redis::AsyncCommands;

use crate::config::PushProxyConfig;

pub struct RedisDeviceTokenRegistry {
    client: redis::Client,
    key_prefix: String,
}

impl RedisDeviceTokenRegistry {
    pub fn new(config: Arc<PushProxyConfig>) -> Result<Self> {
        let client = redis::Client::open(config.device_token_redis_url.as_str())?;
        Ok(Self {
            client,
            key_prefix: config.device_token_key_prefix.clone(),
        })
    }

    async fn conn(&self) -> Result<redis::aio::MultiplexedConnection> {
        Ok(self.client.get_multiplexed_tokio_connection().await?)
    }

    pub async fn register(&self, token: &DevicePushToken) -> Result<()> {
        let key =
            device_push_token_registry_key(&self.key_prefix, &token.tenant_id, &token.user_id);
        let field = device_push_token_registry_field(&token.provider, &token.device_id);
        let value = serde_json::to_string(token)?;
        let mut conn = self.conn().await?;
        let _: () = conn.hset(key, field, value).await?;
        Ok(())
    }

    pub async fn unregister(
        &self,
        tenant_id: &str,
        user_id: &str,
        provider: &str,
        device_id: &str,
    ) -> Result<()> {
        let key = device_push_token_registry_key(&self.key_prefix, tenant_id, user_id);
        let field = device_push_token_registry_field(provider, device_id);
        let mut conn = self.conn().await?;
        let _: () = conn.hdel(key, field).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_field_is_provider_scoped() {
        assert_eq!(
            device_push_token_registry_field("GETUI", "ios-1"),
            "getui:ios-1"
        );
    }
}
