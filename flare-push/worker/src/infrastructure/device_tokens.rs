//! Redis-backed mobile push token registry.
//!
//! Each user owns a hash keyed by tenant/user. The field is the client device id
//! and the value is a serialized [`DevicePushToken`]. Vendor-specific identifiers
//! such as Getui CID stay outside `PushTaskEnvelope`.

use flare_im_contracts::{
    Ctx, DevicePushToken, device_push_token_registry_field, device_push_token_registry_key,
};
use flare_server_core::error::{ErrorCode, FlareError, map_infra_error};
use redis::aio::ConnectionManager;

use crate::domain::DeviceTokenRepository;

pub struct RedisDeviceTokenRepository {
    conn: ConnectionManager,
    key_prefix: String,
}

impl RedisDeviceTokenRepository {
    pub async fn connect(redis_url: &str, key_prefix: String) -> Result<Self, FlareError> {
        let client = redis::Client::open(redis_url).map_err(|error| {
            FlareError::system(format!("device token redis url invalid: {error}"))
        })?;
        let conn = client.get_connection_manager().await.map_err(|error| {
            map_infra_error(
                error,
                ErrorCode::ServiceUnavailable,
                "device token redis connect",
            )
        })?;
        Ok(Self { conn, key_prefix })
    }
}

#[async_trait::async_trait]
impl DeviceTokenRepository for RedisDeviceTokenRepository {
    async fn list_user_tokens(
        &self,
        _ctx: &Ctx,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<Vec<DevicePushToken>, FlareError> {
        let key = device_push_token_registry_key(&self.key_prefix, tenant_id, user_id);
        let mut conn = self.conn.clone();
        let values: Vec<String> = redis::cmd("HVALS")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .map_err(|error| {
                map_infra_error(
                    error,
                    ErrorCode::ServiceUnavailable,
                    "device token HVALS failed",
                )
            })?;

        let mut tokens = Vec::with_capacity(values.len());
        for value in values {
            match decode_device_token_record(&value) {
                Ok(token) if token.tenant_id == tenant_id && token.user_id == user_id => {
                    tokens.push(token)
                }
                Ok(token) => {
                    tracing::warn!(
                        key = %key,
                        token_tenant_id = %token.tenant_id,
                        token_user_id = %token.user_id,
                        "device token registry record does not match requested owner"
                    );
                }
                Err(error) => {
                    tracing::warn!(key = %key, error = %error, "invalid device token record");
                }
            }
        }
        Ok(tokens)
    }

    async fn remove_device_token(
        &self,
        _ctx: &Ctx,
        token: &DevicePushToken,
    ) -> Result<(), FlareError> {
        let key =
            device_push_token_registry_key(&self.key_prefix, &token.tenant_id, &token.user_id);
        let field = device_push_token_registry_field(&token.provider, &token.device_id);
        let mut conn = self.conn.clone();
        let _: () = redis::cmd("HDEL")
            .arg(&key)
            .arg(&field)
            .query_async(&mut conn)
            .await
            .map_err(|error| {
                map_infra_error(
                    error,
                    ErrorCode::ServiceUnavailable,
                    "device token HDEL failed",
                )
            })?;
        Ok(())
    }
}

pub fn decode_device_token_record(value: &str) -> Result<DevicePushToken, FlareError> {
    serde_json::from_str::<DevicePushToken>(value).map_err(|error| {
        map_infra_error(
            error,
            ErrorCode::InvalidParameter,
            "invalid device token registry record",
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_token_key_is_tenant_and_user_scoped() {
        assert_eq!(
            device_push_token_registry_key("flare:im:push:device_tokens:", "tenant-a", "user-1"),
            "flare:im:push:device_tokens:tenant-a:user-1"
        );
    }

    #[test]
    fn device_token_record_decodes_strict_shape() {
        let record = r#"{"tenant_id":"tenant-a","user_id":"user-1","device_id":"ios-1","platform":"ios","provider":"getui","token":"cid-1"}"#;
        let token = decode_device_token_record(record).expect("decode");

        assert_eq!(token.tenant_id, "tenant-a");
        assert!(token.usable_for_provider("getui"));
        assert!(!token.usable_for_provider("apns"));
    }
}
