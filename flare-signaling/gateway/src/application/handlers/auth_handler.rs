use async_trait::async_trait;
use flare_core::common::DeviceInfo;
use flare_core::server::{AuthResult, Authenticator};
use flare_server_core::auth::{CompositeTokenValidator, TokenService};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, instrument, warn};

use crate::constants::{
    AUTH_FAILURE_MSG_TOKEN_INVALID, DEFAULT_TENANT_ID, ENV_DEFAULT_TENANT_ID,
    METADATA_KEY_DEVICE_ID, METADATA_KEY_TENANT_ID, METADATA_KEY_USER_ID,
};

pub struct AuthHandler {
    token_validator: CompositeTokenValidator,
}

impl AuthHandler {
    pub fn new(token_service: Arc<TokenService>) -> Self {
        Self {
            token_validator: CompositeTokenValidator::new(token_service),
        }
    }

    pub fn with_trusted_issuers(
        token_service: Arc<TokenService>,
        trusted: &[(String, String)],
    ) -> Self {
        let mut validator = CompositeTokenValidator::new(token_service);
        for (secret, issuer) in trusted {
            validator.push_trusted_issuer(secret.clone(), issuer.clone());
        }
        Self {
            token_validator: validator,
        }
    }

    /// 验证 token（主 issuer + 可选信任 issuer）
    fn verify_token(&self, token: &str) -> Option<flare_server_core::TokenClaims> {
        match self.token_validator.validate_token(token) {
            Ok(claims) => Some(claims),
            Err(err) => {
                warn!(?err, "Token validation failed");
                None
            }
        }
    }

    /// 获取 token 预览（用于日志记录）
    fn token_preview(&self, token: &str) -> String {
        if token.len() > 12 {
            format!("{}...", &token[..12])
        } else {
            token.to_string()
        }
    }
}

#[async_trait]
impl Authenticator for AuthHandler {
    #[instrument(skip(self), fields(connection_id, token_len = token.len()))]
    async fn authenticate(
        &self,
        token: &str,
        connection_id: &str,
        device_info: Option<&DeviceInfo>,
        _metadata: Option<&HashMap<String, Vec<u8>>>,
    ) -> flare_core::client::Result<AuthResult> {
        // 记录设备信息（用于 tracing 层级）
        if let Some(device) = device_info {
            tracing::Span::current().record("device_id", &device.device_id);
            tracing::Span::current().record("platform", device.platform.as_str());
        }

        debug!( connection_id = %connection_id,token_preview = %self.token_preview(token), device_id = ?device_info.map(|d| d.device_id.clone()),"验证 token");

        match self.verify_token(token) {
            Some(claims) => {
                let user_id = claims.sub.clone();

                // 构建用户元数据（包含 tenant_id、device_id 等信息）
                // 关键：确保 tenant_id 总是存在，如果没有则使用默认值
                let mut user_metadata = std::collections::HashMap::new();
                user_metadata.insert(METADATA_KEY_USER_ID.to_string(), user_id.clone());

                // 从 token claims 提取 tenant_id，如果没有则使用默认值
                let tenant_id = claims.tenant_id.unwrap_or_else(|| {
                    std::env::var(ENV_DEFAULT_TENANT_ID)
                        .ok()
                        .unwrap_or_else(|| DEFAULT_TENANT_ID.to_string())
                });
                user_metadata.insert(METADATA_KEY_TENANT_ID.to_string(), tenant_id.clone());

                if let Some(device_id) = claims.device_id {
                    user_metadata.insert(METADATA_KEY_DEVICE_ID.to_string(), device_id);
                }

                debug!( connection_id = %connection_id,user_id = %user_id,tenant_id = %tenant_id,"✅ Token 验证成功，租户ID已设置");
                Ok(AuthResult::success_with_metadata(
                    Some(user_id),
                    user_metadata,
                ))
            }
            None => {
                warn!(
                    connection_id = %connection_id,
                    token_preview = %self.token_preview(token),
                    "❌ Token 验证失败"
                );
                Ok(AuthResult::failure(
                    AUTH_FAILURE_MSG_TOKEN_INVALID.to_string(),
                ))
            }
        }
    }
}
