use async_trait::async_trait;
use flare_core::common::DeviceInfo;
use flare_core::server::{AuthResult, Authenticator};
use flare_im_service_kit::tenant_runtime::{TenantAccessPolicy, TenantLookup, TenantRuntimeCache};
use flare_server_core::auth::{AuthenticatedPrincipal, TokenValidationRequest, TokenValidator};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, instrument, warn};

use crate::constants::{
    AUTH_FAILURE_MSG_TENANT_RUNTIME_UNREADABLE, AUTH_FAILURE_MSG_TENANT_UNAVAILABLE,
    AUTH_FAILURE_MSG_TOKEN_INVALID, DEFAULT_TENANT_ID, ENV_DEFAULT_TENANT_ID,
    METADATA_KEY_DEVICE_ID, METADATA_KEY_USER_ID,
};

/// 租户闸门的判定。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TenantAdmission {
    Admitted,
    /// 确定的租户结论：停用 / 删除中，或严格策略下没有投影。客户端收到 `TENANT_UNAVAILABLE`，
    /// 会终止会话、不再重连。
    Rejected,
    /// 严格策略下投影读不到（数据库抖动等）：不是租户结论，只是此刻放不了行。按可重试拒绝，
    /// 客户端收到 `SERVICE_UNAVAILABLE` 后退避重连。
    Unreadable,
}

pub struct AuthHandler {
    token_validator: Arc<dyn TokenValidator>,
    default_tenant_id: String,
    /// 租户投影：token 合法后再看租户能不能接入。
    tenant_runtime: TenantRuntimeCache,
    tenant_policy: TenantAccessPolicy,
}

impl AuthHandler {
    pub fn new(
        token_validator: Arc<dyn TokenValidator>,
        tenant_runtime: TenantRuntimeCache,
        tenant_policy: TenantAccessPolicy,
    ) -> Self {
        Self::with_default_tenant_id(
            token_validator,
            default_tenant_id(),
            tenant_runtime,
            tenant_policy,
        )
    }

    pub fn with_default_tenant_id(
        token_validator: Arc<dyn TokenValidator>,
        default_tenant_id: impl Into<String>,
        tenant_runtime: TenantRuntimeCache,
        tenant_policy: TenantAccessPolicy,
    ) -> Self {
        Self {
            token_validator,
            default_tenant_id: default_tenant_id.into(),
            tenant_runtime,
            tenant_policy,
        }
    }

    /// 租户闸门：`suspended` / `deleting` 一律拒；未投影或投影库不可用按策略（宽松放行、严格拒绝）。
    /// 严格策略下投影库不可用的拒绝是暂时性的，与租户确实不可用分开报。
    async fn tenant_admission(&self, tenant_id: &str, connection_id: &str) -> TenantAdmission {
        let lookup = self.tenant_runtime.lookup(tenant_id).await;
        if lookup.admits_connection(self.tenant_policy) {
            return TenantAdmission::Admitted;
        }
        let (tenant_state, admission) = match &lookup {
            TenantLookup::Known(snapshot) => (snapshot.status.as_str(), TenantAdmission::Rejected),
            TenantLookup::Unknown => ("unknown", TenantAdmission::Rejected),
            TenantLookup::Unavailable => ("unavailable", TenantAdmission::Unreadable),
        };
        if admission == TenantAdmission::Unreadable {
            warn!(
                connection_id = %connection_id,
                tenant_id = %tenant_id,
                tenant_state,
                policy = self.tenant_policy.as_str(),
                "connection rejected for now: tenant runtime projection unreadable (retryable)"
            );
        } else {
            warn!(
                connection_id = %connection_id,
                tenant_id = %tenant_id,
                tenant_state,
                policy = self.tenant_policy.as_str(),
                "connection rejected: tenant unavailable"
            );
        }
        admission
    }

    /// 验证 token（由 server-core auth provider 统一处理 JWT / Hook / SSO）。
    async fn verify_token(
        &self,
        token: &str,
        connection_id: &str,
    ) -> Option<AuthenticatedPrincipal> {
        let request = TokenValidationRequest {
            token: token.to_string(),
            trace_id: Some(connection_id.to_string()),
            request_id: Some(connection_id.to_string()),
            path: Some("flare-signaling/gateway/auth".to_string()),
            method: Some("LONG_CONNECTION_AUTH".to_string()),
        };

        match self.token_validator.validate(request).await {
            Ok(principal) => Some(principal),
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

    fn resolve_tenant_id(&self, principal: &AuthenticatedPrincipal) -> String {
        principal
            .tenant_id
            .as_deref()
            .map(str::trim)
            .filter(|tenant_id| !tenant_id.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| self.default_tenant_id.clone())
    }

    fn resolve_bound_device_id(
        &self,
        principal: &AuthenticatedPrincipal,
        device_info: Option<&DeviceInfo>,
        connection_id: &str,
    ) -> Result<Option<String>, String> {
        let claimed_device_id = principal
            .device_id
            .as_deref()
            .map(str::trim)
            .filter(|device_id| !device_id.is_empty());
        let presented_device_id = device_info
            .map(|device| device.device_id.as_str())
            .map(str::trim)
            .filter(|device_id| !device_id.is_empty());

        if let (Some(claimed), Some(presented)) = (claimed_device_id, presented_device_id)
            && claimed != presented
        {
            warn!(
                connection_id = %connection_id,
                claimed_device_id = %claimed,
                presented_device_id = %presented,
                "Token device binding mismatch"
            );
            return Err(AUTH_FAILURE_MSG_TOKEN_INVALID.to_string());
        }

        Ok(claimed_device_id
            .or(presented_device_id)
            .map(ToOwned::to_owned))
    }
}

#[async_trait]
impl Authenticator for AuthHandler {
    // The token and the CONNECT metadata (which repeats it) must never reach a span.
    #[instrument(
        skip_all,
        fields(
            connection_id = %connection_id,
            token_len = token.len(),
            device_id = tracing::field::Empty,
            platform = tracing::field::Empty
        )
    )]
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

        match self.verify_token(token, connection_id).await {
            Some(principal) => {
                let user_id = principal.user_id.clone();
                let tenant_id = self.resolve_tenant_id(&principal);
                match self.tenant_admission(&tenant_id, connection_id).await {
                    TenantAdmission::Admitted => {}
                    TenantAdmission::Rejected => {
                        return Ok(AuthResult::failure(
                            AUTH_FAILURE_MSG_TENANT_UNAVAILABLE.to_string(),
                        ));
                    }
                    // flare-core 以 SERVICE_UNAVAILABLE 拒绝 CONNECT；说明文本不下发给客户端
                    TenantAdmission::Unreadable => {
                        return Ok(AuthResult::retryable_failure(
                            AUTH_FAILURE_MSG_TENANT_RUNTIME_UNREADABLE.to_string(),
                        ));
                    }
                }
                let device_id =
                    match self.resolve_bound_device_id(&principal, device_info, connection_id) {
                        Ok(device_id) => device_id,
                        Err(reason) => return Ok(AuthResult::failure(reason)),
                    };

                // 构建用户元数据（包含 tenant_id、device_id 等信息）
                let mut user_metadata = std::collections::HashMap::new();
                user_metadata.insert(METADATA_KEY_USER_ID.to_string(), user_id.clone());

                if let Some(device_id) = device_id {
                    user_metadata.insert(METADATA_KEY_DEVICE_ID.to_string(), device_id);
                }

                debug!( connection_id = %connection_id,user_id = %user_id,tenant_id = %tenant_id,"✅ Token 验证成功，租户ID已设置");
                // 关键：已验证的租户总是存在（令牌没带就用默认租户）。flare-core 用它给设备冲突
                // 按（租户，用户）分桶，并写进连接元数据；两个租户里 user_id 相同的用户不会互踢。
                Ok(
                    AuthResult::success_with_metadata(Some(user_id), user_metadata)
                        .with_tenant_id(tenant_id),
                )
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

fn default_tenant_id() -> String {
    std::env::var(ENV_DEFAULT_TENANT_ID)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_TENANT_ID.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::METADATA_KEY_TENANT_ID;
    use flare_core::common::DevicePlatform;
    use flare_im_contracts::domain::tenant::{TenantRuntimeSnapshot, TenantStatus};
    use flare_im_service_kit::tenant_runtime::{TenantRuntimeCacheOptions, TenantRuntimeSource};
    use flare_server_core::auth::{AuthError, AuthenticatedPrincipal};
    use std::sync::Mutex;

    struct StaticTokenValidator {
        principal: Option<AuthenticatedPrincipal>,
    }

    /// 内存租户投影：`None` = 未投影。
    struct StaticTenants(Mutex<HashMap<String, TenantStatus>>);

    #[async_trait]
    impl TenantRuntimeSource for StaticTenants {
        async fn load(&self, tenant_id: &str) -> Result<Option<TenantRuntimeSnapshot>, String> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .get(tenant_id)
                .map(|status| TenantRuntimeSnapshot {
                    tenant_id: tenant_id.to_string(),
                    status: *status,
                    quota: Default::default(),
                    version: 1,
                }))
        }
    }

    /// 投影库读不到（如 PG 抖动）。
    struct UnreachableTenants;

    #[async_trait]
    impl TenantRuntimeSource for UnreachableTenants {
        async fn load(&self, _tenant_id: &str) -> Result<Option<TenantRuntimeSnapshot>, String> {
            Err("connection refused".to_string())
        }
    }

    fn unreachable_tenants() -> TenantRuntimeCache {
        TenantRuntimeCache::with_source(
            Arc::new(UnreachableTenants),
            TenantRuntimeCacheOptions::default(),
        )
    }

    fn tenants(rows: &[(&str, TenantStatus)]) -> TenantRuntimeCache {
        TenantRuntimeCache::with_source(
            Arc::new(StaticTenants(Mutex::new(
                rows.iter().map(|(id, s)| (id.to_string(), *s)).collect(),
            ))),
            TenantRuntimeCacheOptions::default(),
        )
    }

    fn handler(
        principal: Option<AuthenticatedPrincipal>,
        tenant_runtime: TenantRuntimeCache,
        policy: TenantAccessPolicy,
    ) -> AuthHandler {
        AuthHandler::with_default_tenant_id(
            Arc::new(StaticTokenValidator { principal }),
            "tenant-default",
            tenant_runtime,
            policy,
        )
    }

    #[async_trait]
    impl TokenValidator for StaticTokenValidator {
        async fn validate(
            &self,
            _request: TokenValidationRequest,
        ) -> Result<AuthenticatedPrincipal, AuthError> {
            self.principal
                .clone()
                .ok_or_else(|| AuthError::InvalidToken("test token rejected".to_string()))
        }
    }

    fn principal(device_id: Option<&str>) -> AuthenticatedPrincipal {
        AuthenticatedPrincipal {
            user_id: "user-a".to_string(),
            tenant_id: Some("tenant-a".to_string()),
            device_id: device_id.map(ToOwned::to_owned),
            app_id: None,
            expires_at: None,
            scopes: Vec::new(),
            metadata: Default::default(),
        }
    }

    // 「令牌不进日志」的用例在 tests/auth_log_redaction.rs：它断言日志输出，要独占进程，
    // 和这里并行跑的用例共享 tracing 的 callsite 缓存会间歇失败。

    #[tokio::test]
    async fn authenticate_injects_principal_context() {
        let handler = handler(
            Some(principal(Some("device-a"))),
            TenantRuntimeCache::disabled(),
            TenantAccessPolicy::Lenient,
        );
        let device = DeviceInfo::new("device-a".to_string(), DevicePlatform::Web);

        let result = handler
            .authenticate("token", "conn-a", Some(&device), None)
            .await
            .expect("auth result");

        assert!(result.authenticated);
        assert_eq!(result.user_id.as_deref(), Some("user-a"));
        // flare-core 按这个租户给设备冲突分桶
        assert_eq!(result.tenant_id(), Some("tenant-a"));
        let metadata = result.user_metadata.expect("metadata");
        assert_eq!(
            metadata.get(METADATA_KEY_USER_ID).map(String::as_str),
            Some("user-a")
        );
        assert_eq!(
            metadata.get(METADATA_KEY_TENANT_ID).map(String::as_str),
            Some("tenant-a")
        );
        assert_eq!(
            metadata.get(METADATA_KEY_DEVICE_ID).map(String::as_str),
            Some("device-a")
        );
    }

    #[tokio::test]
    async fn authenticate_rejects_device_binding_mismatch() {
        let handler = handler(
            Some(principal(Some("device-a"))),
            TenantRuntimeCache::disabled(),
            TenantAccessPolicy::Lenient,
        );
        let device = DeviceInfo::new("device-b".to_string(), DevicePlatform::Web);

        let result = handler
            .authenticate("token", "conn-a", Some(&device), None)
            .await
            .expect("auth result");

        assert!(!result.authenticated);
        assert_eq!(
            result.error_message.as_deref(),
            Some(AUTH_FAILURE_MSG_TOKEN_INVALID)
        );
    }

    #[tokio::test]
    async fn authenticate_uses_default_tenant_when_principal_has_none() {
        let mut principal = principal(None);
        principal.tenant_id = None;
        let handler = handler(
            Some(principal),
            TenantRuntimeCache::disabled(),
            TenantAccessPolicy::Lenient,
        );

        let result = handler
            .authenticate("token", "conn-a", None, None)
            .await
            .expect("auth result");

        assert!(result.authenticated);
        assert_eq!(result.tenant_id(), Some("tenant-default"));
        let metadata = result.user_metadata.expect("metadata");
        assert_eq!(
            metadata.get(METADATA_KEY_TENANT_ID).map(String::as_str),
            Some("tenant-default")
        );
    }

    #[tokio::test]
    async fn suspended_and_deleting_tenants_are_rejected_regardless_of_policy() {
        for status in [TenantStatus::Suspended, TenantStatus::Deleting] {
            for policy in [TenantAccessPolicy::Lenient, TenantAccessPolicy::Strict] {
                let handler = handler(
                    Some(principal(Some("device-a"))),
                    tenants(&[("tenant-a", status)]),
                    policy,
                );
                let result = handler
                    .authenticate("token", "conn-a", None, None)
                    .await
                    .expect("auth result");
                assert!(!result.authenticated, "{status:?}/{policy:?}");
                assert!(!result.retryable, "{status:?}/{policy:?}");
                assert_eq!(
                    result.error_message.as_deref(),
                    Some(AUTH_FAILURE_MSG_TENANT_UNAVAILABLE)
                );
            }
        }
    }

    /// 严格策略下投影库读不到：仍然拒绝（fail-closed），但这是暂时性拒绝，不是 `TENANT_UNAVAILABLE`。
    /// 客户端收到 `TENANT_UNAVAILABLE` 会终止会话、不再重连，一次数据库抖动就会让这段时间里
    /// 连进来的客户端一直离线；暂时性拒绝经 flare-core 变成 `SERVICE_UNAVAILABLE`，客户端退避重连。
    #[tokio::test]
    async fn projection_outage_under_strict_is_a_retryable_rejection() {
        let handler = handler(
            Some(principal(Some("device-a"))),
            unreachable_tenants(),
            TenantAccessPolicy::Strict,
        );
        let result = handler
            .authenticate("token", "conn-a", None, None)
            .await
            .expect("auth result");

        assert!(!result.authenticated);
        assert!(result.retryable, "a projection outage is not a verdict");
        assert_ne!(
            result.error_message.as_deref(),
            Some(AUTH_FAILURE_MSG_TENANT_UNAVAILABLE)
        );
        assert_eq!(
            result.error_message.as_deref(),
            Some(AUTH_FAILURE_MSG_TENANT_RUNTIME_UNREADABLE)
        );
    }

    /// 宽松策略下投影库读不到照常放行（会话登记时 Online 再判一次）。
    #[tokio::test]
    async fn projection_outage_under_lenient_admits() {
        let handler = handler(
            Some(principal(Some("device-a"))),
            unreachable_tenants(),
            TenantAccessPolicy::Lenient,
        );
        let result = handler
            .authenticate("token", "conn-a", None, None)
            .await
            .expect("auth result");

        assert!(result.authenticated);
        assert_eq!(result.tenant_id(), Some("tenant-a"));
    }

    #[tokio::test]
    async fn active_tenant_is_admitted_under_strict() {
        let handler = handler(
            Some(principal(Some("device-a"))),
            tenants(&[("tenant-a", TenantStatus::Active)]),
            TenantAccessPolicy::Strict,
        );
        let result = handler
            .authenticate("token", "conn-a", None, None)
            .await
            .expect("auth result");
        assert!(result.authenticated);
    }

    #[tokio::test]
    async fn unknown_tenant_follows_policy() {
        let lenient = handler(
            Some(principal(Some("device-a"))),
            tenants(&[]),
            TenantAccessPolicy::Lenient,
        );
        assert!(
            lenient
                .authenticate("token", "conn-a", None, None)
                .await
                .unwrap()
                .authenticated
        );

        let strict = handler(
            Some(principal(Some("device-a"))),
            tenants(&[]),
            TenantAccessPolicy::Strict,
        );
        let result = strict
            .authenticate("token", "conn-a", None, None)
            .await
            .unwrap();
        assert!(!result.authenticated);
        assert!(!result.retryable, "no projection under strict is a verdict");
        assert_eq!(
            result.error_message.as_deref(),
            Some(AUTH_FAILURE_MSG_TENANT_UNAVAILABLE)
        );
    }

    #[tokio::test]
    async fn invalid_token_is_still_reported_as_token_error_not_tenant() {
        let handler = handler(None, tenants(&[]), TenantAccessPolicy::Strict);
        let result = handler
            .authenticate("token", "conn-a", None, None)
            .await
            .unwrap();
        assert!(!result.authenticated);
        assert_eq!(
            result.error_message.as_deref(),
            Some(AUTH_FAILURE_MSG_TOKEN_INVALID)
        );
    }
}
