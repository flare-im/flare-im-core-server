use async_trait::async_trait;
use flare_core::common::DeviceInfo;
use flare_core::common::constants::metadata_keys;
use flare_core::server::{AuthResult, Authenticator};
use flare_im_contracts::utils::{normalize_optional_tenant_id, normalize_tenant_id};
use flare_im_service_kit::tenant_runtime::{TenantAccessPolicy, TenantLookup, TenantRuntimeCache};
use flare_server_core::auth::{
    AuthError, AuthenticatedPrincipal, TokenValidationRequest, TokenValidator,
};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, instrument, warn};

use crate::constants::{
    AUTH_FAILURE_MSG_AUTH_PROVIDER_UNAVAILABLE, AUTH_FAILURE_MSG_TENANT_RUNTIME_UNREADABLE,
    AUTH_FAILURE_MSG_TENANT_UNAVAILABLE, AUTH_FAILURE_MSG_TOKEN_INVALID, DEFAULT_TENANT_ID,
    ENV_DEFAULT_TENANT_ID, METADATA_KEY_APP_BUILD, METADATA_KEY_DEVICE_ID,
    METADATA_KEY_DISTRIBUTION, METADATA_KEY_USER_ID,
};
use crate::domain::service::client_build_gate::{
    UpgradeRequired, gate_platform, reported_build, upgrade_verdict,
};

/// 令牌校验的结论。
enum TokenVerdict {
    Valid(AuthenticatedPrincipal),
    /// 令牌缺失、无效、过期，或主体不被允许：这是对令牌的结论，客户端应换令牌。
    Rejected,
    /// 认证提供方暂时不可用（如 hook 请求失败）：没有对令牌下结论。按可重试拒绝，客户端收到
    /// `SERVICE_UNAVAILABLE` 后退避重连；报成令牌无效会让客户端去换一个本来有效的令牌。
    ProviderUnavailable,
}

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
    metrics: Option<Arc<flare_im_service_kit::metrics::AccessGatewayMetrics>>,
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
            metrics: None,
        }
    }

    /// 每次放行时记录客户端上报的接入清单 serial 与投影的差距。
    #[must_use]
    pub fn with_metrics(
        mut self,
        metrics: Arc<flare_im_service_kit::metrics::AccessGatewayMetrics>,
    ) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// 这条连接的 CONNECT_ACK 要报的接入清单 serial：应用投影的最新值，读不到是 0。
    async fn latest_access_serial(&self, tenant_id: &str, device_info: Option<&DeviceInfo>) -> u64 {
        let latest = self.tenant_runtime.access_serial(tenant_id).await;
        if let Some(metrics) = &self.metrics {
            let reported = device_info
                .and_then(|device| device.metadata.get(metadata_keys::ACCESS_SERIAL))
                .and_then(|serial| serial.trim().parse().ok());
            metrics.record_connect_access_serial(tenant_id, reported, latest);
        }
        latest
    }

    /// 记一次被拒的 CONNECT（`handshake_rejected_total{reason}`）。
    fn record_rejection(&self, reason: &str) {
        if let Some(metrics) = &self.metrics {
            metrics.record_handshake_rejected(reason);
        }
    }

    /// 版本门禁：按应用的客户端策略判定这台设备的构建是否必须升级。没有设备信息、平台没有发布清单
    /// （鸿蒙等）或应用没有策略时放行；没报构建号按 0 判。
    async fn upgrade_gate(
        &self,
        tenant_id: &str,
        device_info: Option<&DeviceInfo>,
        connection_id: &str,
    ) -> Option<UpgradeRequired> {
        let device = device_info?;
        let distribution = device
            .metadata
            .get(METADATA_KEY_DISTRIBUTION)
            .map(String::as_str);
        let platform = gate_platform(device.platform.as_str(), distribution)?;
        let policy = self.tenant_runtime.client_policy(tenant_id).await?;
        let build = reported_build(
            device
                .metadata
                .get(METADATA_KEY_APP_BUILD)
                .map(String::as_str),
        );
        let verdict = upgrade_verdict(&policy, platform, build, now_ms())?;
        info!(
            connection_id = %connection_id,
            tenant_id = %tenant_id,
            platform,
            app_build = build,
            min_build = verdict.min_build,
            reason = verdict.reason.metric_label(),
            "connection rejected: client build must be upgraded"
        );
        Some(verdict)
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
    async fn verify_token(&self, token: &str, connection_id: &str) -> TokenVerdict {
        let request = TokenValidationRequest {
            token: token.to_string(),
            trace_id: Some(connection_id.to_string()),
            request_id: Some(connection_id.to_string()),
            path: Some("flare-signaling/gateway/auth".to_string()),
            method: Some("LONG_CONNECTION_AUTH".to_string()),
        };

        match self.token_validator.validate(request).await {
            Ok(principal) => TokenVerdict::Valid(principal),
            Err(err @ AuthError::ProviderUnavailable(_)) => {
                warn!(
                    ?err,
                    "auth provider unavailable, connection rejected for now (retryable)"
                );
                TokenVerdict::ProviderUnavailable
            }
            Err(err) => {
                warn!(?err, "Token validation failed");
                TokenVerdict::Rejected
            }
        }
    }

    /// 令牌在日志里的指纹：SHA-256 的前 8 个十六进制字符，不可逆，足以把同一枚令牌的多次失败
    /// 对上。令牌本身的任何片段都不进日志：以前记前 12 个字符，对不透明令牌那可能就是秘密的一部分
    /// （按字节切片遇到多字节字符还会 panic）。
    fn token_fingerprint(token: &str) -> String {
        use sha2::{Digest, Sha256};
        Sha256::digest(token.as_bytes())[..4]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    /// 连接的租户：令牌里的租户，没有就用网关的默认租户；两者都按 `normalize_tenant_id` 归一，
    /// `default` 与 `0` 是同一个默认租户。这个值会写进连接元数据，flare-core 按它给设备冲突、
    /// 按用户投递分桶，网关的连接查询也按它精确比对，所以必须与租户闸门、Online 用的写法一致。
    fn resolve_tenant_id(&self, principal: &AuthenticatedPrincipal) -> String {
        normalize_optional_tenant_id(principal.tenant_id.as_deref())
            .unwrap_or_else(|| normalize_tenant_id(&self.default_tenant_id))
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

        debug!(
            connection_id = %connection_id,
            token_len = token.len(),
            token_sha256 = %Self::token_fingerprint(token),
            device_id = ?device_info.map(|d| d.device_id.clone()),
            "验证 token"
        );

        match self.verify_token(token, connection_id).await {
            TokenVerdict::Valid(principal) => {
                let user_id = principal.user_id.clone();
                let tenant_id = self.resolve_tenant_id(&principal);
                match self.tenant_admission(&tenant_id, connection_id).await {
                    TenantAdmission::Admitted => {}
                    TenantAdmission::Rejected => {
                        self.record_rejection("tenant_unavailable");
                        return Ok(AuthResult::failure(
                            AUTH_FAILURE_MSG_TENANT_UNAVAILABLE.to_string(),
                        ));
                    }
                    // flare-core 以 SERVICE_UNAVAILABLE 拒绝 CONNECT；说明文本不下发给客户端
                    TenantAdmission::Unreadable => {
                        self.record_rejection("tenant_unreadable");
                        return Ok(AuthResult::retryable_failure(
                            AUTH_FAILURE_MSG_TENANT_RUNTIME_UNREADABLE.to_string(),
                        ));
                    }
                }
                let device_id =
                    match self.resolve_bound_device_id(&principal, device_info, connection_id) {
                        Ok(device_id) => device_id,
                        Err(reason) => {
                            self.record_rejection("device_binding_mismatch");
                            return Ok(AuthResult::failure(reason));
                        }
                    };
                // 版本门禁在租户准入之后：停用的租户先按租户不可用拒，不提示升级。
                // flare-core 以 UPGRADE_REQUIRED 拒绝 CONNECT，客户端进入强制更新、不重连。
                if let Some(upgrade) = self
                    .upgrade_gate(&tenant_id, device_info, connection_id)
                    .await
                {
                    self.record_rejection(upgrade.reason.metric_label());
                    // `release_hint` 恒为空：网关手里只有投影的构建策略，没有发布清单（版本说明、
                    // 下载入口都在客户端已验签的发布清单里，客户端按 min_build 从那里取）。
                    return Ok(AuthResult::upgrade_required(upgrade.min_build, None));
                }

                // 构建用户元数据（包含 tenant_id、device_id 等信息）
                let mut user_metadata = std::collections::HashMap::new();
                user_metadata.insert(METADATA_KEY_USER_ID.to_string(), user_id.clone());

                if let Some(device_id) = device_id {
                    user_metadata.insert(METADATA_KEY_DEVICE_ID.to_string(), device_id);
                }

                // Only the authenticated provider response may establish a Social sid.
                // CONNECT/device metadata is never consulted for this binding.
                if principal
                    .metadata
                    .get("identity_issuer")
                    .map(String::as_str)
                    == Some("flare-im-identity")
                {
                    let sid = principal
                        .metadata
                        .get("identity_session_id")
                        .filter(|sid| uuid::Uuid::parse_str(sid).is_ok());
                    let Some(sid) = sid else {
                        return Ok(AuthResult::failure(
                            AUTH_FAILURE_MSG_TOKEN_INVALID.to_string(),
                        ));
                    };
                    user_metadata.insert("identity_session_id".into(), sid.clone());
                }
                let access_serial = self.latest_access_serial(&tenant_id, device_info).await;
                debug!( connection_id = %connection_id,user_id = %user_id,tenant_id = %tenant_id,"Token 验证成功，租户ID已设置");
                // 关键：已验证的租户总是存在（令牌没带就用默认租户）。flare-core 用它给设备冲突
                // 按（租户，用户）分桶，并写进连接元数据；两个租户里 user_id 相同的用户不会互踢。
                Ok(
                    AuthResult::success_with_metadata(Some(user_id), user_metadata)
                        .with_tenant_id(tenant_id)
                        .with_access_serial(access_serial),
                )
            }
            TokenVerdict::Rejected => {
                warn!(
                    connection_id = %connection_id,
                    token_len = token.len(),
                    token_sha256 = %Self::token_fingerprint(token),
                    "Token 验证失败"
                );
                self.record_rejection("token_invalid");
                Ok(AuthResult::failure(
                    AUTH_FAILURE_MSG_TOKEN_INVALID.to_string(),
                ))
            }
            // flare-core 以 SERVICE_UNAVAILABLE 拒绝 CONNECT；说明文本不下发给客户端
            TokenVerdict::ProviderUnavailable => {
                self.record_rejection("auth_provider_unavailable");
                Ok(AuthResult::retryable_failure(
                    AUTH_FAILURE_MSG_AUTH_PROVIDER_UNAVAILABLE.to_string(),
                ))
            }
        }
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
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
    use flare_im_contracts::domain::tenant::{
        TenantClientPolicy, TenantRuntimeSnapshot, TenantStatus,
    };
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

    /// `default` 与 `0` 是同一个默认租户：令牌里的租户与网关默认租户都按 normalize_tenant_id 归一，
    /// 否则同一个默认租户的用户会因为写法不同落进两个桶，设备冲突与按用户投递都对不上。
    #[tokio::test]
    async fn authenticate_normalizes_the_default_tenant_alias() {
        for (principal_tenant, gateway_default, expected) in [
            (Some("default"), "tenant-default", "0"),
            (Some(" default "), "tenant-default", "0"),
            (Some(" tenant-a "), "tenant-default", "tenant-a"),
            (None, "default", "0"),
            (Some("  "), "default", "0"),
        ] {
            let mut principal = principal(None);
            principal.tenant_id = principal_tenant.map(ToOwned::to_owned);
            let handler = AuthHandler::with_default_tenant_id(
                Arc::new(StaticTokenValidator {
                    principal: Some(principal),
                }),
                gateway_default,
                TenantRuntimeCache::disabled(),
                TenantAccessPolicy::Lenient,
            );

            let result = handler
                .authenticate("token", "conn-a", None, None)
                .await
                .expect("auth result");

            assert!(result.authenticated, "{principal_tenant:?}");
            assert_eq!(result.tenant_id(), Some(expected), "{principal_tenant:?}");
            assert_eq!(
                result
                    .user_metadata
                    .as_ref()
                    .and_then(|metadata| metadata.get(METADATA_KEY_TENANT_ID))
                    .map(String::as_str),
                Some(expected),
                "{principal_tenant:?}"
            );
        }
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

    /// 每个应用一个已投影的 serial。
    struct ProjectedSerials(HashMap<String, u64>);

    #[async_trait]
    impl TenantRuntimeSource for ProjectedSerials {
        async fn load(&self, _tenant_id: &str) -> Result<Option<TenantRuntimeSnapshot>, String> {
            Ok(None)
        }

        async fn load_client_policy(
            &self,
            tenant_id: &str,
        ) -> Result<Option<TenantClientPolicy>, String> {
            Ok(self.0.get(tenant_id).map(|serial| TenantClientPolicy {
                tenant_id: tenant_id.to_string(),
                version: 1,
                access_serial: *serial,
                ..Default::default()
            }))
        }
    }

    fn projected(rows: &[(&str, u64)]) -> TenantRuntimeCache {
        TenantRuntimeCache::with_source(
            Arc::new(ProjectedSerials(
                rows.iter().map(|(id, s)| (id.to_string(), *s)).collect(),
            )),
            TenantRuntimeCacheOptions::default(),
        )
    }

    fn device_reporting(serial: Option<&str>) -> DeviceInfo {
        let device = DeviceInfo::new("device-a".to_string(), DevicePlatform::Web);
        match serial {
            Some(serial) => {
                device.with_metadata(metadata_keys::ACCESS_SERIAL.to_string(), serial.to_string())
            }
            None => device,
        }
    }

    /// 认证结果带上应用投影的 serial；没投影的应用是 0，放行不受影响。
    #[tokio::test]
    async fn an_admitted_connection_carries_the_projected_serial_of_its_app() {
        let device = device_reporting(None);
        let result = handler(
            Some(principal(None)),
            projected(&[("tenant-a", 42), ("tenant-b", 7)]),
            TenantAccessPolicy::Lenient,
        )
        .authenticate("token", "conn-a", Some(&device), None)
        .await
        .unwrap();
        assert_eq!(result.access_serial, 42);

        let result = handler(
            Some(principal(None)),
            projected(&[("tenant-b", 7)]),
            TenantAccessPolicy::Lenient,
        )
        .authenticate("token", "conn-a", Some(&device), None)
        .await
        .unwrap();
        assert!(result.authenticated);
        assert_eq!(result.access_serial, 0);
    }

    /// 真实握手：网关的认证器接在 flare-core 上，CONNECT_ACK 带的是投影里的 serial。
    #[tokio::test]
    async fn the_connect_ack_reports_the_projected_serial() {
        use flare_core::common::link::ConnectAckInfo;
        use flare_core::common::protocol::flare::core::commands::command;
        use flare_core::common::protocol::{
            Reliability, SerializationFormat, builder, frame_with_system_command,
        };
        use flare_core::server::ServerConfig;
        use flare_core::server::transports::server_core::ServerCore;

        let authenticator = handler(
            Some(principal(None)),
            projected(&[("tenant-a", 42)]),
            TenantAccessPolicy::Lenient,
        );
        let config = ServerConfig {
            auth_enabled: true,
            ..ServerConfig::default()
        };
        let core = ServerCore::new(&config, None).with_authenticator(Some(Arc::new(authenticator)));
        let connect = frame_with_system_command(
            builder::connect(
                SerializationFormat::Json,
                HashMap::from([
                    ("token".to_string(), b"token".to_vec()),
                    (metadata_keys::ACCESS_SERIAL.to_string(), b"40".to_vec()),
                ]),
            ),
            Reliability::AtLeastOnce,
        );
        let (ack, _) = core
            .handle_connect_message(&connect, "conn-a")
            .await
            .expect("negotiated");
        let Some(command::Type::System(system)) = ack.command.and_then(|c| c.r#type) else {
            panic!("system command expected");
        };
        assert_eq!(
            ConnectAckInfo::from_metadata(&system.metadata).access_serial,
            Some(42)
        );
    }

    /// 采用率的分子分母：上报的 serial 与投影比较后计入对应的桶。
    #[tokio::test]
    async fn reported_serials_are_counted_against_the_projection() {
        let metrics = Arc::new(flare_im_service_kit::metrics::AccessGatewayMetrics::new());
        let handler = handler(
            Some(principal(None)),
            projected(&[("tenant-a", 42)]),
            TenantAccessPolicy::Lenient,
        )
        .with_metrics(metrics.clone());
        for (reported, state) in [
            (Some("42"), "current"),
            (Some("41"), "behind"),
            (Some("43"), "ahead"),
            (None, "unreported"),
            (Some("not-a-number"), "unreported"),
        ] {
            let label = flare_im_service_kit::metrics::tenant_label("tenant-a");
            let counter = metrics
                .connect_access_serial_total
                .with_label_values(&[label, state]);
            let before = counter.get();
            let device = device_reporting(reported);
            handler
                .authenticate("token", "conn-a", Some(&device), None)
                .await
                .unwrap();
            assert_eq!(counter.get(), before + 1, "{reported:?}");
        }
    }

    /// 应用 tenant-a 在 android 上要求最低构建 200，并封禁 300–301。
    struct BuildPolicies;

    #[async_trait]
    impl TenantRuntimeSource for BuildPolicies {
        async fn load(&self, _tenant_id: &str) -> Result<Option<TenantRuntimeSnapshot>, String> {
            Ok(None)
        }

        async fn load_client_policy(
            &self,
            tenant_id: &str,
        ) -> Result<Option<TenantClientPolicy>, String> {
            use flare_im_contracts::domain::tenant::{ClientBuildPolicy, ClientBuildRange};
            Ok((tenant_id == "tenant-a").then(|| TenantClientPolicy {
                tenant_id: tenant_id.to_string(),
                version: 1,
                build_policies: std::collections::BTreeMap::from([(
                    "android".to_string(),
                    ClientBuildPolicy {
                        min_build: 200,
                        min_build_effective_from_ms: 0,
                        blocked: vec![ClientBuildRange { from: 300, to: 301 }],
                    },
                )]),
                ..Default::default()
            }))
        }
    }

    fn device_with_build(platform: DevicePlatform, build: Option<&str>) -> DeviceInfo {
        let device = DeviceInfo::new("device-a".to_string(), platform);
        match build {
            Some(build) => {
                device.with_metadata(METADATA_KEY_APP_BUILD.to_string(), build.to_string())
            }
            None => device,
        }
    }

    /// 版本门禁：低于最低构建、没报构建、命中封禁区间都以升级拒绝并计数；达到最低构建、
    /// 没有策略的平台（ios）和没有发布清单的平台（鸿蒙）放行。
    #[tokio::test]
    async fn the_version_gate_refuses_old_and_blocked_builds() {
        let metrics = Arc::new(flare_im_service_kit::metrics::AccessGatewayMetrics::new());
        let handler = handler(
            Some(principal(None)),
            TenantRuntimeCache::with_source(
                Arc::new(BuildPolicies),
                TenantRuntimeCacheOptions::default(),
            ),
            TenantAccessPolicy::Lenient,
        )
        .with_metrics(metrics.clone());
        let rejected = |reason: &str| {
            metrics
                .handshake_rejected_total
                .with_label_values(&[reason])
                .get()
        };

        for (name, device, expected_min_build, reason) in [
            (
                "below min",
                device_with_build(DevicePlatform::Android, Some("199")),
                Some(200),
                "below_min_build",
            ),
            (
                "missing build",
                device_with_build(DevicePlatform::Android, None),
                Some(200),
                "below_min_build",
            ),
            (
                "unparseable build",
                device_with_build(DevicePlatform::Android, Some("1.2.3")),
                Some(200),
                "below_min_build",
            ),
            (
                "blocked build",
                device_with_build(DevicePlatform::Android, Some("300")),
                Some(302),
                "blocked_build",
            ),
            (
                "at min",
                device_with_build(DevicePlatform::Android, Some("200")),
                None,
                "",
            ),
            (
                "platform without policy",
                device_with_build(DevicePlatform::IOS, Some("1")),
                None,
                "",
            ),
            (
                "platform without release manifest",
                device_with_build(DevicePlatform::HarmonyOS, Some("1")),
                None,
                "",
            ),
        ] {
            let before = (!reason.is_empty()).then(|| rejected(reason));
            let result = handler
                .authenticate("token", "conn-a", Some(&device), None)
                .await
                .unwrap();
            match expected_min_build {
                Some(min_build) => {
                    assert!(!result.authenticated, "{name}");
                    assert!(!result.retryable, "{name}");
                    assert_eq!(
                        result.upgrade_required.map(|upgrade| upgrade.min_build),
                        Some(min_build),
                        "{name}"
                    );
                    assert_eq!(rejected(reason), before.unwrap() + 1, "{name}");
                }
                None => {
                    assert!(result.authenticated, "{name}");
                    assert_eq!(result.upgrade_required, None, "{name}");
                }
            }
        }

        // 别的应用没有策略：同样的旧构建放行
        let mut other = principal(None);
        other.tenant_id = Some("tenant-b".to_string());
        let result = handler_with(other)
            .authenticate(
                "token",
                "conn-a",
                Some(&device_with_build(DevicePlatform::Android, Some("1"))),
                None,
            )
            .await
            .unwrap();
        assert!(result.authenticated);
    }

    fn handler_with(principal: AuthenticatedPrincipal) -> AuthHandler {
        handler(
            Some(principal),
            TenantRuntimeCache::with_source(
                Arc::new(BuildPolicies),
                TenantRuntimeCacheOptions::default(),
            ),
            TenantAccessPolicy::Lenient,
        )
    }

    /// 令牌校验按给定错误失败。
    struct FailingTokenValidator(fn() -> AuthError);

    #[async_trait]
    impl TokenValidator for FailingTokenValidator {
        async fn validate(
            &self,
            _request: TokenValidationRequest,
        ) -> Result<AuthenticatedPrincipal, AuthError> {
            Err((self.0)())
        }
    }

    fn failing_handler(error: fn() -> AuthError) -> AuthHandler {
        AuthHandler::with_default_tenant_id(
            Arc::new(FailingTokenValidator(error)),
            "tenant-default",
            TenantRuntimeCache::disabled(),
            TenantAccessPolicy::Lenient,
        )
    }

    /// 认证提供方不可用（hook 请求失败等）不是对令牌的结论：按暂时性拒绝，flare-core 以可重试的
    /// `SERVICE_UNAVAILABLE` 拒绝 CONNECT。报成令牌无效会让客户端去换一个本来有效的令牌。
    #[tokio::test]
    async fn auth_provider_outage_is_a_retryable_rejection() {
        let result = failing_handler(|| AuthError::ProviderUnavailable("hook down".to_string()))
            .authenticate("token", "conn-a", None, None)
            .await
            .expect("auth result");

        assert!(!result.authenticated);
        assert!(result.retryable, "a provider outage is not a verdict");
        assert_eq!(
            result.error_message.as_deref(),
            Some(AUTH_FAILURE_MSG_AUTH_PROVIDER_UNAVAILABLE)
        );
    }

    /// 令牌本身被拒（缺失、无效、主体不被允许）仍是确定的结论，客户端应换令牌。
    #[tokio::test]
    async fn token_verdicts_stay_non_retryable() {
        for error in [
            (|| AuthError::MissingToken) as fn() -> AuthError,
            || AuthError::InvalidToken("bad signature".to_string()),
            || AuthError::Forbidden("scope".to_string()),
        ] {
            let result = failing_handler(error)
                .authenticate("token", "conn-a", None, None)
                .await
                .expect("auth result");

            assert!(!result.authenticated, "{:?}", error());
            assert!(!result.retryable, "{:?}", error());
            assert_eq!(
                result.error_message.as_deref(),
                Some(AUTH_FAILURE_MSG_TOKEN_INVALID),
                "{:?}",
                error()
            );
        }
    }
    #[tokio::test]
    async fn social_session_binding_comes_only_from_the_verified_provider() {
        let sid = uuid::Uuid::new_v4().to_string();
        let mut authenticated = principal(Some("device-a"));
        authenticated
            .metadata
            .insert("identity_issuer".into(), "flare-im-identity".into());
        authenticated
            .metadata
            .insert("identity_session_id".into(), sid.clone());
        let mut device = DeviceInfo::new("device-a".into(), DevicePlatform::Web);
        device.metadata.insert(
            "identity_session_id".into(),
            uuid::Uuid::new_v4().to_string(),
        );
        let result = handler(
            Some(authenticated),
            TenantRuntimeCache::disabled(),
            TenantAccessPolicy::Lenient,
        )
        .authenticate("token", "connection", Some(&device), None)
        .await
        .unwrap();
        assert!(result.authenticated);
        assert_eq!(
            result.user_metadata.unwrap().get("identity_session_id"),
            Some(&sid)
        );
        let result = handler(
            Some(principal(Some("device-a"))),
            TenantRuntimeCache::disabled(),
            TenantAccessPolicy::Lenient,
        )
        .authenticate("token", "connection", Some(&device), None)
        .await
        .unwrap();
        assert!(
            !result
                .user_metadata
                .unwrap()
                .contains_key("identity_session_id")
        );
        let mut malformed = principal(Some("device-a"));
        malformed
            .metadata
            .insert("identity_issuer".into(), "flare-im-identity".into());
        assert!(
            !handler(
                Some(malformed),
                TenantRuntimeCache::disabled(),
                TenantAccessPolicy::Lenient
            )
            .authenticate("token", "connection", Some(&device), None)
            .await
            .unwrap()
            .authenticated
        );
    }
}
