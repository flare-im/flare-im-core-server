use flare_server_core::auth::{AppCredential, AuthProviderConfig, AuthProviderMode};
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError, Result};
use serde::Deserialize;
use std::str::FromStr;

use crate::clients::CapabilityDownstreamConfig;
pub use crate::clients::DownstreamGrpcConfig as GatewayGrpcConfig;
use crate::discovery::{discovery_route_authority, is_discovery_route_authority};
use crate::service_names::{
    CAPABILITY, CONVERSATION, MEDIA, MESSAGE_INGEST, ORCHESTRATOR, SIGNALING_ONLINE, STORAGE_READER,
};

/// 网关环境配置作用域。
///
/// 每个运行进程只读取自己作用域下的变量，避免 public gateway 和 admin gateway
/// 在生产部署中误共享认证、端口或下游路由配置。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayEnvScope {
    Api,
    Admin,
}

impl GatewayEnvScope {
    pub fn env_prefix(self) -> &'static str {
        match self {
            Self::Api => "FLARE_API_GATEWAY",
            Self::Admin => "FLARE_ADMIN_GATEWAY",
        }
    }

    fn default_port(self) -> &'static str {
        match self {
            Self::Api => "50050",
            Self::Admin => "50051",
        }
    }

    fn default_tracing_service_name(self) -> &'static str {
        match self {
            Self::Api => "flare-api-gateway",
            Self::Admin => "flare-admin-gateway",
        }
    }
}

/// IM Gateway 运行配置。
#[derive(Debug, Clone, Deserialize)]
pub struct GatewaySettings {
    /// HTTP 服务器配置
    pub server: ServerConfig,
    /// gRPC 客户端配置
    pub grpc: GatewayGrpcConfig,
    /// 认证配置。只有 API 作用域读取 `*_AUTH_*`；Admin 作用域恒为默认值，见
    /// [`ignored_admin_settings`]。
    pub auth: AuthProviderConfig,
    /// 限流配置。只有 API 作用域读取 `*_RATE_LIMIT_*`；Admin 作用域恒为
    /// [`RateLimitConfig::not_applied`]。
    pub rate_limit: RateLimitConfig,
    /// 追踪配置
    pub tracing: TracingConfig,
}

/// HTTP 服务器配置
#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    /// 监听地址
    pub bind: String,
    /// HTTP 端口
    pub port: u16,
    /// 请求超时(秒)
    pub timeout_secs: u64,
    /// 最大请求体大小(字节)
    pub max_body_size: usize,
}

/// 限流配置
#[derive(Debug, Clone, Deserialize)]
pub struct RateLimitConfig {
    /// 是否启用
    pub enabled: bool,
    /// 每秒请求数
    pub requests_per_second: u32,
    /// 突发容量
    pub burst_capacity: u32,
}

impl RateLimitConfig {
    /// 不做请求限流的网关（Admin Gateway）报告的取值：关闭，速率与突发都是 0。
    pub fn not_applied() -> Self {
        Self {
            enabled: false,
            requests_per_second: 0,
            burst_capacity: 0,
        }
    }
}

/// 追踪配置
#[derive(Debug, Clone, Deserialize)]
pub struct TracingConfig {
    /// 是否启用
    pub enabled: bool,
    /// 服务名称
    pub service_name: String,
    /// 采样率 (0.0 - 1.0)
    pub sample_rate: f64,
}

impl GatewaySettings {
    /// 从 API Gateway 环境变量加载配置。
    pub fn from_env() -> Result<Self> {
        Self::from_env_for(GatewayEnvScope::Api)
    }

    /// 从指定 Gateway 作用域的环境变量加载配置。
    pub fn from_env_for(scope: GatewayEnvScope) -> Result<Self> {
        dotenvy::dotenv().ok();
        Self::from_env_source(scope, |key| std::env::var(key).ok())
    }

    /// 从可注入的环境变量源加载配置，便于测试配置边界。
    pub fn from_env_source<F>(scope: GatewayEnvScope, mut source: F) -> Result<Self>
    where
        F: FnMut(&str) -> Option<String>,
    {
        let server = ServerConfig {
            bind: env_or(&mut source, scope, "SERVER_BIND", "0.0.0.0"),
            port: parse_env(&mut source, scope, "SERVER_PORT", scope.default_port())?,
            timeout_secs: parse_env(&mut source, scope, "SERVER_TIMEOUT_SECS", "30")?,
            max_body_size: parse_env(&mut source, scope, "SERVER_MAX_BODY_SIZE", "10485760")?,
        };

        let grpc = GatewayGrpcConfig {
            media_service_url: env_or(
                &mut source,
                scope,
                "GRPC_MEDIA_SERVICE_URL",
                &discovery_route_authority(MEDIA),
            ),
            message_ingest_service_url: env_or(
                &mut source,
                scope,
                "GRPC_MESSAGE_INGEST_SERVICE_URL",
                &discovery_route_authority(MESSAGE_INGEST),
            ),
            message_orchestrator_service_url: env_or(
                &mut source,
                scope,
                "GRPC_MESSAGE_ORCHESTRATOR_SERVICE_URL",
                &discovery_route_authority(ORCHESTRATOR),
            ),
            conversation_service_url: env_or(
                &mut source,
                scope,
                "GRPC_CONVERSATION_SERVICE_URL",
                &discovery_route_authority(CONVERSATION),
            ),
            online_service_url: env_or(
                &mut source,
                scope,
                "GRPC_ONLINE_SERVICE_URL",
                &discovery_route_authority(SIGNALING_ONLINE),
            ),
            storage_reader_service_url: env_or(
                &mut source,
                scope,
                "GRPC_STORAGE_READER_SERVICE_URL",
                &discovery_route_authority(STORAGE_READER),
            ),
            media_static_fallback: env_or(
                &mut source,
                scope,
                "GRPC_MEDIA_STATIC_FALLBACK",
                "http://127.0.0.1:60081",
            ),
            message_ingest_static_fallback: env_or(
                &mut source,
                scope,
                "GRPC_MESSAGE_INGEST_STATIC_FALLBACK",
                "http://127.0.0.1:50182",
            ),
            message_orchestrator_static_fallback: env_or(
                &mut source,
                scope,
                "GRPC_MESSAGE_ORCHESTRATOR_STATIC_FALLBACK",
                "http://127.0.0.1:50181",
            ),
            conversation_static_fallback: env_or(
                &mut source,
                scope,
                "GRPC_CONVERSATION_STATIC_FALLBACK",
                "http://127.0.0.1:50090",
            ),
            online_static_fallback: env_or(
                &mut source,
                scope,
                "GRPC_ONLINE_STATIC_FALLBACK",
                "http://127.0.0.1:50061",
            ),
            storage_reader_static_fallback: env_or(
                &mut source,
                scope,
                "GRPC_STORAGE_READER_STATIC_FALLBACK",
                "http://127.0.0.1:60083",
            ),
            connect_timeout_secs: parse_env(&mut source, scope, "GRPC_CONNECT_TIMEOUT_SECS", "5")?,
            request_timeout_secs: parse_env(&mut source, scope, "GRPC_REQUEST_TIMEOUT_SECS", "10")?,
        };

        let (auth, rate_limit) = match scope {
            GatewayEnvScope::Api => (
                parse_auth(&mut source, scope)?,
                parse_rate_limit(&mut source, scope)?,
            ),
            // Admin Gateway 只用 `FLARE_CONSOLE_PUBLIC_KEY` 验 Console 签发的 admin 断言，也不做
            // 请求限流：这两组变量在 Admin 作用域下不解析、不校验，误配不会挡住启动。
            GatewayEnvScope::Admin => {
                for key in ignored_admin_settings(&mut source) {
                    tracing::warn!(
                        key = %key,
                        "admin-gateway ignores this setting: it verifies Console admin assertions with FLARE_CONSOLE_PUBLIC_KEY and applies no request rate limit"
                    );
                }
                (
                    AuthProviderConfig::default(),
                    RateLimitConfig::not_applied(),
                )
            }
        };

        let tracing = TracingConfig {
            enabled: parse_env(&mut source, scope, "TRACING_ENABLED", "true")?,
            service_name: env_or(
                &mut source,
                scope,
                "TRACING_SERVICE_NAME",
                scope.default_tracing_service_name(),
            ),
            sample_rate: parse_env(&mut source, scope, "TRACING_SAMPLE_RATE", "1.0")?,
        };

        let settings = GatewaySettings {
            server,
            grpc,
            auth,
            rate_limit,
            tracing,
        };

        // 验证配置
        settings.validate()?;

        Ok(settings)
    }

    /// 验证配置
    fn validate(&self) -> Result<()> {
        // 验证端口范围
        if self.server.port == 0 {
            return Err(config_error("Server port cannot be 0"));
        }

        // 验证超时
        if self.server.timeout_secs == 0 {
            return Err(config_error("Server timeout cannot be 0"));
        }

        for (label, route) in [
            ("media", self.grpc.media_service_url.as_str()),
            (
                "message-ingest",
                self.grpc.message_ingest_service_url.as_str(),
            ),
            (
                "message-orchestrator",
                self.grpc.message_orchestrator_service_url.as_str(),
            ),
            ("conversation", self.grpc.conversation_service_url.as_str()),
            ("online", self.grpc.online_service_url.as_str()),
            (
                "storage-reader",
                self.grpc.storage_reader_service_url.as_str(),
            ),
        ] {
            if !is_valid_grpc_route(route) {
                return Err(config_error(format!(
                    "gRPC route for {label} must be discovery://, http://, or https://"
                )));
            }
        }

        // http_hook 模式必须配 hook_url（业务方拥有整套验证）。
        if self.auth.mode == AuthProviderMode::HttpHook
            && self
                .auth
                .hook_url
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .is_none()
        {
            return Err(config_error(
                "AUTH_HOOK_URL is required in the gateway env scope when AUTH_MODE=http_hook",
            ));
        }
        // hook_url 一旦配置（http_hook 模式；或 core_jwt 模式下作为三方验证兜底层），格式与相关项都要合法。
        if let Some(hook_url) = self
            .auth
            .hook_url
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            if !(hook_url.starts_with("http://") || hook_url.starts_with("https://")) {
                return Err(config_error("AUTH_HOOK_URL must be http:// or https://"));
            }
            if self.auth.hook_timeout_ms == 0 {
                return Err(config_error("AUTH_HOOK_TIMEOUT_MS cannot be 0"));
            }
            if self.auth.hook_secret_header.trim().is_empty() {
                return Err(config_error("AUTH_HOOK_SECRET_HEADER cannot be empty"));
            }
        }
        // hook_issue_url 一旦配置就要合法（http_hook 签发委托）。
        if let Some(issue_url) = self
            .auth
            .hook_issue_url
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            && !(issue_url.starts_with("http://") || issue_url.starts_with("https://"))
        {
            return Err(config_error(
                "AUTH_HOOK_ISSUE_URL must be http:// or https://",
            ));
        }
        if self.auth.refresh_grace_secs == 0 {
            return Err(config_error("AUTH_REFRESH_GRACE_SECS cannot be 0"));
        }

        // 验证限流配置
        if self.rate_limit.enabled && self.rate_limit.requests_per_second == 0 {
            return Err(config_error("Rate limit RPS cannot be 0 when enabled"));
        }

        // 验证追踪采样率
        if self.tracing.sample_rate < 0.0 || self.tracing.sample_rate > 1.0 {
            return Err(config_error(
                "Tracing sample rate must be between 0.0 and 1.0",
            ));
        }

        Ok(())
    }
}

/// Admin Gateway 不读取的设置名（不含作用域前缀）。
const ADMIN_IGNORED_SETTINGS: &[&str] = &[
    "AUTH_MODE",
    "AUTH_HOOK_URL",
    "AUTH_HOOK_TIMEOUT_MS",
    "AUTH_HOOK_SECRET_HEADER",
    "AUTH_HOOK_SECRET",
    "AUTH_HOOK_ISSUE_URL",
    "AUTH_DEV_ISSUE",
    "AUTH_APP_CREDENTIALS",
    "AUTH_REFRESH_GRACE_SECS",
    "RATE_LIMIT_ENABLED",
    "RATE_LIMIT_RPS",
    "RATE_LIMIT_BURST",
];

/// 环境里出现、而 Admin Gateway 会忽略的变量全名。只返回名字：其中有 hook 密钥与应用凭据，
/// 值不能进日志。
pub fn ignored_admin_settings<F>(mut source: F) -> Vec<String>
where
    F: FnMut(&str) -> Option<String>,
{
    ADMIN_IGNORED_SETTINGS
        .iter()
        .map(|name| env_key(GatewayEnvScope::Admin, name))
        .filter(|key| source(key).is_some())
        .collect()
}

fn parse_auth<F>(source: &mut F, scope: GatewayEnvScope) -> Result<AuthProviderConfig>
where
    F: FnMut(&str) -> Option<String>,
{
    Ok(AuthProviderConfig {
        mode: parse_env(source, scope, "AUTH_MODE", "core_jwt")?,
        hook_url: env_value(source, scope, "AUTH_HOOK_URL")
            .filter(|value| !value.trim().is_empty()),
        hook_timeout_ms: parse_env(source, scope, "AUTH_HOOK_TIMEOUT_MS", "800")?,
        hook_secret_header: env_or(
            source,
            scope,
            "AUTH_HOOK_SECRET_HEADER",
            "x-flare-auth-hook-secret",
        ),
        hook_secret: env_value(source, scope, "AUTH_HOOK_SECRET")
            .filter(|value| !value.trim().is_empty()),
        hook_issue_url: env_value(source, scope, "AUTH_HOOK_ISSUE_URL")
            .filter(|value| !value.trim().is_empty()),
        dev_issue: parse_env(source, scope, "AUTH_DEV_ISSUE", "false")?,
        app_credentials: match env_value(source, scope, "AUTH_APP_CREDENTIALS") {
            Some(raw) if !raw.trim().is_empty() => AppCredential::parse_list(&raw)
                .map_err(|err| config_error(format!("AUTH_APP_CREDENTIALS: {err}")))?,
            _ => Vec::new(),
        },
        refresh_grace_secs: parse_env(source, scope, "AUTH_REFRESH_GRACE_SECS", "604800")?,
    })
}

fn parse_rate_limit<F>(source: &mut F, scope: GatewayEnvScope) -> Result<RateLimitConfig>
where
    F: FnMut(&str) -> Option<String>,
{
    Ok(RateLimitConfig {
        enabled: parse_env(source, scope, "RATE_LIMIT_ENABLED", "true")?,
        requests_per_second: parse_env(source, scope, "RATE_LIMIT_RPS", "1000")?,
        burst_capacity: parse_env(source, scope, "RATE_LIMIT_BURST", "2000")?,
    })
}

/// Admin Gateway 专用的可选下游 Capability（租户能力开关、租户运行时只读）。
///
/// 只有 [`GatewayEnvScope::Admin`] 才读取
/// `FLARE_ADMIN_GATEWAY_GRPC_CAPABILITY_SERVICE_URL` 与 `..._STATIC_FALLBACK`；
/// api-gateway 恒为 `None`，启动时不会去连 Capability。把 URL 显式设为空串即关闭该下游。
pub fn capability_downstream_from_env(
    scope: GatewayEnvScope,
) -> Result<Option<CapabilityDownstreamConfig>> {
    dotenvy::dotenv().ok();
    capability_downstream_from_env_source(scope, |key| std::env::var(key).ok())
}

pub fn capability_downstream_from_env_source<F>(
    scope: GatewayEnvScope,
    mut source: F,
) -> Result<Option<CapabilityDownstreamConfig>>
where
    F: FnMut(&str) -> Option<String>,
{
    if scope != GatewayEnvScope::Admin {
        return Ok(None);
    }
    let service_url = env_or(
        &mut source,
        scope,
        "GRPC_CAPABILITY_SERVICE_URL",
        &discovery_route_authority(CAPABILITY),
    );
    let service_url = service_url.trim();
    if service_url.is_empty() {
        return Ok(None);
    }
    if !is_valid_grpc_route(service_url) {
        return Err(config_error(
            "gRPC route for capability must be discovery://, http://, or https://",
        ));
    }
    let static_fallback = env_or(
        &mut source,
        scope,
        "GRPC_CAPABILITY_STATIC_FALLBACK",
        "http://127.0.0.1:50110",
    );
    let static_fallback = static_fallback.trim();
    if !static_fallback.is_empty()
        && !(static_fallback.starts_with("http://") || static_fallback.starts_with("https://"))
    {
        return Err(config_error(
            "capability static fallback must be http:// or https://",
        ));
    }
    Ok(Some(CapabilityDownstreamConfig {
        service_url: service_url.to_string(),
        static_fallback: static_fallback.to_string(),
    }))
}

fn config_error(reason: impl Into<String>) -> FlareError {
    ErrorBuilder::new(ErrorCode::ConfigurationError, reason).build_error()
}

pub fn require_secure_token_secret(
    env_key: &str,
    configured: Option<&str>,
    config_path: &str,
) -> Result<String> {
    let secret = std::env::var(env_key)
        .ok()
        .or_else(|| configured.map(ToOwned::to_owned))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            config_error(format!(
                "{config_path} is required; set {env_key} or configure a deployment secret"
            ))
        })?;

    if is_weak_token_secret(&secret) {
        return Err(config_error(format!(
            "{config_path} is weak or uses a known development placeholder; set {env_key} to a random secret with at least 32 bytes"
        )));
    }

    Ok(secret)
}

fn is_weak_token_secret(secret: &str) -> bool {
    let normalized = secret.trim().to_ascii_lowercase();
    secret.len() < 32
        || matches!(
            normalized.as_str(),
            "insecure-secret" | "change-me" | "change-me-in-production" | "secret" | "password"
        )
        || normalized.contains("change-me")
        || normalized.starts_with("insecure")
}

fn is_valid_grpc_route(route: &str) -> bool {
    route.starts_with("http://")
        || route.starts_with("https://")
        || is_discovery_route_authority(route)
}

fn env_key(scope: GatewayEnvScope, name: &str) -> String {
    format!("{}_{}", scope.env_prefix(), name)
}

fn env_value<F>(source: &mut F, scope: GatewayEnvScope, name: &str) -> Option<String>
where
    F: FnMut(&str) -> Option<String>,
{
    source(&env_key(scope, name))
}

fn env_or<F>(source: &mut F, scope: GatewayEnvScope, name: &str, default: &str) -> String
where
    F: FnMut(&str) -> Option<String>,
{
    env_value(source, scope, name).unwrap_or_else(|| default.to_string())
}

fn parse_env<F, T>(source: &mut F, scope: GatewayEnvScope, name: &str, default: &str) -> Result<T>
where
    F: FnMut(&str) -> Option<String>,
    T: FromStr,
    T::Err: std::fmt::Display + Send + Sync + 'static,
{
    let key = env_key(scope, name);
    let value = source(&key).unwrap_or_else(|| default.to_string());
    value.parse::<T>().map_err(|err| {
        ErrorBuilder::new(
            ErrorCode::ConfigurationError,
            format!("Invalid {key}: {value}"),
        )
        .details(err.to_string())
        .build_error()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_default_settings() {
        let settings = GatewaySettings::from_env().unwrap();
        assert_eq!(settings.server.port, 50050);
        assert_eq!(settings.auth.mode, AuthProviderMode::CoreJwt);
        assert!(settings.rate_limit.enabled);
        assert!(is_discovery_route_authority(
            &settings.grpc.message_ingest_service_url
        ));
        assert!(is_discovery_route_authority(
            &settings.grpc.message_orchestrator_service_url
        ));
    }

    #[test]
    fn auth_mode_parses_only_canonical_values() {
        assert_eq!(
            "core_jwt".parse::<AuthProviderMode>().unwrap(),
            AuthProviderMode::CoreJwt
        );
        assert_eq!(
            "http_hook".parse::<AuthProviderMode>().unwrap(),
            AuthProviderMode::HttpHook
        );
        for alias in ["jwt", "core", "hook", "external"] {
            assert!(
                alias.parse::<AuthProviderMode>().is_err(),
                "{alias} must not be accepted as an auth mode"
            );
        }
    }

    #[test]
    fn capability_downstream_is_admin_only_and_optional() {
        // api-gateway 不读取、也不连接 Capability，哪怕环境里配了 admin 的变量。
        let mut api_reads = Vec::new();
        let api = capability_downstream_from_env_source(GatewayEnvScope::Api, |key| {
            api_reads.push(key.to_string());
            Some("http://127.0.0.1:1".to_string())
        })
        .unwrap();
        assert_eq!(api, None);
        assert!(api_reads.is_empty(), "{api_reads:?}");

        let defaults =
            capability_downstream_from_env_source(GatewayEnvScope::Admin, |_| None).unwrap();
        assert_eq!(
            defaults,
            Some(CapabilityDownstreamConfig {
                service_url: "discovery://flare-capability".to_string(),
                static_fallback: "http://127.0.0.1:50110".to_string(),
            })
        );

        let env = HashMap::from([
            (
                "FLARE_ADMIN_GATEWAY_GRPC_CAPABILITY_SERVICE_URL".to_string(),
                "http://capability:50110".to_string(),
            ),
            (
                "FLARE_ADMIN_GATEWAY_GRPC_CAPABILITY_STATIC_FALLBACK".to_string(),
                String::new(),
            ),
        ]);
        let overridden = capability_downstream_from_env_source(GatewayEnvScope::Admin, |key| {
            env.get(key).cloned()
        })
        .unwrap()
        .unwrap();
        assert_eq!(overridden.service_url, "http://capability:50110");
        assert_eq!(overridden.static_fallback, "");

        let disabled = capability_downstream_from_env_source(GatewayEnvScope::Admin, |key| {
            (key == "FLARE_ADMIN_GATEWAY_GRPC_CAPABILITY_SERVICE_URL").then(String::new)
        })
        .unwrap();
        assert_eq!(disabled, None);

        for (key, value) in [
            (
                "FLARE_ADMIN_GATEWAY_GRPC_CAPABILITY_SERVICE_URL",
                "capability:50110",
            ),
            (
                "FLARE_ADMIN_GATEWAY_GRPC_CAPABILITY_STATIC_FALLBACK",
                "capability:50110",
            ),
        ] {
            assert!(
                capability_downstream_from_env_source(GatewayEnvScope::Admin, |k| {
                    (k == key).then(|| value.to_string())
                })
                .is_err(),
                "{key}"
            );
        }
    }

    #[test]
    fn gateway_env_scope_uses_service_specific_prefixes() {
        let env = HashMap::from([
            (
                "FLARE_API_GATEWAY_SERVER_PORT".to_string(),
                "51050".to_string(),
            ),
            (
                "FLARE_ADMIN_GATEWAY_SERVER_PORT".to_string(),
                "51051".to_string(),
            ),
            (
                "FLARE_ADMIN_GATEWAY_AUTH_MODE".to_string(),
                "http_hook".to_string(),
            ),
            (
                "FLARE_ADMIN_GATEWAY_AUTH_HOOK_URL".to_string(),
                "https://admin-auth.example.com/validate".to_string(),
            ),
            (
                "FLARE_API_GATEWAY_AUTH_MODE".to_string(),
                "core_jwt".to_string(),
            ),
        ]);

        let admin =
            GatewaySettings::from_env_source(GatewayEnvScope::Admin, |key| env.get(key).cloned())
                .expect("admin settings");
        let api =
            GatewaySettings::from_env_source(GatewayEnvScope::Api, |key| env.get(key).cloned())
                .expect("api settings");

        assert_eq!(admin.server.port, 51051);
        // Admin 作用域不读认证变量：配了 http_hook 也仍是默认值。
        assert_eq!(admin.auth.mode, AuthProviderMode::CoreJwt);
        assert_eq!(admin.auth.hook_url, None);
        assert_eq!(admin.tracing.service_name, "flare-admin-gateway");

        assert_eq!(api.server.port, 51050);
        assert_eq!(api.auth.mode, AuthProviderMode::CoreJwt);
        assert_eq!(api.auth.hook_url, None);
        assert_eq!(api.tracing.service_name, "flare-api-gateway");
    }
    /// 误配到 Admin 作用域的认证与限流变量：http_hook 没有 URL、超时写错、限流开着却是 0。
    /// 这些放在 API 作用域都会拒绝启动；Admin Gateway 从不使用它们，所以照常启动。
    fn misconfigured(prefix: &str) -> HashMap<String, String> {
        [
            ("AUTH_MODE", "http_hook"),
            ("AUTH_HOOK_TIMEOUT_MS", "not-a-number"),
            ("AUTH_HOOK_SECRET", "hook-secret-marker"),
            ("RATE_LIMIT_ENABLED", "true"),
            ("RATE_LIMIT_RPS", "0"),
        ]
        .into_iter()
        .map(|(name, value)| (format!("{prefix}_{name}"), value.to_string()))
        .collect()
    }

    #[test]
    fn admin_scope_ignores_auth_and_rate_limit_settings() {
        let env = misconfigured("FLARE_ADMIN_GATEWAY");
        let admin =
            GatewaySettings::from_env_source(GatewayEnvScope::Admin, |key| env.get(key).cloned())
                .expect("admin-gateway starts despite unused auth and rate-limit settings");
        assert_eq!(admin.auth.mode, AuthProviderMode::CoreJwt);
        assert_eq!(admin.auth.hook_url, None);
        assert_eq!(admin.auth.hook_secret, None);
        assert!(!admin.auth.dev_issue);
        assert!(admin.auth.app_credentials.is_empty());
        assert!(!admin.rate_limit.enabled);
        assert_eq!(admin.rate_limit.requests_per_second, 0);
        assert_eq!(admin.rate_limit.burst_capacity, 0);

        let mut ignored = ignored_admin_settings(|key| env.get(key).cloned());
        ignored.sort();
        let mut expected: Vec<String> = env.keys().cloned().collect();
        expected.sort();
        assert_eq!(ignored, expected);
        assert!(ignored_admin_settings(|_| None).is_empty());

        // 同样的错误放在 API 作用域照旧拒绝启动。
        let api_env = misconfigured("FLARE_API_GATEWAY");
        assert!(
            GatewaySettings::from_env_source(GatewayEnvScope::Api, |key| api_env.get(key).cloned())
                .is_err()
        );
        let hook_without_url = HashMap::from([(
            "FLARE_API_GATEWAY_AUTH_MODE".to_string(),
            "http_hook".to_string(),
        )]);
        assert!(
            GatewaySettings::from_env_source(GatewayEnvScope::Api, |key| hook_without_url
                .get(key)
                .cloned())
            .is_err()
        );
    }

    /// 忽略时只记变量名：hook 密钥这类值不能进日志。
    #[test]
    fn ignored_admin_settings_are_logged_by_name_only() {
        #[derive(Clone, Default)]
        struct Captured(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for Captured {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
            type Writer = Captured;
            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }

        let env = misconfigured("FLARE_ADMIN_GATEWAY");
        let captured = Captured::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(captured.clone())
            .with_ansi(false)
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            GatewaySettings::from_env_source(GatewayEnvScope::Admin, |key| env.get(key).cloned())
                .unwrap();
        });
        let logged = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
        assert_eq!(logged.matches(" WARN ").count(), env.len(), "{logged}");
        for key in env.keys() {
            assert!(logged.contains(key.as_str()), "{key} not logged: {logged}");
        }
        for value in ["hook-secret-marker", "not-a-number", "http_hook"] {
            assert!(!logged.contains(value), "{value} leaked: {logged}");
        }
    }
}
