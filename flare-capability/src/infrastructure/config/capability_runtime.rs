//! `CapabilityService` 生产运行时参数（环境变量，与网关 / 编排协同）。

use std::path::Path;
use std::time::Duration;

use flare_core_base::config::LayeredConfig;
use serde::Deserialize;

/// 注册中心路由占位前缀：`PluginRouteBook` 中标记按服务名发现的插件。
pub const DISCOVERY_ROUTE_PREFIX: &str = "discovery://";

#[derive(Debug, Clone, Deserialize)]
pub struct PluginDiscoveryEndpoint {
    pub tenant_id: String,
    pub plugin_id: String,
    pub capability_id: String,
    /// 注册中心服务名，如 `flare-strom-sfu`（与 [`flare_im_core::service_names::STROM_SFU`] 对齐）。
    pub service_name: String,
    #[serde(default)]
    pub labels: std::collections::HashMap<String, String>,
}

impl PluginDiscoveryEndpoint {
    pub fn discovery_route_authority(&self) -> String {
        discovery_route_authority(&self.service_name)
    }
}

pub fn discovery_route_authority(service_name: &str) -> String {
    format!("{DISCOVERY_ROUTE_PREFIX}{service_name}")
}

pub fn is_discovery_route_authority(authority: &str) -> bool {
    authority.starts_with(DISCOVERY_ROUTE_PREFIX)
}

pub fn service_name_from_discovery_route(authority: &str) -> Option<&str> {
    authority
        .strip_prefix(DISCOVERY_ROUTE_PREFIX)
        .filter(|s| !s.trim().is_empty())
}

/// 能力 gRPC 的运行时护栏：负载、超时、管理面鉴权、滥用防护。
#[derive(Debug, Clone)]
pub struct CapabilityRuntimeConfig {
    /// `Dispatch.payload_json` 最大字节数（UTF-8）。
    pub max_payload_json_bytes: usize,
    /// 单次 `Dispatch` 端到端超时（含 RTC/媒体控制后端）。
    pub dispatch_timeout: Duration,
    /// 非空时：`Grant` / `Revoke` / `SetTenantCapabilitySwitch` 须在 metadata 携带同值 `x-capability-admin-secret`。
    pub admin_secret: Option<String>,
    /// 为 true 且未配置 `admin_secret` 时，上述变更类 RPC 直接拒绝（强制生产闭环）。
    pub deny_policy_mutations_without_secret: bool,
    /// 每 `(tenant_id, user_id)` 每分钟最多 `Dispatch` 次数；`None` 表示不限制。
    pub dispatch_max_per_minute: Option<u32>,
    /// 启动时自动发现并注册的远程插件 endpoint 列表（JSON）。
    pub plugin_discovery_endpoints: Vec<PluginDiscoveryEndpoint>,
    /// 插件健康检查周期。
    pub plugin_health_check_interval: Duration,
    /// 插件健康检查 / 远程调用超时。
    pub plugin_call_timeout: Duration,
}

impl CapabilityRuntimeConfig {
    /// 配置来源优先级：环境变量 > 配置文件(`config_file`) > 默认值。
    pub fn from_sources(config_file: Option<&Path>) -> Self {
        let layered = LayeredConfig::from_optional_toml(config_file);

        let mut max_payload_json_bytes = 1_048_576usize;
        let mut dispatch_timeout_ms = 25_000u64;
        let mut admin_secret: Option<String> = None;
        let mut deny_policy_mutations_without_secret = false;
        let mut dispatch_max_per_minute: Option<u32> = None;
        let mut plugin_discovery_endpoints: Vec<PluginDiscoveryEndpoint> = vec![];
        let mut plugin_health_check_interval_secs = 15u64;
        let mut plugin_call_timeout_ms = 5_000u64;

        if let Some(v) = layered
            .resolve_usize(
                "FLARE_CAPABILITY_MAX_PAYLOAD_BYTES",
                "capability_runtime.max_payload_json_bytes",
            )
            .filter(|&n: &usize| n > 0 && n <= 16 * 1024 * 1024)
        {
            max_payload_json_bytes = v;
        }

        if let Some(v) = layered
            .resolve_u64(
                "FLARE_CAPABILITY_DISPATCH_TIMEOUT_MS",
                "capability_runtime.dispatch_timeout_ms",
            )
            .filter(|&n: &u64| n >= 1_000 && n <= 120_000)
        {
            dispatch_timeout_ms = v;
        }

        if let Some(v) = layered.resolve_nonempty_string(
            "FLARE_CAPABILITY_ADMIN_SECRET",
            "capability_runtime.admin_secret",
        ) {
            admin_secret = Some(v);
        }

        if let Some(v) = layered.resolve_bool(
            "FLARE_CAPABILITY_DENY_POLICY_MUTATIONS_WITHOUT_SECRET",
            "capability_runtime.deny_policy_mutations_without_secret",
        ) {
            deny_policy_mutations_without_secret = v;
        }

        if let Some(v) = layered
            .resolve_u32(
                "FLARE_CAPABILITY_DISPATCH_MAX_PER_MINUTE",
                "capability_runtime.dispatch_max_per_minute",
            )
            .filter(|&n: &u32| n > 0 && n <= 100_000)
        {
            dispatch_max_per_minute = Some(v);
        }

        if let Some(v) = layered.resolve_json_vec_or_toml_vec::<PluginDiscoveryEndpoint>(
            "FLARE_CAPABILITY_PLUGIN_DISCOVERY_JSON",
            "capability_runtime.plugin_discovery_endpoints",
        ) {
            plugin_discovery_endpoints = v;
        }

        if let Some(v) = layered
            .resolve_u64(
                "FLARE_CAPABILITY_PLUGIN_HEALTH_INTERVAL_SECS",
                "capability_runtime.plugin_health_check_interval_secs",
            )
            .filter(|&n: &u64| n > 0 && n <= 3600)
        {
            plugin_health_check_interval_secs = v;
        }

        if let Some(v) = layered
            .resolve_u64(
                "FLARE_CAPABILITY_PLUGIN_CALL_TIMEOUT_MS",
                "capability_runtime.plugin_call_timeout_ms",
            )
            .filter(|&n: &u64| n >= 200 && n <= 60_000)
        {
            plugin_call_timeout_ms = v;
        }

        let cfg = Self {
            max_payload_json_bytes,
            dispatch_timeout: Duration::from_millis(dispatch_timeout_ms),
            admin_secret,
            deny_policy_mutations_without_secret,
            dispatch_max_per_minute,
            plugin_discovery_endpoints,
            plugin_health_check_interval: Duration::from_secs(plugin_health_check_interval_secs),
            plugin_call_timeout: Duration::from_millis(plugin_call_timeout_ms),
        };

        if cfg.deny_policy_mutations_without_secret && cfg.admin_secret.is_none() {
            tracing::error!(
                "FLARE_CAPABILITY_DENY_POLICY_MUTATIONS_WITHOUT_SECRET=1 but FLARE_CAPABILITY_ADMIN_SECRET is unset; policy mutations will be rejected"
            );
        } else if cfg.admin_secret.is_none() {
            tracing::warn!(
                "FLARE_CAPABILITY_ADMIN_SECRET not set: Grant/Revoke/SetTenantCapabilitySwitch are not cryptographically protected; set secret in production"
            );
        }

        tracing::info!(
            max_payload_bytes = cfg.max_payload_json_bytes,
            dispatch_timeout_ms = dispatch_timeout_ms,
            dispatch_rate_per_minute = ?cfg.dispatch_max_per_minute,
            discovered_plugin_count = cfg.plugin_discovery_endpoints.len(),
            plugin_health_interval_secs = plugin_health_check_interval_secs,
            plugin_call_timeout_ms = plugin_call_timeout_ms,
            admin_secret_configured = cfg.admin_secret.is_some(),
            deny_mutations_without_secret = cfg.deny_policy_mutations_without_secret,
            "CapabilityRuntimeConfig loaded"
        );

        cfg
    }

    /// 无配置文件场景（仅 env + 默认值）。
    pub fn from_env() -> Self {
        Self::from_sources(None)
    }

    /// 选择媒体控制面发现项（按 tenant/plugin 语义）。
    ///
    /// 约定：`capability_id = rtc.media.control`。
    pub fn media_control_endpoints(&self) -> Vec<&PluginDiscoveryEndpoint> {
        self.plugin_discovery_endpoints
            .iter()
            .filter(|ep| {
                let plugin_match = !ep.plugin_id.trim().is_empty();
                let capability_match = ep.capability_id.trim() == "rtc.media.control";
                plugin_match
                    && capability_match
                    && !ep.tenant_id.trim().is_empty()
                    && !ep.service_name.trim().is_empty()
            })
            .collect()
    }

    /// 首选媒体控制面发现项：用于 `RtcCapability` 主链路装配。
    pub fn primary_media_control_endpoint(&self) -> Option<&PluginDiscoveryEndpoint> {
        self.media_control_endpoints().into_iter().next()
    }

    /// 是否允许执行策略变更 RPC（未配置密钥时的策略）。
    pub fn policy_mutations_allowed(&self) -> bool {
        if self.admin_secret.is_some() {
            return true;
        }
        !self.deny_policy_mutations_without_secret
    }
}
