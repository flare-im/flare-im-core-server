//! `CapabilityService` 生产运行时参数（环境变量，与网关 / 编排协同）。

use std::time::Duration;

/// 能力 gRPC 的运行时护栏：负载、超时、管理面鉴权、滥用防护。
#[derive(Debug, Clone)]
pub struct CapabilityRuntimeConfig {
    /// `Dispatch.payload_json` 最大字节数（UTF-8）。
    pub max_payload_json_bytes: usize,
    /// 单次 `Dispatch` 端到端超时（含 RTC/SFU）。
    pub dispatch_timeout: Duration,
    /// 非空时：`Grant` / `Revoke` / `SetTenantCapabilitySwitch` 须在 metadata 携带同值 `x-capability-admin-secret`。
    pub admin_secret: Option<String>,
    /// 为 true 且未配置 `admin_secret` 时，上述变更类 RPC 直接拒绝（强制生产闭环）。
    pub deny_policy_mutations_without_secret: bool,
    /// 每 `(tenant_id, user_id)` 每分钟最多 `Dispatch` 次数；`None` 表示不限制。
    pub dispatch_max_per_minute: Option<u32>,
}

impl CapabilityRuntimeConfig {
    /// 从环境变量加载；非法数值回退到安全默认值。
    pub fn from_env() -> Self {
        let max_payload_json_bytes = std::env::var("FLARE_CAPABILITY_MAX_PAYLOAD_BYTES")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|&n: &usize| n > 0 && n <= 16 * 1024 * 1024)
            .unwrap_or(1_048_576);

        let dispatch_timeout_ms = std::env::var("FLARE_CAPABILITY_DISPATCH_TIMEOUT_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|&n: &u64| n >= 1_000 && n <= 120_000)
            .unwrap_or(25_000);

        let admin_secret = std::env::var("FLARE_CAPABILITY_ADMIN_SECRET")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let deny_policy_mutations_without_secret = std::env::var(
            "FLARE_CAPABILITY_DENY_POLICY_MUTATIONS_WITHOUT_SECRET",
        )
        .map(|v| {
            matches!(
                v.trim(),
                "1" | "true" | "yes" | "on"
            ) || v.eq_ignore_ascii_case("true")
        })
        .unwrap_or(false);

        let dispatch_max_per_minute = std::env::var("FLARE_CAPABILITY_DISPATCH_MAX_PER_MINUTE")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|&n: &u32| n > 0 && n <= 100_000);

        let cfg = Self {
            max_payload_json_bytes,
            dispatch_timeout: Duration::from_millis(dispatch_timeout_ms),
            admin_secret,
            deny_policy_mutations_without_secret,
            dispatch_max_per_minute,
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
            admin_secret_configured = cfg.admin_secret.is_some(),
            deny_mutations_without_secret = cfg.deny_policy_mutations_without_secret,
            "CapabilityRuntimeConfig loaded"
        );

        cfg
    }

    /// 是否允许执行策略变更 RPC（未配置密钥时的策略）。
    pub fn policy_mutations_allowed(&self) -> bool {
        if self.admin_secret.is_some() {
            return true;
        }
        !self.deny_policy_mutations_without_secret
    }
}
