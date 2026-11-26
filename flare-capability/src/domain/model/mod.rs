//! # Hook 控制面领域模型
//!
//! 配置、校验与查询。运行时在 `flare_im_core_hooks`，嵌在 Ingest 里。

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub mod client_policy;
pub mod hook_execution;
pub mod tenant_notice;
pub mod tenant_projection;
pub mod tenant_runtime;
pub use client_policy::{
    ClientPolicyNotice, ClientPolicyRecord, ClientPolicyValidationError, ClientPolicyWatermarks,
};
pub use flare_im_core_hooks::hooks::execution::HOOK_CIRCUIT_OPEN_CODE;
pub use flare_im_core_hooks::hooks::{
    HookSelectorConfig, LoggableName, MAX_HOOK_NAME_CHARS, validate_hook_name,
};
pub use hook_execution::{
    HookExecutionFilter, HookExecutionScope, HookExecutionStatistics, HookExecutionTimeRange,
    MAX_QUERY_TIME_MILLIS, StoredHookExecution,
};
pub use tenant_notice::{NoticeDenied, NoticeOutcome, NoticePublishers};
pub use tenant_projection::{
    TenantProjectionOutcome, TenantProjectionRecord, TenantProjectionValidationError,
};
pub use tenant_runtime::TenantRuntimeView;

/// Hook配置项
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookConfigItem {
    /// Hook名称
    pub name: String,
    /// Hook描述（可选）
    pub description: Option<String>,
    /// 是否启用
    pub enabled: bool,
    /// 同一来源内的先后（0-1000，小的先执行）
    pub order: i32,
    /// 超时时间（毫秒）
    pub timeout_ms: u64,
    /// 必需：目标不可用时回可重试的 503；否则跳过
    pub require_success: bool,
    /// 选择器配置
    pub selector: HookSelectorConfig,
    /// 传输配置
    pub transport: HookTransportConfig,
    /// 元数据
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

/// 负载均衡策略
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum LoadBalanceStrategy {
    /// 轮询（Round Robin）
    #[default]
    RoundRobin,
    /// 随机（Random）
    Random,
    /// 一致性哈希（Consistent Hash）
    ConsistentHash,
    /// 最少连接（Least Connections）
    LeastConn,
}

impl std::str::FromStr for LoadBalanceStrategy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "round_robin" | "round-robin" => Ok(LoadBalanceStrategy::RoundRobin),
            "random" => Ok(LoadBalanceStrategy::Random),
            "consistent_hash" | "consistent-hash" => Ok(LoadBalanceStrategy::ConsistentHash),
            "least_conn" | "least-conn" | "least_connections" | "least-connections" => {
                Ok(LoadBalanceStrategy::LeastConn)
            }
            _ => Err(format!("Unknown load balance strategy: {}", s)),
        }
    }
}

/// Hook传输配置
///
/// `Debug` 手写：Webhook 的签名密钥与自定义头的值不进日志（只显示是否设置、头名）。
#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HookTransportConfig {
    /// gRPC传输（支持服务发现和直接地址两种模式）
    Grpc {
        /// 模式1: 直接地址（endpoint 优先，用于外部系统/开发测试）
        #[serde(default)]
        endpoint: Option<String>,

        /// 模式2: 服务发现（当 endpoint 为空时使用，用于生产环境内部服务）
        #[serde(default)]
        service_name: Option<String>,

        /// 注册中心类型（可选，使用全局配置）
        #[serde(default)]
        registry_type: Option<String>,

        /// 命名空间（可选，使用全局配置）
        #[serde(default)]
        namespace: Option<String>,

        /// 负载均衡策略（默认：RoundRobin）
        #[serde(default)]
        load_balance: Option<LoadBalanceStrategy>,

        /// 请求元数据
        #[serde(default)]
        metadata: HashMap<String, String>,
    },
    /// WebHook传输（必须使用直接URL）
    Webhook {
        /// WebHook端点（HTTP URL）
        endpoint: String,
        /// 密钥（可选）
        #[serde(default)]
        secret: Option<String>,
        /// 请求头（可选）
        #[serde(default)]
        headers: HashMap<String, String>,
    },
}

impl std::fmt::Debug for HookTransportConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Grpc {
                endpoint,
                service_name,
                registry_type,
                namespace,
                load_balance,
                metadata,
            } => f
                .debug_struct("Grpc")
                .field("endpoint", endpoint)
                .field("service_name", service_name)
                .field("registry_type", registry_type)
                .field("namespace", namespace)
                .field("load_balance", load_balance)
                .field("metadata_keys", &metadata.keys().collect::<Vec<_>>())
                .finish(),
            Self::Webhook {
                endpoint,
                secret,
                headers,
            } => f
                .debug_struct("Webhook")
                .field("endpoint", endpoint)
                .field(
                    "secret",
                    &secret.as_ref().map(|_| "<redacted>").unwrap_or("<none>"),
                )
                .field("header_names", &headers.keys().collect::<Vec<_>>())
                .finish(),
        }
    }
}

/// `CapabilityService/Administer` 的 `hook_config.create` 能否登记 `hook_type`；不能时返回原因
/// （只含类型名）。点位目录在 `flare_im_core_hooks`，现在只有发送前一个。
pub fn administrable_hook_type(hook_type: &str) -> Result<(), String> {
    if flare_im_core_hooks::hooks::HOOK_POINTS
        .iter()
        .any(|point| point.hook_type == hook_type)
    {
        return Ok(());
    }
    Err(format!(
        "unknown hook_type {hook_type:?}: the only hook point is {:?}",
        flare_im_core_hooks::hooks::PRE_SEND_HOOK_TYPE
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    /// 传输配置的 `Debug`（日志里 `?config` 的写法）不带签名密钥与头、元数据的值。
    #[test]
    fn transport_debug_never_prints_secrets_or_header_values() {
        let webhook = HookTransportConfig::Webhook {
            endpoint: "http://127.0.0.1:9/hook".into(),
            secret: Some("w12w-debug-secret".into()),
            headers: HashMap::from([("Authorization".into(), "Bearer w12w-header".into())]),
        };
        let grpc = HookTransportConfig::Grpc {
            endpoint: Some("http://127.0.0.1:9".into()),
            service_name: None,
            registry_type: None,
            namespace: None,
            load_balance: None,
            metadata: HashMap::from([("x-api-key".into(), "w12w-metadata".into())]),
        };
        let printed = format!("{webhook:?} {grpc:?} {webhook:#?}");
        for secret in ["w12w-debug-secret", "w12w-header", "w12w-metadata"] {
            assert!(!printed.contains(secret), "{printed}");
        }
        assert!(printed.contains("Authorization") && printed.contains("x-api-key"));
        assert!(printed.contains("<redacted>"));
    }

    #[test]
    fn test_load_balance_strategy_from_str() {
        assert_eq!(
            LoadBalanceStrategy::from_str("round_robin").unwrap(),
            LoadBalanceStrategy::RoundRobin
        );
        assert_eq!(
            LoadBalanceStrategy::from_str("random").unwrap(),
            LoadBalanceStrategy::Random
        );
        assert_eq!(
            LoadBalanceStrategy::from_str("consistent_hash").unwrap(),
            LoadBalanceStrategy::ConsistentHash
        );
        assert_eq!(
            LoadBalanceStrategy::from_str("least_conn").unwrap(),
            LoadBalanceStrategy::LeastConn
        );
        assert!(LoadBalanceStrategy::from_str("invalid").is_err());
    }

    /// Administer 只接受目录里的点位；删掉的点位和不认识的类型都报未知类型。
    #[test]
    fn only_hook_points_are_administrable() {
        assert_eq!(administrable_hook_type("pre_send"), Ok(()));
        for hook_type in [
            "post_send",
            "delivery",
            "recall",
            "message_read",
            "PRE_SEND",
            "",
        ] {
            let reason = administrable_hook_type(hook_type).unwrap_err();
            assert!(
                reason.contains("unknown hook_type"),
                "{hook_type}: {reason}"
            );
        }
    }
}
