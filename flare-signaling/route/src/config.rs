use flare_im_core::config::FlareAppConfig;
use flare_im_core::utils::normalize_tenant_id;
use flare_server_core::error::Result;
use std::env;

#[derive(Debug, Clone)]
pub struct RouteConfig {
    pub default_services: Vec<(String, String)>,
    /// 默认租户ID
    pub default_tenant_id: Option<String>,
    /// 分片数（默认 64）
    pub shard_count: usize,
    /// 会话级 QPS 限制（默认 50）
    pub session_qps_limit: u64,
    /// 群聊 fanout 最大速率（默认 2000/s）
    pub group_fanout_max: u64,
    /// 是否开启流控（默认关闭）
    pub flow_control_enabled: bool,
}

impl RouteConfig {
    /// 从应用配置加载（新方式，推荐）
    pub fn from_app_config(_app: &FlareAppConfig) -> Result<Self> {
        // 路由服务的默认服务端点通过环境变量配置（支持动态发现）
        // 使用新的 SVID 格式（svid.im, svid.customer, svid.ai.bot）
        let default: Vec<(String, String)> = vec![
            (
                "svid.im".to_string(),
                env::var("BUSINESS_SERVICE_IM_ENDPOINT")
                    .or_else(|_| env::var("ORCHESTRATOR_SERVICE"))
                    .unwrap_or_else(|_| flare_im_core::service_names::ORCHESTRATOR.to_string()),
            ),
            (
                "svid.customer".to_string(),
                env::var("BUSINESS_SERVICE_CS_ENDPOINT").unwrap_or_default(),
            ),
            (
                "svid.ai.bot".to_string(),
                env::var("BUSINESS_SERVICE_AI_ENDPOINT").unwrap_or_default(),
            ),
        ]
        .into_iter()
        .filter(|(_, endpoint)| !endpoint.is_empty())
        .collect();

        Ok(Self {
            default_services: if default.is_empty() {
                vec![(
                    "svid.im".to_string(),
                    flare_im_core::service_names::ORCHESTRATOR.to_string(),
                )]
            } else {
                default
            },
            default_tenant_id: env::var("DEFAULT_TENANT_ID").ok().map(normalize_tenant_id),
            shard_count: env::var("ROUTER_SHARD_COUNT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(64),
            session_qps_limit: env::var("ROUTER_SESSION_QPS_LIMIT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(50),
            group_fanout_max: env::var("ROUTER_GROUP_FANOUT_MAX")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(2000),
            flow_control_enabled: env::var("ROUTER_FLOW_CONTROL_ENABLED")
                .ok()
                .map(|v| v.to_lowercase() == "true")
                .unwrap_or(false),
        })
    }
}
