//! # Hook领域模型
//!
//! 定义Hook引擎的核心领域模型

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use flare_im_hooks::{
    DeliveryEvent, HookGroup, HookMetadata, MessageDraft, MessageRecord, PreSendDecision,
    PreSendHook, RecallEvent,
};
use flare_server_core::context::{Context, Ctx};
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError, Result as FlareResult};
use tokio::time::timeout;

pub mod tenant_projection;
pub mod tenant_runtime;
pub use tenant_projection::{
    TenantProjectionOutcome, TenantProjectionRecord, TenantProjectionValidationError,
};
pub use tenant_runtime::TenantRuntimeView;

/// Hook执行模式
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ExecutionMode {
    /// 串行执行（保证顺序，快速失败）
    #[default]
    Sequential,
    /// 并发执行（提高性能）
    Concurrent,
}

/// Hook配置项
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookConfigItem {
    /// Hook名称
    pub name: String,
    /// Hook版本（可选）
    pub version: Option<String>,
    /// Hook描述（可选）
    pub description: Option<String>,
    /// 是否启用
    pub enabled: bool,
    /// 优先级（0-1000，数字越小优先级越高）
    /// 注意：priority < 100 自动归入business组，priority >= 100 自动归入validation组
    pub priority: i32,
    /// Hook分组（可选，如果不指定则根据priority自动分组）
    /// validation: 校验类Hook组（串行执行，快速失败）；Hook 不可达或超时按 `require_success` 拒绝或放行
    /// critical: 关键业务处理Hook组（串行执行，保证顺序）；Hook 不可达或超时一律拒绝，不看 `require_success`
    /// business: 非关键业务处理Hook组（容错）：拒绝与不可达都不中断主流程，`require_success` 只决定
    /// 不可达时记 WARN 还是 DEBUG
    /// 取值只认这三个（不区分大小写），其他值在加载与物化时报配置错误，见 [`HookConfigItem::resolved_group`]。
    #[serde(default)]
    pub group: Option<String>,
    /// 超时时间（毫秒）
    pub timeout_ms: u64,
    /// 最大重试次数（用于Retry策略）
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// 错误策略（fail_fast/retry/ignore）
    #[serde(default = "default_error_policy")]
    pub error_policy: String,
    /// 是否要求成功
    #[serde(default = "default_require_success")]
    pub require_success: bool,
    /// 选择器配置
    pub selector: HookSelectorConfig,
    /// 传输配置
    pub transport: HookTransportConfig,
    /// 元数据
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl HookConfigItem {
    /// 这条 Hook 所在的组：配置了 `group` 就按它（`validation` / `critical` / `business`，不区分大小写，
    /// 两端空白忽略），没配或为空串时按 priority 自动分组（`>= 100` 为 validation，否则 business）。
    ///
    /// 以前 `group` 只被存取、从不参与分组，critical 组因此永远为空。
    pub fn resolved_group(&self) -> FlareResult<HookGroup> {
        match self
            .group
            .as_deref()
            .map(str::trim)
            .filter(|g| !g.is_empty())
        {
            None => Ok(HookGroup::from_priority(self.priority)),
            Some(group) => parse_hook_group(group).ok_or_else(|| {
                ErrorBuilder::new(
                    ErrorCode::InvalidParameter,
                    "hook group must be validation, critical or business",
                )
                .details(format!("hook={}", self.name))
                .build_error()
            }),
        }
    }
}

fn parse_hook_group(value: &str) -> Option<HookGroup> {
    match value.to_ascii_lowercase().as_str() {
        "validation" => Some(HookGroup::Validation),
        "critical" => Some(HookGroup::Critical),
        "business" => Some(HookGroup::Business),
        _ => None,
    }
}

/// 进程内标记：这个拒绝不是 Hook 的决定，而是 Hook 没能给出决定（连不上、对端报错、应答解不开、
/// 超时）。只放在错误的 `params` 里；HookPlugin 据此回可重试的 gRPC 状态，而不是回
/// `allow=false`——调用方（Message Ingest）把 `allow=false` 当成策略拒绝（HTTP 422，不要重试）。
pub const HOOK_UNAVAILABLE_PARAM: &str = "hook_unavailable";

/// 这个错误是否为 Hook 不可用造成的拒绝（见 [`HOOK_UNAVAILABLE_PARAM`]）。
pub fn is_hook_unavailable(error: &FlareError) -> bool {
    matches!(
        error,
        FlareError::Localized {
            params: Some(params),
            ..
        } if params.get(HOOK_UNAVAILABLE_PARAM).is_some_and(|value| value == "true")
    )
}

fn default_max_retries() -> u32 {
    0
}

fn default_error_policy() -> String {
    "fail_fast".to_string()
}

fn default_require_success() -> bool {
    true
}

/// Hook选择器配置
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HookSelectorConfig {
    /// 租户列表（空表示匹配所有租户）
    #[serde(default)]
    pub tenants: Vec<String>,
    /// 会话类型列表（空表示匹配所有会话类型）
    #[serde(default)]
    pub conversation_types: Vec<String>,
    /// 消息类型列表（空表示匹配所有消息类型）
    #[serde(default)]
    pub message_types: Vec<String>,
    /// 用户ID列表（空表示匹配所有用户）
    #[serde(default)]
    pub user_ids: Vec<String>,
    /// 标签匹配
    #[serde(default)]
    pub tags: HashMap<String, String>,
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
#[derive(Debug, Clone, Deserialize, Serialize)]
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
    /// Local Plugin传输
    Local {
        /// 插件目标
        target: String,
    },
}

/// Hook配置
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct HookConfig {
    /// PreSend Hook配置列表
    #[serde(default)]
    pub pre_send: Vec<HookConfigItem>,
    /// PostSend Hook配置列表
    #[serde(default)]
    pub post_send: Vec<HookConfigItem>,
    /// Delivery Hook配置列表
    #[serde(default)]
    pub delivery: Vec<HookConfigItem>,
    /// Recall Hook配置列表
    #[serde(default)]
    pub recall: Vec<HookConfigItem>,
    /// SessionCreate Hook配置列表
    #[serde(default)]
    pub session_create: Vec<HookConfigItem>,
    /// SessionUpdate Hook配置列表
    #[serde(default)]
    pub session_update: Vec<HookConfigItem>,
    /// SessionDelete Hook配置列表
    #[serde(default)]
    pub session_delete: Vec<HookConfigItem>,
    /// UserLogin Hook配置列表
    #[serde(default)]
    pub user_login: Vec<HookConfigItem>,
    /// UserLogout Hook配置列表
    #[serde(default)]
    pub user_logout: Vec<HookConfigItem>,
    /// UserOnline Hook配置列表
    #[serde(default)]
    pub user_online: Vec<HookConfigItem>,
    /// UserOffline Hook配置列表
    #[serde(default)]
    pub user_offline: Vec<HookConfigItem>,
    /// PushPreSend Hook配置列表
    #[serde(default)]
    pub push_pre_send: Vec<HookConfigItem>,
    /// PushPostSend Hook配置列表
    #[serde(default)]
    pub push_post_send: Vec<HookConfigItem>,
    /// PushDelivery Hook配置列表
    #[serde(default)]
    pub push_delivery: Vec<HookConfigItem>,
    /// GetConversationParticipants Hook配置列表
    #[serde(default)]
    pub get_conversation_participants: Vec<HookConfigItem>,
}

/// Hook执行计划
pub struct HookExecutionPlan {
    metadata: HookMetadata,
    /// 所在组：配置项的 `group`，没配时按 priority（见 [`HookConfigItem::resolved_group`]）
    group: HookGroup,
    /// PreSend Hook处理器（可选，用于 Local Plugin）
    pre_send_handler: Option<Arc<dyn PreSendHook>>,
    /// Hook适配器（用于 gRPC/WebHook/Local Plugin）
    adapter: Option<Arc<dyn crate::infrastructure::adapters::HookAdapter>>,
    /// 传输配置（用于创建适配器）
    transport_config: Option<HookTransportConfig>,
    /// Local Plugin target（用于 Local 适配器）
    local_target: Option<String>,
}

impl std::fmt::Debug for HookExecutionPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HookExecutionPlan")
            .field("metadata", &self.metadata)
            .field("group", &self.group)
            .field(
                "pre_send_handler",
                &self.pre_send_handler.as_ref().map(|_| "Some(PreSendHook)"),
            )
            .field("has_adapter", &self.adapter.is_some())
            .finish()
    }
}

impl HookExecutionPlan {
    /// 从HookMetadata创建HookExecutionPlan（用于PreSend）
    pub fn new_pre_send(metadata: HookMetadata, handler: Arc<dyn PreSendHook>) -> Self {
        Self {
            group: HookGroup::from_priority(metadata.priority),
            metadata,
            pre_send_handler: Some(handler),
            adapter: None,
            transport_config: None,
            local_target: None,
        }
    }

    /// 从HookMetadata创建HookExecutionPlan（用于PostSend/Delivery/Recall）
    pub fn new(metadata: HookMetadata) -> Self {
        Self {
            group: HookGroup::from_priority(metadata.priority),
            metadata,
            pre_send_handler: None,
            adapter: None,
            transport_config: None,
            local_target: None,
        }
    }

    /// 指定所在组（覆盖按 priority 的自动分组）
    pub fn with_group(mut self, group: HookGroup) -> Self {
        self.group = group;
        self
    }

    /// 设置适配器
    pub fn with_adapter(
        mut self,
        adapter: Arc<dyn crate::infrastructure::adapters::HookAdapter>,
    ) -> Self {
        self.adapter = Some(adapter);
        self
    }

    /// 设置传输配置和本地目标
    pub fn with_transport(
        mut self,
        transport: HookTransportConfig,
        local_target: Option<String>,
    ) -> Self {
        self.transport_config = Some(transport);
        self.local_target = local_target;
        self
    }

    /// 获取适配器（如果已设置）
    pub fn adapter(&self) -> Option<&Arc<dyn crate::infrastructure::adapters::HookAdapter>> {
        self.adapter.as_ref()
    }

    /// 从HookConfigItem创建HookExecutionPlan
    ///
    /// # 参数
    /// * `config` - Hook配置项
    /// * `hook_type` - Hook类型（pre_send, post_send, delivery, recall等），用于设置HookKind
    pub fn from_hook_config(config: HookConfigItem, hook_type: &str) -> Self {
        use flare_im_hooks::HookErrorPolicy;
        let error_policy = match config.error_policy.as_str() {
            "fail_fast" => HookErrorPolicy::FailFast,
            "retry" => HookErrorPolicy::Retry,
            "ignore" => HookErrorPolicy::Ignore,
            _ => HookErrorPolicy::FailFast,
        };
        use flare_im_hooks::HookKind;
        // 根据hook_type字符串设置HookKind（只支持核心的4种类型，其他类型使用默认值）
        let kind = match hook_type {
            "pre_send" | "push_pre_send" => HookKind::PreSend,
            "post_send" | "push_post_send" => HookKind::PostSend,
            "delivery" | "push_delivery" => HookKind::Delivery,
            "recall" => HookKind::Recall,
            // 其他类型（conversation_lifecycle, user_login等）使用PreSend作为默认值
            _ => HookKind::PreSend,
        };
        let metadata = HookMetadata {
            name: Arc::from(config.name.as_str()),
            version: config.version.as_ref().map(|v| Arc::from(v.as_str())),
            description: config.description.as_ref().map(|d| Arc::from(d.as_str())),
            kind,
            priority: config.priority,
            timeout: Duration::from_millis(config.timeout_ms),
            max_retries: config.max_retries,
            error_policy,
            require_success: config.require_success,
        };
        // 物化前已校验（`validate_hook_item_for_materialization`、`ConfigValidator`）；直接构造的
        // 非法分组按 priority 归组并告警，不让一条配置拖垮整条链。
        let group = config.resolved_group().unwrap_or_else(|error| {
            tracing::warn!(%error, "invalid hook group, falling back to priority grouping");
            HookGroup::from_priority(config.priority)
        });
        Self {
            metadata,
            group,
            pre_send_handler: None,
            adapter: None,
            transport_config: Some(config.transport.clone()),
            local_target: match &config.transport {
                HookTransportConfig::Local { target } => Some(target.clone()),
                _ => None,
            },
        }
    }

    pub fn metadata(&self) -> &HookMetadata {
        &self.metadata
    }

    pub fn name(&self) -> &str {
        &self.metadata.name
    }

    pub fn priority(&self) -> i32 {
        self.metadata.priority
    }

    pub fn timeout(&self) -> Duration {
        self.metadata.timeout
    }

    pub fn group(&self) -> HookGroup {
        self.group
    }

    pub fn require_success(&self) -> bool {
        self.metadata.require_success
    }

    /// Hook 没能给出结果（不可达、对端报错、超时）时是否让这一步失败：critical 组一律失败，
    /// 其余组按 `require_success`。business 组的失败与拒绝在编排层只记日志，不中断主流程。
    pub fn must_succeed(&self) -> bool {
        self.group == HookGroup::Critical || self.metadata.require_success
    }

    /// 执行PreSend Hook
    pub async fn execute(
        &self,
        ctx: &Ctx,
        draft: &mut MessageDraft,
    ) -> FlareResult<PreSendDecision> {
        let fut = async {
            if let Some(ref adapter) = self.adapter {
                adapter.pre_send(ctx, draft).await
            } else if let Some(ref handler) = self.pre_send_handler {
                Ok(handler.handle(ctx, draft).await)
            } else {
                Ok(PreSendDecision::Continue)
            }
        };

        match timeout(self.metadata.timeout, fut).await {
            Ok(Ok(decision)) => Ok(decision),
            // Hook 没能给出决定（连不上、对端报错、应答解码失败）与超时同样处理：必需的 Hook
            // 拒绝，可选的放行。以前这里原样上抛，不可达的可选 Hook 与 business 组也让每次
            // PreSend 失败。
            Ok(Err(cause)) => Ok(self.unavailable_decision("pre-send", &cause)),
            Err(_) => Ok(self.timeout_decision("pre-send")),
        }
    }

    /// Hook 超时时的决定：与不可达同样按 [`Self::must_succeed`] 拒绝或放行，拒绝带
    /// [`HOOK_UNAVAILABLE_PARAM`] 标记。
    fn timeout_decision(&self, stage: &'static str) -> PreSendDecision {
        if self.must_succeed() {
            tracing::warn!(hook = %self.name(), stage, group = ?self.group, "required hook timed out, rejecting");
            PreSendDecision::Reject {
                error: unavailable_rejection(
                    ErrorCode::OperationTimeout,
                    format!("{stage} hook timed out"),
                    self.name(),
                ),
            }
        } else {
            self.log_skipped_unavailable(stage, "timed out");
            PreSendDecision::Continue
        }
    }

    /// 可选 Hook（不要求成功、也不在 critical 组）不可用被跳过：validation 组记 WARN，business 组
    /// 记 DEBUG（business 组本就容错；要求成功的 business Hook 不可用时由编排层记 WARN）。
    fn log_skipped_unavailable(&self, stage: &'static str, what: &'static str) {
        if self.group == HookGroup::Business && !self.require_success() {
            tracing::debug!(hook = %self.name(), stage, what, "optional business hook unavailable, ignored");
        } else {
            tracing::warn!(hook = %self.name(), stage, what, group = ?self.group, "optional hook unavailable, ignored");
        }
    }

    /// Hook 调用失败（非超时）时的决定，与超时分支一致：[`Self::must_succeed`] 时拒绝（带
    /// [`HOOK_UNAVAILABLE_PARAM`] 标记），否则放行。
    ///
    /// 拒绝原因只带 Hook 名，不带下游错误原文（其中可能有内部地址），原文只进日志。
    fn unavailable_decision(&self, stage: &'static str, cause: &FlareError) -> PreSendDecision {
        if self.must_succeed() {
            tracing::warn!(hook = %self.name(), stage, group = ?self.group, error = %cause, "required hook unavailable, rejecting");
            PreSendDecision::Reject {
                error: unavailable_rejection(
                    ErrorCode::ServiceUnavailable,
                    format!("{stage} hook unavailable"),
                    self.name(),
                ),
            }
        } else {
            tracing::debug!(hook = %self.name(), stage, error = %cause, "hook call failed");
            self.log_skipped_unavailable(stage, "call failed");
            PreSendDecision::Continue
        }
    }

    /// 执行PostSend Hook
    pub async fn execute_post_send(
        &self,
        ctx: &Ctx,
        record: &MessageRecord,
        draft: &MessageDraft,
    ) -> FlareResult<()> {
        let fut = async {
            if let Some(ref adapter) = self.adapter {
                adapter.post_send(ctx, record, draft).await
            } else {
                Ok(())
            }
        };

        match timeout(self.metadata.timeout, fut).await {
            Ok(inner) => inner,
            Err(_) => {
                let err =
                    ErrorBuilder::new(ErrorCode::OperationTimeout, "post-send hook timed out")
                        .details(format!("hook={}", self.name()))
                        .build_error();
                if self.must_succeed() {
                    Err(err)
                } else {
                    tracing::warn!(hook = %self.name(), "post-send hook timeout ignored");
                    Ok(())
                }
            }
        }
    }

    /// 执行Delivery Hook
    pub async fn execute_delivery(&self, ctx: &Ctx, event: &DeliveryEvent) -> FlareResult<()> {
        let fut = async {
            if let Some(ref adapter) = self.adapter {
                adapter.delivery(ctx, event).await
            } else {
                Ok(())
            }
        };

        match timeout(self.metadata.timeout, fut).await {
            Ok(inner) => inner,
            Err(_) => {
                let err = ErrorBuilder::new(ErrorCode::OperationTimeout, "delivery hook timed out")
                    .details(format!("hook={}", self.name()))
                    .build_error();
                if self.must_succeed() {
                    Err(err)
                } else {
                    tracing::warn!(hook = %self.name(), "delivery hook timeout ignored");
                    Ok(())
                }
            }
        }
    }

    /// 执行Recall Hook
    pub async fn execute_recall(
        &self,
        ctx: &Context,
        event: &RecallEvent,
    ) -> FlareResult<PreSendDecision> {
        let fut = async {
            if let Some(ref adapter) = self.adapter {
                adapter.recall(ctx, event).await
            } else {
                Ok(PreSendDecision::Continue)
            }
        };

        match timeout(self.metadata.timeout, fut).await {
            Ok(Ok(decision)) => Ok(decision),
            Ok(Err(cause)) => Ok(self.unavailable_decision("recall", &cause)),
            Err(_) => Ok(self.timeout_decision("recall")),
        }
    }
}

/// Hook 不可用造成的拒绝：错误码区分不可达与超时，原因固定，说明只有 Hook 名，带
/// [`HOOK_UNAVAILABLE_PARAM`] 标记。
fn unavailable_rejection(code: ErrorCode, reason: String, hook: &str) -> FlareError {
    ErrorBuilder::new(code, reason)
        .details(format!("hook={hook}"))
        .param(HOOK_UNAVAILABLE_PARAM, "true")
        .build_error()
}

/// Hook执行结果
#[derive(Debug, Clone)]
pub struct HookExecutionResult {
    pub hook_name: String,
    pub executed_at: SystemTime,
    pub success: bool,
    pub latency_ms: u64,
    pub error_message: Option<String>,
}

/// Hook统计信息
#[derive(Debug, Clone, Default)]
pub struct HookStatistics {
    pub total_count: u64,
    pub success_count: u64,
    pub failure_count: u64,
    pub avg_latency_ms: f64,
    pub max_latency_ms: u64,
    pub min_latency_ms: u64,
}

impl HookStatistics {
    pub fn success_rate(&self) -> f64 {
        if self.total_count == 0 {
            return 1.0;
        }
        self.success_count as f64 / self.total_count as f64
    }

    pub fn update(&mut self, result: &HookExecutionResult) {
        self.total_count += 1;
        if result.success {
            self.success_count += 1;
        } else {
            self.failure_count += 1;
        }

        // 更新延迟统计
        if self.total_count == 1 {
            self.avg_latency_ms = result.latency_ms as f64;
            self.max_latency_ms = result.latency_ms;
            self.min_latency_ms = result.latency_ms;
        } else {
            // 计算新的平均值
            self.avg_latency_ms = (self.avg_latency_ms * (self.total_count - 1) as f64
                + result.latency_ms as f64)
                / self.total_count as f64;

            if result.latency_ms > self.max_latency_ms {
                self.max_latency_ms = result.latency_ms;
            }
            if result.latency_ms < self.min_latency_ms {
                self.min_latency_ms = result.latency_ms;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

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

    #[test]
    fn test_hook_statistics() {
        let mut stats = HookStatistics::default();
        assert_eq!(stats.success_rate(), 1.0);

        stats.update(&HookExecutionResult {
            hook_name: "test".to_string(),
            executed_at: SystemTime::now(),
            success: true,
            latency_ms: 100,
            error_message: None,
        });
        assert_eq!(stats.total_count, 1);
        assert_eq!(stats.success_count, 1);
        assert_eq!(stats.failure_count, 0);
        assert_eq!(stats.avg_latency_ms, 100.0);
        assert_eq!(stats.success_rate(), 1.0);

        stats.update(&HookExecutionResult {
            hook_name: "test".to_string(),
            executed_at: SystemTime::now(),
            success: false,
            latency_ms: 200,
            error_message: Some("error".to_string()),
        });
        assert_eq!(stats.total_count, 2);
        assert_eq!(stats.success_count, 1);
        assert_eq!(stats.failure_count, 1);
        assert_eq!(stats.avg_latency_ms, 150.0);
        assert_eq!(stats.success_rate(), 0.5);
    }

    #[test]
    fn test_hook_execution_plan_from_config() {
        let config = HookConfigItem {
            name: "test-hook".to_string(),
            version: Some("1.0.0".to_string()),
            description: Some("Test hook".to_string()),
            enabled: true,
            priority: 50,
            group: None,
            timeout_ms: 1000,
            max_retries: 3,
            error_policy: "retry".to_string(),
            require_success: true,
            selector: HookSelectorConfig::default(),
            transport: HookTransportConfig::Grpc {
                endpoint: Some("http://localhost:50051".to_string()),
                service_name: None,
                registry_type: None,
                namespace: None,
                load_balance: None,
                metadata: HashMap::new(),
            },
            metadata: HashMap::new(),
        };

        let plan = HookExecutionPlan::from_hook_config(config.clone(), "pre_send");
        assert_eq!(plan.name(), "test-hook");
        assert_eq!(plan.priority(), 50);
        assert_eq!(plan.timeout(), Duration::from_millis(1000));

        let plan = HookExecutionPlan::from_hook_config(config.clone(), "post_send");
        assert_eq!(plan.metadata().kind, flare_im_hooks::HookKind::PostSend);

        let plan = HookExecutionPlan::from_hook_config(config.clone(), "delivery");
        assert_eq!(plan.metadata().kind, flare_im_hooks::HookKind::Delivery);

        let plan = HookExecutionPlan::from_hook_config(config, "recall");
        assert_eq!(plan.metadata().kind, flare_im_hooks::HookKind::Recall);
    }

    fn config_item(priority: i32, group: Option<&str>) -> HookConfigItem {
        HookConfigItem {
            name: "grouped".to_string(),
            version: None,
            description: None,
            enabled: true,
            priority,
            group: group.map(str::to_string),
            timeout_ms: 1000,
            max_retries: 0,
            error_policy: "fail_fast".to_string(),
            require_success: false,
            selector: HookSelectorConfig::default(),
            transport: HookTransportConfig::Local {
                target: "noop".to_string(),
            },
            metadata: HashMap::new(),
        }
    }

    /// 配置的 group 优先于 priority；没配或为空串时按 priority；未知值报错。
    #[test]
    fn configured_group_wins_over_priority() {
        use flare_im_hooks::HookGroup;
        for (priority, group, expected) in [
            (10, Some("critical"), HookGroup::Critical),
            (150, Some(" CRITICAL "), HookGroup::Critical),
            (150, Some("business"), HookGroup::Business),
            (10, Some("Validation"), HookGroup::Validation),
            (150, None, HookGroup::Validation),
            (10, None, HookGroup::Business),
            (10, Some("  "), HookGroup::Business),
        ] {
            let item = config_item(priority, group);
            assert_eq!(item.resolved_group().unwrap(), expected, "{group:?}");
            let plan = HookExecutionPlan::from_hook_config(item, "pre_send");
            assert_eq!(plan.group(), expected, "{group:?}");
        }
        assert!(config_item(10, Some("critcal")).resolved_group().is_err());
    }

    /// critical 组的 Hook 不论 `require_success` 都必须成功；其余组按 `require_success`。
    #[test]
    fn critical_hooks_must_succeed() {
        let plan =
            HookExecutionPlan::from_hook_config(config_item(10, Some("critical")), "pre_send");
        assert!(!plan.require_success());
        assert!(plan.must_succeed());
        let plan = HookExecutionPlan::from_hook_config(config_item(150, None), "pre_send");
        assert!(!plan.must_succeed());
    }

    #[test]
    fn test_execution_mode_default() {
        assert_eq!(ExecutionMode::default(), ExecutionMode::Sequential);
    }
}
