//! # Hook领域模型
//!
//! 定义Hook引擎的核心领域模型

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use flare_im_core::error::{ErrorBuilder, ErrorCode, Result as FlareResult};
use flare_im_core::{
    DeliveryEvent, HookGroup, HookMetadata, MessageDraft, MessageRecord, PreSendDecision,
    PreSendHook, RecallEvent,
};
use flare_server_core::context::{Context, Ctx};
use tokio::time::timeout;

/// Hook执行模式
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionMode {
    /// 串行执行（保证顺序，快速失败）
    Sequential,
    /// 并发执行（提高性能）
    Concurrent,
}

impl Default for ExecutionMode {
    fn default() -> Self {
        ExecutionMode::Sequential
    }
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
    /// validation: 校验类Hook组（串行执行，快速失败）
    /// critical: 关键业务处理Hook组（串行执行，保证顺序）
    /// business: 非关键业务处理Hook组（并发执行，容错）
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
pub enum LoadBalanceStrategy {
    /// 轮询（Round Robin）
    RoundRobin,
    /// 随机（Random）
    Random,
    /// 一致性哈希（Consistent Hash）
    ConsistentHash,
    /// 最少连接（Least Connections）
    LeastConn,
}

impl Default for LoadBalanceStrategy {
    fn default() -> Self {
        LoadBalanceStrategy::RoundRobin
    }
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
            metadata,
            pre_send_handler: None,
            adapter: None,
            transport_config: None,
            local_target: None,
        }
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
        use flare_im_core::HookErrorPolicy;
        let error_policy = match config.error_policy.as_str() {
            "fail_fast" => HookErrorPolicy::FailFast,
            "retry" => HookErrorPolicy::Retry,
            "ignore" => HookErrorPolicy::Ignore,
            _ => HookErrorPolicy::FailFast,
        };
        use flare_im_core::HookKind;
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
        Self {
            metadata,
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
        // HookGroup::from_priority 会根据 priority 自动分组
        HookGroup::from_priority(self.metadata.priority)
    }

    pub fn require_success(&self) -> bool {
        self.metadata.require_success
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
            Ok(inner) => inner,
            Err(_) => {
                let err = ErrorBuilder::new(ErrorCode::OperationTimeout, "pre-send hook timed out")
                    .details(format!("hook={}", self.name()))
                    .build_error();
                if self.require_success() {
                    Ok(PreSendDecision::Reject { error: err })
                } else {
                    tracing::warn!(hook = %self.name(), "pre-send hook timeout ignored");
                    Ok(PreSendDecision::Continue)
                }
            }
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
                if self.require_success() {
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
                if self.require_success() {
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
            Ok(inner) => inner,
            Err(_) => {
                let err = ErrorBuilder::new(ErrorCode::OperationTimeout, "recall hook timed out")
                    .details(format!("hook={}", self.name()))
                    .build_error();
                if self.require_success() {
                    Ok(PreSendDecision::Reject { error: err })
                } else {
                    tracing::warn!(hook = %self.name(), "recall hook timeout ignored");
                    Ok(PreSendDecision::Continue)
                }
            }
        }
    }
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
        assert_eq!(plan.metadata().kind, flare_im_core::HookKind::PostSend);

        let plan = HookExecutionPlan::from_hook_config(config.clone(), "delivery");
        assert_eq!(plan.metadata().kind, flare_im_core::HookKind::Delivery);

        let plan = HookExecutionPlan::from_hook_config(config, "recall");
        assert_eq!(plan.metadata().kind, flare_im_core::HookKind::Recall);
    }

    #[test]
    fn test_execution_mode_default() {
        assert_eq!(ExecutionMode::default(), ExecutionMode::Sequential);
    }
}
