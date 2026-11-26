use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::Ctx;
use async_trait::async_trait;
use flare_server_core::error::{ErrorBuilder, FlareError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Hook 的配置来源，也是调用链上的先后：应用 → 平台 → 部署。
///
/// 部署排最后：部署 Hook 看到的是最终内容，它的放行是最终结论，后面不会再有 Hook 否决。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookSource {
    /// `hook_configs`，`tenant_id` 为应用。
    Application,
    /// `hook_configs`，`tenant_id` 为空。
    Platform,
    /// 部署文件。
    Deployment,
}

impl HookSource {
    /// 日志与摘要里的取值（固定）。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Application => "application",
            Self::Platform => "platform",
            Self::Deployment => "deployment",
        }
    }
}

// Hook 特定的数据通过 Context 的自定义数据存储（见 HookContextData）

/// 消息草稿（Pre-Send 阶段可修改）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageDraft {
    pub message_id: Option<String>,
    pub client_message_id: Option<String>,
    pub conversation_id: Option<String>,
    pub payload: Vec<u8>,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
    #[serde(default)]
    pub extra: HashMap<String, JsonValue>,
}

impl MessageDraft {
    pub fn new(payload: Vec<u8>) -> Self {
        Self {
            message_id: None,
            client_message_id: None,
            conversation_id: None,
            payload,
            headers: HashMap::new(),
            metadata: HashMap::new(),
            extra: HashMap::new(),
        }
    }

    pub fn set_message_id<T: Into<String>>(&mut self, message_id: T) {
        self.message_id = Some(message_id.into());
    }

    pub fn set_client_message_id<T: Into<String>>(&mut self, message_id: T) {
        self.client_message_id = Some(message_id.into());
    }

    pub fn set_conversation_id<T: Into<String>>(&mut self, conversation_id: T) {
        self.conversation_id = Some(conversation_id.into());
    }

    pub fn header<T: Into<String>, U: Into<String>>(&mut self, key: T, value: U) {
        self.headers.insert(key.into(), value.into());
    }

    pub fn metadata<T: Into<String>, U: Into<String>>(&mut self, key: T, value: U) {
        self.metadata.insert(key.into(), value.into());
    }

    pub fn extra<T: Into<String>>(&mut self, key: T, value: JsonValue) {
        self.extra.insert(key.into(), value);
    }
}

/// Pre-Send Hook 的决策
#[derive(Debug)]
pub enum PreSendDecision {
    Continue,
    Reject { error: FlareError },
}

impl PreSendDecision {
    pub fn is_continue(&self) -> bool {
        matches!(self, PreSendDecision::Continue)
    }
}

impl From<Result<()>> for PreSendDecision {
    fn from(value: Result<()>) -> Self {
        match value {
            Ok(_) => PreSendDecision::Continue,
            Err(err) => PreSendDecision::Reject { error: err },
        }
    }
}

/// 进程内标记：PreSend 扩展明确拒绝了这条消息（gRPC Hook 回 `allow=false`、WebHook 回
/// `allow: false`），而不是扩展不可用、超时或出错。只放在错误的 `params` 里，不进
/// `ErrorDetail`，客户端看到的错误码、原因与说明都不变；摄入服务据此把策略拒绝回成 gRPC
/// `FAILED_PRECONDITION`，调用方不再把它当成下游故障。
pub const PRE_SEND_REJECTION_PARAM: &str = "pre_send_rejection";

/// 构建 PreSend 扩展的明确拒绝，并打上 [`PRE_SEND_REJECTION_PARAM`] 标记。
pub fn pre_send_rejection(error: ErrorBuilder) -> FlareError {
    error.param(PRE_SEND_REJECTION_PARAM, "true").build_error()
}

/// 这个错误是否为 PreSend 扩展的明确拒绝（见 [`PRE_SEND_REJECTION_PARAM`]）。
pub fn is_pre_send_rejection(error: &FlareError) -> bool {
    matches!(
        error,
        FlareError::Localized {
            params: Some(params),
            ..
        } if params.get(PRE_SEND_REJECTION_PARAM).is_some_and(|value| value == "true")
    )
}

impl fmt::Display for PreSendDecision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PreSendDecision::Continue => write!(f, "continue"),
            PreSendDecision::Reject { error } => write!(f, "reject: {error}"),
        }
    }
}

/// Pre-Send Hook Trait
///
/// 注意：此 trait 使用 #[async_trait] 是因为 Hook 系统需要运行时多态，
/// 允许在运行时动态注册和执行不同的 Hook 实现。
#[async_trait]
pub trait PreSendHook: Send + Sync {
    async fn handle(&self, ctx: &Ctx, draft: &mut MessageDraft) -> PreSendDecision;
}

#[async_trait]
impl<T> PreSendHook for Arc<T>
where
    T: PreSendHook + ?Sized,
{
    async fn handle(&self, ctx: &Ctx, draft: &mut MessageDraft) -> PreSendDecision {
        (**self).handle(ctx, draft).await
    }
}

/// Hook 注册元信息
#[derive(Debug, Clone)]
pub struct HookMetadata {
    pub name: Arc<str>,
    pub description: Option<Arc<str>>,
    pub source: HookSource,
    /// 同一来源内的先后，小的先执行。
    pub order: i32,
    pub timeout: Duration,
    /// 必需：目标不可用时回可重试的 503；否则跳过。明确拒绝不看它。
    pub require_success: bool,
    /// 加载它的 `hook_configs` 行号；部署文件的 Hook 没有。熔断与执行记录按它区分配置的 Hook：
    /// 删掉后同名重建的是另一条 Hook，统计和熔断从零开始。
    pub config_id: Option<i64>,
}

impl Default for HookMetadata {
    fn default() -> Self {
        Self {
            name: Arc::from("anonymous"),
            description: None,
            source: HookSource::Deployment,
            order: 0,
            timeout: Duration::from_millis(3_000),
            require_success: true,
            config_id: None,
        }
    }
}

impl HookMetadata {
    pub fn with_name<T: Into<Arc<str>>>(mut self, name: T) -> Self {
        self.name = name.into();
        self
    }

    pub fn with_description<T: Into<Arc<str>>>(mut self, description: Option<T>) -> Self {
        self.description = description.map(Into::into);
        self
    }

    pub fn with_source(mut self, source: HookSource) -> Self {
        self.source = source;
        self
    }

    pub fn with_order(mut self, order: i32) -> Self {
        self.order = order;
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_require_success(mut self, require_success: bool) -> Self {
        self.require_success = require_success;
        self
    }

    pub fn with_config_id(mut self, config_id: Option<i64>) -> Self {
        self.config_id = config_id;
        self
    }
}
