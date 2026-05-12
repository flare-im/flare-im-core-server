use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::SystemTime;

use crate::Ctx;
use crate::error::{ErrorBuilder, ErrorCode, FlareError, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Hook 类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HookKind {
    PreSend,
    PostSend,
    Delivery,
    Recall,
}

/// Hook 执行策略
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum HookErrorPolicy {
    /// 失败时终止主流程（快速失败）
    #[default]
    FailFast,
    /// 失败时重试（超过最大重试次数后记录告警）
    Retry,
    /// 失败时忽略（记录告警）
    Ignore,
}

/// Hook分组
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum HookGroup {
    /// 校验类Hook组（串行执行，快速失败）
    Validation,
    /// 关键业务处理Hook组（串行执行，保证顺序）
    Critical,
    /// 非关键业务处理Hook组（并发执行，容错）
    #[default]
    Business,
}

impl HookGroup {
    /// 根据priority自动分组
    pub fn from_priority(priority: i32) -> Self {
        if priority >= 100 {
            HookGroup::Validation
        } else {
            HookGroup::Business
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

/// 消息持久化记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageRecord {
    pub message_id: String,
    pub client_message_id: Option<String>,
    pub conversation_id: String,
    pub sender_id: String,
    pub conversation_type: Option<String>,
    pub message_type: Option<String>,
    pub persisted_at: SystemTime,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

/// 投递事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryEvent {
    pub message_id: String,
    pub user_id: String,
    pub channel: String,
    pub delivered_at: SystemTime,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

/// 撤回事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecallEvent {
    pub message_id: String,
    pub operator_id: String,
    pub recalled_at: SystemTime,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
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

impl fmt::Display for PreSendDecision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PreSendDecision::Continue => write!(f, "continue"),
            PreSendDecision::Reject { error } => write!(f, "reject: {error}"),
        }
    }
}

/// Hook 执行结果
#[derive(Debug)]
pub enum HookOutcome {
    Completed,
    Failed(FlareError),
}

impl HookOutcome {
    pub fn is_completed(&self) -> bool {
        matches!(self, HookOutcome::Completed)
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

/// Post-Send Hook Trait
///
/// 注意：此 trait 使用 #[async_trait] 是因为 Hook 系统需要运行时多态。
#[async_trait]
pub trait PostSendHook: Send + Sync {
    async fn handle(&self, ctx: &Ctx, record: &MessageRecord, draft: &MessageDraft) -> HookOutcome;
}

/// Delivery Hook Trait
///
/// 注意：此 trait 使用 #[async_trait] 是因为 Hook 系统需要运行时多态。
#[async_trait]
pub trait DeliveryHook: Send + Sync {
    async fn handle(&self, ctx: &Ctx, event: &DeliveryEvent) -> HookOutcome;
}

/// Recall Hook Trait
///
/// 注意：此 trait 使用 #[async_trait] 是因为 Hook 系统需要运行时多态。
#[async_trait]
pub trait RecallHook: Send + Sync {
    async fn handle(&self, ctx: &Ctx, event: &RecallEvent) -> HookOutcome;
}

/// GetConversationParticipants Hook Trait
///
/// 业务系统可以通过实现此 Hook 来提供会话参与者列表
/// 如果 Hook 未实现或返回错误，系统将降级到数据库查询
///
/// 注意：此 trait 使用 #[async_trait] 是因为 Hook 系统需要运行时多态。
#[async_trait]
pub trait GetConversationParticipantsHook: Send + Sync {
    /// 获取会话的所有参与者
    ///
    /// # 参数
    /// * `ctx` - Hook上下文
    /// * `conversation_id` - 会话ID
    ///
    /// # 返回
    /// * `Ok(Some(participants))` - 成功获取参与者列表
    /// * `Ok(None)` - Hook未处理，降级到数据库查询
    /// * `Err(e)` - Hook执行失败，降级到数据库查询
    async fn get_participants(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
    ) -> anyhow::Result<Option<Vec<String>>>;
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

#[async_trait]
impl<T> PostSendHook for Arc<T>
where
    T: PostSendHook + ?Sized,
{
    async fn handle(&self, ctx: &Ctx, record: &MessageRecord, draft: &MessageDraft) -> HookOutcome {
        (**self).handle(ctx, record, draft).await
    }
}

#[async_trait]
impl<T> DeliveryHook for Arc<T>
where
    T: DeliveryHook + ?Sized,
{
    async fn handle(&self, ctx: &Ctx, event: &DeliveryEvent) -> HookOutcome {
        (**self).handle(ctx, event).await
    }
}

#[async_trait]
impl<T> RecallHook for Arc<T>
where
    T: RecallHook + ?Sized,
{
    async fn handle(&self, ctx: &Ctx, event: &RecallEvent) -> HookOutcome {
        (**self).handle(ctx, event).await
    }
}

impl HookOutcome {
    pub fn into_result(self, metadata: &HookMetadata) -> Result<()> {
        match self {
            HookOutcome::Completed => Ok(()),
            HookOutcome::Failed(err) => {
                if metadata.error_policy == HookErrorPolicy::Ignore {
                    tracing::warn!(
                        hook = %metadata.name,
                        "hook failed but configured to ignore: {err}"
                    );
                    Ok(())
                } else {
                    Err(err)
                }
            }
        }
    }
}

/// Hook 注册元信息
#[derive(Debug, Clone)]
pub struct HookMetadata {
    pub name: Arc<str>,
    pub version: Option<Arc<str>>,
    pub description: Option<Arc<str>>,
    pub kind: HookKind,
    pub priority: i32,
    pub timeout: std::time::Duration,
    pub max_retries: u32,
    pub error_policy: HookErrorPolicy,
    pub require_success: bool,
}

impl Default for HookMetadata {
    fn default() -> Self {
        Self {
            name: Arc::from("anonymous"),
            version: None,
            description: None,
            kind: HookKind::PreSend,
            priority: 0,
            timeout: std::time::Duration::from_millis(3_000),
            max_retries: 0,
            error_policy: HookErrorPolicy::FailFast,
            require_success: true,
        }
    }
}

impl HookMetadata {
    pub fn with_kind(mut self, kind: HookKind) -> Self {
        self.kind = kind;
        self
    }

    pub fn with_name<T: Into<Arc<str>>>(mut self, name: T) -> Self {
        self.name = name.into();
        self
    }

    pub fn with_version<T: Into<Arc<str>>>(mut self, version: Option<T>) -> Self {
        self.version = version.map(Into::into);
        self
    }

    pub fn with_description<T: Into<Arc<str>>>(mut self, description: Option<T>) -> Self {
        self.description = description.map(Into::into);
        self
    }

    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    pub fn with_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_error_policy(mut self, policy: HookErrorPolicy) -> Self {
        self.error_policy = policy;
        self
    }

    pub fn with_require_success(mut self, require_success: bool) -> Self {
        self.require_success = require_success;
        self
    }

    pub fn build_error(&self, code: ErrorCode, message: &str) -> FlareError {
        ErrorBuilder::new(code, message)
            .details(format!("hook={}", self.name))
            .build_error()
    }
}
