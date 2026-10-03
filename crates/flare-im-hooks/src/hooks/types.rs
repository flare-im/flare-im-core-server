use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::SystemTime;

use crate::Ctx;
use async_trait::async_trait;
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Hook 类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HookKind {
    PreSend,
    PostSend,
    Delivery,
    Recall,
    MessageRead,
    MessageReaction,
    ConversationLifecycle,
    ConversationMember,
    PushPreSend,
    PushPostSend,
    PushDelivery,
    Presence,
    UserLogin,
    UserLogout,
    UserOnline,
    UserOffline,
    Custom,
}

/// Hook 执行策略
///
/// 配置里的取值不区分大小写、忽略下划线：`FailFast`、`fail_fast`、`failfast` 等价，`Retry`、
/// `Ignore` 同理（Message Ingest 的配置写 `FailFast`，Capability 的配置与库里写 `fail_fast`）。
/// 其他取值是配置错误，在加载时拒绝，不按 `FailFast` 处理。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub enum HookErrorPolicy {
    /// 失败时终止主流程（快速失败）
    #[default]
    FailFast,
    /// 失败时重试（超过最大重试次数后记录告警）
    Retry,
    /// 失败时忽略（记录告警）
    Ignore,
}

impl HookErrorPolicy {
    /// 配置里的 `error_policy`：不区分大小写、忽略下划线；不认识的取值返回 `None`。
    pub fn parse(value: &str) -> Option<Self> {
        let normalized: String = value
            .chars()
            .filter(|c| *c != '_')
            .map(|c| c.to_ascii_lowercase())
            .collect();
        match normalized.as_str() {
            "failfast" => Some(Self::FailFast),
            "retry" => Some(Self::Retry),
            "ignore" => Some(Self::Ignore),
            _ => None,
        }
    }
}

/// 不认识的 `error_policy` 取值。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownHookErrorPolicy;

impl fmt::Display for UnknownHookErrorPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(
            "error_policy must be fail_fast, retry or ignore (case and underscores do not matter)",
        )
    }
}

impl std::error::Error for UnknownHookErrorPolicy {}

impl std::str::FromStr for HookErrorPolicy {
    type Err = UnknownHookErrorPolicy;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        Self::parse(value).ok_or(UnknownHookErrorPolicy)
    }
}

impl TryFrom<String> for HookErrorPolicy {
    type Error = UnknownHookErrorPolicy;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        value.parse()
    }
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
    pub conversation_id: Option<String>,
    pub operator_id: String,
    pub recalled_at: SystemTime,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

/// 已读回执事件。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageReadEvent {
    pub conversation_id: String,
    pub message_id: String,
    pub reader_user_id: String,
    pub device_id: Option<String>,
    pub read_at: SystemTime,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

/// 消息表态 / Reaction 事件。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageReactionEvent {
    pub conversation_id: String,
    pub message_id: String,
    pub actor_user_id: String,
    pub reaction_key: String,
    pub added: bool,
    pub occurred_at: SystemTime,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

/// 会话生命周期事件类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationLifecycleEventKind {
    Created,
    Updated,
    Archived,
    Muted,
    Unmuted,
    DissolvePending,
    Suspended,
    Dissolved,
    Restored,
}

/// 会话生命周期事件。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationLifecycleEvent {
    pub conversation_id: String,
    pub conversation_type: Option<String>,
    pub event: ConversationLifecycleEventKind,
    pub operator_user_id: Option<String>,
    pub participant_version: Option<u64>,
    pub occurred_at: SystemTime,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

/// 会话成员变更类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationMemberChangeKind {
    Invited,
    Joined,
    Removed,
    Left,
    RoleChanged,
    Muted,
    Unmuted,
    Blocked,
    Unblocked,
}

/// 会话成员变更事件。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationMemberEvent {
    pub conversation_id: String,
    pub tenant_id: Option<String>,
    pub change: ConversationMemberChangeKind,
    pub operator_user_id: Option<String>,
    pub affected_user_id: String,
    pub previous_role: Option<String>,
    pub new_role: Option<String>,
    pub participant_version: Option<u64>,
    pub occurred_at: SystemTime,
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

/// 可拦截型事件 Hook 的决策。
#[derive(Debug)]
pub enum HookDecision {
    Allow,
    Reject { error: FlareError },
}

impl HookDecision {
    pub fn is_allow(&self) -> bool {
        matches!(self, HookDecision::Allow)
    }

    pub fn into_result(self) -> Result<()> {
        match self {
            HookDecision::Allow => Ok(()),
            HookDecision::Reject { error } => Err(error),
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

/// Message-Read Hook Trait。
///
/// 只用于观测已读水位/会话已读，不允许修改消息事实。
#[async_trait]
pub trait MessageReadHook: Send + Sync {
    async fn handle(&self, ctx: &Ctx, event: &MessageReadEvent) -> HookOutcome;
}

/// Message-Reaction Hook Trait。
///
/// 可用于业务表态白名单、风控与旁路统计；是否允许阻断由宿主命令决定。
#[async_trait]
pub trait MessageReactionHook: Send + Sync {
    async fn handle(&self, ctx: &Ctx, event: &MessageReactionEvent) -> HookDecision;
}

/// Conversation-Lifecycle Hook Trait。
///
/// 会话事实应先提交，再异步通知业务侧；Hook 失败不得回滚已提交生命周期。
#[async_trait]
pub trait ConversationLifecycleHook: Send + Sync {
    async fn handle(&self, ctx: &Ctx, event: &ConversationLifecycleEvent) -> HookOutcome;
}

/// Conversation-Member Hook Trait。
///
/// 用于成员增删、角色、禁言等变化的业务观察或拦截。
#[async_trait]
pub trait ConversationMemberHook: Send + Sync {
    async fn handle(&self, ctx: &Ctx, event: &ConversationMemberEvent) -> HookDecision;
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
    ) -> flare_server_core::error::Result<Option<Vec<String>>>;
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

#[async_trait]
impl<T> MessageReadHook for Arc<T>
where
    T: MessageReadHook + ?Sized,
{
    async fn handle(&self, ctx: &Ctx, event: &MessageReadEvent) -> HookOutcome {
        (**self).handle(ctx, event).await
    }
}

#[async_trait]
impl<T> MessageReactionHook for Arc<T>
where
    T: MessageReactionHook + ?Sized,
{
    async fn handle(&self, ctx: &Ctx, event: &MessageReactionEvent) -> HookDecision {
        (**self).handle(ctx, event).await
    }
}

#[async_trait]
impl<T> ConversationLifecycleHook for Arc<T>
where
    T: ConversationLifecycleHook + ?Sized,
{
    async fn handle(&self, ctx: &Ctx, event: &ConversationLifecycleEvent) -> HookOutcome {
        (**self).handle(ctx, event).await
    }
}

#[async_trait]
impl<T> ConversationMemberHook for Arc<T>
where
    T: ConversationMemberHook + ?Sized,
{
    async fn handle(&self, ctx: &Ctx, event: &ConversationMemberEvent) -> HookDecision {
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
