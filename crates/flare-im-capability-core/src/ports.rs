//! Capability extension ports.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use flare_core_base::context::Ctx;

use crate::context::{ConversationKind, PreSendEvaluateInput, ResolveTrigger};
use crate::error::{CapabilityError, GuardDecision, Result as CapabilityResult};
use crate::grant::{TenantCapabilitySwitch, UserCapabilityGrant};
use crate::recipient::{RecipientResolveRequest, RecipientResolveResult};

#[async_trait]
pub trait PreSendGuard: Send + Sync {
    fn id(&self) -> &str;

    async fn evaluate(
        &self,
        ctx: &Ctx,
        input: &PreSendEvaluateInput,
    ) -> CapabilityResult<GuardDecision>;
}

#[async_trait]
pub trait PreSendGuardPipeline: Send + Sync {
    async fn evaluate(
        &self,
        ctx: &Ctx,
        input: &PreSendEvaluateInput,
    ) -> CapabilityResult<GuardDecision>;
}

#[async_trait]
pub trait RecipientResolver: Send + Sync {
    fn id(&self) -> &str;

    fn supports(&self, kind: &ConversationKind, trigger: ResolveTrigger) -> bool;

    async fn resolve(
        &self,
        ctx: &Ctx,
        req: &RecipientResolveRequest,
    ) -> CapabilityResult<RecipientResolveResult>;
}

#[async_trait]
pub trait CapabilityPolicyBackend: Send + Sync {
    async fn ensure_dispatch_allowed(
        &self,
        tenant_id: &str,
        user_id: &str,
        capability_id: &str,
    ) -> CapabilityResult<()>;

    /// 严格租户开关检查；缺少开关时拒绝。开关不代表平台审批授权，
    /// 也不能代替调用者的 operation 权限检查。
    async fn ensure_tenant_capability_enabled(
        &self,
        _tenant_id: &str,
        _capability_id: &str,
    ) -> CapabilityResult<()> {
        Err(CapabilityError::NotSupported(
            "this policy backend does not implement tenant capability switches".into(),
        ))
    }

    /// Emergency stop for a capability whose authorization is not a per-user grant (plugin
    /// operations, authorized by the tenant's installation projection): refused when the global
    /// switch is off, when the tenant switch of `capability_id` exists and is off, or when a
    /// tenant switch `<prefix>.*` whose prefix (with its dot) starts `capability_id` exists and
    /// is off — `example.poll.*` stops every operation of the plugin `example.poll` with one row.
    /// Any matching switch that is off stops the call; a more specific one that is on does not
    /// lift it. An absent tenant switch is not a refusal, and no user grant is read.
    ///
    /// No default: a backend that forgot it would leave the emergency stop silently inert. The
    /// lifecycle worker (`entityLifecycle.unbound`) and staff admin calls do not consult it — a
    /// stopped plugin still releases funds and staff can still inspect and repair it.
    async fn ensure_not_suspended(
        &self,
        tenant_id: &str,
        capability_id: &str,
    ) -> CapabilityResult<()>;

    async fn list_user_grants(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> CapabilityResult<Vec<UserCapabilityGrant>>;

    async fn grant_user_capability(
        &self,
        tenant_id: &str,
        user_id: &str,
        capability_id: &str,
        expires_at: Option<DateTime<Utc>>,
        plan_code: Option<String>,
        source: Option<String>,
    ) -> CapabilityResult<()>;

    async fn revoke_user_capability(
        &self,
        tenant_id: &str,
        user_id: &str,
        capability_id: &str,
    ) -> CapabilityResult<()>;

    async fn set_tenant_capability(
        &self,
        tenant_id: &str,
        capability_id: &str,
        enabled: bool,
    ) -> CapabilityResult<()>;

    /// 租户下已写入的全部能力开关，按 `capability_id` 升序；供管理员只读。
    ///
    /// 默认返回未支持：后端没实现就明确报错，而不是返回空列表冒充「一个都没装」。
    async fn list_tenant_switches(
        &self,
        _tenant_id: &str,
    ) -> CapabilityResult<Vec<TenantCapabilitySwitch>> {
        Err(CapabilityError::NotSupported(
            "this policy backend does not list tenant capability switches".into(),
        ))
    }
}

/// 能力分发路由：**把「哪些 capability_id 交给哪个后端」变成可注册的数据。**
///
/// A dispatcher that matched id prefixes itself would know concrete plugin kinds and grow
/// with every new one. Instead the core asks each registered route, in order, whether it owns
/// the id, and sends unclaimed ids to the remote plugin route book. No concrete plugin is
/// named in core code.
///
/// # 为什么 `matches` 不是返回前缀字符串
///
/// 前缀只是**今天**够用的匹配方式。把判定权交给路由自己，将来按 kind、按版本、
/// 按租户灰度来选路，都不需要改这个 trait —— 契约一旦有人依赖就改不动了，
/// 所以这里刻意留出判定自由度。
#[async_trait]
pub trait CapabilityDispatchRoute: Send + Sync {
    /// Route id for logs and conflict diagnosis.
    fn route_id(&self) -> &str;

    /// 该 capability_id 是否由本路由接管。
    fn matches(&self, capability_id: &str) -> bool;

    /// 接管后的实际分发。
    async fn dispatch(
        &self,
        ctx: &Ctx,
        req: &crate::dispatch::CapabilityDispatchCommand,
    ) -> CapabilityResult<crate::dispatch::CapabilityDispatchResult>;
}

/// 插件健康探针：**把「用哪种协议探活」也变成可注册的数据。**
///
/// A generic checker that embedded a protocol-specific client would need a new branch for every
/// plugin with special liveness semantics. Instead the generic side looks up the probe of the
/// protocol the plugin declares and falls back to the generic protocol.
///
/// # 为什么探针拿的是 authority 而不是已建好的连接
///
/// 不同协议对连接的要求不同（超时、TLS、复用策略）。把建连交给探针自己，
/// 通用侧就不必知道任何一种协议的连接细节 —— 这是 kind 专有数据保持
/// 不透明的必要条件。
#[async_trait]
pub trait PluginHealthProbe: Send + Sync {
    /// 协议名，与插件注册时 `labels["health_protocol"]` 的取值对应。
    fn protocol(&self) -> &str;

    /// 探活。`Err` 的内容会被记进路由簿的 `last_error`，要能指明原因。
    async fn probe(&self, grpc_authority: &str, timeout: std::time::Duration)
    -> Result<(), String>;
}
