//! Capability extension ports.

use async_trait::async_trait;
use flare_core_base::context::Ctx;

use crate::context::{ConversationKind, ResolveTrigger};
use crate::error::{CapabilityError, Result as CapabilityResult};
use crate::grant::TenantCapabilitySwitch;
use crate::recipient::{RecipientResolveRequest, RecipientResolveResult};

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

    /// Emergency stop for plugin operations, which the tenant's installation projection
    /// authorizes: refused when the global
    /// switch is off, when the tenant switch of `capability_id` exists and is off, or when a
    /// tenant switch `<prefix>.*` whose prefix (with its dot) starts `capability_id` exists and
    /// is off — `example.poll.*` stops every operation of the plugin `example.poll` with one row.
    /// Any matching switch that is off stops the call; a more specific one that is on does not
    /// lift it. An absent tenant switch is not a refusal.
    ///
    /// No default: a backend that forgot it would leave the emergency stop silently inert. The
    /// lifecycle worker (`entityLifecycle.unbound`) and staff admin calls do not consult it — a
    /// stopped plugin still releases funds and staff can still inspect and repair it.
    async fn ensure_not_suspended(
        &self,
        tenant_id: &str,
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
