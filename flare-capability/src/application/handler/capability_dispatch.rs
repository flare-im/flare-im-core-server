//! 能力 **Dispatch** 应用编排：从扩展注册表解析 RTC 后端，委托领域 [`crate::domain::capability::execute_capability_dispatch`]。

use std::sync::Arc;
use std::time::Duration;

use flare_core_base::context::{Context, Ctx};

use crate::domain::capability::{
    CapabilityDispatchCommand, CapabilityDispatchResult, CapabilityPolicyBackend, Result,
};
use crate::infrastructure::capability::{CapabilityExtensionRegistry, PluginRouteBook};

/// RTC 选路使用的上下文：与策略层 `tenant` 语义一致。
///
/// gRPC 侧常为 `ContextLayer::allow_missing()`，metadata 无 `x-tenant-id` 时 `ctx.tenant_id()` 为空，
/// 但 `Dispatch` 请求体里可有 `tenant_id`（空则默认 `"0"`，与 [`CapabilityDispatchCommand`] 策略校验一致）。
/// 若仍用原始 `ctx`，[`RtcCapabilityRouter`] 无法命中 `set_backend_for_tenant("0", …)` 的媒体后端。
fn ctx_for_rtc_dispatch(ctx: &Ctx, req: &CapabilityDispatchCommand) -> Ctx {
    if ctx
        .tenant_id()
        .map(|t| !t.trim().is_empty())
        .unwrap_or(false)
    {
        return Arc::clone(ctx);
    }
    let tid = req
        .tenant_id
        .as_deref()
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .unwrap_or("0");
    let base: Context = (**ctx).clone();
    Arc::new(base.with_tenant_id(tid))
}

/// `CapabilityService.Dispatch` 的应用入口：装配端口后调用领域服务。
pub async fn dispatch_capability_command(
    ctx: &Ctx,
    registry: &CapabilityExtensionRegistry,
    plugin_routes: &Arc<PluginRouteBook>,
    policy: &Arc<dyn CapabilityPolicyBackend>,
    plugin_timeout: Duration,
    plugin_health_stale: Duration,
    req: &CapabilityDispatchCommand,
) -> Result<CapabilityDispatchResult> {
    let tenant = req.tenant_id.clone().unwrap_or_else(|| "0".into());
    let user = req.user_id.clone().ok_or_else(|| {
        crate::domain::capability::CapabilityError::PolicyDenied("user_id required".into())
    })?;
    policy
        .ensure_dispatch_allowed(&tenant, &user, &req.capability_id)
        .await?;

    if req.capability_id.starts_with("rtc.") {
        let rtc = registry.rtc_router().await;
        let ctx_rtc = ctx_for_rtc_dispatch(ctx, req);
        return crate::domain::capability::dispatch_rtc_by_capability_id(
            &ctx_rtc, &rtc, req,
        )
        .await;
    }

    super::dispatch_remote_by_capability_id(
        ctx,
        req,
        plugin_routes,
        plugin_timeout,
        plugin_health_stale,
    )
    .await
}
