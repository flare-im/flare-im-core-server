//! RTC 的分发路由：把「`rtc.` 前缀归 RTC」这条知识从核心分发器搬到 RTC 自己这边。
//!
//! 之前应用层的 `dispatch_capability_command` 里写着
//! `if req.capability_id.starts_with("rtc.")`，核心因此认识一个具体插件种类。
//! 现在它由组合根注册进 [`CapabilityExtensionRegistry`]，核心只按注册顺序询问。

use std::sync::Arc;

use async_trait::async_trait;
use flare_core_base::context::{Context, Ctx};

use crate::domain::capability::{
    CapabilityDispatchCommand, CapabilityDispatchResult, CapabilityDispatchRoute, Result,
    dispatch_rtc_by_capability_id,
};
use crate::infrastructure::capability::routing::RtcCapabilityRouter;

/// 默认接管的前缀。**不是硬编码在核心里** —— 构造时可换，
/// 部署方要把 RTC 挂到别的命名空间也不必改核心。
pub const DEFAULT_RTC_CAPABILITY_PREFIX: &str = "rtc.";

pub struct RtcDispatchRoute {
    prefix: String,
    /// 克隆自注册表。`RtcCapabilityRouter` 内部是 `Arc<RwLock<..>>` 且注册表里的
    /// 那个字段从不被整体替换，所以这份克隆与后续 `set_backend_for_tenant`
    /// 共享同一状态，不会读到陈旧后端。
    router: RtcCapabilityRouter,
}

impl RtcDispatchRoute {
    pub fn new(router: RtcCapabilityRouter) -> Self {
        Self::with_prefix(router, DEFAULT_RTC_CAPABILITY_PREFIX)
    }

    pub fn with_prefix(router: RtcCapabilityRouter, prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
            router,
        }
    }
}

/// RTC 选路使用的上下文：与策略层 `tenant` 语义一致。
///
/// gRPC 侧常为 `ContextLayer::allow_missing()`，metadata 无 `x-tenant-id` 时
/// `ctx.tenant_id()` 为空，但 `Dispatch` 请求体里可有 `tenant_id`（空则默认 `"0"`）。
/// 若仍用原始 `ctx`，[`RtcCapabilityRouter`] 无法命中
/// `set_backend_for_tenant("0", …)` 注册的媒体后端。
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

#[async_trait]
impl CapabilityDispatchRoute for RtcDispatchRoute {
    fn route_id(&self) -> &str {
        "rtc"
    }

    fn matches(&self, capability_id: &str) -> bool {
        capability_id.starts_with(&self.prefix)
    }

    async fn dispatch(
        &self,
        ctx: &Ctx,
        req: &CapabilityDispatchCommand,
    ) -> Result<CapabilityDispatchResult> {
        let ctx_rtc = ctx_for_rtc_dispatch(ctx, req);
        dispatch_rtc_by_capability_id(&ctx_rtc, &self.router, req).await
    }
}
