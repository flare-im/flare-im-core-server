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
    dispatch_rtc_by_capability_id, require_dispatch_tenant,
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
/// 上下文没有租户时取 `Dispatch` 请求体的租户，[`RtcCapabilityRouter`] 才能命中该租户注册的
/// 媒体后端。两者都缺（含空白）时以 `PolicyDenied` 拒绝，不再按租户 "0" 选后端（W13-D）。
fn ctx_for_rtc_dispatch(ctx: &Ctx, req: &CapabilityDispatchCommand) -> Result<Ctx> {
    if ctx.tenant_id().is_some_and(|t| !t.trim().is_empty()) {
        return Ok(Arc::clone(ctx));
    }
    let tenant_id = require_dispatch_tenant(req)?;
    let base: Context = (**ctx).clone();
    Ok(Arc::new(base.with_tenant_id(tenant_id.trim())))
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
        let ctx_rtc = ctx_for_rtc_dispatch(ctx, req)?;
        dispatch_rtc_by_capability_id(&ctx_rtc, &self.router, req).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use flare_core_base::context::{Context, Ctx};
    use serde_json::Value;

    use super::{RtcDispatchRoute, ctx_for_rtc_dispatch};
    use crate::domain::capability::{
        AcceptCallRequest, AcceptCallResponse, CapabilityDispatchCommand, CapabilityDispatchRoute,
        CapabilityError, CreateCallRequest, CreateCallResponse, GetJoinTokenRequest,
        GetJoinTokenResponse, HangupCallRequest, HangupCallResponse, ListParticipantsRequest,
        ListParticipantsResponse, RejectCallRequest, RejectCallResponse, Result, RtcCapability,
    };
    use crate::infrastructure::capability::routing::RtcCapabilityRouter;

    /// 记下建呼叫时上下文里的租户；其余动作本用例不涉及。
    #[derive(Default)]
    struct ContextTenantRtc {
        tenants: Mutex<Vec<Option<String>>>,
    }

    fn not_used<T>(action: &str) -> Result<T> {
        Err(CapabilityError::NotSupported(format!("{action} test-only")))
    }

    #[async_trait]
    impl RtcCapability for ContextTenantRtc {
        fn id(&self) -> &str {
            "context-tenant"
        }
        async fn create_call(
            &self,
            ctx: &Ctx,
            _req: &CreateCallRequest,
        ) -> Result<CreateCallResponse> {
            self.tenants
                .lock()
                .unwrap()
                .push(ctx.tenant_id().map(str::to_string));
            Ok(CreateCallResponse {
                call_id: "call-1".into(),
                room_id: "room-1".into(),
                ext: Value::Null,
            })
        }
        async fn accept_call(
            &self,
            _ctx: &Ctx,
            _req: &AcceptCallRequest,
        ) -> Result<AcceptCallResponse> {
            not_used("accept_call")
        }
        async fn reject_call(
            &self,
            _ctx: &Ctx,
            _req: &RejectCallRequest,
        ) -> Result<RejectCallResponse> {
            not_used("reject_call")
        }
        async fn hangup_call(
            &self,
            _ctx: &Ctx,
            _req: &HangupCallRequest,
        ) -> Result<HangupCallResponse> {
            not_used("hangup_call")
        }
        async fn get_join_token(
            &self,
            _ctx: &Ctx,
            _req: &GetJoinTokenRequest,
        ) -> Result<GetJoinTokenResponse> {
            not_used("get_join_token")
        }
        async fn list_participants(
            &self,
            _ctx: &Ctx,
            _req: &ListParticipantsRequest,
        ) -> Result<ListParticipantsResponse> {
            not_used("list_participants")
        }
    }

    fn video_call(tenant_id: Option<&str>) -> CapabilityDispatchCommand {
        CapabilityDispatchCommand {
            capability_id: "rtc.call.video".into(),
            tenant_id: tenant_id.map(str::to_string),
            user_id: Some("u1".into()),
            conversation_id: Some("conv-1".into()),
            payload: None,
            request_id: Some("req-1".into()),
        }
    }

    /// 上下文与请求体都缺租户（含空白）时拒绝，不再按租户 "0" 选媒体后端（W13-D）；上下文缺、
    /// 请求体有租户时照常按请求体的租户选后端。
    #[tokio::test]
    async fn rtc_route_without_any_tenant_is_denied_not_routed_to_tenant_zero() {
        let zero = Arc::new(ContextTenantRtc::default());
        let tenant_a = Arc::new(ContextTenantRtc::default());
        let router = RtcCapabilityRouter::new();
        router.set_backend_for_tenant("0", Some(zero.clone())).await;
        router
            .set_backend_for_tenant("tenant-a", Some(tenant_a.clone()))
            .await;
        let route = RtcDispatchRoute::new(router);
        let no_tenant: Ctx = Arc::new(Context::with_request_id("req-1"));
        let blank_tenant: Ctx = Arc::new(Context::with_request_id("req-1").with_tenant_id(" "));

        for ctx in [&no_tenant, &blank_tenant] {
            for tenant_id in [None, Some(""), Some("  ")] {
                let error = route
                    .dispatch(ctx, &video_call(tenant_id))
                    .await
                    .expect_err("an RTC dispatch without any tenant must be denied");
                assert!(
                    matches!(&error, CapabilityError::PolicyDenied(reason) if reason == "tenant_id required"),
                    "{:?}/{tenant_id:?}: {error:?}",
                    ctx.tenant_id()
                );
            }
        }
        assert!(
            zero.tenants.lock().unwrap().is_empty(),
            "缺租户不该落到租户 0 的后端"
        );

        route
            .dispatch(&no_tenant, &video_call(Some("tenant-a")))
            .await
            .expect("the body tenant selects the backend");
        assert_eq!(
            *tenant_a.tenants.lock().unwrap(),
            [Some("tenant-a".to_string())]
        );
    }

    /// 选路上下文本身：上下文与请求体都缺租户（含空白）时拒绝，不再补成租户 "0"；上下文已有
    /// 租户时原样沿用，上下文缺、请求体有时补上请求体的租户（W13-D）。
    #[test]
    fn the_rtc_routing_context_never_falls_back_to_tenant_zero() {
        let no_tenant: Ctx = Arc::new(Context::with_request_id("req-1"));
        let blank_tenant: Ctx = Arc::new(Context::with_request_id("req-1").with_tenant_id(" "));
        for ctx in [&no_tenant, &blank_tenant] {
            for tenant_id in [None, Some(""), Some("  ")] {
                let error = ctx_for_rtc_dispatch(ctx, &video_call(tenant_id))
                    .map(|ctx| ctx.tenant_id().map(str::to_string))
                    .expect_err("no tenant anywhere must be denied");
                assert!(
                    matches!(&error, CapabilityError::PolicyDenied(reason) if reason == "tenant_id required"),
                    "{:?}/{tenant_id:?}: {error:?}",
                    ctx.tenant_id()
                );
            }
            let routed = ctx_for_rtc_dispatch(ctx, &video_call(Some("tenant-a"))).unwrap();
            assert_eq!(routed.tenant_id(), Some("tenant-a"));
        }
        let tenant_b: Ctx = Arc::new(Context::with_request_id("req-1").with_tenant_id("tenant-b"));
        let routed = ctx_for_rtc_dispatch(&tenant_b, &video_call(None)).unwrap();
        assert_eq!(routed.tenant_id(), Some("tenant-b"));
    }
}
