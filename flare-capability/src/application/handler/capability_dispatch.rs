//! 能力 **Dispatch** 应用编排：授权校验 → 查分发路由表 → 委托对应后端。
//!
//! # 核心不认识任何具体插件
//!
//! 这里曾经写着 `if req.capability_id.starts_with("rtc.")` —— 核心因此认识一个
//! 具体插件种类，每加一类插件都要回来改这个函数。插件系统最典型的老化方式就是
//! 这样积累起来的：支持的插件越多，核心越臃肿，而且改核心才能加插件，
//! 意味着插件的发版永远绑着核心的发版。
//!
//! 现在的规则只有两条：
//!
//! 1. 按注册顺序问每条 [`CapabilityDispatchRoute`]「这个 capability_id 归你吗」；
//! 2. 没人认领 → 走远端插件路由表（`PluginRouteBook`）。
//!
//! RTC 的前缀知识搬到了 `RtcDispatchRoute`，由组合根注册。判据很直接：
//! **本文件里 grep 任何具体插件名，应当零命中。**

use std::sync::Arc;
use std::time::Duration;

use flare_core_base::context::Ctx;

use crate::domain::capability::{
    CapabilityDispatchCommand, CapabilityDispatchResult, CapabilityPolicyBackend, Result,
};
use crate::infrastructure::capability::{CapabilityExtensionRegistry, PluginRouteBook};

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

    // 授权是 fail-closed：拒绝就是拒绝，不降级。
    // （可用性类失败才降级——那发生在下面各后端内部的超时/健康判定里。）
    policy
        .ensure_dispatch_allowed(&tenant, &user, &req.capability_id)
        .await?;

    for route in registry.dispatch_routes().await {
        if route.matches(&req.capability_id) {
            tracing::trace!(
                capability_id = %req.capability_id,
                route = %route.route_id(),
                "capability.dispatch route matched"
            );
            return route.dispatch(ctx, req).await;
        }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use flare_core_base::context::Ctx;
    use flare_server_core::Context;

    use super::dispatch_capability_command;
    use crate::domain::capability::{
        CapabilityDispatchCommand, CapabilityDispatchResult, CapabilityDispatchRoute,
        CapabilityPolicyBackend, Result, UserCapabilityGrant,
    };
    use crate::infrastructure::capability::{CapabilityExtensionRegistry, PluginRouteBook};

    /// 放行一切的策略后端：这些用例验的是**路由选择**，不是授权。
    struct AllowAll;

    #[async_trait]
    impl CapabilityPolicyBackend for AllowAll {
        async fn ensure_dispatch_allowed(&self, _t: &str, _u: &str, _c: &str) -> Result<()> {
            Ok(())
        }
        async fn list_user_grants(&self, _t: &str, _u: &str) -> Result<Vec<UserCapabilityGrant>> {
            Ok(vec![])
        }
        async fn grant_user_capability(
            &self,
            _t: &str,
            _u: &str,
            _c: &str,
            _expires_at: Option<DateTime<Utc>>,
            _plan_code: Option<String>,
            _source: Option<String>,
        ) -> Result<()> {
            unimplemented!("本用例不涉及授权变更")
        }
        async fn revoke_user_capability(&self, _t: &str, _u: &str, _c: &str) -> Result<()> {
            unimplemented!("本用例不涉及授权变更")
        }
        async fn set_tenant_capability(&self, _t: &str, _c: &str, _e: bool) -> Result<()> {
            unimplemented!("本用例不涉及租户开关")
        }
    }

    /// 只认领指定前缀的假路由，记录自己有没有被调用。
    struct SpyRoute {
        id: &'static str,
        prefix: &'static str,
        called: Arc<AtomicBool>,
    }

    #[async_trait]
    impl CapabilityDispatchRoute for SpyRoute {
        fn route_id(&self) -> &str {
            self.id
        }
        fn matches(&self, capability_id: &str) -> bool {
            capability_id.starts_with(self.prefix)
        }
        async fn dispatch(
            &self,
            _ctx: &Ctx,
            req: &CapabilityDispatchCommand,
        ) -> Result<CapabilityDispatchResult> {
            self.called.store(true, Ordering::SeqCst);
            Ok(CapabilityDispatchResult::ok(
                req.request_id.clone().unwrap_or_default(),
                self.id,
                req.capability_id.clone(),
                serde_json::Value::Null,
            ))
        }
    }

    fn cmd(capability_id: &str) -> CapabilityDispatchCommand {
        CapabilityDispatchCommand {
            capability_id: capability_id.to_string(),
            tenant_id: Some("0".into()),
            user_id: Some("u1".into()),
            conversation_id: None,
            payload: None,
            request_id: Some("req-1".into()),
        }
    }

    async fn dispatch(
        registry: &CapabilityExtensionRegistry,
        capability_id: &str,
    ) -> Result<CapabilityDispatchResult> {
        let ctx: Ctx = Arc::new(Context::default());
        let policy: Arc<dyn CapabilityPolicyBackend> = Arc::new(AllowAll);
        let routes = Arc::new(PluginRouteBook::new());
        dispatch_capability_command(
            &ctx,
            registry,
            &routes,
            &policy,
            Duration::from_millis(50),
            Duration::from_secs(30),
            &cmd(capability_id),
        )
        .await
    }

    async fn with_spy(
        prefix: &'static str,
        id: &'static str,
    ) -> (CapabilityExtensionRegistry, Arc<AtomicBool>) {
        let registry = CapabilityExtensionRegistry::new();
        let called = Arc::new(AtomicBool::new(false));
        registry
            .register_dispatch_route(Arc::new(SpyRoute {
                id,
                prefix,
                called: called.clone(),
            }))
            .await;
        (registry, called)
    }

    /// 路由是**注册数据**，不是分发器里的 if —— 注册什么前缀就接管什么。
    #[tokio::test]
    async fn registered_route_takes_over_its_prefix() {
        let (registry, called) = with_spy("vendorx.", "spy").await;
        let out = dispatch(&registry, "vendorx.thing.do")
            .await
            .expect("应当被 spy 接管");
        assert!(called.load(Ordering::SeqCst), "路由未被调用");
        assert_eq!(out.plugin_id, "spy");
    }

    /// 顺序即优先级：先注册的先被询问。
    #[tokio::test]
    async fn first_registered_route_wins() {
        let (registry, first) = with_spy("shared.", "first").await;
        let second = Arc::new(AtomicBool::new(false));
        registry
            .register_dispatch_route(Arc::new(SpyRoute {
                id: "second",
                prefix: "shared.",
                called: second.clone(),
            }))
            .await;

        let out = dispatch(&registry, "shared.x")
            .await
            .expect("应当被第一条接管");
        assert!(first.load(Ordering::SeqCst));
        assert!(!second.load(Ordering::SeqCst), "第二条不该被调用");
        assert_eq!(out.plugin_id, "first");
    }

    /// 没有路由认领时落到远端插件路由表。
    ///
    /// 断言的是「没有被 spy 接管」而不是最终成功：路由表是空的，远端分发必然失败。
    /// 要点在于失败来自远端路径，而不是被错误地路由走了。
    #[tokio::test]
    async fn unclaimed_capability_falls_through_to_remote_routes() {
        let (registry, called) = with_spy("vendorx.", "spy").await;
        let out = dispatch(&registry, "other.thing").await;
        assert!(!called.load(Ordering::SeqCst), "不该被 spy 接管");
        assert!(out.is_err(), "空路由表下远端分发应当失败");
    }

    /// 空路由表 = 核心对插件零知识。
    ///
    /// 这条是本次改造的核心断言：**连 `rtc.*` 也不再被特殊对待**，
    /// 不注册 RtcDispatchRoute 它就只是个普通的未认领 id。
    /// 改造前这里会走进 RTC 分支。
    #[tokio::test]
    async fn empty_route_table_gives_core_zero_plugin_knowledge() {
        let registry = CapabilityExtensionRegistry::new();
        assert!(registry.dispatch_routes().await.is_empty());
        assert!(dispatch(&registry, "rtc.call.video").await.is_err());
    }
}
