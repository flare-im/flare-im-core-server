//! **能力扩展栈装配**（写路径基础设施）：策略后端、`CapabilityGrpcServer`、内建 Resolver。
//!
//! **RTC / Extension 适配在本 crate 内部装配**。此处仅构建基础注册表与服务；
//! 媒体/RTC 后端在启动阶段仅登记 lazy 路由（不拨号）；调用 `Dispatch` 时再经服务发现解析。

use std::sync::Arc;

use flare_server_core::error::Result;
use sqlx::PgPool;

use crate::domain::capability::CapabilityPolicyBackend;
use crate::infrastructure::capability::DispatchRateLimiter;
use crate::infrastructure::capability::PluginRouteBook;
use crate::infrastructure::config::CapabilityRuntimeConfig;
use crate::infrastructure::persistence::PostgresCapabilityAuditLog;
use crate::interface::grpc::{
    CapabilityGrpcServer, CapabilityInvocationMetrics, HookServiceServer,
};

/// 初始化能力扩展栈（不直接依赖具体 RTC 实现类型）：
///
/// - 策略后端：内存（DEV） / Postgres（配置了 `db_pool` 时）。
/// - Recipient Resolver：内建的 `DirectConversationRecipientResolver`。
/// - 返回的 [`CapabilityExtensionRegistry`](crate::infrastructure::capability::CapabilityExtensionRegistry)
///   已就绪，后续由启动流程按配置调用内部适配装配逻辑挂入运行时后端。
pub async fn init_capability_extension_stack(
    runtime_config_file: Option<std::path::PathBuf>,
    db_pool: Option<Arc<PgPool>>,
    hook_governance: Option<Arc<HookServiceServer>>,
    plugin_routes: Arc<PluginRouteBook>,
) -> Result<(
    crate::infrastructure::capability::CapabilityExtensionRegistry,
    Arc<dyn CapabilityPolicyBackend>,
    CapabilityGrpcServer,
)> {
    use crate::infrastructure::capability::builtin::DirectConversationRecipientResolver;
    use crate::infrastructure::capability::{
        CapabilityExtensionRegistry, InMemoryCapabilityGrants, register_discovered_media_plugins,
    };
    use crate::infrastructure::persistence::PostgresCapabilityPolicy;

    let registry = CapabilityExtensionRegistry::new();

    // 分发路由由**组合根**装配，核心分发器不认识任何具体插件。
    // 「本部署包含 RTC」是装配决策，不是核心知识 —— 换个部署不带 RTC，
    // 这里不注册即可，核心一行都不用改。
    {
        use crate::infrastructure::capability::routing::{RtcDispatchRoute, SfuControlHealthProbe};
        let rtc_router = registry.rtc_router().await;
        registry
            .register_dispatch_route(Arc::new(RtcDispatchRoute::new(rtc_router)))
            .await;
        // 媒体端点在登记时声明 health_protocol=sfu_control；这里注册对应探针。
        // 漏注册不会静默降级成通用探活 —— 通用检查器会直接把这类实例判为
        // 「声明的协议没有探针」，因为静默降级等于把装配缺失伪装成健康。
        registry
            .register_health_probe(Arc::new(SfuControlHealthProbe))
            .await;
    }

    let runtime = Arc::new(CapabilityRuntimeConfig::from_sources(
        runtime_config_file.as_deref(),
    ));
    // 审计随 DB 挂载：没有 DB 就没有审计。这本身是合理的降级（开发期不该被
    // 强制起一个库），但**不能静默** —— 授权的授予/吊销/租户开关一旦无痕，
    // 计费争议就无从对账，而运维往往到出事才发现这个部署根本没在记。
    let audit = db_pool
        .as_ref()
        .map(|p| Arc::new(PostgresCapabilityAuditLog::new(p.clone())));
    if audit.is_none() {
        tracing::warn!(
            "capability policy audit is DISABLED (no db_pool): grant / revoke / \
             tenant_switch will leave no trace. Do not run a billed deployment this way."
        );
    }
    let rate_limiter = runtime
        .dispatch_max_per_minute
        .filter(|&n| n > 0)
        .map(|n| Arc::new(DispatchRateLimiter::new(n)));
    let capability_metrics = Arc::new(CapabilityInvocationMetrics::default());

    let capability_policy: Arc<dyn CapabilityPolicyBackend> = if let Some(pool) = db_pool {
        Arc::new(PostgresCapabilityPolicy::new(pool))
    } else {
        let mem = InMemoryCapabilityGrants::new();
        mem.grant_user_capability(
            "0",
            "*",
            "rtc.*",
            None,
            Some("dev_default".to_string()),
            Some("bootstrap".to_string()),
        );
        Arc::new(mem)
    };

    registry
        .register_recipient_resolver(Arc::new(DirectConversationRecipientResolver::new()))
        .await;

    register_discovered_media_plugins(&registry, &plugin_routes, runtime.as_ref()).await?;

    let capability_grpc = CapabilityGrpcServer::new(
        registry.clone(),
        Arc::clone(&capability_policy),
        runtime,
        audit,
        rate_limiter,
        capability_metrics,
        hook_governance,
        Arc::clone(&plugin_routes),
    );

    tracing::info!("capability extension stack initialized (media plugins via service discovery)");

    Ok((registry, capability_policy, capability_grpc))
}
