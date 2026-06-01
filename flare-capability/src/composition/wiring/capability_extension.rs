//! **能力扩展栈装配**（写路径基础设施）：策略后端、`CapabilityGrpcServer`、内建 Resolver。
//!
//! **RTC / Extension 适配在本 crate 内部装配**。此处仅构建基础注册表与服务；
//! 媒体/RTC 后端在启动阶段仅登记 lazy 路由（不拨号）；调用 `Dispatch` 时再经服务发现解析。

use std::sync::Arc;

use anyhow::Result;
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

    let runtime = Arc::new(CapabilityRuntimeConfig::from_sources(
        runtime_config_file.as_deref(),
    ));
    let audit = db_pool
        .as_ref()
        .map(|p| Arc::new(PostgresCapabilityAuditLog::new(p.clone())));
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
