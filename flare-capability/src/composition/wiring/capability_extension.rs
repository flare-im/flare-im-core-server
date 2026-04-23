//! **能力扩展栈装配**（写路径基础设施）：策略后端、`CapabilityGrpcServer`、RTC（进程内 SFU / strom gRPC）。

use std::sync::Arc;

use anyhow::Result;
use sqlx::PgPool;

use crate::domain::capability::CapabilityPolicyBackend;
use crate::infrastructure::capability::DispatchRateLimiter;
use crate::infrastructure::capability::PluginRouteBook;
use crate::infrastructure::config::CapabilityRuntimeConfig;
use crate::infrastructure::persistence::PostgresCapabilityAuditLog;
use crate::interface::grpc::{CapabilityGrpcServer, CapabilityInvocationMetrics, HookServiceServer};

/// 初始化能力扩展栈：内存 / Postgres 策略、可选 SFU RTC、内建单聊 Resolver。
///
/// RTC 后端选择：
/// - 默认：进程内 `flare-sfu` + [`SfuRtcCapability`](crate::infrastructure::capability::SfuRtcCapability)。
/// - `FLARE_CAPABILITY_RTC_BACKEND=strom`（或 `strom-grpc` / `strom_grpc`）且设置
///   `FLARE_STROM_SFU_GRPC_ENDPOINT` 时：独立 **flare-strom-sfu** `SfuControl` gRPC，不启动进程内 `SfuPlugin`。
///
/// 供本进程与 **Orchestrator 等内嵌调用方** 共用；调用方可再向返回的
/// [`CapabilityExtensionRegistry`](crate::infrastructure::capability::CapabilityExtensionRegistry) 注册额外 Guard / Resolver。
pub async fn init_capability_extension_stack(
    db_pool: Option<Arc<PgPool>>,
    hook_governance: Option<Arc<HookServiceServer>>,
    plugin_routes: Arc<PluginRouteBook>,
) -> Result<(
    crate::infrastructure::capability::CapabilityExtensionRegistry,
    Arc<dyn CapabilityPolicyBackend>,
    CapabilityGrpcServer,
    Option<Arc<crate::infrastructure::capability::StromSfuGrpcRtcCapability>>,
)> {
    use crate::infrastructure::capability::builtin::DirectConversationRecipientResolver;
    use crate::infrastructure::capability::{
        CapabilityExtensionRegistry, InMemoryCapabilityGrants, SfuRtcCapability,
    };
    use crate::infrastructure::persistence::PostgresCapabilityPolicy;

    let registry = CapabilityExtensionRegistry::new();

    let runtime = Arc::new(CapabilityRuntimeConfig::from_env());
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

    if av_plugins_disabled_by_env() {
        tracing::info!("AV capability / SFU disabled (FLARE_CAPABILITY_AV_PLUGINS)");
        return Ok((registry, capability_policy, capability_grpc, None));
    }

    let rtc_backend = std::env::var("FLARE_CAPABILITY_RTC_BACKEND")
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let strom_endpoint = std::env::var("FLARE_STROM_SFU_GRPC_ENDPOINT")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty());

    if matches!(
        rtc_backend.as_str(),
        "strom" | "strom-grpc" | "strom_grpc"
    ) {
        let Some(ep) = strom_endpoint else {
            return Err(anyhow::anyhow!(
                "FLARE_CAPABILITY_RTC_BACKEND=strom requires non-empty FLARE_STROM_SFU_GRPC_ENDPOINT (e.g. http://127.0.0.1:50051)"
            ));
        };
        use crate::infrastructure::capability::{
            register_strom_sfu_plugin_route, StromSfuGrpcRtcCapability,
        };
        let rtc = Arc::new(
            StromSfuGrpcRtcCapability::connect(ep.clone())
                .await
                .map_err(|e| anyhow::anyhow!("StromSfuGrpcRtcCapability::connect: {e}"))?,
        );
        registry.set_rtc_backend(Some(rtc.clone())).await;
        register_strom_sfu_plugin_route(plugin_routes.as_ref(), &ep);
        tracing::info!(
            %ep,
            "Strom SFU gRPC RtcCapability registered (in-process flare-sfu not started)"
        );
        return Ok((registry, capability_policy, capability_grpc, Some(rtc)));
    }

    let sfu = Arc::new(
        flare_sfu::interface::plugin::SfuPlugin::new(flare_sfu::domain::SfuConfig::default())
            .map_err(|e| anyhow::anyhow!("SfuPlugin init: {e}"))?,
    );
    sfu.start()
        .await
        .map_err(|e| anyhow::anyhow!("SfuPlugin start: {e}"))?;

    let rtc = Arc::new(SfuRtcCapability::new(sfu));
    registry.set_rtc_backend(Some(rtc)).await;

    tracing::info!("SFU-backed RtcCapability registered (in-process flare-sfu)");

    Ok((registry, capability_policy, capability_grpc, None))
}

fn av_plugins_disabled_by_env() -> bool {
    std::env::var("FLARE_CAPABILITY_AV_PLUGINS")
        .map(|v| {
            matches!(v.trim(), "0" | "false" | "off" | "no" | "disabled")
                || v.eq_ignore_ascii_case("false")
                || v.eq_ignore_ascii_case("off")
                || v.eq_ignore_ascii_case("no")
        })
        .unwrap_or(false)
}
