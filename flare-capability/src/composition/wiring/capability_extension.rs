//! **能力扩展栈装配**（写路径基础设施）：策略后端、`CapabilityGrpcServer`、内建 Resolver。
//!
//! 此处仅构建基础注册表与服务；远端插件经 `RegisterPluginEndpoint` 登记，调用 `Dispatch` 时再解析。

use std::sync::Arc;

use flare_server_core::error::Result;
use sqlx::PgPool;

use super::internal_ctx::{admin_staff_verifier_from_env, internal_ctx_verifier_from_env};
use crate::domain::capability::CapabilityPolicyBackend;
use crate::infrastructure::capability::DispatchRateLimiter;
use crate::infrastructure::capability::PluginRouteBook;
use crate::infrastructure::config::CapabilityRuntimeConfig;
use crate::infrastructure::persistence::PostgresCapabilityAuditLog;
use crate::interface::grpc::{
    CapabilityGrpcServer, CapabilityInvocationMetrics, HookServiceServer,
};

/// 初始化能力扩展栈：
///
/// - 策略后端：内存（DEV） / Postgres（配置了 `db_pool` 时）。
/// - Recipient Resolver：内建的 `DirectConversationRecipientResolver`。
/// - 返回的 [`CapabilityExtensionRegistry`](crate::infrastructure::capability::CapabilityExtensionRegistry)
///   已就绪。
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
        CapabilityExtensionRegistry, InMemoryCapabilityGrants,
    };
    use crate::infrastructure::persistence::PostgresCapabilityPolicy;

    let registry = CapabilityExtensionRegistry::new();

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
        Arc::new(InMemoryCapabilityGrants::new())
    };

    registry
        .register_recipient_resolver(Arc::new(DirectConversationRecipientResolver::new()))
        .await;

    // Dispatch 与插件自注册验 Cell 公钥集里的任何组件钥；管理 RPC 的 Staff 上下文钉死 kid=gateway。
    let dispatch_verifier = internal_ctx_verifier_from_env("Capability Dispatch")?;
    let staff_verifier = admin_staff_verifier_from_env("Capability management RPCs")?;

    let mut capability_grpc = CapabilityGrpcServer::new(
        registry.clone(),
        Arc::clone(&capability_policy),
        runtime,
        audit,
        rate_limiter,
        capability_metrics,
        hook_governance,
        Arc::clone(&plugin_routes),
        dispatch_verifier,
        staff_verifier,
    );
    match std::env::var("FLARE_PLATFORM_PUBLIC_KEY") {
        Ok(key) => {
            capability_grpc = capability_grpc.with_registration_verifier(
                flare_server_core::TokenService::from_ed25519_public_key(&key, "flare-platform")
                    .map_err(|_| {
                        flare_server_core::error::FlareError::system(
                            "invalid platform registration public key",
                        )
                    })?,
            )
        }
        Err(std::env::VarError::NotPresent) => (),
        Err(_) => {
            return Err(flare_server_core::error::FlareError::system(
                "invalid platform registration public key encoding",
            ));
        }
    }

    tracing::info!("capability extension stack initialized");

    Ok((registry, capability_policy, capability_grpc))
}
