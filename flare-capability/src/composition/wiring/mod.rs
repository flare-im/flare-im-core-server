//! **依赖装配（Wiring）**：按 DDD 组合根惯例拆分为配置源、Hook 运行时、能力扩展栈，再由 `initialize` 串联。
//!
//! 不包含 gRPC `Server::serve` 循环（见 [`super::bootstrap::ApplicationBootstrap`]）。

mod capability_extension;
mod config_sources;
mod hook_runtime;

pub(crate) use hook_runtime::HookExecutionBackground;
mod internal_ctx;
mod plugin_routes;
mod resource_bridge;

use std::sync::Arc;

use flare_server_core::error::Result;

use crate::infrastructure::persistence::PostgresTenantProjectionRepository;
use crate::interface::grpc::{CapabilityAdminGrpcServer, TenantProjectionGrpcServer};

use crate::composition::process_config::CapabilityServiceConfig;
use crate::composition::runtime_context::ApplicationContext;
use crate::infrastructure::config::HookExecutionSettings;
use crate::interface::grpc::hooks::ImHookPluginServer;

pub use capability_extension::init_capability_extension_stack;

/// 构建完整 [`ApplicationContext`]（配置 → Hook → Capability）。
pub(crate) async fn initialize(config: CapabilityServiceConfig) -> Result<ApplicationContext> {
    let mut sources = config_sources::prepare_config_sources(&config).await?;
    let runtime_config_file = config
        .runtime_config_file
        .clone()
        .or_else(|| config.config_file.clone());
    let execution_settings = HookExecutionSettings::from_sources(runtime_config_file.as_deref());
    let hook = hook_runtime::build_hook_runtime(&sources, &execution_settings);
    let hook_config_listener = sources.listener.take();

    let db_pool = sources
        .hook_config_repository
        .as_ref()
        .map(|r| r.connection_pool());
    // 有库时登记写 capability_plugin_endpoints；没有库时只在本进程（plugin_routes 里说明一次）。
    let plugin_routes = Arc::new(plugin_routes::runtime_authorization(
        plugin_routes::plugin_route_book(db_pool.as_ref()),
        db_pool.as_ref(),
    )?);
    let (capability_registry, capability_policy, capability_grpc) =
        init_capability_extension_stack(
            runtime_config_file,
            db_pool.clone(),
            hook.hook_governance.clone(),
            Arc::clone(&plugin_routes),
        )
        .await?;
    let capability_runtime = capability_grpc.runtime_config();
    // 配置发现的端点已在上面进簿；再载入库里的登记（同键时登记覆盖配置），然后起周期重读与清理。
    plugin_routes::load_and_maintain(&plugin_routes, &capability_runtime).await?;

    // 租户投影只在有库时可用：没有库就没有 `tenants` 表可写，控制面推送会得到 Unimplemented 的路由缺席错误。
    // 投影只认平台 worker 的控制断言（FLARE_PLATFORM_PUBLIC_KEY）；员工不经这里。
    let platform_verifier =
        flare_im_service_kit::control_auth::PlatformAssertionVerifier::from_env().map_err(
            |_| flare_server_core::error::FlareError::system("invalid FLARE_PLATFORM_PUBLIC_KEY"),
        )?;
    if platform_verifier.is_none() {
        tracing::warn!("platform public key missing; tenant projection RPCs fail closed");
    }
    let tenant_projection_repository = db_pool
        .as_ref()
        .map(|pool| Arc::new(PostgresTenantProjectionRepository::new(Arc::clone(pool))));
    let tenant_projection = tenant_projection_repository.as_ref().map(|repository| {
        TenantProjectionGrpcServer::new(Arc::clone(repository), platform_verifier.clone())
    });
    // 管理员只读与投影接收端读同一张 `tenants` 表；开关读同一个策略后端。只认 admin-gateway
    // 转发的 Staff 上下文（Cell 公钥集，Staff 钉死 kid=gateway）。
    let capability_admin = CapabilityAdminGrpcServer::new(
        Arc::clone(&capability_policy),
        tenant_projection_repository,
        internal_ctx::admin_staff_verifier_from_env("Capability admin RPCs")?,
    );

    let extension_router = capability_registry.extension_router().await;
    // ExtensionPlugin.Call 只认 Cell 签名的上下文；没有公钥集时只回答健康检查。
    let extension_verifier = internal_ctx::internal_ctx_verifier_from_env("ExtensionPlugin")?;
    // HookPlugin 只收 Cell 签名的调用（Message Ingest 等原样转发的 `x-flare-ctx`）；没有公钥时全部拒绝。
    let hook_plugin_verifier = internal_ctx::internal_ctx_verifier_from_env("HookPlugin")?;
    let im_hook_plugin = ImHookPluginServer::new(
        hook.command_handler,
        hook.registry,
        hook.adapter_factory,
        capability_registry.clone(),
        hook_plugin_verifier,
    )
    .with_execution_guard(hook.execution_guard);

    let plugin_resource_bridge =
        resource_bridge::compose(&plugin_routes, &capability_policy, db_pool.as_ref()).await?;
    let audit_delivery = if let Some(pool) = db_pool {
        let publisher =
            flare_im_service_kit::audit_effects::AuditEffects::from_env("flare-capability").await?;
        if publisher.enabled() {
            Some((pool, publisher))
        } else {
            tracing::warn!(
                "Core source audit evidence is retained in PostgreSQL; broker delivery is disabled"
            );
            None
        }
    } else {
        None
    };
    Ok(ApplicationContext {
        plugin_resource_bridge,
        audit_delivery,
        im_hook_plugin,
        hook_governance: hook.hook_governance,
        plugin_routes,
        extension_router,
        extension_verifier,
        capability_registry,
        capability_policy,
        capability_grpc,
        capability_runtime,
        tenant_projection,
        capability_admin,
        hook_config_listener,
        hook_execution_background: hook.execution_background,
    })
}
