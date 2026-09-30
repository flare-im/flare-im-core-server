//! **依赖装配（Wiring）**：按 DDD 组合根惯例拆分为配置源、Hook 运行时、能力扩展栈，再由 `initialize` 串联。
//!
//! 不包含 gRPC `Server::serve` 循环（见 [`super::bootstrap::ApplicationBootstrap`]）。

mod capability_extension;
mod config_sources;
mod hook_runtime;

use std::sync::Arc;

use flare_server_core::error::Result;

use crate::infrastructure::capability::PluginRouteBook;
use crate::infrastructure::persistence::PostgresTenantProjectionRepository;
use crate::interface::grpc::{CapabilityAdminGrpcServer, TenantProjectionGrpcServer};

use crate::composition::process_config::CapabilityServiceConfig;
use crate::composition::runtime_context::ApplicationContext;
use crate::interface::grpc::hooks::ImHookPluginServer;

pub use capability_extension::init_capability_extension_stack;

/// 构建完整 [`ApplicationContext`]（配置 → Hook → Capability）。
pub(crate) async fn initialize(config: CapabilityServiceConfig) -> Result<ApplicationContext> {
    let sources = config_sources::prepare_config_sources(&config).await?;
    let hook = hook_runtime::build_hook_runtime(&sources);

    let plugin_routes = Arc::new(PluginRouteBook::new());

    let db_pool = sources
        .hook_config_repository
        .as_ref()
        .map(|r| r.connection_pool());
    let (capability_registry, capability_policy, capability_grpc) =
        init_capability_extension_stack(
            config
                .runtime_config_file
                .clone()
                .or_else(|| config.config_file.clone()),
            db_pool.clone(),
            hook.hook_governance.clone(),
            Arc::clone(&plugin_routes),
        )
        .await?;
    let capability_runtime = capability_grpc.runtime_config();

    // 租户投影只在有库时可用：没有库就没有 `tenants` 表可写，控制面推送会得到 Unimplemented 的路由缺席错误。
    let control_verifier = flare_im_service_kit::control_auth::ConsoleControlVerifier::from_env()
        .map_err(|_| {
        flare_server_core::error::FlareError::system("invalid FLARE_CONSOLE_PUBLIC_KEY")
    })?;
    if control_verifier.is_none() {
        tracing::warn!("Console control public key missing; tenant projection RPCs fail closed");
    }
    let tenant_projection_repository = db_pool
        .as_ref()
        .map(|pool| Arc::new(PostgresTenantProjectionRepository::new(Arc::clone(pool))));
    let tenant_projection = tenant_projection_repository.as_ref().map(|repository| {
        TenantProjectionGrpcServer::new(Arc::clone(repository), control_verifier.clone())
    });
    // 管理员只读与投影接收端读同一张 `tenants` 表；开关读同一个策略后端。
    let capability_admin = CapabilityAdminGrpcServer::new(
        Arc::clone(&capability_policy),
        tenant_projection_repository,
        control_verifier.clone(),
    );

    let extension_router = capability_registry.extension_router().await;
    let im_hook_plugin = ImHookPluginServer::new(
        hook.command_handler,
        hook.registry,
        hook.adapter_factory,
        capability_registry.clone(),
    );

    Ok(ApplicationContext {
        im_hook_plugin,
        hook_governance: hook.hook_governance,
        plugin_routes,
        extension_router,
        capability_registry,
        capability_policy,
        capability_grpc,
        capability_runtime,
        tenant_projection,
        capability_admin,
    })
}
