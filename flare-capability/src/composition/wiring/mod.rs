//! **依赖装配（Wiring）**：按 DDD 组合根惯例拆分为数据库、Hook 治理、能力扩展栈，再由 `initialize` 串联。
//!
//! 不包含 gRPC `Server::serve` 循环（见 [`super::bootstrap::ApplicationBootstrap`]）。

mod capability_extension;
mod database;
mod hook_runtime;

pub(crate) use hook_runtime::HookRetentionBackground;
mod internal_ctx;
pub(crate) mod message_events;
mod plugin_reconcile;
mod plugin_routes;
mod resource_bridge;

use std::sync::Arc;

use flare_server_core::error::Result;

use crate::application::handler::tenant_notice_relay::TenantNoticeRelay;
use crate::infrastructure::persistence::PostgresTenantProjectionRepository;
use crate::infrastructure::persistence::postgres_client_policy::PostgresTenantNoticeOutbox;
use crate::interface::grpc::{CapabilityAdminGrpcServer, TenantProjectionGrpcServer};

use crate::composition::process_config::CapabilityServiceConfig;
use crate::composition::runtime_context::ApplicationContext;
use crate::infrastructure::config::HookRetentionConfig;

pub use capability_extension::init_capability_extension_stack;

/// 构建完整 [`ApplicationContext`]（数据库 → Hook 治理 → Capability）。
pub(crate) async fn initialize(
    config: CapabilityServiceConfig,
    extensions: crate::composition::extensions::CapabilityExtensions,
) -> Result<ApplicationContext> {
    let hook_config_repository = database::open_database(&config).await?;
    let runtime_config_file = config.runtime_config_file.clone();
    let retention_settings = HookRetentionConfig::from_sources(runtime_config_file.as_deref());
    let hook =
        hook_runtime::build_hook_governance(hook_config_repository.as_ref(), &retention_settings);

    let db_pool = hook_config_repository.as_ref().map(|r| r.connection_pool());
    let plugin_routes = Arc::new(
        plugin_routes::card_posting(
            plugin_routes::runtime_authorization(
                crate::infrastructure::capability::PluginRouteBook::new(),
                db_pool.as_ref(),
            )?,
            db_pool.as_ref(),
        )
        .await?,
    );
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
    // 插件实体生命周期通知（卡片撤回、代发失败）：后台按系统票据投递 outbox。
    let entity_lifecycle_worker =
        plugin_routes::lifecycle_worker(&plugin_routes, capability_runtime.plugin_call_timeout);

    // 租户投影只在有库时可用：没有库就没有 `tenants` 表可写，控制面推送会得到 Unimplemented 的路由缺席错误。
    // 投影只认平台 worker 的控制断言（FLARE_PLATFORM_PUBLIC_KEY）；员工不经这里。
    let platform_verifier =
        flare_im_service_kit::control_auth::PlatformAssertionVerifier::from_env().map_err(
            |_| flare_server_core::error::FlareError::system("invalid FLARE_PLATFORM_PUBLIC_KEY"),
        )?;
    if platform_verifier.is_none() {
        tracing::warn!("platform public key missing; tenant projection RPCs fail closed");
    }
    let plugin_reconciler = capability_grpc.plugin_reconciler();
    let tenant_projection_repository = db_pool
        .as_ref()
        .map(|pool| Arc::new(PostgresTenantProjectionRepository::new(Arc::clone(pool))));
    let notices = tenant_notices(&config)?;
    let tenant_notice_relay = db_pool.clone().zip(notices.clone()).map(|(pool, notices)| {
        TenantNoticeRelay::new(Arc::new(PostgresTenantNoticeOutbox { pool }), notices)
    });
    let tenant_projection = tenant_projection_repository.as_ref().map(|repository| {
        TenantProjectionGrpcServer::new(Arc::clone(repository), platform_verifier.clone())
            .with_notices(notices.clone())
            .with_reconcile_signal(plugin_reconciler.as_ref().map(|worker| worker.signal()))
    });
    // 在线构建分布：网关每 30s 写进 token_store Redis，这里汇总；鉴权与投影接收端相同。
    let client_build_stats = connection_build_stats(&config)?.map(|source| {
        crate::interface::grpc::ClientBuildStatsGrpcServer::new(source, platform_verifier.clone())
    });
    // 管理员只读与投影接收端读同一张 `tenants` 表；开关读同一个策略后端。只认 admin-gateway
    // 转发的 Staff 上下文（Cell 公钥集，Staff 钉死 kid=gateway）。
    let capability_admin = CapabilityAdminGrpcServer::new(
        Arc::clone(&capability_policy),
        tenant_projection_repository,
        internal_ctx::admin_staff_verifier_from_env("Capability admin RPCs")?,
    );

    if config.notice_publishers.is_empty() {
        tracing::warn!(
            "FLARE_TENANT_NOTICE_PUBLISHERS empty; TenantNoticeService accepts no publisher"
        );
    }
    let tenant_notice = crate::interface::grpc::TenantNoticeGrpcServer::new(
        notices.clone(),
        config.notice_publishers.clone(),
        internal_ctx::internal_ctx_verifier_from_env("TenantNoticeService")?,
    );

    let extension_router = capability_registry.extension_router().await;
    // ExtensionPlugin.Call 只认 Cell 签名的上下文；没有公钥集时只回答健康检查。
    let extension_verifier = internal_ctx::internal_ctx_verifier_from_env("ExtensionPlugin")?;

    let plugin_resource_bridge = resource_bridge::compose(
        &plugin_routes,
        &capability_policy,
        db_pool.as_ref(),
        extensions.service_dispatch.as_ref(),
    )
    .await?;
    let plugin_message_events =
        message_events::tasks(&plugin_routes, Arc::clone(&capability_policy)).await?;
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
        plugin_message_events,
        audit_delivery,
        hook_governance: hook.hook_governance,
        plugin_routes,
        extension_router,
        extension_verifier,
        capability_registry,
        capability_policy,
        capability_grpc,
        capability_runtime,
        tenant_projection,
        client_build_stats,
        tenant_notice,
        capability_admin,
        hook_retention_background: hook.retention,
        entity_lifecycle_worker,
        tenant_notice_relay,
        plugin_reconciler,
    })
}

/// The online build distribution, read from the notice (token store) Redis the gateways write to.
fn connection_build_stats(
    config: &CapabilityServiceConfig,
) -> Result<Option<Arc<dyn crate::domain::repository::connection_builds::ConnectionBuildStats>>> {
    use crate::infrastructure::adapters::redis_connection_builds::RedisConnectionBuildStats;
    let Some(url) = config.notice_redis_url.as_deref() else {
        tracing::warn!("no token store Redis; ClientBuildStats is not served");
        return Ok(None);
    };
    let client = redis::Client::open(url).map_err(|_| {
        flare_server_core::error::FlareError::system("invalid connection build Redis URL")
    })?;
    Ok(Some(Arc::new(RedisConnectionBuildStats::new(client))))
}

/// The tenant notice channel, from the notice Redis URL. Absent: accepted plugin projections are
/// not announced and online clients pick them up when they next refresh their catalog.
fn tenant_notices(
    config: &CapabilityServiceConfig,
) -> Result<Option<Arc<dyn crate::domain::repository::tenant_notice::TenantNotices>>> {
    use crate::infrastructure::adapters::redis_tenant_notices::RedisTenantNotices;
    let Some(url) = config.notice_redis_url.as_deref() else {
        tracing::warn!(
            "no notice Redis (token_store profile or FLARE_CAPABILITY_NOTICE_REDIS_URL); plugin catalog and client policy changes are not announced"
        );
        return Ok(None);
    };
    let client = redis::Client::open(url).map_err(|_| {
        flare_server_core::error::FlareError::system("invalid tenant notice Redis URL")
    })?;
    Ok(Some(Arc::new(RedisTenantNotices::new(
        client,
        config.notice_channel.clone(),
    ))))
}
