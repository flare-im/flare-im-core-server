//! **运行时上下文**（CQRS 写侧装配结果）：供接口层 gRPC 与扩展栈共享的句柄集合。
//!
//! 不包含启动循环；进程生命周期见 [`super::bootstrap::ApplicationBootstrap`]。

use std::sync::Arc;

use crate::composition::wiring::HookExecutionBackground;
use crate::domain::capability::CapabilityPolicyBackend;
use crate::infrastructure::capability::{CapabilityExtensionRegistry, PluginRouteBook};
use crate::infrastructure::config::{CapabilityRuntimeConfig, HookConfigListener};
use crate::infrastructure::persistence::PostgresTenantProjectionRepository;
use crate::interface::grpc::{
    CapabilityAdminGrpcServer, CapabilityGrpcServer, ExtensionPluginRouter, HookServiceServer,
    ImHookPluginServer, TenantProjectionGrpcServer,
};

/// 应用上下文：接口层 gRPC 依赖 + 能力注册表 + 策略后端。
pub struct ApplicationContext {
    pub plugin_resource_bridge:
        Option<crate::interface::grpc::resource_bridge::PluginHostBridgeGrpc>,
    /// Source-owned queue delivery, independent of IM ordering. Disabled broker retains rows.
    pub audit_delivery: Option<(
        Arc<sqlx::PgPool>,
        flare_im_service_kit::audit_effects::AuditEffects,
    )>,
    /// IM `HookPlugin` 入站适配器。
    pub im_hook_plugin: ImHookPluginServer,
    /// Hook 配置治理（写路径）；供 `CapabilityService.Administer` 与内部复用。
    pub hook_governance: Option<Arc<HookServiceServer>>,
    /// 插件 endpoint 登记簿（与 `CapabilityGrpcServer` 共享 `Arc`）。
    pub plugin_routes: Arc<PluginRouteBook>,
    /// 通用 `ExtensionPlugin` 路由器：具体 operation 由插件注册（核心不感知实现）。
    pub extension_router: ExtensionPluginRouter,
    /// `ExtensionPlugin.Call` 的验签器（Cell 公钥集）；没有时路由器只回答健康检查。
    pub extension_verifier: Option<flare_server_core::SignedContextVerifier>,
    /// 能力扩展：Guard / Resolver / RTC 注册表。
    pub capability_registry: CapabilityExtensionRegistry,
    /// 用户授权与租户开关（内存或 PostgreSQL，与 `CapabilityService` 共用）。
    pub capability_policy: Arc<dyn CapabilityPolicyBackend>,
    /// 能力 gRPC 实现（与主监听地址同端口注册）。
    pub capability_grpc: CapabilityGrpcServer,
    /// runtime 配置快照（供插件装配阶段读取配置文件/环境融合后的结果）。
    pub capability_runtime: Arc<CapabilityRuntimeConfig>,
    /// 控制面租户投影接收端（`flare.control.v1.TenantProjection`）；仅当配置了 PostgreSQL。
    pub tenant_projection: Option<TenantProjectionGrpcServer<PostgresTenantProjectionRepository>>,
    /// 管理员只读（`CapabilityAdminService`）：租户开关与本地租户投影，只认转发的 Staff 上下文。
    pub capability_admin: CapabilityAdminGrpcServer<PostgresTenantProjectionRepository>,
    /// Hook 配置跨实例失效通知的监听任务（有库时）；进程停机时停掉。
    pub hook_config_listener: Option<HookConfigListener>,
    /// Hook 执行证据的批量写入与保留期清理（有库且开启记录时）；随进程停机结束，停机时写完缓冲。
    pub(crate) hook_execution_background: Option<HookExecutionBackground>,
    /// 插件实体生命周期 outbox 的投递任务（有库且配置了插件调用签名器时）；随进程停机结束。
    pub(crate) entity_lifecycle_worker:
        Option<crate::application::handler::entity_lifecycle::EntityLifecycleWorker>,
}
