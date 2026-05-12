//! **运行时上下文**（CQRS 写侧装配结果）：供接口层 gRPC 与扩展栈共享的句柄集合。
//!
//! 不包含启动循环；进程生命周期见 [`super::bootstrap::ApplicationBootstrap`]。

use std::sync::Arc;

use crate::domain::capability::CapabilityPolicyBackend;
use crate::infrastructure::capability::{CapabilityExtensionRegistry, PluginRouteBook};
use crate::infrastructure::config::CapabilityRuntimeConfig;
use crate::interface::grpc::{
    CapabilityGrpcServer, ExtensionPluginRouter, HookServiceServer, ImHookPluginServer,
};

/// 应用上下文：接口层 gRPC 依赖 + 能力注册表 + 策略后端。
pub struct ApplicationContext {
    /// IM `HookPlugin` 入站适配器。
    pub im_hook_plugin: ImHookPluginServer,
    /// Hook 配置治理（写路径）；供 `CapabilityService.Administer` 与内部复用。
    pub hook_governance: Option<Arc<HookServiceServer>>,
    /// 插件 endpoint 登记簿（与 `CapabilityGrpcServer` 共享 `Arc`）。
    pub plugin_routes: Arc<PluginRouteBook>,
    /// 通用 `ExtensionPlugin` 路由器：具体 operation 由插件注册（核心不感知实现）。
    pub extension_router: ExtensionPluginRouter,
    /// 能力扩展：Guard / Resolver / RTC 注册表。
    pub capability_registry: CapabilityExtensionRegistry,
    /// 用户授权与租户开关（内存或 PostgreSQL，与 `CapabilityService` 共用）。
    pub capability_policy: Arc<dyn CapabilityPolicyBackend>,
    /// 能力 gRPC 实现（与主监听地址同端口注册）。
    pub capability_grpc: CapabilityGrpcServer,
    /// runtime 配置快照（供插件装配阶段读取配置文件/环境融合后的结果）。
    pub capability_runtime: Arc<CapabilityRuntimeConfig>,
}
