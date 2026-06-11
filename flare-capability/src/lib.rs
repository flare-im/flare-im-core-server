//! # Flare Capability 服务库
//!
//! ## 分层（DDD + CQRS）
//!
//! - **Domain**（`domain`）：Hook 编排模型、Hook 集成策略、能力扩展端口（Guard / Resolver / RTC）。
//! - **Application**（`application`）：仅 **`commands` / `handler` / `queries`** — 物化、编排、读目录；**Dispatch / Hook 执行规则在 [`domain`](crate::domain)**。
//! - **Infrastructure**（`infrastructure`）：配置仓储、适配器工厂、能力扩展（注册 / 路由 等）、**插件路由登记簿**
//!   [`PluginRouteBook`](crate::infrastructure::capability::PluginRouteBook)。
//! - **Interface**（`interface::grpc`）：gRPC 按子包组织（`capability` / `hooks` / `extensions` / `shared`）。
//!   - [`CapabilityGrpcServer`](crate::interface::grpc::CapabilityGrpcServer) — `CapabilityService`：
//!     目录/授权/Dispatch、**Register/Deregister/List 插件 endpoint**、**`Administer`**（Hook 治理，
//!     `flare.extension.v1.hook_config.*`）。
//!   - [`ImHookPluginServer`](crate::interface::grpc::ImHookPluginServer) — `HookPlugin.Call`（IM 生命周期）。
//!   - [`ExtensionPluginRouter`](crate::interface::grpc::ExtensionPluginRouter) — `ExtensionPlugin.Call`
//!     通用路由器：按 `operation` 前缀分发给插件注册的
//!     [`ExtensionOperationHandler`](crate::domain::capability::ExtensionOperationHandler)；
//!     核心不认识任何具体后端（媒体控制协议实现 / LiveKit / Janus …）。
//! - **Composition**（`composition`）：进程组合根 — `process_config` / `runtime_context` / **`wiring`**（`initialize` 总装）/ `bootstrap` / `hook_registry`；[`ApplicationBootstrap`](crate::composition::ApplicationBootstrap)、[`init_capability_extension_stack`](crate::composition::init_capability_extension_stack)。
//!
//! **RTC 插件编排**实现位于 [`infrastructure::rtc`](crate::infrastructure::rtc)，crate 根再导出为 [`rtc`](crate::rtc) 以保持稳定路径。
//!
//! 编排器经 `flare_im_hooks::hooks` 的 gRPC 客户端调用本进程 **`HookPlugin`**；Hook 配置 CRUD 经 **`CapabilityService.Administer`**。

pub mod application;
/// 进程组合根：依赖图与启动（原 `service` 模块）。
pub mod composition;
pub mod domain;
pub mod infrastructure;
pub mod interface;

pub use flare_im_capability_core as capability_core;

/// RTC 插件编排（与 [`crate::infrastructure::rtc`] 同一模块，便于对外 `flare_capability::rtc::*`）。
pub use infrastructure::rtc;

// Re-export Hook 引擎常用类型（稳定 crate 根路径）
pub use application::commands::materialize_hook_execution_plan;
pub use application::queries::{HookIntegrationChannelDoc, list_hook_integration_channels};
pub use composition::{
    ApplicationBootstrap, ApplicationContext, CapabilityServiceConfig,
    init_capability_extension_stack,
};
pub use domain::hook_integration::{HookTransportSurface, classify_transport};
pub use domain::model::{
    ExecutionMode, HookConfig, HookExecutionPlan, HookExecutionResult, HookStatistics,
};
pub use infrastructure::capability::{CapabilityExtensionRegistry, PluginRouteBook};
pub use infrastructure::config::{ConfigLoader, ConfigWatcher};
