//! # Flare Capability 服务库
//!
//! ## 分层（DDD + CQRS）
//!
//! - **Domain**（`domain`）：Hook 配置模型与写入校验、能力扩展端口（Resolver）。
//! - **Application**（`application`）：仅 **`commands` / `handler` / `queries`** — 编排、读目录；**Dispatch 规则在 [`domain`](crate::domain)**。
//! - **Infrastructure**（`infrastructure`）：配置仓储、适配器、能力扩展（注册 / 路由 等）、**插件路由登记簿**
//!   [`PluginRouteBook`](crate::infrastructure::capability::PluginRouteBook)。
//! - **Interface**（`interface::grpc`）：gRPC 按子包组织（`capability` / `hooks` / `extensions` / `shared`）。
//!   - [`CapabilityGrpcServer`](crate::interface::grpc::CapabilityGrpcServer) — `CapabilityService`：
//!     目录/授权/Dispatch、**Register/Deregister/List 插件 endpoint**、**`Administer`**（Hook 治理，
//!     `flare.extension.v1.hook_config.*`）。
//!   - [`ExtensionPluginRouter`](crate::interface::grpc::ExtensionPluginRouter) — `ExtensionPlugin.Call`
//!     通用路由器：按 `operation` 前缀分发给插件注册的
//!     [`ExtensionOperationHandler`](crate::domain::capability::ExtensionOperationHandler)；
//!     核心不认识任何具体后端。
//! - **Composition**（`composition`）：进程组合根 — `process_config` / `runtime_context` / **`wiring`**（`initialize` 总装）/ `bootstrap`；[`ApplicationBootstrap`](crate::composition::ApplicationBootstrap)、[`init_capability_extension_stack`](crate::composition::init_capability_extension_stack)。
//!
//!
//! Hook 只有一个运行时，在 Message Ingest 里（`flare_im_core_hooks`）。这里是 Hook 的控制面：配置 CRUD、
//! 统计、执行记录查询经 **`CapabilityService.Administer`**，保留期清理在后台。

pub mod application;
/// 进程组合根：依赖图与启动（原 `service` 模块）。
pub mod composition;
pub mod domain;
pub mod infrastructure;
pub mod interface;

pub use flare_im_core_capability_core as capability_core;

pub use application::queries::{HookIntegrationChannelDoc, list_hook_integration_channels};
pub use composition::{
    ApplicationBootstrap, ApplicationContext, CapabilityServiceConfig,
    init_capability_extension_stack,
};
pub use domain::hook_integration::{HookTransportSurface, classify_transport};
pub use domain::model::HookExecutionStatistics;
pub use infrastructure::capability::{CapabilityExtensionRegistry, PluginRouteBook};
