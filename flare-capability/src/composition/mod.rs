//! 进程 **组合根**（Composition Root）：按 **DDD + CQRS** 拆分为配置 DTO、运行时上下文、**wiring** 子模块与启动器。
//!
//! - **`process_config`**：进程启动参数（非领域模型）。
//! - **`runtime_context`**：装配结果句柄（供接口层使用，对应「应用运行时」读/写侧入口）。
//! - **`wiring`**：依赖图构建（配置源 → Hook 运行时 → 能力扩展栈）；**`initialize`** 为唯一总装入口。
//! - **`bootstrap`**：监听地址解析 + gRPC `Server` + `ServiceRuntime`（基础设施边界）。
//! - **`hook_registry`**：Hook 配置 **查询** 侧薄适配（基于 `ConfigWatcher` 快照）。
//!
//! 与 [`crate::domain::service`]（Hook 执行领域编排）区分：此处 **不实现** 业务规则，只做接线。

pub mod adapter_wiring;
pub mod bootstrap;
pub mod hook_registry;
pub mod process_config;
pub mod runtime_context;
pub mod wiring;

pub use adapter_wiring::wire_runtime_adapters;
pub use bootstrap::{ApplicationBootstrap, PluginContext};
pub use hook_registry::CoreHookRegistry;
pub use process_config::CapabilityServiceConfig;
pub use runtime_context::ApplicationContext;
pub use wiring::init_capability_extension_stack;
