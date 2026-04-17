//! # Flare Capability 服务库
//!
//! - **Hook 子系统**（DDD）：配置、调度、gRPC 扩展，见 `domain` / `application` / `infrastructure` / `interface::grpc`。
//! - **能力扩展子系统**（DDD + CQRS）：
//!   - `domain::capability`：模型与端口（Guard / Resolver / RTC、策略）
//!   - `application::capability`：目录查询、分发用例、参考编排示例
//!   - `infrastructure::capability`：注册表、SFU 适配、策略存储、gRPC 占位适配器
//!   - `interface::grpc`：`CapabilityService`（能力面经 gRPC 暴露，HTTP 由网关转发）
//!
//! 编排器推荐通过 `flare_im_core::hooks` 的 gRPC Hook 调用本服务 `HookExtension`，而非依赖本 crate 进程内嵌。

pub mod application;
pub mod domain;
pub mod error;
pub mod infrastructure;
pub mod interface;
pub mod service;

// Re-export Hook 引擎常用类型
pub use domain::model::{
    ExecutionMode, HookConfig, HookExecutionPlan, HookExecutionResult, HookStatistics,
};
pub use infrastructure::config::{ConfigLoader, ConfigWatcher};
pub use infrastructure::capability::CapabilityExtensionRegistry;
pub use service::{init_capability_extension_stack, ApplicationBootstrap};
