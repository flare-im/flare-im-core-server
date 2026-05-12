//! # Hook 集成限界上下文（出站：gRPC / Webhook / Local）
//!
//! 与 **Hook 编排**（[`crate::domain::service::HookOrchestrationService`]）分离：
//! - **本模块**：传输分类、物化前策略校验（**纯领域规则**）。
//! - **应用层** [`crate::application::commands`]：**CQRS 写路径**——将配置 + `HookAdapterFactory` 物化为 [`crate::domain::model::HookExecutionPlan`]。
//! - **基础设施** [`crate::infrastructure::adapters`]：`HookAdapter` / `GrpcHookAdapter` / `WebhookHookAdapter` / `LocalHookAdapter`。
//! - **接口** [`crate::interface::grpc`]：`HookPlugin`（编排器调用）、`CapabilityService.Administer`（配置 CRUD）。
//!
//! ## CQRS 对齐
//!
//! - **Command 侧**：物化执行计划、执行 PreSend/PostSend/…（命令在 `HookCommandHandler` + 本 crate 已有 gRPC）。
//! - **Query 侧**：传输面枚举、运维向「支持哪些集成形态」查询（见 [`crate::application::queries`]）。

mod policy;
mod transport;

pub use policy::validate_hook_item_for_materialization;
pub use transport::{HookTransportSurface, classify_transport};
