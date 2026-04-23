//! RTC 能力插件编排层（增量）：多实现注册、健康检查、draining 选路与 **既有** 领域 `RtcCapability` 协同。
//!
//! - **领域 RTC 端口**仍以 [`crate::domain::capability::ports::RtcCapability`] 为准（create_call / accept_call 等）。
//! - 本模块补充 **进程级插件元数据、实例选择、与 flare-strom-sfu gRPC 控制面** 的编排骨架（后续接线 `sfu_control.proto`）。

pub mod capability;
pub mod health;
pub mod manager;
pub mod plugin;
pub mod registry;
pub mod selector;

pub use capability::{CapabilityKind, RtcBackendDescriptor};
pub use health::CapabilityHealthChecker;
pub use manager::CapabilityManager;
pub use plugin::CapabilityPlugin;
pub use registry::CapabilityRegistry;
pub use selector::CapabilitySelector;
