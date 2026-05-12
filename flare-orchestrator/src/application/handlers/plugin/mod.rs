//! 插件相关 Handler（编排层）：对接 capability / 媒体后端等外部插件能力。

mod call_capability_bridge;

pub use call_capability_bridge::CallCapabilityBridge;
