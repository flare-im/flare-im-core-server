//! 通话信令网关桥接骨架：与既有实时控制通路共存，不进入 durable IM Event。
//!
//! - **领域 FSM** 在 `flare-call`。
//! - **能力 enrich** 在 `flare-orchestrator::CallCapabilityBridge`。
//! - 本模块只做 **连接侧路由提示**（`CapabilityRouteHint`），后续注入 `CallBindingLookup` 实现。

pub mod bridge;
pub mod event;
pub mod router;

pub use bridge::CallSignalBridge;
pub use event::{CallSignalRouteView, CallSignalType};
pub use router::{CallBindingLookup, CallSignalRouter, CapabilityRouteHint};
