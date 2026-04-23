//! 通话信令（`EVENT_CALL_SIGNAL`）网关桥接骨架：与既有 IM 信令通路共存，不新建平行协议。
//!
//! - **领域 FSM** 在 `flare-conversation::domain::call`。
//! - **能力 enrich** 在 `flare-orchestrator::CallCapabilityBridge`。
//! - 本模块只做 **连接侧路由提示**（`CapabilityRouteHint`），后续注入 `CallBindingLookup` 实现。

pub mod bridge;
pub mod event;
pub mod router;

pub use bridge::CallSignalBridge;
pub use event::{try_unwrap_call_signal, CallSignalType, EVENT_CALL_SIGNAL};
pub use router::{CallBindingLookup, CallSignalRouter, CapabilityRouteHint};
