//! 通话信令网关桥接：与既有实时控制通路共存，不进入 durable IM Event。
//!
//! - **领域 FSM / CQRS 命令处理** 在 `flare-call`。
//! - **RTC/SFU 编排** 仍由 capability/plugin 实现承接。
//! - 本模块只做 gateway 运行时接线、生命周期命令转发与连接侧路由提示。

pub mod bridge;
pub mod event;
pub mod repository;
pub mod router;

pub use bridge::CallSignalBridge;
pub use event::{CallSignalRouteView, CallSignalType};
pub use repository::InMemoryCallSessionRepository;
pub use router::{CallBindingLookup, CallSignalRouter, CapabilityRouteHint};
