//! 应用层（CQRS）：编排见 `handlers`，防打扰闸门见 `notify_gate`，群会话的波次见 `notify_wave`，
//! 领域规则见 `crate::domain`。

pub mod handlers;
pub mod notify_gate;
pub mod notify_wave;

pub use handlers::PushRouterHandler;
pub use notify_gate::NotifyGate;
pub use notify_wave::NotifyWaves;
