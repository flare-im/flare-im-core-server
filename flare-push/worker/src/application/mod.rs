//! 应用层（CQRS）：编排见 `handlers`，网关推送领域规则见 `crate::domain`。

pub mod application_delivery;
pub mod binding_delivery;
pub mod digest_coalescer;
pub mod handlers;
pub mod vendor_governance;

pub use handlers::GatewayPushExecutor;
