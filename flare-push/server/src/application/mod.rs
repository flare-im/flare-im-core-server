//! 应用层（CQRS）：编排见 `handlers`，领域规则见 `crate::domain`。

pub mod handlers;

pub use handlers::PushRouterHandler;
