//! 应用层（DDD + CQRS）
//!
//! - **编排（写）**：消息 / 事件 / Ack / Data 分线，见 `handlers`
//!   - `handlers::MessageRoutingHandler` — 路由「发送消息」至 MessageSendService.SendMessage
//!   - `handlers::EventRoutingHandler` — 路由「操作事件」至 MessageEventService.ExecuteEvent
//! - **下行推送**：由 `flare-push-worker` 直连 Online + Access Gateway，不再经本服务。

pub mod dto;
pub mod handlers;

pub use dto::*;
pub use handlers::*;
