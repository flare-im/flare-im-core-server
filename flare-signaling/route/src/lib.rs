//! Signaling Route 服务
//!
//! **上行**链路（Client → Gateway → **Router** → Ingest/Orchestrator）：顺序保证、流控、权限校验，按载荷分流：
//! - `Message`（forward_message）→ flare-message-ingest（`MessageSendService.SendMessage`）
//! - `Event`（forward_event）→ flare-orchestrator（`MessageEventService.ExecuteEvent`）
//!
//! 下行（JetStream → Push Worker → Online 选端 → GatewayRouter → Access Gateway → Client）不经本 Route 的 gRPC。
//! 详见 `flare-im-core/docs/message_event_flow.md`。

pub mod application;
pub mod config;
pub mod convert;
pub mod domain;
pub mod infrastructure;
pub mod interface;
pub mod service;

pub use domain::Ctx;
pub use service::ApplicationBootstrap;
pub use service::ApplicationContext;
