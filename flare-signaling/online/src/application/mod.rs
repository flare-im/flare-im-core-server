//! 应用层（DDD + CQRS）
//!
//! 与 flare-storage 一致：commands、handlers、queries 分目录。

pub mod commands;
pub mod handlers;
pub mod queries;

pub use commands::*;
pub use handlers::{
    OnlineCommandHandler, OnlinePresenceWatcherHandler, OnlineQueryHandler, OnlineUserHandler,
};
pub use queries::*;
