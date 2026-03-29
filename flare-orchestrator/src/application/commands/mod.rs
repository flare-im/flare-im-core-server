//! 命令结构体定义（Command DTO）
//!
//! 存储链路使用 common.Message 原形；envelope（sync/tags/metadata）在 Message.extra 中。

pub mod message_action_commands;
pub mod message_send_commands;

pub use message_action_commands::*;
pub use message_send_commands::*;
