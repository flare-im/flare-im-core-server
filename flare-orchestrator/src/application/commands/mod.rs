//! 命令结构体定义（Command DTO）
//!
//! 存储链路使用 common.Message 原形；envelope（sync/tags/metadata）在 Message.extra 中。

use flare_proto::message::SendMessageRequest;
use flare_proto::common::Message;

/// 发送消息命令（包含消息类别判断和路由逻辑）
/// 请求/租户信息通过调用链的 Context 传递，不在此结构体中承载。
#[derive(Debug, Clone)]
pub struct SendMessageCommand {
    /// 消息
    pub message: Message,
    /// 会话ID
    pub conversation_id: String,
    /// 是否同步
    pub sync: bool,
}

/// 批量发送消息命令
#[derive(Debug, Clone)]
pub struct BatchSendMessageCommand {
    /// 批量发送请求
    pub requests: Vec<SendMessageRequest>,
}

/// 存储消息命令（payload 为 common.Message，envelope 在 extra）
#[derive(Debug, Clone)]
pub struct StoreMessageCommand {
    pub request: Message,
}

/// 批量存储消息命令
#[derive(Debug, Clone)]
pub struct BatchStoreMessageCommand {
    pub requests: Vec<Message>,
}

pub mod message_operation_commands;

pub use message_operation_commands::*;

pub use message_operation_commands::HandleTemporaryMessageCommand;