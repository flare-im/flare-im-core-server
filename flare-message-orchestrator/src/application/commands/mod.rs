//! 命令结构体定义（Command DTO）

use flare_proto::message::{SendMessageRequest, BatchSendMessageRequest};
use flare_proto::storage::StoreMessage;
use flare_proto::common::{Message, RequestContext, TenantContext};

/// 发送消息命令（包含消息类别判断和路由逻辑）
#[derive(Debug, Clone)]
pub struct SendMessageCommand {
    /// 消息
    pub message: Message,
    /// 会话ID
    pub conversation_id: String,
    /// 是否同步
    pub sync: bool,
    /// 请求上下文
    pub context: Option<RequestContext>,
    /// 租户上下文
    pub tenant: Option<TenantContext>,
}

/// 批量发送消息命令
#[derive(Debug, Clone)]
pub struct BatchSendMessageCommand {
    /// 批量发送请求
    pub requests: Vec<SendMessageRequest>,
}

/// 存储消息命令
#[derive(Debug, Clone)]
pub struct StoreMessageCommand {
    /// 原始请求
    pub request: StoreMessage,
}

/// 批量存储消息命令
#[derive(Debug, Clone)]
pub struct BatchStoreMessageCommand {
    /// 批量请求
    pub requests: Vec<StoreMessage>,
}

pub mod message_operation_commands;

pub use message_operation_commands::*;

// Re-export HandleTemporaryMessageCommand
pub use message_operation_commands::HandleTemporaryMessageCommand;

// pub mod app_layer_operation_commands;
// pub use app_layer_operation_commands::*;