//! 消息发送相关命令：`SendMessage`、批量发送、系统消息、临时消息等。

use flare_grpc_proto::message::SendMessageRequest;
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

/// 发送系统消息命令
#[derive(Debug, Clone)]
pub struct SendSystemMessageCommand {
    pub conversation_id: String,
    pub message: Message,
    pub system_message_type: String,
}

/// 处理临时消息命令（只推送，不持久化）
#[derive(Debug, Clone)]
pub struct HandleTemporaryMessageCommand {
    /// 消息（proto 类型）
    pub message: Message,
}
