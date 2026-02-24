//! 命令结构体定义（Command DTO）

use flare_proto::storage::{StoreMessage, BatchStoreMessage};
use flare_proto::common::Message;
use std::collections::HashMap;

/// 内部使用的存储消息命令结构（用于替换不存在的 StoreMessageRequest）
#[derive(Debug, Clone)]
pub struct StoreMessageCommandInternal {
    pub conversation_id: String,
    pub message: Option<Message>,
    pub sync: bool,
    pub context: Option<flare_proto::common::RequestContext>,
    pub tenant: Option<flare_proto::common::TenantContext>,
    pub tags: HashMap<String, String>,
    pub metadata: HashMap<String, String>,
}

/// 处理存储消息命令
#[derive(Debug, Clone)]
pub struct ProcessStoreMessageCommand {
    pub command: StoreMessage,
}



pub mod process_message_operation;
pub use process_message_operation::ProcessMessageOperationCommand;
