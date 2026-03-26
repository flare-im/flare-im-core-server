//! 命令结构体定义（与 event_bus / topic_envelope 信封解耦后的 DTO）

use std::collections::HashMap;

use crate::domain::model::{Message, RequestContext, TenantContext};

/// 存储消息命令（从 MessageEnvelope / TopicEventEnvelope 解析后进入应用层）
#[derive(Debug, Clone)]
pub struct ProcessStoreMessageCommand {
    pub conversation_id: String,
    pub message: Option<Message>,
    pub sync: bool,
    pub context: Option<RequestContext>,
    pub tenant: Option<TenantContext>,
    pub tags: HashMap<String, String>,
    pub metadata: HashMap<String, String>,
}



pub mod process_message_operation;
pub use process_message_operation::ProcessEventCommand;
