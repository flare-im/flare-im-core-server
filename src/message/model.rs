//! 统一消息领域模型（与 common/message.proto 严格对齐）

use std::collections::HashMap;

/// 消息领域模型（与 common/message.proto Message 一一对应）
#[derive(Debug, Clone, Default)]
pub struct Message {
    pub server_id: String,
    pub conversation_id: String,
    pub client_msg_id: String,
    pub sender_id: String,
    pub source: i32,
    pub seq: u64,
    pub timestamp: Option<prost_types::Timestamp>,
    pub conversation_type: i32,
    pub message_type: i32,
    /// 会话频道 ID：单聊=对方 user_id，群聊=群 ID，频道/话题=对应 ID
    pub channel_id: String,
    pub sender_name: String,
    pub sender_avatar: String,
    pub content: Vec<u8>,
    pub status: i32,
    pub offline_push_info: Option<flare_proto::common::OfflinePushInfo>,
    pub extra: HashMap<String, String>,
    pub extensions: HashMap<String, Vec<u8>>,
}

impl Message {
    /// 单聊时对方 user_id（= channel_id）；群聊/频道返回空。proto 无 receiver_id，以此替代。
    pub fn single_chat_receiver(&self) -> &str {
        if self.conversation_type == flare_proto::common::ConversationType::Single as i32 {
            self.channel_id.as_str()
        } else {
            ""
        }
    }
}

/// 附件（业务解析 content 或 extensions 时使用，proto Message 无此字段）
#[derive(Debug, Clone, Default)]
pub struct Attachment {
    pub url: String,
    pub name: Option<String>,
    pub content_type: Option<String>,
    pub size: i64,
}
