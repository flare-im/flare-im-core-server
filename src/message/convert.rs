//! Proto 与统一消息领域模型互转（与 common/message.proto 严格 1:1）

use super::model::Message;

/// 从 proto Message 转为领域 Message
pub fn message_from_proto(p: &flare_proto::common::Message) -> Message {
    Message {
        server_id: p.server_id.clone(),
        conversation_id: p.conversation_id.clone(),
        client_msg_id: p.client_msg_id.clone(),
        sender_id: p.sender_id.clone(),
        source: p.source,
        seq: p.seq,
        timestamp: p.timestamp.clone(),
        conversation_type: p.conversation_type,
        message_type: p.message_type,
        channel_id: p.channel_id.clone(),
        sender_name: p.sender_name.clone(),
        sender_avatar: p.sender_avatar.clone(),
        content: p.content.clone(),
        status: p.status,
        offline_push_info: p.offline_push_info.clone(),
        extra: p.extra.clone(),
        extensions: p.extensions.clone(),
    }
}

/// 从领域 Message 转为 proto Message
pub fn message_to_proto(m: &Message) -> flare_proto::common::Message {
    flare_proto::common::Message {
        server_id: m.server_id.clone(),
        conversation_id: m.conversation_id.clone(),
        client_msg_id: m.client_msg_id.clone(),
        sender_id: m.sender_id.clone(),
        source: m.source,
        seq: m.seq,
        timestamp: m.timestamp.clone(),
        conversation_type: m.conversation_type,
        message_type: m.message_type,
        channel_id: m.channel_id.clone(),
        sender_name: m.sender_name.clone(),
        sender_avatar: m.sender_avatar.clone(),
        content: m.content.clone(),
        status: m.status,
        offline_push_info: m.offline_push_info.clone(),
        extra: m.extra.clone(),
        extensions: m.extensions.clone(),
        ..Default::default()
    }
}
