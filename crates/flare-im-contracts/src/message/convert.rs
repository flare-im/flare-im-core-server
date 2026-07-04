//! Proto 与统一消息领域模型互转（与 common/message.proto 严格 1:1）

use super::model::Message;
use crate::utils::{millis_to_timestamp, timestamp_to_millis};

/// 从 proto Message 转为领域 Message
pub fn message_from_proto(p: &flare_proto::common::Message) -> Message {
    Message {
        server_id: p.server_id.clone(),
        conversation_id: p.conversation_id.clone(),
        client_msg_id: p.client_msg_id.clone(),
        sender_id: p.sender_id.clone(),
        source: p.source,
        conversation_seq: p.conversation_seq,
        timestamp: if p.created_at > 0 {
            millis_to_timestamp(p.created_at)
        } else {
            None
        },
        conversation_type: p.conversation_type,
        message_type: p.message_type,
        message_seq: p.message_seq,
        channel_id: p.channel_id.clone(),
        sender_name: p.sender_name.clone(),
        sender_avatar: p.sender_avatar.clone(),
        thread_id: p.thread_id.clone(),
        content: p.content.clone(),
        status: p.status,
        retention_policy: p.retention_policy.clone(),
        retention_state: p.retention_state.clone(),
        offline_push_info: p.offline_push_info.clone(),
        extra: p.attributes.clone(),
        extensions: p.extensions.clone(),
    }
}

/// [`message_to_proto`] 的按值版：move 全部 String/bytes/map（批量端点整页转换免深拷贝）。
pub fn message_into_proto(m: Message) -> flare_proto::common::Message {
    flare_proto::common::Message {
        created_at: m
            .timestamp
            .as_ref()
            .and_then(timestamp_to_millis)
            .unwrap_or_default(),
        server_id: m.server_id,
        conversation_id: m.conversation_id,
        client_msg_id: m.client_msg_id,
        sender_id: m.sender_id,
        source: m.source,
        conversation_seq: m.conversation_seq,
        conversation_type: m.conversation_type,
        message_type: m.message_type,
        message_seq: m.message_seq,
        channel_id: m.channel_id,
        sender_name: m.sender_name,
        sender_avatar: m.sender_avatar,
        thread_id: m.thread_id,
        content: m.content,
        status: m.status,
        retention_policy: m.retention_policy,
        retention_state: m.retention_state,
        offline_push_info: m.offline_push_info,
        attributes: m.extra,
        extensions: m.extensions,
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
        conversation_seq: m.conversation_seq,
        created_at: m
            .timestamp
            .as_ref()
            .and_then(timestamp_to_millis)
            .unwrap_or_default(),
        conversation_type: m.conversation_type,
        message_type: m.message_type,
        message_seq: m.message_seq,
        channel_id: m.channel_id.clone(),
        sender_name: m.sender_name.clone(),
        sender_avatar: m.sender_avatar.clone(),
        thread_id: m.thread_id.clone(),
        content: m.content.clone(),
        status: m.status,
        retention_policy: m.retention_policy.clone(),
        retention_state: m.retention_state.clone(),
        offline_push_info: m.offline_push_info.clone(),
        attributes: m.extra.clone(),
        extensions: m.extensions.clone(),
    }
}
