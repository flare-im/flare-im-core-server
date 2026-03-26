//! Proto ↔ 领域模型转换（read / manage 共用）

use chrono::{DateTime, TimeZone, Utc};
use flare_proto::common::ConversationDetail as ProtoConversationDetail;
use flare_proto::common::ConversationSummary as ProtoConversationSummary;
use flare_proto::conversation::{
    Conversation as ProtoConversation, ConversationParticipant as ProtoConversationParticipant,
    ConversationPolicy as ProtoConversationPolicy, DevicePresence as ProtoDevicePresence,
};
use prost_types::Timestamp;

use crate::domain::model::{
    Conversation, ConversationParticipant, ConversationPolicy, ConversationSummary, ConversationVisibility,
    DevicePresence,
};

pub fn proto_summary(summary: ConversationSummary) -> ProtoConversationSummary {
    let last_message_time = summary.last_message_time.and_then(timestamp_from_datetime);

    ProtoConversationSummary {
        conversation_id: summary.conversation_id,
        conversation_type: summary.conversation_type.unwrap_or_default(),
        business_type: summary.business_type.unwrap_or_default(),
        display_name: summary.display_name.unwrap_or_default(),
        avatar_url: String::new(),
        last_message: Some(flare_proto::common::MessagePreview {
            message_id: summary.last_message_id.unwrap_or_default(),
            sender_id: summary.last_sender_id.unwrap_or_default(),
            r#type: summary.last_message_type.unwrap_or_default(),
            text: String::new(),
            time: last_message_time,
        }),
        unread_count: summary.unread_count as u32,
        max_seq: 0,
        last_read_seq: 0,
        is_muted: false,
        is_pinned: false,
        mute_until: None,
        updated_at: last_message_time,
        created_at: None,
        labels: Vec::new(),
        member_count: 0,
        channel_id: summary.metadata.get("channel_id").cloned().unwrap_or_default(),
        ext: summary.metadata,
    }
}

pub fn proto_device(device: DevicePresence) -> ProtoDevicePresence {
    let last_seen_at = device.last_seen_at.and_then(timestamp_from_datetime);

    ProtoDevicePresence {
        device_id: device.device_id,
        device_platform: device.device_platform.unwrap_or_default(),
        state: device.state.as_proto(),
        last_seen_at,
    }
}

pub fn proto_policy(policy: ConversationPolicy) -> ProtoConversationPolicy {
    ProtoConversationPolicy {
        conflict_resolution: policy.conflict_resolution.as_proto(),
        max_devices: policy.max_devices,
        allow_anonymous: policy.allow_anonymous,
        allow_history_sync: policy.allow_history_sync,
        metadata: policy.metadata,
    }
}

pub fn proto_common_policy(policy: ConversationPolicy) -> flare_proto::common::ConversationPolicy {
    flare_proto::common::ConversationPolicy {
        conflict_resolution: policy.conflict_resolution.as_proto(),
        max_devices: policy.max_devices,
        allow_anonymous: policy.allow_anonymous,
        allow_history_sync: policy.allow_history_sync,
        metadata: policy.metadata,
        allow_message_search: false,
        allow_file_transfer: true,
    }
}

pub fn timestamp_from_datetime(dt: DateTime<Utc>) -> Option<Timestamp> {
    Some(Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    })
}

pub fn internal_error(err: anyhow::Error) -> tonic::Status {
    tonic::Status::internal(err.to_string())
}

pub fn participant_proto_to_domain(p: ProtoConversationParticipant) -> ConversationParticipant {
    ConversationParticipant {
        user_id: p.user_id,
        roles: p.roles,
        muted: p.muted,
        pinned: p.pinned,
        attributes: p.attributes,
    }
}

/// 领域 `Conversation` → `common.ConversationDetail`（读接口 `GetConversationDetail`）
pub fn domain_to_conversation_detail(conversation: Conversation) -> ProtoConversationDetail {
    let attrs = &conversation.attributes;
    let display_name = conversation
        .display_name
        .clone()
        .or_else(|| attrs.get("display_name").cloned())
        .unwrap_or_default();
    let avatar_url = attrs.get("avatar_url").cloned().unwrap_or_default();
    let description = attrs.get("description").cloned().unwrap_or_default();
    let announcement = attrs.get("announcement").cloned().unwrap_or_default();
    let announcement_updated_by = attrs.get("announcement_updated_by").cloned().unwrap_or_default();
    let announcement_updated_at = attrs
        .get("announcement_updated_at")
        .and_then(|s| s.parse::<i64>().ok())
        .and_then(|ms| chrono::Utc.timestamp_millis_opt(ms).single())
        .and_then(timestamp_from_datetime);

    let member_count = conversation.participants.len() as i32;

    ProtoConversationDetail {
        conversation_id: conversation.conversation_id,
        conversation_type: conversation.conversation_type,
        business_type: conversation.business_type,
        display_name,
        avatar_url,
        description,
        announcement,
        announcement_updated_at,
        announcement_updated_by,
        visibility: conversation.visibility.as_proto(),
        lifecycle_state: conversation.lifecycle_state.as_proto(),
        policy: conversation.policy.map(proto_common_policy),
        participants: conversation
            .participants
            .into_iter()
            .map(|p| flare_proto::common::ConversationParticipant {
                user_id: p.user_id,
                roles: p.roles,
                muted: p.muted,
                pinned: p.pinned,
                attributes: p.attributes,
                joined_at: None,
                nickname: String::new(),
            })
            .collect(),
        presence: None,
        created_at: timestamp_from_datetime(conversation.created_at),
        updated_at: timestamp_from_datetime(conversation.updated_at),
        member_count,
        attributes: conversation.attributes,
        ext: std::collections::HashMap::new(),
    }
}

pub fn participant_domain_to_proto(p: ConversationParticipant) -> ProtoConversationParticipant {
    ProtoConversationParticipant {
        user_id: p.user_id,
        roles: p.roles,
        muted: p.muted,
        pinned: p.pinned,
        attributes: p.attributes,
    }
}

pub fn domain_to_proto_conversation(conversation: Conversation) -> ProtoConversation {
    ProtoConversation {
        conversation_id: conversation.conversation_id,
        conversation_type: conversation.conversation_type,
        business_type: conversation.business_type,
        attributes: conversation.attributes,
        participants: conversation
            .participants
            .into_iter()
            .map(|p| flare_proto::common::ConversationParticipant {
                user_id: p.user_id,
                roles: p.roles,
                muted: p.muted,
                pinned: p.pinned,
                attributes: p.attributes,
                joined_at: None,
                nickname: String::new(),
            })
            .collect(),
        visibility: conversation.visibility.as_proto(),
        lifecycle_state: conversation.lifecycle_state.as_proto(),
        created_at: Some(
            timestamp_from_datetime(conversation.created_at).unwrap_or_default(),
        ),
        updated_at: Some(
            timestamp_from_datetime(conversation.updated_at).unwrap_or_default(),
        ),
        policy: conversation.policy.map(proto_common_policy),
    }
}
