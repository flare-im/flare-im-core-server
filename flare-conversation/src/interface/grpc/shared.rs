//! Proto ↔ 领域模型转换（read / manage 共用）

use chrono::{DateTime, TimeZone, Utc};
use flare_grpc_proto::conversation::{
    Conversation as ProtoConversation, ConversationPolicy as ProtoConversationPolicy,
    DevicePresence as ProtoDevicePresence,
};
use flare_proto::common::ConversationDetail as ProtoConversationDetail;
use flare_proto::common::ConversationParticipant as ProtoConversationParticipant;
use flare_proto::common::ConversationSummary as ProtoConversationSummary;
use prost_types::Timestamp;

use crate::domain::model::{
    Conversation, ConversationParticipant, ConversationPolicy, ConversationSummary, DevicePresence,
    millis_to_datetime,
};

pub fn proto_summary(summary: ConversationSummary) -> ProtoConversationSummary {
    let last_message_time = summary.last_message_time.and_then(timestamp_from_datetime);
    // Sync 编排器用 `updated_at` 做会话列表排序与增量游标过滤，必须与「会话/成员变更时间」一致。
    // 仅填 last_message_time 会导致无最近消息预览时为空 → ts=0，增量同步在客户端游标非零时会误过滤掉整行。
    let list_change_time = summary
        .last_message_time
        .or_else(|| summary.server_cursor_ts.and_then(millis_to_datetime));
    let updated_at_for_sync = list_change_time.and_then(timestamp_from_datetime);

    let member_preview =
        if summary.conversation_type == crate::domain::model::ConversationType::Single {
            Vec::new()
        } else {
            summary
                .member_preview
                .into_iter()
                .map(|p| flare_proto::common::ConversationParticipant {
                    user_id: p.user_id,
                    roles: p.roles,
                    muted: p.muted,
                    pinned: p.pinned,
                    attributes: p.attributes,
                    joined_at: None,
                })
                .collect()
        };

    ProtoConversationSummary {
        conversation_id: summary.conversation_id,
        conversation_type: summary.conversation_type.as_int().to_string(),
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
        max_seq: summary.last_message_seq.unwrap_or(0).max(0) as u64,
        last_read_seq: summary.last_read_seq.max(0) as u64,
        is_muted: false,
        is_pinned: false,
        mute_until: None,
        updated_at: updated_at_for_sync.or(last_message_time),
        created_at: None,
        labels: Vec::new(),
        member_count: summary
            .metadata
            .get("member_count")
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or(0),
        channel_id: summary.channel_id,
        participant_version: summary.participant_version,
        member_preview,
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
    let announcement_updated_by = attrs
        .get("announcement_updated_by")
        .cloned()
        .unwrap_or_default();
    let announcement_updated_at = attrs
        .get("announcement_updated_at")
        .and_then(|s| s.parse::<i64>().ok())
        .and_then(|ms| chrono::Utc.timestamp_millis_opt(ms).single())
        .and_then(timestamp_from_datetime);

    let member_count = conversation.participants.len() as i32;
    let channel_id = conversation.channel_id.clone();

    ProtoConversationDetail {
        conversation_id: conversation.conversation_id,
        conversation_type: conversation.conversation_type.as_int().to_string(),
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
        presence: None,
        created_at: timestamp_from_datetime(conversation.created_at),
        updated_at: timestamp_from_datetime(conversation.updated_at),
        member_count,
        channel_id,
        ext: conversation.attributes,
    }
}

pub fn participant_domain_to_proto(p: ConversationParticipant) -> ProtoConversationParticipant {
    ProtoConversationParticipant {
        user_id: p.user_id,
        roles: p.roles,
        muted: p.muted,
        pinned: p.pinned,
        attributes: p.attributes,
        joined_at: None,
    }
}

pub fn domain_to_proto_conversation(conversation: Conversation) -> ProtoConversation {
    ProtoConversation {
        conversation_id: conversation.conversation_id,
        conversation_type: conversation.conversation_type.as_proto(),
        business_type: conversation.business_type,
        channel_id: conversation.channel_id,
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
            })
            .collect(),
        visibility: conversation.visibility.as_proto(),
        lifecycle_state: conversation.lifecycle_state.as_proto(),
        created_at: Some(timestamp_from_datetime(conversation.created_at).unwrap_or_default()),
        updated_at: Some(timestamp_from_datetime(conversation.updated_at).unwrap_or_default()),
        policy: conversation.policy.map(proto_common_policy),
    }
}
