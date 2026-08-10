//! Proto ↔ 领域模型转换（read / manage 共用）

use chrono::{DateTime, Utc};
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
};

pub fn proto_summary(summary: ConversationSummary) -> ProtoConversationSummary {
    let last_message_time = summary
        .last_message_time
        .as_ref()
        .map(|dt| dt.timestamp_millis());
    // Sync 编排器用 `updated_at` 做会话列表排序与增量游标过滤，必须与「会话/成员变更时间」一致。
    // 仅填 last_message_time 会导致无最近消息预览时为空 → ts=0，增量同步在客户端游标非零时会误过滤掉整行。
    let updated_at_for_sync = last_message_time.or(summary.server_cursor_ts);

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
                    joined_at: 0,
                    visible_from_seq: p.visible_from_seq,
                })
                .collect()
        };

    let max_seq = summary.last_message_seq.unwrap_or(0).max(0) as u64;
    let visible_after_seq = summary.visible_after_seq.max(0) as u64;
    let unread_count = if visible_after_seq > 0 && max_seq <= visible_after_seq {
        0
    } else {
        summary.unread_count as u32
    };
    let display_name = summary
        .display_name
        .clone()
        .filter(|name| !name.trim().is_empty())
        .or_else(|| {
            if summary.conversation_type == crate::domain::model::ConversationType::Single
                && !summary.channel_id.trim().is_empty()
            {
                Some(summary.channel_id.clone())
            } else {
                None
            }
        })
        .unwrap_or_default();

    ProtoConversationSummary {
        conversation_id: summary.conversation_id,
        conversation_type: summary.conversation_type.as_str().to_string(),
        display_name,
        avatar_url: String::new(),
        last_message: Some(flare_proto::common::MessagePreview {
            message_id: summary.last_message_id.unwrap_or_default(),
            sender_id: summary.last_sender_id.unwrap_or_default(),
            r#type: summary.last_message_type.unwrap_or_default(),
            text: summary.last_message_preview.unwrap_or_default(),
            created_at: last_message_time.unwrap_or_default(),
        }),
        unread_count,
        max_conversation_seq: max_seq,
        last_read_seq: (summary.last_read_seq.max(0) as u64).max(visible_after_seq),
        is_muted: summary.is_muted,
        is_pinned: summary.is_pinned,
        mute_until: None,
        is_archived: summary.is_archived,
        user_settings_version: summary.settings_version,
        draft: summary.draft.clone().unwrap_or_default(),
        visible_after_conversation_seq: visible_after_seq,
        updated_at: updated_at_for_sync.unwrap_or_default(),
        created_at: 0,
        labels: Vec::new(),
        member_count: summary
            .metadata
            .get("member_count")
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or(0),
        channel_id: summary.channel_id,
        participant_version: summary.participant_version,
        member_preview,
        attributes: summary.metadata,
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
        attributes: policy.metadata,
        allow_message_search: false,
        allow_file_transfer: true,
    }
}

pub fn millis_from_datetime(dt: DateTime<Utc>) -> i64 {
    dt.timestamp_millis()
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
        visible_from_seq: p.visible_from_seq,
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
        .and_then(|s| s.parse::<i64>().ok());

    let member_count = conversation.participants.len() as i32;
    let channel_id = conversation.channel_id.clone();

    ProtoConversationDetail {
        conversation_id: conversation.conversation_id,
        conversation_type: conversation.conversation_type.as_str().to_string(),
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
        created_at: millis_from_datetime(conversation.created_at),
        updated_at: millis_from_datetime(conversation.updated_at),
        member_count,
        channel_id,
        attributes: conversation.attributes,
    }
}

pub fn participant_domain_to_proto(p: ConversationParticipant) -> ProtoConversationParticipant {
    ProtoConversationParticipant {
        user_id: p.user_id,
        roles: p.roles,
        muted: p.muted,
        pinned: p.pinned,
        attributes: p.attributes,
        joined_at: 0,
        visible_from_seq: p.visible_from_seq,
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
                joined_at: 0,
                visible_from_seq: p.visible_from_seq,
            })
            .collect(),
        visibility: conversation.visibility.as_proto(),
        lifecycle_state: conversation.lifecycle_state.as_proto(),
        created_at: Some(timestamp_from_datetime(conversation.created_at).unwrap_or_default()),
        updated_at: Some(timestamp_from_datetime(conversation.updated_at).unwrap_or_default()),
        policy: conversation.policy.map(proto_common_policy),
    }
}

#[cfg(test)]
mod tests {
    use super::{domain_to_conversation_detail, proto_summary};
    use crate::domain::model::{
        Conversation, ConversationLifecycleState, ConversationSummary, ConversationType,
        ConversationVisibility,
    };
    use chrono::Utc;
    use std::collections::HashMap;

    #[test]
    fn proto_summary_emits_canonical_conversation_type_name() {
        let proto = proto_summary(ConversationSummary {
            conversation_id: "c1".to_string(),
            conversation_type: ConversationType::Single,
            ..test_summary()
        });

        assert_eq!(proto.conversation_type, "single");
    }

    #[test]
    fn proto_summary_emits_last_message_preview_text() {
        let proto = proto_summary(ConversationSummary {
            conversation_id: "c1".to_string(),
            last_message_id: Some("m1".to_string()),
            last_message_preview: Some("hello preview".to_string()),
            ..test_summary()
        });

        let last_message = proto.last_message.expect("last message preview");
        assert_eq!(last_message.message_id, "m1");
        assert_eq!(last_message.text, "hello preview");
    }

    #[test]
    fn conversation_detail_emits_canonical_conversation_type_name() {
        let now = Utc::now();
        let proto = domain_to_conversation_detail(Conversation {
            tenant_id: "0".to_string(),
            conversation_id: "c1".to_string(),
            conversation_type: ConversationType::Channel,
            business_type: String::new(),
            channel_id: "channel-1".to_string(),
            display_name: None,
            attributes: HashMap::new(),
            participants: Vec::new(),
            visibility: ConversationVisibility::Private,
            lifecycle_state: ConversationLifecycleState::Active,
            policy: None,
            created_at: now,
            updated_at: now,
        });

        assert_eq!(proto.conversation_type, "channel");
    }

    fn test_summary() -> ConversationSummary {
        ConversationSummary {
            conversation_id: String::new(),
            conversation_type: ConversationType::Unspecified,
            business_type: None,
            last_message_id: None,
            last_message_time: None,
            last_sender_id: None,
            last_message_type: None,
            last_content_type: None,
            last_message_preview: None,
            unread_count: 0,
            last_read_seq: 0,
            metadata: HashMap::new(),
            server_cursor_ts: None,
            display_name: None,
            last_message_seq: None,
            channel_id: String::new(),
            participant_version: 0,
            member_preview: Vec::new(),
            is_muted: false,
            is_pinned: false,
            is_archived: false,
            settings_version: 0,
            draft: None,
            visible_after_seq: 0,
        }
    }
}
