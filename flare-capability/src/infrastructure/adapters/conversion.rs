//! # 类型转换辅助模块
//!
//! 提供 flare-im-core 类型和 protobuf 类型之间的转换

use prost_types::Timestamp;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::infrastructure::adapters::hook_context_data::{HookContextData, set_hook_context_data};
use flare_grpc_proto::capability::{
    HookDeliveryEvent, HookInvocationContext, HookMessageDraft, HookMessageRecord, HookRecallEvent,
    PreSendHookResponse, RecallHookResponse,
};
use flare_im_hooks::{DeliveryEvent, MessageDraft, MessageRecord, PreSendDecision, RecallEvent};
use flare_server_core::context::Context;

/// 将 flare_server_core::Context 转换为 HookInvocationContext
pub fn context_to_proto(ctx: &Context) -> HookInvocationContext {
    use crate::infrastructure::adapters::hook_context_data::get_hook_context_data;

    let hook_data = get_hook_context_data(ctx).cloned().unwrap_or_default();

    let operator_user_id = ctx
        .actor()
        .map(|a| a.actor_id().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| ctx.user_id().map(|s| s.to_string()))
        .unwrap_or_default();

    HookInvocationContext {
        conversation_id: hook_data.conversation_id.clone().unwrap_or_default(),
        conversation_type: hook_data.conversation_type.clone().unwrap_or_default(),
        corridor: hook_data
            .attributes
            .get("corridor")
            .cloned()
            .unwrap_or_else(|| "messaging".to_string()),
        tags: hook_data.tags.clone(),
        attributes: hook_data.attributes.clone(),
        tenant_id: ctx.tenant_id().unwrap_or("").to_string(),
        app_id: String::new(),
        operator_user_id,
        request_id: ctx.request_id().to_string(),
        extension_bag: None,
    }
}

// 使用 Context 和 HookContextData 进行转换

/// 将 MessageDraft 转换为 HookMessageDraft
pub fn message_draft_to_proto(draft: &MessageDraft) -> HookMessageDraft {
    HookMessageDraft {
        message_id: draft.message_id.clone().unwrap_or_default(),
        client_message_id: draft.client_message_id.clone().unwrap_or_default(),
        conversation_id: draft.conversation_id.clone().unwrap_or_default(),
        payload: draft.payload.clone(),
        headers: draft.headers.clone(),
        metadata: draft.metadata.clone(),
        message_type: String::new(),
        parent_message_id: String::new(),
        is_silent: false,
        extension_bag: None,
    }
}

/// 将 HookMessageDraft 转换为 MessageDraft
pub fn proto_to_message_draft(proto: &HookMessageDraft) -> MessageDraft {
    let mut draft = MessageDraft::new(proto.payload.clone());
    if !proto.message_id.is_empty() {
        draft.set_message_id(proto.message_id.clone());
    }
    if !proto.client_message_id.is_empty() {
        draft.set_client_message_id(proto.client_message_id.clone());
    }
    if !proto.conversation_id.is_empty() {
        draft.set_conversation_id(proto.conversation_id.clone());
    }
    draft.headers = proto.headers.clone();
    draft.metadata = proto.metadata.clone();
    draft
}

/// 将 MessageRecord 转换为 HookMessageRecord
pub fn message_record_to_proto(record: &MessageRecord) -> HookMessageRecord {
    // 将 MessageRecord 转换为 protobuf Message
    let created_at = record
        .persisted_at
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default();
    let proto_message = flare_proto::common::Message {
        server_id: record.message_id.clone(),
        conversation_id: record.conversation_id.clone(),
        client_msg_id: record.client_message_id.clone().unwrap_or_default(),
        sender_id: record.sender_id.clone(),
        sender_name: String::new(),
        sender_avatar: String::new(),
        source: 1,
        conversation_seq: 0,
        created_at,
        conversation_type: record
            .conversation_type
            .as_deref()
            .map(|t| match t.to_ascii_lowercase().as_str() {
                "single" | "1" => 1,
                "group" | "2" => 2,
                "channel" | "3" => 3,
                _ => 0,
            })
            .unwrap_or(0),
        message_type: 0,
        message_seq: None,
        channel_id: String::new(),
        content: None,
        status: 1,
        retention_policy: None,
        retention_state: None,
        attributes: std::collections::HashMap::new(),
        extensions: std::collections::HashMap::new(),
        ..Default::default()
    };

    HookMessageRecord {
        message: Some(proto_message),
        persisted_at: Some(system_time_to_timestamp(record.persisted_at)),
        metadata: record.metadata.clone(),
        server_seq: 0,
        extension_bag: None,
    }
}

/// 将 DeliveryEvent 转换为 HookDeliveryEvent
pub fn delivery_event_to_proto(event: &DeliveryEvent) -> HookDeliveryEvent {
    HookDeliveryEvent {
        message_id: event.message_id.clone(),
        user_id: event.user_id.clone(),
        channel: event.channel.clone(),
        delivered_at: Some(system_time_to_timestamp(event.delivered_at)),
        metadata: event.metadata.clone(),
        device_id: String::new(),
        extension_bag: None,
    }
}

/// 将 RecallEvent 转换为 HookRecallEvent
pub fn recall_event_to_proto(event: &RecallEvent) -> HookRecallEvent {
    HookRecallEvent {
        message_id: event.message_id.clone(),
        operator_id: event.operator_id.clone(),
        recalled_at: Some(system_time_to_timestamp(event.recalled_at)),
        metadata: event.metadata.clone(),
        recall_scope: String::new(),
        extension_bag: None,
    }
}

/// 将 PreSendHookResponse 转换为 PreSendDecision
pub fn proto_to_pre_send_decision(
    response: &PreSendHookResponse,
    draft: &mut MessageDraft,
) -> PreSendDecision {
    if response.allow {
        // 如果允许发送，更新 draft（如果有修改）
        if let Some(ref updated_draft) = response.draft {
            *draft = proto_to_message_draft(updated_draft);
        }
        PreSendDecision::Continue
    } else {
        use flare_server_core::error::{ErrorBuilder, ErrorCode};
        let reason = if response.deny_reason_code.trim().is_empty() {
            "HOOK_REJECTED"
        } else {
            response.deny_reason_code.trim()
        };
        let message = if response.deny_reason_message.trim().is_empty() {
            "Hook rejected the request"
        } else {
            response.deny_reason_message.trim()
        };
        let error = ErrorBuilder::new(ErrorCode::PermissionDenied, reason)
            .details(message)
            .build_error();
        PreSendDecision::Reject { error }
    }
}

/// 将 RecallHookResponse 转换为 PreSendDecision
pub fn proto_to_recall_decision(response: &RecallHookResponse) -> PreSendDecision {
    if response.allow {
        PreSendDecision::Continue
    } else {
        use flare_server_core::error::ErrorCode;
        let error = flare_server_core::flare_err!(
            ErrorCode::PermissionDenied,
            "Hook rejected the recall request"
        );
        PreSendDecision::Reject { error }
    }
}

/// 将 SystemTime 转换为 Timestamp
pub fn system_time_to_timestamp(time: SystemTime) -> Timestamp {
    time.duration_since(UNIX_EPOCH)
        .map(|d| Timestamp {
            seconds: d.as_secs() as i64,
            nanos: d.subsec_nanos() as i32,
        })
        .unwrap_or_else(|_| Timestamp {
            seconds: 0,
            nanos: 0,
        })
}

/// 将 Timestamp 转换为 SystemTime
pub fn timestamp_to_system_time(ts: &Timestamp) -> SystemTime {
    UNIX_EPOCH
        + std::time::Duration::from_secs(ts.seconds as u64)
        + std::time::Duration::from_nanos(ts.nanos as u64)
}

/// 将 protobuf HookInvocationContext 转换为 flare_server_core::Context
pub fn proto_to_context(proto: &HookInvocationContext) -> Context {
    let request_id = if proto.request_id.is_empty() {
        uuid::Uuid::new_v4().to_string()
    } else {
        proto.request_id.clone()
    };

    let mut ctx = Context::with_request_id(request_id);

    if !proto.tenant_id.is_empty() {
        ctx = ctx.with_tenant_id(proto.tenant_id.clone());
    }
    if !proto.operator_user_id.is_empty() {
        ctx = ctx.with_user_id(proto.operator_user_id.clone());
    }

    // 设置会话ID（从 conversation_id 中提取）
    if !proto.conversation_id.is_empty() {
        ctx = ctx.with_session_id(proto.conversation_id.clone());
    }

    // 创建 HookContextData 并存储到 Context
    let hook_data = HookContextData {
        conversation_id: if proto.conversation_id.is_empty() {
            None
        } else {
            Some(proto.conversation_id.clone())
        },
        conversation_type: if proto.conversation_type.is_empty() {
            None
        } else {
            Some(proto.conversation_type.clone())
        },
        message_type: None,
        sender_id: None,
        tags: proto.tags.clone(),
        attributes: proto.attributes.clone(),
        request_metadata: std::collections::HashMap::new(),
        occurred_at: None,
    };

    set_hook_context_data(ctx, hook_data)
}
