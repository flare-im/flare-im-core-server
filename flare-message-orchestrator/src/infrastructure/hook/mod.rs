use std::collections::HashMap;
use std::time::SystemTime;

use flare_im_core::hooks::{MessageDraft, MessageRecord};
use flare_im_core::hooks::hook_context_data::{HookContextData, set_hook_context_data};
use flare_server_core::context::Context;
use flare_proto::common::{RequestContext, TenantContext};
use flare_proto::common::Message;
use flare_proto::storage::StoreMessageRequest;
use flare_proto::MessageContentExt;
use serde_json::json;

use crate::domain::model::MessageSubmission;

fn tenant_id(tenant: &Option<TenantContext>, default: Option<&String>) -> String {
    tenant
        .as_ref()
        .and_then(|ctx| non_empty(ctx.tenant_id.clone()))
        .or_else(|| default.cloned())
        .unwrap_or_else(|| "default".to_string())
}

fn non_empty(value: String) -> Option<String> {
    if value.trim().is_empty() {
        None
    } else {
        Some(value)
    }
}

fn extract_client_message_id(message: &Message) -> Option<String> {
    message
        .extra
        .get("client_message_id")
        .cloned()
        .filter(|id| !id.is_empty())
}

pub fn build_hook_context(
    request: &StoreMessageRequest,
    default_tenant: Option<&String>,
) -> Context {
    // 使用 domain 层的实现
    crate::domain::service::hook_builder::build_hook_context(request, default_tenant)
}

pub fn build_draft_from_request(request: &StoreMessageRequest) -> anyhow::Result<MessageDraft> {
    let message = request
        .message
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("StoreMessageRequest.message must be set"))?;

    let content_bytes = message.content.as_ref()
        .and_then(|c| c.encode_to_bytes().ok())
        .unwrap_or_default();
    let mut draft = MessageDraft::new(content_bytes);
    let message_type_label = detect_message_type(message);

    if let Some(id) = non_empty(message.server_id.clone()) {
        draft.set_message_id(id);
    }

    if let Some(conv) = non_empty(request.conversation_id.clone()) {
        draft.set_conversation_id(conv);
    }

    draft.headers = request.tags.clone();

    let mut metadata = message.extra.clone();
    metadata
        .entry("business_type".into())
        .or_insert(message.business_type.clone());
    metadata
        .entry("conversation_type".into())
        .or_insert(message.conversation_type.clone());
    metadata
        .entry("message_type".into())
        .or_insert(message_type_label.to_string());
    // content_type 从 MessageContent 推断
    let content_type_label = message.content.as_ref()
        .map(|c| match &c.content {
            Some(flare_proto::common::message_content::Content::Text(_)) => "text",
            Some(flare_proto::common::message_content::Content::Image(_)) => "image",
            Some(flare_proto::common::message_content::Content::Video(_)) => "video",
            Some(flare_proto::common::message_content::Content::Audio(_)) => "audio",
            Some(flare_proto::common::message_content::Content::File(_)) => "file",
            Some(flare_proto::common::message_content::Content::Location(_)) => "location",
            Some(flare_proto::common::message_content::Content::Card(_)) => "card",
            Some(flare_proto::common::message_content::Content::Notification(_)) => "notification",
            Some(flare_proto::common::message_content::Content::Custom(_)) => "custom",
            Some(flare_proto::common::message_content::Content::Forward(_)) => "forward",
            Some(flare_proto::common::message_content::Content::Typing(_)) => "typing",
            Some(flare_proto::common::message_content::Content::Vote(_)) => "vote",
            Some(flare_proto::common::message_content::Content::Task(_)) => "task",
            Some(flare_proto::common::message_content::Content::Schedule(_)) => "schedule",
            Some(flare_proto::common::message_content::Content::Announcement(_)) => "announcement",
            Some(flare_proto::common::message_content::Content::SystemEvent(_)) => "system_event",
            None => "unspecified",
        })
        .unwrap_or("unspecified");
    metadata
        .entry("content_type".into())
        .or_insert(content_type_label.to_string());
    metadata
        .entry("sender_id".into())
        .or_insert(message.sender_id.clone());
    metadata
        .entry("receiver_id".into())
        .or_insert(message.receiver_id.clone());
    draft.metadata = metadata;

    draft.extra("conversation_id", json!(request.conversation_id));
    draft.extra("sync", json!(request.sync));

    // request.context 是 RequestContext，我们需要从中提取信息构建 JSON
    if let Some(request_ctx) = request.context.as_ref() {
        let mut request_context_json = json!({
            "request_id": request_ctx.request_id,
        });
        
        if let Some(trace_ctx) = request_ctx.trace.as_ref() {
            if !trace_ctx.trace_id.is_empty() {
                request_context_json["trace_id"] = json!(trace_ctx.trace_id);
            }
            if !trace_ctx.span_id.is_empty() {
                request_context_json["span_id"] = json!(trace_ctx.span_id);
            }
        }
        
        // 将 attributes 添加到 JSON
        if !request_ctx.attributes.is_empty() {
            request_context_json["attributes"] = json!(request_ctx.attributes);
        }
        
        draft.extra("request_context", request_context_json);
    }

    if let Some(tenant) = request.tenant.as_ref() {
        draft.extra(
            "tenant_context",
            json!({
                "tenant_id": tenant.tenant_id,
                "business_type": tenant.business_type,
                "environment": tenant.environment,
                "attributes": tenant.attributes,
            }),
        );
    }

    Ok(draft)
}

pub fn apply_draft_to_request(request: &mut StoreMessageRequest, draft: &MessageDraft) {
    if let Some(conv) = draft.conversation_id.as_ref() {
        request.conversation_id = conv.clone();
    }

    request.tags = draft.headers.clone();

    if let Some(message) = request.message.as_mut() {
        if let Some(id) = draft.message_id.as_ref() {
            message.server_id = id.clone();
        }

        if let Some(conv) = draft.conversation_id.as_ref() {
            message.conversation_id = conv.clone();
        }

        // message.content 是 MessageContent，需要根据 draft.payload 构建
        // message.extra 用于存储扩展信息
        message.extra = draft.metadata.clone();
        // message_type 从 extra 中的 message_type 获取，如果没有则使用默认值
        if let Some(label) = message.extra.get("message_type") {
            use flare_proto::common::MessageType;
            message.message_type = match label.as_str() {
                "text" => MessageType::Text as i32,
                "image" => MessageType::Image as i32,
                "video" => MessageType::Video as i32,
                "audio" => MessageType::Audio as i32,
                "file" => MessageType::File as i32,
                "location" => MessageType::Location as i32,
                "card" => MessageType::Card as i32,
                "notification" => MessageType::Notification as i32,
                "typing" => MessageType::Typing as i32,
                "recall" | "read" | "operation" => MessageType::Operation as i32, // 统一操作类型
                "forward" => MessageType::MergeForward as i32,
                "vote" => MessageType::Vote as i32,
                "task" => MessageType::Task as i32,
                "schedule" => MessageType::Schedule as i32,
                "announcement" => MessageType::Announcement as i32,
                "custom" => MessageType::Custom as i32,
                _ => MessageType::Unspecified as i32,
            };
        }

        for (key, value) in &draft.extra {
            if let Ok(serialized) = serde_json::to_string(value) {
                message.extra.insert(key.clone(), serialized);
            }
        }
    }
}

pub fn build_message_record(
    submission: &MessageSubmission,
    request: &StoreMessageRequest,
) -> MessageRecord {
    let message = &submission.message;
    let mut metadata: HashMap<String, String> = message.extra.clone();

    metadata.insert("business_type".into(), message.business_type.clone());
    metadata.insert("conversation_type".into(), message.conversation_type.clone());
    // 推断 content_type
    let content_type = message.content.as_ref()
        .map(|c| match &c.content {
            Some(flare_proto::common::message_content::Content::Text(_)) => "text/plain",
            Some(flare_proto::common::message_content::Content::Image(_)) => "image/*",
            Some(flare_proto::common::message_content::Content::Video(_)) => "video/*",
            Some(flare_proto::common::message_content::Content::Audio(_)) => "audio/*",
            Some(flare_proto::common::message_content::Content::File(_)) => "application/octet-stream",
            Some(flare_proto::common::message_content::Content::Location(_)) => "location",
            Some(flare_proto::common::message_content::Content::Card(_)) => "card",
            Some(flare_proto::common::message_content::Content::Notification(_)) => "notification",
            Some(flare_proto::common::message_content::Content::Custom(_)) => "application/custom",
            Some(flare_proto::common::message_content::Content::Forward(_)) => "forward",
            Some(flare_proto::common::message_content::Content::Typing(_)) => "typing",
            Some(flare_proto::common::message_content::Content::Vote(_)) => "vote",
            Some(flare_proto::common::message_content::Content::Task(_)) => "task",
            Some(flare_proto::common::message_content::Content::Schedule(_)) => "schedule",
            Some(flare_proto::common::message_content::Content::Announcement(_)) => "announcement",
            Some(flare_proto::common::message_content::Content::SystemEvent(_)) => "system_event",
            None => "application/unknown",
        })
        .unwrap_or("application/unknown");
    metadata.insert("content_type".into(), content_type.to_string());

    if let Some(message) = submission.kafka_payload.message.as_ref() {
        if let Some(client_msg_id) = extract_client_message_id(message) {
            metadata
                .entry("client_message_id".into())
                .or_insert(client_msg_id);
        }
    }

    for (key, value) in &request.tags {
        metadata.insert(format!("tag::{}", key), value.clone());
    }

    MessageRecord {
        message_id: message.server_id.clone(),
        client_message_id: Some(message.client_msg_id.clone()),
        conversation_id: message.conversation_id.clone(),
        sender_id: message.sender_id.clone(),
        conversation_type: Some(message.conversation_type.clone()),
        message_type: metadata.get("content_type").cloned(),
        persisted_at: SystemTime::now(),
        metadata,
    }
}

pub fn draft_from_submission(submission: &MessageSubmission) -> anyhow::Result<MessageDraft> {
    build_draft_from_request(&submission.kafka_payload)
}

pub fn merge_context(original: &Context, updated: Context) -> Context {
    // 使用 domain 层的实现
    crate::domain::service::hook_builder::merge_context(original, updated)
}

fn detect_message_type(message: &Message) -> &'static str {
    use std::convert::TryFrom;
    use flare_proto::common::MessageType;
    
    // 优先从 extra 中获取 message_type 标签
    if let Some(label) = message.extra.get("message_type") {
        return match label.as_str() {
            "text" | "text/plain" => "text",
            "binary" => "binary",
            "json" => "json",
            "image" => "image",
            "video" => "video",
            "audio" => "audio",
            "file" => "file",
            "sticker" => "sticker",
            "location" => "location",
            "card" => "card",
            "command" => "command",
            "event" => "event",
            "system" => "system",
            _ => "custom",
        };
    }
    
    // 从 MessageType 枚举推断（支持所有 22 种消息类型）
    match MessageType::try_from(message.message_type) {
        // 基础消息类型（9种）
        Ok(MessageType::Text) => "text",
        Ok(MessageType::Image) => "image",
        Ok(MessageType::Video) => "video",
        Ok(MessageType::Audio) => "audio",
        Ok(MessageType::File) => "file",
        Ok(MessageType::Location) => "location",
        Ok(MessageType::Card) => "card",
        Ok(MessageType::Custom) => "custom",
        Ok(MessageType::Notification) => "notification",
        // 功能消息类型（8种）
        Ok(MessageType::Typing) => "typing",
        Ok(MessageType::Operation) => "operation", // 统一操作类型（包含 recall/read/edit 等）
        Ok(MessageType::Forward) => "forward",
        Ok(MessageType::Vote) => "vote",
        Ok(MessageType::Task) => "task",
        Ok(MessageType::Schedule) => "schedule",
        Ok(MessageType::Announcement) => "announcement",
        // 扩展消息类型（5种）
        Ok(MessageType::MiniProgram) => "mini_program",
        Ok(MessageType::LinkCard) => "link_card",
        Ok(MessageType::Quote) => "quote",
        Ok(MessageType::Thread) => "thread",
        Ok(MessageType::MergeForward) => "merge_forward",
        Ok(MessageType::Unspecified) | Err(_) => {
            // 从 MessageContent 推断类型
            if let Some(content) = message.content.as_ref() {
                match &content.content {
                    Some(flare_proto::common::message_content::Content::Text(_)) => "text",
                    Some(flare_proto::common::message_content::Content::Image(_)) => "image",
                    Some(flare_proto::common::message_content::Content::Video(_)) => "video",
                    Some(flare_proto::common::message_content::Content::Audio(_)) => "audio",
                    Some(flare_proto::common::message_content::Content::File(_)) => "file",
                    Some(flare_proto::common::message_content::Content::Location(_)) => "location",
                    Some(flare_proto::common::message_content::Content::Card(_)) => "card",
                    Some(flare_proto::common::message_content::Content::Notification(_)) => "notification",
                    Some(flare_proto::common::message_content::Content::Custom(_)) => "custom",
                    Some(flare_proto::common::message_content::Content::Forward(_)) => "forward",
                    Some(flare_proto::common::message_content::Content::Typing(_)) => "typing",
                    Some(flare_proto::common::message_content::Content::Vote(_)) => "vote",
                    Some(flare_proto::common::message_content::Content::Task(_)) => "task",
                    Some(flare_proto::common::message_content::Content::Schedule(_)) => "schedule",
                    Some(flare_proto::common::message_content::Content::Announcement(_)) => "announcement",
                    Some(flare_proto::common::message_content::Content::SystemEvent(_)) => "system_event",
                    Some(flare_proto::common::message_content::Content::Quote(_)) => "quote",
                    Some(flare_proto::common::message_content::Content::LinkCard(_)) => "link_card",
                    None => "unknown",
                }
            } else {
                "unknown"
            }
        }
        _ => "custom",
    }
}

#[allow(dead_code)]
fn infer_from_content_type(raw: &str) -> &'static str {
    match raw.trim().to_lowercase().as_str() {
        // 基础消息类型
        "text/plain" | "text" | "plain_text" => "text",
        "markdown" | "text/markdown" | "rich_text" | "rich-text" => "rich_text",
        "image" | "image/png" | "image/jpeg" | "image/jpg" => "image",
        "video" | "video/mp4" | "video/mpeg" => "video",
        "audio" | "audio/aac" | "audio/mpeg" | "voice" => "audio",
        "file" | "application/octet-stream" | "application/pdf" | "application/zip" => "file",
        "location" | "geo" | "geolocation" => "location",
        "card" | "share_card" | "invite_card" => "card",
        "notification" | "system_notification" => "notification",
        // 功能消息类型
        "typing" => "typing",
        "recall" => "recall",
        "read" => "read",
        "forward" => "forward",
        "vote" => "vote",
        "task" => "task",
        "schedule" => "schedule",
        "announcement" => "announcement",
        // 扩展消息类型
        "mini_program" | "miniprogram" => "mini_program",
        "link_card" | "linkcard" => "link_card",
        "quote" => "quote",
        "thread" => "thread",
        "merge_forward" | "mergeforward" => "merge_forward",
        // 其他
        "sticker" | "emoji" | "gif" => "sticker",
        "command" | "cmd" => "command",
        "event" => "event",
        "system" | "system_message" => "system",
        _ => "custom",
    }
}
