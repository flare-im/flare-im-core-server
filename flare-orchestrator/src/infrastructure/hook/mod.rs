use std::collections::HashMap;
use std::time::SystemTime;

use flare_im_core::hooks::{MessageDraft, MessageRecord};
use flare_im_core::hooks::hook_context_data::{HookContextData, set_hook_context_data};
use flare_server_core::context::Ctx;
use flare_proto::common::Message;
use flare_proto::MessageContentExt;
use serde_json::json;

use crate::domain::model::MessageSubmission;

fn tenant_id_str(tenant: Option<&str>, default: Option<&String>) -> String {
    tenant
        .and_then(|s| if s.trim().is_empty() { None } else { Some(s) })
        .or_else(|| default.as_deref())
        .unwrap_or("default")
        .to_string()
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

pub fn build_hook_context(request: &Message, default_tenant: Option<&String>) -> Ctx {
    crate::domain::service::hook_builder::build_hook_context(request, default_tenant)
}

pub fn build_draft_from_request(request: &Message) -> anyhow::Result<MessageDraft> {
    let content_bytes = request.content.as_ref()
        .and_then(|c| c.encode_to_bytes().ok())
        .unwrap_or_default();
    let mut draft = MessageDraft::new(content_bytes);
    let message_type_label = detect_message_type(request);
    if let Some(id) = non_empty(request.server_id.clone()) {
        draft.set_message_id(id);
    }
    if let Some(conv) = non_empty(request.conversation_id.clone()) {
        draft.set_conversation_id(conv);
    }
    draft.headers = request
        .extra
        .get(flare_im_core::abstractions::storage_payload::EXTRA_KEY_TAGS)
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default();
    let mut metadata = request.extra.clone();
    metadata.remove(flare_im_core::abstractions::storage_payload::EXTRA_KEY_SYNC);
    metadata.remove(flare_im_core::abstractions::storage_payload::EXTRA_KEY_TAGS);
    metadata
        .entry("business_type".into())
        .or_insert(request.extra.get("business_type").cloned().unwrap_or_default());
    metadata
        .entry("conversation_type".into())
        .or_insert(request.conversation_type.to_string());
    metadata
        .entry("message_type".into())
        .or_insert(message_type_label.to_string());
    let content_type_label = request.content.as_ref()
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
        .or_insert(request.sender_id.clone());
    metadata
        .entry("receiver_id".into())
        .or_insert(request.channel_id.clone());
    draft.metadata = metadata;
    draft.extra("conversation_id", json!(request.conversation_id));
    let sync = request.extra.get(flare_im_core::abstractions::storage_payload::EXTRA_KEY_SYNC).map(|s| s.as_str()) == Some("true");
    draft.extra("sync", json!(sync));
    if let Some(rid) = request.extra.get("x-request-id") {
        draft.extra("request_context", json!({ "request_id": rid }));
    }
    if let Some(tid) = request.extra.get("x-tenant-id") {
        draft.extra("tenant_context", json!({ "tenant_id": tid }));
    }
    Ok(draft)
}

pub fn apply_draft_to_request(request: &mut Message, draft: &MessageDraft) {
    if let Some(conv) = draft.conversation_id.as_ref() {
        request.conversation_id = conv.clone();
    }
    if let Ok(tags_json) = serde_json::to_string(&draft.headers) {
        request.extra.insert(flare_im_core::abstractions::storage_payload::EXTRA_KEY_TAGS.to_string(), tags_json);
    }
    if let Some(id) = draft.message_id.as_ref() {
        request.server_id = id.clone();
    }
    request.extra = draft.metadata.clone();
    if let Some(label) = request.extra.get("message_type") {
        use flare_proto::common::MessageType;
        request.message_type = match label.as_str() {
            "text" => MessageType::Text as i32,
            "image" => MessageType::Image as i32,
            "video" => MessageType::Video as i32,
            "audio" => MessageType::Audio as i32,
            "file" => MessageType::File as i32,
            "location" => MessageType::Location as i32,
            "card" => MessageType::Card as i32,
            "notification" => MessageType::Notification as i32,
            "typing" => MessageType::Typing as i32,
            "recall" | "read" | "operation" => MessageType::Operation as i32,
            "forward" => MessageType::MergeForward as i32,
            "vote" => MessageType::Poll as i32,
            "task" => MessageType::Task as i32,
            "schedule" => MessageType::Schedule as i32,
            "announcement" => MessageType::Announcement as i32,
            "custom" => MessageType::Custom as i32,
            _ => MessageType::Unspecified as i32,
        };
    }
    for (key, value) in &draft.extra {
        if let Ok(serialized) = serde_json::to_string(value) {
            request.extra.insert(key.clone(), serialized);
        }
    }
}

pub fn build_message_record(
    _submission: &MessageSubmission,
    request: &Message,
) -> MessageRecord {
    let mut metadata: HashMap<String, String> = request.extra.clone();
    metadata.insert("business_type".into(), request.extra.get("business_type").cloned().unwrap_or_default());
    metadata.insert("conversation_type".into(), request.conversation_type.to_string());
    let content_type = request.content.as_ref()
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
    if let Some(client_msg_id) = extract_client_message_id(request) {
        metadata.entry("client_message_id".into()).or_insert(client_msg_id);
    }
    let tags: HashMap<String, String> = request
        .extra
        .get(flare_im_core::abstractions::storage_payload::EXTRA_KEY_TAGS)
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default();
    for (key, value) in &tags {
        metadata.insert(format!("tag::{}", key), value.clone());
    }
    MessageRecord {
        message_id: request.server_id.clone(),
        client_message_id: Some(request.client_msg_id.clone()),
        conversation_id: request.conversation_id.clone(),
        sender_id: request.sender_id.clone(),
        conversation_type: Some(
            match flare_proto::common::ConversationType::try_from(request.conversation_type) {
                Ok(flare_proto::common::ConversationType::Single) => "single".to_string(),
                Ok(flare_proto::common::ConversationType::Group) => "group".to_string(),
                Ok(flare_proto::common::ConversationType::Channel) => "channel".to_string(),
                _ => "unknown".to_string(),
            },
        ),
        message_type: metadata.get("content_type").cloned(),
        persisted_at: SystemTime::now(),
        metadata,
    }
}

pub fn draft_from_submission(submission: &MessageSubmission) -> anyhow::Result<MessageDraft> {
    build_draft_from_request(&submission.kafka_payload)
}

pub fn merge_context(original: &Ctx, updated: Ctx) -> Ctx {
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
        Ok(MessageType::Poll) => "vote",
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
