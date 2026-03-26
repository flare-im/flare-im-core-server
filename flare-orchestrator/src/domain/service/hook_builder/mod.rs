use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use flare_im_core::hooks::{MessageDraft, MessageRecord};
use flare_im_core::hooks::hook_context_data::{set_hook_context_data, HookContextData};
use flare_server_core::context::{Context, Ctx};
use flare_server_core::mq::kafka::consumer::context_from_kafka_headers;
use flare_im_core::abstractions::storage_payload::{EXTRA_KEY_SYNC, EXTRA_KEY_TAGS};
use flare_proto::common::Message;
use prost::Message as _;
use serde_json::json;

use crate::domain::model::MessageSubmission;

#[allow(dead_code)]
fn tenant_id_from_opt(tenant: Option<&str>, default: Option<&String>) -> String {
    tenant
        .and_then(|s| non_empty(s.to_string()))
        .or_else(|| default.cloned())
        .unwrap_or_else(|| "0".to_string())
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

/// 从 Ctx + Message 构建 hook_context（Message 的 envelope 在 extra）
pub fn build_hook_context_from_ctx(ctx: &Ctx, request: &Message) -> Ctx {
    use flare_im_core::hooks::hook_context_data::{get_hook_context_data, set_hook_context_data};
    let mut hook_ctx = (**ctx).clone();
    if hook_ctx.request_id().is_empty() {
        if let Some(request_id) = request.extra.get("x-request-id").filter(|id| !id.is_empty()) {
            let mut new_ctx = Context::with_request_id(request_id.clone());
            if let Some(tenant_id) = ctx.tenant_id() {
                new_ctx = new_ctx.with_tenant_id(tenant_id.to_string());
            }
            if let Some(user_id) = ctx.user_id() {
                new_ctx = new_ctx.with_user_id(user_id.to_string());
            }
            hook_ctx = new_ctx;
        }
    }
    let mut hook_data = get_hook_context_data(&hook_ctx).cloned().unwrap_or_default();
    hook_data.conversation_id = non_empty(request.conversation_id.clone());
    hook_data.tags = request
        .extra
        .get(EXTRA_KEY_TAGS)
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default();
    if let Some(trace_id) = request.extra.get("x-trace-id").filter(|s| !s.is_empty()) {
        hook_ctx = hook_ctx.with_trace_id(trace_id.clone());
    }
    if let Some(user_id) = request.extra.get("x-user-id").filter(|s| !s.is_empty()) {
        hook_ctx = hook_ctx.with_user_id(user_id.clone());
    }
    hook_data.sender_id = non_empty(request.sender_id.clone());
    let conversation_type_str = match flare_proto::common::ConversationType::try_from(request.conversation_type) {
        Ok(flare_proto::common::ConversationType::Single) => "single".to_string(),
        Ok(flare_proto::common::ConversationType::Group) => "group".to_string(),
        Ok(flare_proto::common::ConversationType::Channel) => "channel".to_string(),
        _ => "unknown".to_string(),
    };
    hook_data.conversation_type = Some(conversation_type_str);
    let message_type_str = match request.message_type {
        1 => "text",
        2 => "image",
        3 => "video",
        4 => "audio",
        5 => "file",
        _ => "unknown",
    };
    hook_data.message_type = Some(message_type_str.to_string());
    Arc::new(set_hook_context_data(hook_ctx, hook_data))
}

/// 从 Message.extra 还原 Ctx（MQ 编解码）
pub fn build_hook_context(request: &Message, default_tenant: Option<&String>) -> Ctx {
    let mut ctx = context_from_kafka_headers(&request.extra);
    if ctx.tenant_id().is_none() || ctx.tenant_id().map(|s| s.is_empty()).unwrap_or(true) {
        let mut c = (*ctx).clone();
        if let Some(t) = default_tenant {
            c = c.with_tenant_id(t.as_str());
        } else {
            c = c.with_tenant_id("0");
        }
        ctx = Arc::new(c);
    }
    let mut hook_data = HookContextData::new();
    hook_data.conversation_id = non_empty(request.conversation_id.clone());
    hook_data.tags = request
        .extra
        .get(EXTRA_KEY_TAGS)
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default();
    for (k, v) in &request.extra {
        if k.as_str() != EXTRA_KEY_TAGS && !k.starts_with("x-") {
            hook_data.tags.insert(k.clone(), v.clone());
        }
    }
    if let Some(tenant_id_val) = request.extra.get("x-tenant-id") {
        hook_data.attributes.entry("tenant_id".into()).or_insert(tenant_id_val.clone());
    }

    if !request.content.is_empty() {
    hook_data.sender_id = non_empty(request.sender_id.clone());
    let conversation_type_str = match flare_proto::common::ConversationType::try_from(request.conversation_type) {
        Ok(flare_proto::common::ConversationType::Single) => "single".to_string(),
        Ok(flare_proto::common::ConversationType::Group) => "group".to_string(),
        Ok(flare_proto::common::ConversationType::Channel) => "channel".to_string(),
        _ => "unknown".to_string(),
    };
    hook_data.conversation_type = non_empty(conversation_type_str.clone());
    let message_type_label = detect_message_type(request);
    hook_data.message_type = Some(message_type_label.to_string());
    if let Some(conv_id) = &hook_data.conversation_id {
        let mut c = (*ctx).clone();
        c = c.with_session_id(conv_id.clone());
        ctx = Arc::new(c);
    }
    hook_data.attributes
        .entry("business_type".into())
        .or_insert(request.extra.get("business_type").cloned().unwrap_or_default());
    hook_data.attributes
        .entry("conversation_type".into())
        .or_insert(conversation_type_str.clone());
    if request.conversation_type == flare_proto::common::ConversationType::Single as i32
        && !request.channel_id.is_empty()
    {
        hook_data.attributes
            .entry("receiver_id".into())
            .or_insert(request.channel_id.clone());
    }
    if !request.conversation_id.is_empty() {
        hook_data.attributes
            .entry("conversation_id".into())
            .or_insert(request.conversation_id.clone());
    }
    if let Some(client_msg_id) = extract_client_message_id(request) {
        hook_data.attributes
            .entry("client_message_id".into())
            .or_insert(client_msg_id);
    }
    hook_data.attributes
        .entry("message_type_label".into())
        .or_insert(message_type_label.to_string());
    let sync = request.extra.get(EXTRA_KEY_SYNC).map(|s| s.as_str()) == Some("true");
    hook_data.attributes.entry("sync".into()).or_insert(sync.to_string());
    }

    let hook_data = hook_data.occurred_now();

    // 将 HookContextData 存储到 Context
    let mut c = (*ctx).clone();
    c = set_hook_context_data(c, hook_data);
    Arc::new(c)
}

pub fn build_draft_from_request(request: &Message) -> anyhow::Result<MessageDraft> {
    let content_bytes = request.content.clone();
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
        .get(EXTRA_KEY_TAGS)
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default();
    let mut metadata = request.extra.clone();
    metadata.remove(EXTRA_KEY_SYNC);
    metadata.remove(EXTRA_KEY_TAGS);
    metadata
        .entry("business_type".into())
        .or_insert(request.extra.get("business_type").cloned().unwrap_or_default());
    let conversation_type_str = match flare_proto::common::ConversationType::try_from(request.conversation_type) {
        Ok(flare_proto::common::ConversationType::Single) => "single".to_string(),
        Ok(flare_proto::common::ConversationType::Group) => "group".to_string(),
        Ok(flare_proto::common::ConversationType::Channel) => "channel".to_string(),
        _ => "unknown".to_string(),
    };
    metadata.entry("conversation_type".into()).or_insert(conversation_type_str);
    metadata.entry("message_type".into()).or_insert(message_type_label.to_string());
    // content_type 从 MessageContent 推断（Message.content 为 bytes）
    let decoded_content = if request.content.is_empty() {
        None
    } else {
        flare_proto::common::MessageContent::decode(request.content.as_slice()).ok()
    };
    let content_type_label = decoded_content
        .as_ref()
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
            Some(flare_proto::common::message_content::Content::LinkCard(_)) => "link_card",
            Some(flare_proto::common::message_content::Content::Thread(_)) => "thread",
            None | _ => "unspecified",
        })
        .unwrap_or("unspecified");
    metadata
        .entry("content_type".into())
        .or_insert(content_type_label.to_string());
    metadata
        .entry("sender_id".into())
        .or_insert(request.sender_id.clone());
    let receiver_list = if request.conversation_type == flare_proto::common::ConversationType::Single as i32
        && !request.channel_id.is_empty()
    {
        vec![request.channel_id.clone()]
    } else {
        Vec::new()
    };
    metadata
        .entry("receiver_id".into())
        .or_insert(receiver_list.first().cloned().unwrap_or_default());
    if !request.conversation_id.is_empty() {
        metadata
            .entry("conversation_id".into())
            .or_insert(request.conversation_id.clone());
    }
    draft.metadata = metadata;
    draft.extra("conversation_id", json!(request.conversation_id));
    let sync = request.extra.get(EXTRA_KEY_SYNC).map(|s| s.as_str()) == Some("true");
    draft.extra("sync", json!(sync));
    let request_id_value = request.extra.get("x-request-id").cloned().unwrap_or_default();
    let mut request_context_json = json!({ "request_id": request_id_value });
    if let Some(trace_id) = request.extra.get("x-trace-id").filter(|s| !s.is_empty()) {
        request_context_json["trace_id"] = json!(trace_id);
    }
    let mut attrs = serde_json::Map::new();
    for (k, v) in &request.extra {
        if k.as_str() != EXTRA_KEY_SYNC && k.as_str() != EXTRA_KEY_TAGS && !k.starts_with("x-") {
            attrs.insert(k.clone(), json!(v));
        }
    }
    if !attrs.is_empty() {
        request_context_json["attributes"] = json!(attrs);
    }
    draft.extra("request_context", request_context_json);
    if let Some(tenant_id) = request.extra.get("x-tenant-id") {
        draft.extra(
            "tenant_context",
            json!({
                "tenant_id": tenant_id,
                "business_type": request.extra.get("business_type").unwrap_or(&"".to_string()),
                "environment": request.extra.get("environment").unwrap_or(&"".to_string()),
                "attributes": { "labels": {}, "custom_attributes": request.extra.clone() },
            }),
        );
    }
    Ok(draft)
}

pub fn apply_draft_to_request(request: &mut Message, draft: &MessageDraft) {
    if let Some(conv) = draft.conversation_id.as_ref() {
        request.conversation_id = conv.clone();
    }
    if let Some(tags_json) = serde_json::to_string(&draft.headers).ok() {
        request.extra.insert(EXTRA_KEY_TAGS.to_string(), tags_json);
    }
    if let Some(message_id) = &draft.message_id {
        request.server_id = message_id.clone();
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
    let conversation_type_str = match flare_proto::common::ConversationType::try_from(request.conversation_type) {
        Ok(flare_proto::common::ConversationType::Single) => "single",
        Ok(flare_proto::common::ConversationType::Group) => "group",
        Ok(flare_proto::common::ConversationType::Channel) => "channel",
        _ => "unknown",
    };
    metadata.insert("conversation_type".into(), conversation_type_str.to_string());
    let decoded_msg_content = if !request.content.is_empty() {
        flare_proto::common::MessageContent::decode(request.content.as_slice()).ok()
    } else {
        None
    };
    let content_type = decoded_msg_content
        .as_ref()
        .map(|c| match &c.content {
            Some(flare_proto::common::message_content::Content::Text(_)) => "text/plain",
            Some(flare_proto::common::message_content::Content::Image(_)) => "image/*",
            Some(flare_proto::common::message_content::Content::Video(_)) => "video/*",
            Some(flare_proto::common::message_content::Content::Audio(_)) => "audio/*",
            Some(flare_proto::common::message_content::Content::File(_)) => {
                "application/octet-stream"
            }
            Some(flare_proto::common::message_content::Content::Location(_)) => "location",
            Some(flare_proto::common::message_content::Content::Card(_)) => "card",
            Some(flare_proto::common::message_content::Content::Notification(_)) => "notification",
            Some(flare_proto::common::message_content::Content::Custom(_)) => "application/custom",
            Some(flare_proto::common::message_content::Content::Forward(_)) => "forward",
            Some(flare_proto::common::message_content::Content::Thread(_)) => "thread",
            Some(flare_proto::common::message_content::Content::LinkCard(_)) => "link_card",
            None | _ => "application/unknown",
        })
        .unwrap_or("application/unknown");
    metadata.insert("content_type".into(), content_type.to_string());

    if let Some(client_msg_id) = extract_client_message_id(request) {
        metadata
            .entry("client_message_id".into())
            .or_insert(client_msg_id);
    }
    let tags: HashMap<String, String> = request
        .extra
        .get(EXTRA_KEY_TAGS)
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
        conversation_type: Some(conversation_type_str.to_string()),
        message_type: metadata.get("content_type").cloned(),
        persisted_at: SystemTime::now(),
        metadata,
    }
}

pub fn draft_from_submission(submission: &MessageSubmission) -> anyhow::Result<MessageDraft> {
    build_draft_from_request(&submission.kafka_payload)
}

pub fn merge_context(original: &Ctx, updated: Ctx) -> Ctx {
    use flare_im_core::hooks::hook_context_data::{get_hook_context_data, set_hook_context_data};
    
    let original_data = get_hook_context_data(original.as_ref()).cloned().unwrap_or_default();
    let mut updated_data = get_hook_context_data(updated.as_ref()).cloned().unwrap_or_default();
    
    // 合并 trace_id（如果 updated 没有）
    let mut merged_ctx = (*updated).clone();
    if merged_ctx.trace_id().is_empty() && !original.trace_id().is_empty() {
        merged_ctx = merged_ctx.with_trace_id(original.trace_id().to_string());
    }
    
    // 合并 tenant_id（如果 updated 没有）
    if merged_ctx.tenant_id().is_none() || merged_ctx.tenant_id().unwrap().is_empty() {
        if let Some(tenant_id) = original.tenant_id() {
            if !tenant_id.is_empty() {
                merged_ctx = merged_ctx.with_tenant_id(tenant_id.to_string());
            }
        }
    }
    
    // 合并 HookContextData
    if updated_data.sender_id.is_none() {
        updated_data.sender_id = original_data.sender_id.clone();
    }
    if updated_data.conversation_type.is_none() {
        updated_data.conversation_type = original_data.conversation_type.clone();
    }
    if updated_data.message_type.is_none() {
        updated_data.message_type = original_data.message_type.clone();
    }

    if updated_data.tags.is_empty() {
        updated_data.tags = original_data.tags.clone();
    }

    if updated_data.attributes.is_empty() {
        updated_data.attributes = original_data.attributes.clone();
    } else {
        for (key, value) in &original_data.attributes {
            updated_data
                .attributes
                .entry(key.clone())
                .or_insert(value.clone());
        }
    }

    if updated_data.request_metadata.is_empty() {
        updated_data.request_metadata = original_data.request_metadata.clone();
    }

    // 将合并后的 HookContextData 存储到 Context
    Arc::new(set_hook_context_data(merged_ctx, updated_data))
}

fn detect_message_type(message: &Message) -> &'static str {
    use flare_proto::common::MessageType;
    use std::convert::TryFrom;

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

    // 从 MessageType 枚举推断（支持所有消息类型）
    match MessageType::try_from(message.message_type) {
        // 基础消息类型
        Ok(MessageType::Text) => "text",
        Ok(MessageType::Image) => "image",
        Ok(MessageType::Video) => "video",
        Ok(MessageType::Audio) => "audio",
        Ok(MessageType::File) => "file",
        Ok(MessageType::Location) => "location",
        Ok(MessageType::Card) => "card",
        Ok(MessageType::Custom) => "custom",
        Ok(MessageType::Notification) => "notification",
        Ok(MessageType::MergeForward) => "forward",
        Ok(MessageType::LinkCard) => "link_card",
        Ok(MessageType::MiniProgram) => "mini_program",
        Ok(MessageType::Thread) => "thread",
        Ok(MessageType::Poll) => "vote",
        Ok(MessageType::Task) => "task",
        Ok(MessageType::Schedule) => "schedule",
        Ok(MessageType::Announcement) => "announcement",
        Ok(_) | Err(_) => {
            let decoded = if !message.content.is_empty() {
                flare_proto::common::MessageContent::decode(message.content.as_slice()).ok()
            } else {
                None
            };
            if let Some(content) = decoded.as_ref() {
                match &content.content {
                    Some(flare_proto::common::message_content::Content::Text(_)) => "text",
                    Some(flare_proto::common::message_content::Content::Image(_)) => "image",
                    Some(flare_proto::common::message_content::Content::Video(_)) => "video",
                    Some(flare_proto::common::message_content::Content::Audio(_)) => "audio",
                    Some(flare_proto::common::message_content::Content::File(_)) => "file",
                    Some(flare_proto::common::message_content::Content::Location(_)) => "location",
                    Some(flare_proto::common::message_content::Content::Card(_)) => "card",
                    Some(flare_proto::common::message_content::Content::Notification(_)) => {
                        "notification"
                    }
                    Some(flare_proto::common::message_content::Content::Custom(_)) => "custom",
                    Some(flare_proto::common::message_content::Content::Forward(_)) => "forward",
                    Some(flare_proto::common::message_content::Content::LinkCard(_)) => "link_card",
                    Some(flare_proto::common::message_content::Content::Thread(_)) => "thread",
                    None | _ => "unknown",
                }
            } else {
                "unknown"
            }
        }
    }
}

#[allow(dead_code)]
fn infer_from_content_type(raw: &str) -> &'static str {
    match raw.trim().to_lowercase().as_str() {
        "text/plain" | "text" | "plain_text" => "text",
        "markdown" | "text/markdown" | "rich_text" | "rich-text" => "rich_text",
        "image" | "image/png" | "image/jpeg" | "image/jpg" => "image",
        "video" | "video/mp4" | "video/mpeg" => "video",
        "audio" | "audio/aac" | "audio/mpeg" | "voice" => "audio",
        "file" | "application/octet-stream" | "application/pdf" | "application/zip" => "file",
        "sticker" | "emoji" | "gif" => "sticker",
        "location" | "geo" | "geolocation" => "location",
        "card" | "share_card" | "invite_card" => "card",
        "command" | "cmd" => "command",
        "event" => "event",
        "system" | "system_message" => "system",
        _ => "custom",
    }
}
