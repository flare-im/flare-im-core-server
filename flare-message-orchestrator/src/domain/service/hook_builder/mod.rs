use std::collections::HashMap;
use std::time::SystemTime;

use flare_im_core::hooks::{MessageDraft, MessageRecord};
use flare_im_core::hooks::hook_context_data::{set_hook_context_data, HookContextData};
use flare_server_core::context::{Context, ContextExt};
use flare_proto::common::Message;
use flare_proto::common::{RequestContext, TenantContext};
use flare_proto::storage::StoreMessage;
use flare_proto::MessageContentExt;
use serde_json::json;

use crate::domain::model::MessageSubmission;

fn tenant_id(tenant: &Option<TenantContext>, default: Option<&String>) -> String {
    tenant
        .as_ref()
        .and_then(|ctx| non_empty(ctx.tenant_id.clone()))
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

/// 从Context构建hook_context（统一入口）
pub fn build_hook_context_from_ctx(
    ctx: &Context,
    request: &StoreMessage,
) -> Context {
    use flare_im_core::hooks::hook_context_data::{get_hook_context_data, set_hook_context_data};
    
    let mut hook_ctx = ctx.clone();
    
    // 从request中提取request_id（如果Context中没有）
    if hook_ctx.request_id().is_empty() {
        // 从 metadata 中查找 request_id
        if let Some(request_id) = request
            .metadata
            .get("request_id")
            .filter(|id| !id.is_empty())
        {
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
    
    // 创建或更新 HookContextData
    let mut hook_data = get_hook_context_data(&hook_ctx).cloned().unwrap_or_default();
    hook_data.conversation_id = non_empty(request.conversation_id.clone());
    hook_data.tags = request.tags.clone();

    // 从 metadata 中获取 trace 和 actor 信息
    if let Some(trace_id) = request.metadata.get("trace_id") {
        if !trace_id.is_empty() {
            hook_ctx = hook_ctx.with_trace_id(trace_id.clone());
        }
    }

    if let Some(user_id) = request.metadata.get("user_id") {
        if !user_id.is_empty() {
            hook_ctx = hook_ctx.with_user_id(user_id.clone());
        }
    }

    if let Some(message) = request.message.as_ref() {
        hook_data.sender_id = non_empty(message.sender_id.clone());
        let conversation_type_str =
            match flare_proto::common::ConversationType::try_from(message.conversation_type) {
                Ok(flare_proto::common::ConversationType::Single) => "single".to_string(),
                Ok(flare_proto::common::ConversationType::Group) => "group".to_string(),
                Ok(flare_proto::common::ConversationType::Channel) => "channel".to_string(),
                _ => "unknown".to_string(),
            };
        hook_data.conversation_type = Some(conversation_type_str);
        
        let message_type_str = match message.message_type {
            1 => "text",
            2 => "image",
            3 => "video",
            4 => "audio",
            5 => "file",
            _ => "unknown",
        };
        hook_data.message_type = Some(message_type_str.to_string());
    }

    set_hook_context_data(hook_ctx, hook_data)
}

/// 从request构建hook_context（向后兼容）
pub fn build_hook_context(
    request: &StoreMessage,
    default_tenant: Option<&String>,
) -> Context {
    // 从 metadata 获取租户ID
    let tenant_id_str = request
        .metadata
        .get("tenant_id")
        .cloned()
        .or_else(|| default_tenant.cloned())
        .unwrap_or_else(|| "0".to_string());
    
    // 创建 Context
    let request_id = request
        .metadata
        .get("request_id")
        .cloned()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    
    let request_id_clone = request_id.clone();
    let mut ctx = Context::with_request_id(request_id);
    
    // 设置租户ID（确保总是有值，即使为空字符串也使用默认值）
    let final_tenant_id = if tenant_id_str.is_empty() {
        default_tenant.cloned().unwrap_or_else(|| "0".to_string())
    } else {
        tenant_id_str
    };
    ctx = ctx.with_tenant_id(final_tenant_id);
    
    // 创建 HookContextData
    let mut hook_data = HookContextData::new();
    hook_data.conversation_id = non_empty(request.conversation_id.clone());
    hook_data.tags = request.tags.clone();

    // 从 metadata 获取额外信息
    if let Some(trace_id) = request.metadata.get("trace_id") {
        if !trace_id.is_empty() {
            ctx = ctx.with_trace_id(trace_id.clone());
        }
    }

    // 将 metadata 中的属性添加到 hook_data.tags
    for (k, v) in &request.metadata {
        if k != "tenant_id" && k != "request_id" && k != "trace_id" {
            hook_data.tags.insert(k.clone(), v.clone());
        }
    }
    
    // 设置用户ID（从 metadata 中提取）
    if let Some(user_id) = request.metadata.get("user_id") {
        if user_id != "0" && !user_id.is_empty() {
            ctx = ctx.with_user_id(user_id.clone());
        }
    }

    // 从 metadata 获取租户相关信息
    if let Some(tenant_id_val) = request.metadata.get("tenant_id") {
        hook_data.attributes
            .entry("tenant_id".into())
            .or_insert(tenant_id_val.clone());
    }

    if let Some(message) = request.message.as_ref() {
        let message_type_label = detect_message_type(message);
        hook_data.sender_id = non_empty(message.sender_id.clone());
        // conversation_type 是 i32 枚举，转换为字符串
        let conversation_type_str =
            match flare_proto::common::ConversationType::try_from(message.conversation_type) {
                Ok(flare_proto::common::ConversationType::Single) => "single".to_string(),
                Ok(flare_proto::common::ConversationType::Group) => "group".to_string(),
                Ok(flare_proto::common::ConversationType::Channel) => "channel".to_string(),
                _ => "unknown".to_string(),
            };
        hook_data.conversation_type = non_empty(conversation_type_str.clone());
        hook_data.message_type = Some(message_type_label.to_string());
        
        // 设置会话ID
        if let Some(conv_id) = &hook_data.conversation_id {
            ctx = ctx.with_session_id(conv_id.clone());
        }

        hook_data.attributes
            .entry("business_type".into())
            .or_insert(message.business_type.clone());
        hook_data.attributes
            .entry("conversation_type".into())
            .or_insert(conversation_type_str.clone());
        
        // 提取接收者信息（优先使用 receiver_id 和 channel_id）
        // 单聊：使用 receiver_id
        if message.conversation_type == flare_proto::common::ConversationType::Single as i32 {
            if !message.receiver_id.is_empty() {
                hook_data.attributes
                    .entry("receiver_id".into())
                    .or_insert(message.receiver_id.clone());
            }
        }
        // 群聊/频道：使用 channel_id
        if !message.channel_id.is_empty() {
            hook_data.attributes
                .entry("channel_id".into())
                .or_insert(message.channel_id.clone());
        }
        if let Some(client_msg_id) = extract_client_message_id(message) {
            hook_data.attributes
                .entry("client_message_id".into())
                .or_insert(client_msg_id);
        }
        hook_data.attributes
            .entry("message_type_label".into())
            .or_insert(message_type_label.to_string());
    }

    hook_data.attributes
        .entry("sync".into())
        .or_insert(request.sync.to_string());
    
    let hook_data = hook_data.occurred_now();

    // 将 HookContextData 存储到 Context
    ctx = set_hook_context_data(ctx, hook_data);

    ctx
}

pub fn build_draft_from_request(request: &StoreMessage) -> anyhow::Result<MessageDraft> {
    let message = request
        .message
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("StoreMessageRequest.message must be set"))?;

    let content_bytes = message
        .content
        .as_ref()
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

    // conversation_type 是 i32 枚举，需要转换为字符串
    let conversation_type_str = match flare_proto::common::ConversationType::try_from(message.conversation_type) {
        Ok(flare_proto::common::ConversationType::Single) => "single".to_string(),
        Ok(flare_proto::common::ConversationType::Group) => "group".to_string(),
        Ok(flare_proto::common::ConversationType::Channel) => "channel".to_string(),
        _ => "unknown".to_string(),
    };
    metadata
        .entry("conversation_type".into())
        .or_insert(conversation_type_str);
    metadata
        .entry("message_type".into())
        .or_insert(message_type_label.to_string());
    // content_type 从 MessageContent 推断
    let content_type_label = message
        .content
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
            Some(flare_proto::common::message_content::Content::Typing(_)) => "typing",
            // 富交互类型（注意：Vote、Task、Schedule、Announcement 在新版 protobuf 中已移除）
            Some(flare_proto::common::message_content::Content::SystemEvent(_)) => "system_event",
            // Quote 已废弃：现在通过 Message.quote 字段处理
            Some(flare_proto::common::message_content::Content::LinkCard(_)) => "link_card",
            Some(flare_proto::common::message_content::Content::Thread(_)) => "thread",
            Some(flare_proto::common::message_content::Content::Operation(_)) => "operation",
            None => "unspecified",
        })
        .unwrap_or("unspecified");
    metadata
        .entry("content_type".into())
        .or_insert(content_type_label.to_string());
    metadata
        .entry("sender_id".into())
        .or_insert(message.sender_id.clone());

    // 提取接收者信息（优先使用 receiver_id 和 channel_id）
    // 单聊：使用 receiver_id
    let receiver_list = if message.conversation_type == flare_proto::common::ConversationType::Single as i32 {
        if !message.receiver_id.is_empty() {
            vec![message.receiver_id.clone()]
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    metadata
        .entry("receiver_id".into())
        .or_insert(receiver_list.first().cloned().unwrap_or_default());

    // 群聊/频道：使用 channel_id
    if !message.channel_id.is_empty() {
        metadata
            .entry("channel_id".into())
            .or_insert(message.channel_id.clone());
    }

    draft.metadata = metadata;

    draft.extra("conversation_id", json!(request.conversation_id));
    draft.extra("sync", json!(request.sync));
    if !message.channel_id.is_empty() {
        draft.extra("channel_id", json!(message.channel_id));
    }

    // 从 metadata 中提取请求上下文信息构建 JSON
    let request_id_value = request.metadata.get("request_id").cloned().unwrap_or_default();
    let mut request_context_json = json!({
        "request_id": request_id_value,
    });

    if let Some(trace_id) = request.metadata.get("trace_id") {
        if !trace_id.is_empty() {
            request_context_json["trace_id"] = json!(trace_id);
        }
    }
    
    // 将 metadata 中的其他属性添加到 JSON
    let mut attrs = serde_json::Map::new();
    for (k, v) in &request.metadata {
        if k != "request_id" && k != "trace_id" && k != "tenant_id" && k != "user_id" {
            attrs.insert(k.clone(), json!(v));
        }
    }
    if !attrs.is_empty() {
        request_context_json["attributes"] = json!(attrs);
    }

    draft.extra("request_context", request_context_json);

    // 从 metadata 中提取租户上下文信息
    if let Some(tenant_id) = request.metadata.get("tenant_id") {
        draft.extra(
            "tenant_context",
            json!({
                "tenant_id": tenant_id,
                "business_type": request.metadata.get("business_type").unwrap_or(&"".to_string()),
                "environment": request.metadata.get("environment").unwrap_or(&"".to_string()),
                "attributes": {
                    "labels": {},
                    "custom_attributes": request.metadata.clone(),
                },
            }),
        );
    }

    Ok(draft)
}

pub fn apply_draft_to_request(request: &mut StoreMessage, draft: &MessageDraft) {
    if let Some(conv) = draft.conversation_id.as_ref() {
        request.conversation_id = conv.clone();
    }

    request.tags = draft.headers.clone();

    // 将 draft 的信息添加到 request.metadata
    if let Some(message_id) = &draft.message_id {
        if let Some(message) = request.message.as_mut() {
            message.server_id = message_id.clone();
        }
    }

    if let Some(message) = request.message.as_mut() {
        if let Some(conv) = draft.conversation_id.as_ref() {
            message.conversation_id = conv.clone();
        }

        // message.content 是 MessageContent，需要根据 draft.payload 构建
        // message.extra 用于存储扩展信息
        message.extra = draft.metadata.clone();
        // message_type 从 extra 中的 message_type 获取，如果没有则使用默认值
        if let Some(label) = message.extra.get("message_type") {
            // 根据 label 设置 message_type 枚举
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
    request: &StoreMessage,
) -> MessageRecord {
    let message = &submission.message;
    let mut metadata: HashMap<String, String> = message.extra.clone();

    metadata.insert("business_type".into(), message.business_type.clone());
    // conversation_type 是 i32 枚举，转换为字符串
    let conversation_type_str = match flare_proto::common::ConversationType::try_from(message.conversation_type) {
        Ok(flare_proto::common::ConversationType::Single) => "single",
        Ok(flare_proto::common::ConversationType::Group) => "group",
        Ok(flare_proto::common::ConversationType::Channel) => "channel",
        _ => "unknown",
    };
    metadata.insert("conversation_type".into(), conversation_type_str.to_string());
    // 推断 content_type
    let content_type = message
        .content
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
            Some(flare_proto::common::message_content::Content::Typing(_)) => "typing",
            Some(flare_proto::common::message_content::Content::Thread(_)) => "thread",
            
            Some(flare_proto::common::message_content::Content::SystemEvent(_)) => "system_event",
            // Quote 已废弃：现在通过 Message.quote 字段处理
            Some(flare_proto::common::message_content::Content::LinkCard(_)) => "link_card",
            Some(flare_proto::common::message_content::Content::Operation(_)) => "operation",
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
        conversation_type: Some(conversation_type_str.to_string()),
        message_type: metadata.get("content_type").cloned(),
        persisted_at: SystemTime::now(),
        metadata,
    }
}

pub fn draft_from_submission(submission: &MessageSubmission) -> anyhow::Result<MessageDraft> {
    build_draft_from_request(&submission.kafka_payload)
}

pub fn merge_context(original: &Context, updated: Context) -> Context {
    use flare_im_core::hooks::hook_context_data::{get_hook_context_data, set_hook_context_data};
    
    let original_data = get_hook_context_data(original).cloned().unwrap_or_default();
    let mut updated_data = get_hook_context_data(&updated).cloned().unwrap_or_default();
    
    // 合并 trace_id（如果 updated 没有）
    let mut merged_ctx = updated;
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
    set_hook_context_data(merged_ctx, updated_data)
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
        // 功能消息类型
        Ok(MessageType::Typing) => "typing",
        Ok(MessageType::Operation) => "operation", // 统一操作类型（包含 recall/read/edit 等）
        Ok(MessageType::MergeForward) => "forward",
        // 业务扩展消息类型
        Ok(MessageType::Vote) => "vote",
        Ok(MessageType::Task) => "task",
        Ok(MessageType::Schedule) => "schedule",
        Ok(MessageType::Announcement) => "announcement",
        // 扩展消息类型
        // MessageType::Quote 已废弃：现在通过 Message.quote 字段处理
        Ok(MessageType::LinkCard) => "link_card",
        Ok(MessageType::MergeForward) => "merge_forward",
        Ok(MessageType::MiniProgram) => "mini_program",
        // 系统消息类型
        Ok(MessageType::SystemEvent) => "system_event",
        // Thread 类型（如果存在）
        Ok(MessageType::Thread) => "thread",
        // 未知或错误情况
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
                    Some(flare_proto::common::message_content::Content::Notification(_)) => {
                        "notification"
                    }
                    Some(flare_proto::common::message_content::Content::Custom(_)) => "custom",
                    Some(flare_proto::common::message_content::Content::Forward(_)) => "forward",
                    Some(flare_proto::common::message_content::Content::Typing(_)) => "typing",
                    Some(flare_proto::common::message_content::Content::SystemEvent(_)) => {
                        "system_event"
                    }
                    // Quote 已废弃：现在通过 Message.quote 字段处理
                    Some(flare_proto::common::message_content::Content::LinkCard(_)) => "link_card",
                    Some(flare_proto::common::message_content::Content::Thread(_)) => "thread",
                    Some(flare_proto::common::message_content::Content::Operation(_)) => "operation",
                    None => "unknown",
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
