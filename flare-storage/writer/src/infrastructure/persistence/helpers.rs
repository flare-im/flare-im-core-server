use anyhow::Result;
use chrono::{DateTime, Utc};
use flare_im_core::utils::timestamp_to_datetime;
use flare_proto::common::{ContentType, Message, MessageSource, MessageStatus, MessageType};
use serde_json::{to_value, Map, Value};

pub fn infer_content_type(message: &Message) -> &'static str {
    message
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
            Some(flare_proto::common::message_content::Content::Location(_)) => {
                "application/location"
            }
            Some(flare_proto::common::message_content::Content::Card(_)) => "application/card",
            // Quote 不在 Content oneof 中，而是作为 Message.quote 字段
            Some(flare_proto::common::message_content::Content::LinkCard(_)) => {
                "application/link_card"
            }
            Some(flare_proto::common::message_content::Content::Forward(_)) => {
                "application/forward"
            }
            Some(flare_proto::common::message_content::Content::Thread(_)) => {
                "application/thread"
            }
            Some(flare_proto::common::message_content::Content::Custom(_)) => {
                "application/custom"
            }
            Some(flare_proto::common::message_content::Content::Notification(_)) => {
                "application/notification"
            }
            Some(flare_proto::common::message_content::Content::Typing(_)) => {
                "application/typing"
            }
            Some(flare_proto::common::message_content::Content::SystemEvent(_)) => {
                "application/system_event"
            }
            Some(flare_proto::common::message_content::Content::Operation(_)) => {
                "application/operation"
            }
            None => "application/unknown",
        })
        .unwrap_or_else(|| {
            match message.content_type {
                x if x == ContentType::PlainText as i32 => "text/plain",
                x if x == ContentType::Html as i32 => "text/html",
                x if x == ContentType::Markdown as i32 => "text/markdown",
                x if x == ContentType::Json as i32 => "application/json",
                _ => "application/unknown",
            }
        })
}

/// 编码消息内容为字节数组
///
/// **已迁移到 flare-proto::encode_message_content**
/// 使用统一的实现确保一致性和性能
pub fn encode_message_content(message: &Message) -> Vec<u8> {
    flare_proto::encode_message_content(message)
}

pub fn build_extra_value(message: &Message) -> Result<Map<String, Value>> {
    let mut extra_value = Map::new();

    let source_str = match std::convert::TryFrom::try_from(message.source) {
        Ok(MessageSource::User) => "user",
        Ok(MessageSource::System) => "system",
        Ok(MessageSource::Bot) => "bot",
        Ok(MessageSource::Admin) => "admin",
        _ => "",
    };
    if !source_str.is_empty() {
        extra_value.insert(
            "sender_type".to_string(),
            Value::String(source_str.to_string()),
        );
    }

    if message.conversation_type != 0 {
        let conversation_type_str = match message.conversation_type {
            1 => "single",
            2 => "group",
            3 => "channel",
            _ => "unknown",
        };
        extra_value.insert(
            "conversation_type".to_string(),
            Value::String(conversation_type_str.to_string()),
        );
    }

    if !message.tags.is_empty() {
        extra_value.insert(
            "tags".to_string(),
            Value::Array(
                message
                    .tags
                    .iter()
                    .map(|t| Value::String(t.clone()))
                    .collect(),
            ),
        );
    }

    if !message.attributes.is_empty() {
        for (k, v) in &message.attributes {
            extra_value.insert(k.clone(), Value::String(v.clone()));
        }
    }

    if let Ok(existing_extra) =
        serde_json::from_value::<Map<String, Value>>(to_value(&message.extra)?)
    {
        for (k, v) in existing_extra {
            extra_value.insert(k, v);
        }
    }

    Ok(extra_value)
}

pub fn message_type_to_string(message_type: i32) -> Option<String> {
    std::convert::TryFrom::try_from(message_type)
        .ok()
        .map(|mt| match mt {
            MessageType::Text => "text",
            MessageType::Image => "image",
            MessageType::Video => "video",
            MessageType::Audio => "audio",
            MessageType::File => "file",
            MessageType::Location => "location",
            MessageType::Card => "card",
            MessageType::Custom => "custom",
            MessageType::Notification => "notification",
            MessageType::Typing => "typing",
            MessageType::Operation => "operation", // 统一操作类型（包含 recall/read/edit 等）
            _ => "unknown",
        })
        .map(|s| s.to_string())
}

pub fn message_status_to_string(status: i32) -> String {
    std::convert::TryFrom::try_from(status)
        .ok()
        .map(|s| match s {
            // Message FSM 状态映射到数据库 CHECK 约束值
            // 数据库约束: CHECK (status IN ('INIT', 'SENT', 'EDITED', 'RECALLED', 'DELETED_HARD', 'DELETED_SOFT'))
            MessageStatus::Created => "INIT",      // Created 状态映射到 INIT（服务端构建中）
            MessageStatus::Sent => "SENT",        // Sent 状态映射到 SENT（已发送，正常态）
            MessageStatus::Recalled => "RECALLED", // Recalled 状态映射到 RECALLED（已撤回，终态）
            MessageStatus::DeletedHard => "DELETED_HARD", // DeletedHard 状态映射到 DELETED_HARD（硬删除，终态）
            MessageStatus::DeletedSoft => "DELETED_SOFT", // DeletedSoft 状态映射到 DELETED_SOFT（软删除，用户维度）
            // 其他状态映射（如果存在）
            MessageStatus::Delivered => "SENT",   // Delivered 视为已发送
            MessageStatus::Read => "SENT",         // Read 视为已发送（已读是 User-Message FSM，不影响 Message FSM）
            MessageStatus::Failed => "SENT",       // Failed 暂时视为已发送（实际应该根据业务逻辑处理）
            _ => "SENT",                          // 未知状态默认映射到 SENT
        })
        .unwrap_or("SENT")                        // 如果转换失败，默认返回 SENT
        .to_string()
}

pub fn get_message_timestamp(message: &Message) -> DateTime<Utc> {
    message
        .timestamp
        .as_ref()
        .and_then(|ts| timestamp_to_datetime(ts))
        .unwrap_or_else(|| Utc::now())
}

pub fn extract_seq_from_extra(extra_value: &Map<String, Value>) -> Option<i64> {
    extra_value.get("seq").and_then(|v| v.as_i64())
}

