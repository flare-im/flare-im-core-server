//! 辅助函数模块
//!
//! 提供消息存储相关的公共辅助函数，用于简化代码和提高可维护性

use std::collections::HashMap;
use flare_proto::common::{ContentType, Message, MessageSource, MessageStatus, MessageType};
use prost::Message as ProstMessage;
use prost_types::Timestamp;
use serde_json::{Value, from_value};

/// 从消息中推断内容类型
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
            Some(flare_proto::common::message_content::Content::Notification(_)) => {
                "application/notification"
            }
            Some(flare_proto::common::message_content::Content::Forward(_)) => {
                "application/forward"
            }
            Some(flare_proto::common::message_content::Content::Typing(_)) => {
                "application/typing"
            }
            Some(flare_proto::common::message_content::Content::Thread(_)) => {
                "application/thread"
            }
            Some(flare_proto::common::message_content::Content::Custom(c)) => {
                // Vote/Task/Schedule/Announcement 现在通过 Custom + content_type 实现
                match c.r#type.as_str() {
                    "vote" => "application/vote",
                    "task" => "application/task",
                    "schedule" => "application/schedule",
                    "announcement" => "application/announcement",
                    _ => "application/custom",
                }
            }
            Some(flare_proto::common::message_content::Content::SystemEvent(_)) => {
                "application/system_event"
            }
            // Quote 不在 Content oneof 中，而是作为 Message.quote 字段
            Some(flare_proto::common::message_content::Content::LinkCard(_)) => {
                "application/link_card"
            }
            Some(flare_proto::common::message_content::Content::Operation(_)) => {
                "application/operation"
            }
            None => "application/unknown",
        })
        .unwrap_or_else(|| match message.content_type {
            x if x == ContentType::PlainText as i32 => "text/plain",
            x if x == ContentType::Html as i32 => "text/html",
            x if x == ContentType::Markdown as i32 => "text/markdown",
            x if x == ContentType::Json as i32 => "application/json",
            _ => "application/unknown",
        })
}

/// 编码消息内容为字节数组
///
/// **已迁移到 flare-proto::encode_message_content**
/// 使用统一的实现确保一致性和性能
pub fn encode_message_content(message: &Message) -> Vec<u8> {
    flare_proto::encode_message_content(message)
}

/// 将消息类型转换为字符串
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

/// 将消息状态转换为字符串
pub fn message_status_to_string(status: i32) -> String {
    std::convert::TryFrom::try_from(status)
        .ok()
        .map(|s| match s {
            MessageStatus::Created => "created",
            MessageStatus::Sent => "sent",
            MessageStatus::Delivered => "delivered",
            MessageStatus::Read => "read",
            MessageStatus::Failed => "failed",
            MessageStatus::Recalled => "recalled",
            _ => "unknown",
        })
        .unwrap_or("unknown")
        .to_string()
}

/// 从 extra JSONB 中提取 seq
pub fn extract_seq_from_extra(extra: &serde_json::Map<String, Value>) -> Option<i64> {
    extra.get("seq").and_then(|v| v.as_i64())
}

/// 从 extra JSONB 中解析租户信息
pub fn parse_tenant_from_extra(
    extra_map: &HashMap<String, String>,
) -> Option<flare_proto::common::TenantContext> {
    extra_map.get("tenant_id").map(|tenant_id| {
        let mut labels = HashMap::new();
        if let Some(labels_str) = extra_map.get("labels") {
            if let Ok(labels_obj) = serde_json::from_str::<HashMap<String, String>>(labels_str) {
                labels = labels_obj;
            }
        }
        let mut tenant_attributes = HashMap::new();
        if let Some(attrs_str) = extra_map.get("tenant_attributes") {
            if let Ok(attrs_obj) = serde_json::from_str::<HashMap<String, String>>(attrs_str) {
                tenant_attributes = attrs_obj;
            }
        }
        flare_proto::common::TenantContext {
            tenant_id: tenant_id.clone(),
            business_type: extra_map.get("business_type").cloned().unwrap_or_default(),
            environment: extra_map.get("environment").cloned().unwrap_or_default(),
            organization_id: extra_map
                .get("organization_id")
                .cloned()
                .unwrap_or_default(),
            labels,
            attributes: tenant_attributes,
        }
    })
}

/// 从 extra JSONB 中解析消息源
pub fn parse_message_source_from_extra(extra_map: &HashMap<String, String>) -> i32 {
    let source_str = extra_map.get("sender_type").cloned().unwrap_or_default();
    match source_str.as_str() {
        "user" => MessageSource::User as i32,
        "system" => MessageSource::System as i32,
        "bot" => MessageSource::Bot as i32,
        "admin" => MessageSource::Admin as i32,
        _ => MessageSource::Unspecified as i32,
    }
}

/// 从 extra JSONB 中解析标签
pub fn parse_tags_from_extra(extra_map: &HashMap<String, String>) -> Vec<String> {
    extra_map
        .get("tags")
        .and_then(|tags_str| serde_json::from_str::<Vec<String>>(tags_str).ok())
        .unwrap_or_default()
}

/// 从 extra JSONB 中解析属性（排除系统字段）
pub fn parse_attributes_from_extra(extra_map: &HashMap<String, String>) -> HashMap<String, String> {
    let mut attributes = HashMap::new();
    for (k, v) in extra_map {
        if !matches!(
            k.as_str(),
            "tenant_id"
                | "business_id"
                | "receiver_id"
                | "conversation_type"
                | "sender_type"
                | "tags"
                | "seq"
        ) {
            attributes.insert(k.clone(), v.clone());
        }
    }
    attributes
}

/// 从 JSONB 解析 MessageReadRecord 列表
pub fn parse_read_by_from_jsonb(
    read_by: Option<Value>,
) -> Vec<flare_proto::common::MessageReadRecord> {
    read_by
        .and_then(|v| {
            from_value::<Vec<serde_json::Value>>(v)
                .ok()
                .and_then(|records| {
                    let mut result = Vec::new();
                    for record in records {
                        if let (Some(user_id), read_at_opt, burned_at_opt) = (
                            record.get("user_id").and_then(|v| v.as_str()),
                            record.get("read_at"),
                            record.get("burned_at"),
                        ) {
                            let read_at = read_at_opt
                                .and_then(|v| v.as_object())
                                .and_then(|obj| {
                                    let seconds = obj.get("seconds")?.as_i64()?;
                                    let nanos = obj.get("nanos")?.as_i64()?;
                                    Some(Timestamp {
                                        seconds,
                                        nanos: nanos as i32,
                                    })
                                });
                            let burned_at = burned_at_opt
                                .and_then(|v| v.as_object())
                                .and_then(|obj| {
                                    let seconds = obj.get("seconds")?.as_i64()?;
                                    let nanos = obj.get("nanos")?.as_i64()?;
                                    Some(Timestamp {
                                        seconds,
                                        nanos: nanos as i32,
                                    })
                                });
                            result.push(flare_proto::common::MessageReadRecord {
                                user_id: user_id.to_string(),
                                read_at,
                                burned_at,
                            });
                        }
                    }
                    Some(result)
                })
        })
        .unwrap_or_default()
}

/// 从 JSONB 解析 MessageOperation 列表
pub fn parse_operations_from_jsonb(
    operations: Option<Value>,
) -> Vec<flare_proto::common::MessageOperation> {
    operations
        .and_then(|v| {
            from_value::<Vec<serde_json::Value>>(v)
                .ok()
                .and_then(|ops_json| {
                    let mut result = Vec::new();
                    for op_json in ops_json {
                        if let (
                            Some(operation_type),
                            Some(target_message_id),
                            Some(operator_id),
                            timestamp_opt,
                            show_notice,
                            notice_text,
                        ) = (
                            op_json
                                .get("operation_type")
                                .and_then(|v| v.as_i64())
                                .map(|v| v as i32),
                            op_json.get("target_message_id").and_then(|v| v.as_str()),
                            op_json.get("operator_id").and_then(|v| v.as_str()),
                            op_json.get("timestamp"),
                            op_json
                                .get("show_notice")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(true),
                            op_json
                                .get("notice_text")
                                .and_then(|v| v.as_str())
                                .unwrap_or(""),
                        ) {
                            let timestamp = timestamp_opt
                                .and_then(|ts_obj| ts_obj.as_object())
                                .and_then(|obj| {
                                    let seconds = obj.get("seconds")?.as_i64()?;
                                    let nanos = obj.get("nanos")?.as_i64()?;
                                    Some(Timestamp {
                                        seconds,
                                        nanos: nanos as i32,
                                    })
                                });

                            let operation_data = op_json.get("operation_data").and_then(|_od| {
                                // 简化：operation_data 的解析需要根据 operation_type 判断具体类型
                                // 这里暂时返回 None，后续可以完善
                                None
                            });

                            let mut metadata = HashMap::new();
                            if let Some(metadata_obj) =
                                op_json.get("metadata").and_then(|v| v.as_object())
                            {
                                for (k, v) in metadata_obj {
                                    if let Some(v_str) = v.as_str() {
                                        metadata.insert(k.clone(), v_str.to_string());
                                    }
                                }
                            }

                            result.push(flare_proto::common::MessageOperation {
                                operation_type,
                                target_message_id: target_message_id.to_string(),
                                operator_id: operator_id.to_string(),
                                timestamp,
                                show_notice,
                                notice_text: notice_text.to_string(),
                                target_user_id: op_json
                                    .get("target_user_id")
                                    .and_then(|v| v.as_str())
                                    .map(|s| s.to_string())
                                    .unwrap_or_default(),
                                operation_data,
                                metadata,
                            });
                        }
                    }
                    Some(result)
                })
        })
        .unwrap_or_default()
}

/// 将字符串转换为消息类型枚举值
pub fn string_to_message_type(s: Option<&str>) -> i32 {
    s.and_then(|s| match s {
        "text" => Some(MessageType::Text as i32),
        "image" => Some(MessageType::Image as i32),
        "video" => Some(MessageType::Video as i32),
        "audio" => Some(MessageType::Audio as i32),
        "file" => Some(MessageType::File as i32),
        "location" => Some(MessageType::Location as i32),
        "card" => Some(MessageType::Card as i32),
        "custom" => Some(MessageType::Custom as i32),
        "notification" => Some(MessageType::Notification as i32),
        "typing" => Some(MessageType::Typing as i32),
        "recall" | "read" | "operation" => Some(MessageType::Operation as i32), // 统一操作类型
        _ => None,
    })
    .unwrap_or(MessageType::Unspecified as i32)
}

/// 将字符串转换为内容类型枚举值
pub fn string_to_content_type(s: Option<&str>) -> i32 {
    s.and_then(|s| match s {
        "text/plain" => Some(ContentType::PlainText as i32),
        "text/html" => Some(ContentType::Html as i32),
        "text/markdown" => Some(ContentType::Markdown as i32),
        "application/json" => Some(ContentType::Json as i32),
        _ => None,
    })
    .unwrap_or(ContentType::Unspecified as i32)
}

