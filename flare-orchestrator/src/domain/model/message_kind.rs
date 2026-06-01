use flare_proto::common::{Message as StorageMessage, MessageType};
use prost::Message as _;

use crate::domain::MessageCategory;

/// 从 Message.content 解析 NotificationContent.persistent；非 Notification 返回 None。
pub fn notification_persistent(message: &StorageMessage) -> Option<bool> {
    if message.content.is_empty() {
        return None;
    }
    let content = flare_proto::common::MessageContent::decode(message.content.as_slice()).ok()?;
    match content.content.as_ref()? {
        flare_proto::common::message_content::Content::Notification(n) => Some(n.persistent),
        _ => None,
    }
}

/// 统一的消息类型推断与归一化
#[derive(Debug, Clone)]
pub struct MessageProfile {
    message_type: MessageType,
    message_type_label: String,
    category: MessageCategory,
}

impl MessageProfile {
    pub fn ensure(message: &mut StorageMessage) -> Self {
        // 从 extra 中获取 message_type 标签，或从 content 推断
        let message_type_label = message
            .extra
            .get("message_type")
            .cloned()
            .or_else(|| {
                if !message.content.is_empty() {
                    let content =
                        flare_proto::common::MessageContent::decode(message.content.as_slice())
                            .ok()?;
                    match content.content.as_ref() {
                        Some(flare_proto::common::message_content::Content::Text(_)) => {
                            Some("text".to_string())
                        }
                        Some(flare_proto::common::message_content::Content::Image(_)) => {
                            Some("image".to_string())
                        }
                        Some(flare_proto::common::message_content::Content::Video(_)) => {
                            Some("video".to_string())
                        }
                        Some(flare_proto::common::message_content::Content::Audio(_)) => {
                            Some("audio".to_string())
                        }
                        Some(flare_proto::common::message_content::Content::File(_)) => {
                            Some("file".to_string())
                        }
                        Some(flare_proto::common::message_content::Content::Location(_)) => {
                            Some("location".to_string())
                        }
                        Some(flare_proto::common::message_content::Content::Card(_)) => {
                            Some("card".to_string())
                        }
                        Some(flare_proto::common::message_content::Content::Notification(_)) => {
                            Some("notification".to_string())
                        }
                        Some(flare_proto::common::message_content::Content::Custom(custom)) => {
                            if custom.r#type.is_empty() {
                                Some("custom".to_string())
                            } else {
                                Some(custom.r#type.clone())
                            }
                        }
                        Some(flare_proto::common::message_content::Content::Forward(_)) => {
                            Some("forward".to_string())
                        }
                        Some(flare_proto::common::message_content::Content::Thread(_)) => {
                            Some("thread".to_string())
                        }
                        Some(flare_proto::common::message_content::Content::LinkCard(_)) => {
                            Some("link_card".to_string())
                        }
                        Some(flare_proto::common::message_content::Content::MiniProgram(_)) => {
                            Some("mini_program".to_string())
                        }
                        Some(flare_proto::common::message_content::Content::Vote(_)) => {
                            Some("vote".to_string())
                        }
                        Some(flare_proto::common::message_content::Content::Task(_)) => {
                            Some("task".to_string())
                        }
                        Some(flare_proto::common::message_content::Content::Schedule(_)) => {
                            Some("schedule".to_string())
                        }
                        Some(flare_proto::common::message_content::Content::Announcement(_)) => {
                            Some("announcement".to_string())
                        }
                        None => None,
                        _ => Some("custom".to_string()),
                    }
                } else {
                    None
                }
            })
            .unwrap_or_else(|| "custom".to_string());

        // 根据标签推断 MessageType 枚举值（基于新的枚举定义，支持所有22种消息类型）
        let message_type = match message_type_label.as_str() {
            // 基础消息类型（9种）
            "text" | "text/plain" | "plain_text" => MessageType::Text,
            "image" => MessageType::Image,
            "video" => MessageType::Video,
            "audio" => MessageType::Audio,
            "file" => MessageType::File,
            "location" => MessageType::Location,
            "card" => MessageType::Card,
            "custom" | "json" | "sticker" | "command" | "event" | "system" => MessageType::Custom,
            "notification" => MessageType::Notification,

            // 功能消息类型（typing/operation 在 proto 中已移除，用 Unspecified + label 区分）
            "typing" | "system_event" => MessageType::Unspecified,
            "recall" | "operation" | "read" => MessageType::Unspecified,
            "forward" => MessageType::MergeForward,

            // 扩展消息类型（5种）
            "mini_program" | "miniprogram" => MessageType::MiniProgram,
            "link_card" | "linkcard" => MessageType::LinkCard,
            "merge_forward" | "mergeforward" => MessageType::MergeForward,

            _ => MessageType::Unspecified,
        };

        // 设置 message_type 字段
        message.message_type = message_type as i32;

        // 将 message_type_label 保存到 extra
        message
            .extra
            .entry("message_type".into())
            .or_insert_with(|| message_type_label.clone());

        // 判断消息类别（Temporary/Notification/Operation/Normal）
        let category = Self::determine_category(&message_type, &message_type_label, &message.extra);

        MessageProfile {
            message_type,
            message_type_label,
            category,
        }
    }

    /// 判断消息类别
    ///
    /// 规则：
    /// - MESSAGE_TYPE_TYPING (200) 或 MESSAGE_TYPE_SYSTEM_EVENT (201) => Temporary
    /// - MESSAGE_TYPE_OPERATION (302) => Operation
    /// - MESSAGE_TYPE_NOTIFICATION (101) 且 notification_type = "message_operation" => Operation
    /// - MESSAGE_TYPE_NOTIFICATION (101) => Notification
    /// - 其他 => Normal
    fn determine_category(
        message_type: &MessageType,
        message_type_label: &str,
        extra: &std::collections::HashMap<String, String>,
    ) -> MessageCategory {
        use MessageType::*;
        // typing/operation 在最新 proto 中无对应 MessageType，仅通过 label 判断
        if matches!(message_type_label, "typing" | "system_event") {
            return MessageCategory::Temporary;
        }
        if message_type_label == "operation"
            || message_type_label == "recall"
            || message_type_label == "read"
        {
            return MessageCategory::Operation;
        }
        match *message_type {
            Notification => {
                // **关键修复**：检查是否为操作消息（notification_type = "message_operation"）
                // 操作消息应该被识别为 Operation 类别，而不是 Notification 类别
                if let Some(notification_type) = extra.get("notification_type") {
                    if notification_type == "message_operation" {
                        return MessageCategory::Operation;
                    }
                }
                MessageCategory::Notification
            }
            _ => match message_type_label {
                "notification" => MessageCategory::Notification,
                _ => MessageCategory::Normal,
            },
        }
    }

    pub fn message_type(&self) -> MessageType {
        self.message_type
    }

    pub fn message_type_label(&self) -> &str {
        &self.message_type_label
    }

    pub fn category(&self) -> MessageCategory {
        self.category
    }

    /// 判断是否需要持久化
    pub fn needs_persistence(&self) -> bool {
        self.category.needs_persistence()
    }

    /// 判断是否需要写入WAL
    pub fn needs_wal(&self) -> bool {
        self.category.needs_wal()
    }

    /// 判断是否为临时消息（只推送，不持久化）
    pub fn is_temporary(&self) -> bool {
        self.category.is_temporary()
    }

    /// 判断是否为操作消息
    pub fn is_operation(&self) -> bool {
        self.category.is_operation()
    }

    /// 判断是否为通知消息
    pub fn is_notification(&self) -> bool {
        self.category.is_notification()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_proto::common::{Message, MessageContent, TextContent};
    use prost::Message as _;

    fn message_with_extra(message_type_label: &str, message_type: i32) -> Message {
        let mut msg = Message::default();
        msg.message_type = message_type;
        msg.extra
            .insert("message_type".to_string(), message_type_label.to_string());
        let msg_content = MessageContent {
            content: Some(flare_proto::common::message_content::Content::Text(
                TextContent {
                    text: "test".to_string(),
                    mentions: vec![],
                },
            )),
        };
        msg.content = msg_content.encode_to_vec();
        msg
    }

    #[test]
    fn infer_from_extra_text() {
        let mut msg = message_with_extra("text", 0);
        let profile = MessageProfile::ensure(&mut msg);
        assert_eq!(profile.message_type(), MessageType::Text);
        assert_eq!(profile.message_type_label(), "text");
    }

    #[test]
    fn preserve_explicit_type() {
        let mut msg = message_with_extra("custom", MessageType::Custom as i32);
        let profile = MessageProfile::ensure(&mut msg);
        assert_eq!(profile.message_type(), MessageType::Custom);
        assert_eq!(profile.message_type_label(), "custom");
    }
}
