use flare_proto::common::{Message as StorageMessage, MessageType};

use crate::domain::MessageCategory;

fn extension_string(message: &StorageMessage, key: &str) -> Option<String> {
    message
        .extensions
        .get(key)
        .and_then(|bytes| String::from_utf8(bytes.clone()).ok())
        .filter(|value| !value.is_empty())
}

/// 从 Message.content 解析 NotificationContent.persistent；非 Notification 返回 None。
pub fn notification_persistent(message: &StorageMessage) -> Option<bool> {
    match message.content.as_ref()?.content.as_ref()? {
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
        let message_type_label = extension_string(message, "message_type")
            .or_else(|| {
                if let Some(content) = message.content.as_ref() {
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
                        Some(flare_proto::common::message_content::Content::AppCard(_)) => {
                            Some("app_card".to_string())
                        }
                        Some(flare_proto::common::message_content::Content::RichText(_)) => {
                            Some("rich_text".to_string())
                        }
                        Some(flare_proto::common::message_content::Content::ImageGroup(_)) => {
                            Some("image_group".to_string())
                        }
                        Some(flare_proto::common::message_content::Content::Sticker(_)) => {
                            Some("sticker".to_string())
                        }
                        Some(flare_proto::common::message_content::Content::Emoji(_)) => {
                            Some("emoji".to_string())
                        }
                        Some(flare_proto::common::message_content::Content::Quote(_)) => {
                            Some("quote".to_string())
                        }
                        Some(flare_proto::common::message_content::Content::Placeholder(_)) => {
                            Some("placeholder".to_string())
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
            "custom" | "json" | "command" | "event" => MessageType::Custom,
            "sticker" => MessageType::Sticker,
            "emoji" => MessageType::Emoji,
            "system" => MessageType::System,
            "notification" => MessageType::Notification,

            // 功能消息类型（typing/operation 在 proto 中已移除，用 Unspecified + label 区分）
            "typing" | "system_event" => MessageType::Unspecified,
            "recall" | "operation" | "read" => MessageType::Unspecified,
            "forward" => MessageType::Forward,

            // 通用扩展消息类型
            "link_card" | "linkcard" => MessageType::LinkCard,
            "merge_forward" | "mergeforward" => MessageType::Forward,
            "app_card" | "appcard" => MessageType::AppCard,
            "rich_text" | "richtext" => MessageType::RichText,
            "image_group" | "imagegroup" => MessageType::ImageGroup,
            "quote" => MessageType::Quote,
            "placeholder" => MessageType::Placeholder,

            _ => MessageType::Unspecified,
        };

        // 设置 message_type 字段
        message.message_type = message_type as i32;

        if message.message_type == MessageType::Unspecified as i32 {
            message.message_type = message_type as i32;
        }

        let category = Self::determine_category(
            &message_type,
            &message_type_label,
            extension_string(message, "notification_type").as_deref(),
        );

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
        notification_type: Option<&str>,
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
                if notification_type == Some("message_operation") {
                    return MessageCategory::Operation;
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

    fn message_with_extra(message_type_label: &str, message_type: i32) -> Message {
        let mut msg = Message {
            message_type,
            ..Default::default()
        };
        msg.extensions.insert(
            "message_type".to_string(),
            message_type_label.as_bytes().to_vec(),
        );
        let msg_content = MessageContent {
            content: Some(flare_proto::common::message_content::Content::Text(
                TextContent {
                    text: "test".to_string(),
                    mentions: vec![],
                },
            )),
        };
        msg.content = Some(msg_content);
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
