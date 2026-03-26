use flare_proto::common::{Message as StorageMessage, MessageType};
use prost::Message as _;

/// 消息类别（用于决定处理策略）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageCategory {
    /// 临时消息（TYPING、SYSTEM_EVENT）：只推送，不持久化，不经过 WAL
    Temporary,
    /// 通知消息（NOTIFICATION）：根据 persistent 标志决定是否持久化，但都推送
    Notification,
    /// 操作消息（OPERATION）：根据操作类型决定同步/异步处理
    Operation,
    /// 普通消息：推送+持久化+WAL
    Normal,
}

/// 消息处理类型（用于决定是否需要持久化）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageProcessingType {
    /// 普通消息：需要持久化+推送
    Normal,
    /// 通知消息：仅推送，不持久化，离线舍弃
    Notification,
}

/// 统一的消息类型推断与归一化。
pub struct MessageProfile {
    message_type: MessageType,
    message_type_label: String,
    category: MessageCategory,
    processing_type: MessageProcessingType,
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
                    let content = flare_proto::common::MessageContent::decode(
                        message.content.as_slice(),
                    )
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

        // 判断消息处理类型（Normal vs Notification）
        let processing_type = Self::determine_processing_type(&category, &message_type_label, &message.content, &message.extra);

        MessageProfile {
            message_type,
            message_type_label,
            category,
            processing_type,
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
        if message_type_label == "operation" || message_type_label == "recall" || message_type_label == "read" {
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
            _ => {
                match message_type_label {
                    "notification" => MessageCategory::Notification,
                    _ => MessageCategory::Normal,
                }
            }
        }
    }

    /// 判断消息处理类型
    ///
    /// 规则：
    /// - Temporary 类别：Notification（不持久化）
    /// - Notification 类别：根据 NotificationContent.persistent 标志决定，默认 Notification
    /// - Operation 类别：Normal（需要持久化）
    /// - Normal 类别：Normal（需要持久化）
    fn determine_processing_type(
        category: &MessageCategory,
        _message_type_label: &str,
        content: &[u8],
        extra: &std::collections::HashMap<String, String>,
    ) -> MessageProcessingType {
        match *category {
            MessageCategory::Temporary => MessageProcessingType::Notification,
            MessageCategory::Notification => {
                if !content.is_empty() {
                    if let Ok(msg_content) = flare_proto::common::MessageContent::decode(content) {
                        if let Some(flare_proto::common::message_content::Content::Notification(notif)) = &msg_content.content {
                            if notif.persistent {
                                return MessageProcessingType::Normal;
                            }
                        }
                    }
                }
                // 如果 content 中没有，检查 extra 中的 persistent 标志（兼容性处理）
                if let Some(flag) = extra.get("persistent") {
            if flag == "true" || flag == "1" {
                        return MessageProcessingType::Normal;
            }
        }
                MessageProcessingType::Notification
        }
            MessageCategory::Operation => MessageProcessingType::Normal,
            MessageCategory::Normal => MessageProcessingType::Normal,
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

    pub fn processing_type(&self) -> MessageProcessingType {
        self.processing_type
    }

    /// 判断是否需要持久化
    pub fn needs_persistence(&self) -> bool {
        self.processing_type == MessageProcessingType::Normal
    }

    /// 判断是否需要写入WAL
    ///
    /// 规则：
    /// - Temporary: 不需要
    /// - Notification: 根据 persistent 标志
    /// - Operation: 需要
    /// - Normal: 需要
    pub fn needs_wal(&self) -> bool {
        match self.category {
            MessageCategory::Temporary => false,
            MessageCategory::Notification => self.processing_type == MessageProcessingType::Normal,
            MessageCategory::Operation => true,
            MessageCategory::Normal => true,
        }
    }

    /// 判断是否为临时消息（只推送，不持久化）
    pub fn is_temporary(&self) -> bool {
        self.category == MessageCategory::Temporary
    }

    /// 判断是否为操作消息
    pub fn is_operation(&self) -> bool {
        self.category == MessageCategory::Operation
    }

    /// 判断是否为通知消息
    pub fn is_notification(&self) -> bool {
        self.category == MessageCategory::Notification
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
            extensions: vec![],
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
