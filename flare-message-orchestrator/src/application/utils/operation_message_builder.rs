//! 操作消息构建器
//!
//! 将操作请求（RecallMessageRequest、EditMessageRequest 等）转换为操作消息（Message with MessageOperation）
//! 统一通过 SendMessage 处理

use anyhow::{Context, Result};
use chrono::Utc;
use prost::Message as ProstMessage;
use prost_types;
use uuid::Uuid;

use flare_proto::common::{
    message_content::Content, message_operation::OperationData, DeleteType, MarkType,
    Message, MessageContent, MessageOperation, OperationType, ReactionAction,
};
use flare_proto::MessageContentExt;
use flare_proto::message::{
    AddReactionRequest, DeleteMessageRequest, EditMessageRequest, MarkMessageRequest,
    PinMessageRequest, RecallMessageRequest, RemoveReactionRequest, UnmarkMessageRequest,
    UnpinMessageRequest,
};

/// 操作消息构建器
pub struct OperationMessageBuilder;

impl OperationMessageBuilder {
    /// 构建撤回操作消息
    pub fn build_recall_message(
        req: &RecallMessageRequest,
        conversation_id: &str,
        operator_id: &str,
    ) -> Result<Message> {
        let now = Utc::now();
        let timestamp = prost_types::Timestamp {
            seconds: now.timestamp(),
            nanos: now.timestamp_subsec_nanos() as i32,
        };

        let operation = MessageOperation {
            operation_type: OperationType::Recall as i32,
            target_message_id: req.message_id.clone(),
            operator_id: operator_id.to_string(),
            timestamp: Some(timestamp.clone()),
            show_notice: true,
            notice_text: format!("{} 撤回了消息", operator_id),
            target_user_id: String::new(),
            operation_data: Some(OperationData::Recall(
                flare_proto::common::RecallOperationData {
                    reason: req.reason.clone(),
                    time_limit_seconds: req.recall_time_limit_seconds,
                    allow_admin_recall: false,
                },
            )),
            metadata: std::collections::HashMap::new(),
        };

        let mut message = Message::default();
        message.server_id = format!("op_{}", Uuid::new_v4());
        message.conversation_id = conversation_id.to_string();
        message.sender_id = operator_id.to_string();
        message.message_type = flare_proto::MessageType::Operation as i32;
        message.timestamp = Some(timestamp.clone());
        message.content = Some(MessageContent {
            content: Some(Content::Operation(operation)),
            extensions: Vec::new(),
        });
        message.extra.insert("message_type".to_string(), "operation".to_string());
        message.extra.insert("operation_type".to_string(), "recall".to_string());

        Ok(message)
    }

    /// 构建编辑操作消息
    pub fn build_edit_message(
        req: &EditMessageRequest,
        conversation_id: &str,
        operator_id: &str,
    ) -> Result<Message> {
        let now = Utc::now();
        let timestamp = prost_types::Timestamp {
            seconds: now.timestamp(),
            nanos: now.timestamp_subsec_nanos() as i32,
        };

        let new_content = req
            .new_content
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("new_content is required"))?;

        // 将 MessageContent 序列化为 Vec<u8>（使用统一的编码方法）
        let new_content_buf = new_content.encode_to_bytes().unwrap_or_default();

        let operation = MessageOperation {
            operation_type: OperationType::Edit as i32,
            target_message_id: req.message_id.clone(),
            operator_id: operator_id.to_string(),
            timestamp: Some(timestamp.clone()),
            show_notice: req.show_edited_mark,
            notice_text: if req.show_edited_mark {
                "消息已编辑".to_string()
            } else {
                String::new()
            },
            target_user_id: String::new(),
            operation_data: Some(OperationData::Edit(
                flare_proto::common::EditOperationData {
                    new_content: new_content_buf,
                    edit_version: req.edit_version,
                    reason: req.reason.clone(),
                    show_edited_mark: req.show_edited_mark,
                },
            )),
            metadata: std::collections::HashMap::new(),
        };

        let mut message = Message::default();
        message.server_id = format!("op_{}", Uuid::new_v4());
        message.conversation_id = conversation_id.to_string();
        message.sender_id = operator_id.to_string();
        message.message_type = flare_proto::MessageType::Operation as i32;
        message.timestamp = Some(timestamp.clone());
        message.content = Some(MessageContent {
            content: Some(Content::Operation(operation)),
            extensions: Vec::new(),
        });
        message.extra.insert("message_type".to_string(), "operation".to_string());
        message.extra.insert("operation_type".to_string(), "edit".to_string());

        Ok(message)
    }

    /// 构建删除操作消息
    pub fn build_delete_message(
        req: &DeleteMessageRequest,
        operator_id: &str,
    ) -> Result<Message> {
        let now = Utc::now();
        let timestamp = prost_types::Timestamp {
            seconds: now.timestamp(),
            nanos: now.timestamp_subsec_nanos() as i32,
        };

        // 删除操作针对多条消息，这里为每条消息创建一个操作消息
        // 注意：实际实现中可能需要批量处理
        if req.message_ids.is_empty() {
            return Err(anyhow::anyhow!("message_ids is required"));
        }

        // 为第一条消息创建操作消息（批量删除时可能需要特殊处理）
        let target_message_id = req.message_ids[0].clone();

        let operation = MessageOperation {
            operation_type: OperationType::Delete as i32,
            target_message_id: target_message_id.clone(),
            operator_id: operator_id.to_string(),
            timestamp: Some(timestamp.clone()),
            show_notice: req.notify_others,
            notice_text: if req.notify_others {
                "消息已删除".to_string()
            } else {
                String::new()
            },
            target_user_id: String::new(),
            operation_data: Some(OperationData::Delete(
                flare_proto::common::DeleteOperationData {
                    delete_type: req.delete_type as i32,
                    reason: req.reason.clone(),
                    notify_others: req.notify_others,
                },
            )),
            metadata: {
                let mut meta = std::collections::HashMap::new();
                // 存储所有要删除的消息ID
                meta.insert(
                    "message_ids".to_string(),
                    req.message_ids.join(","),
                );
                meta
            },
        };

        let mut message = Message::default();
        message.server_id = format!("op_{}", Uuid::new_v4());
        message.conversation_id = req.conversation_id.clone();
        message.sender_id = operator_id.to_string();
        message.message_type = flare_proto::MessageType::Operation as i32;
        message.timestamp = Some(timestamp.clone());
        message.content = Some(MessageContent {
            content: Some(Content::Operation(operation)),
            extensions: Vec::new(),
        });
        message.extra.insert("message_type".to_string(), "operation".to_string());
        message.extra.insert("operation_type".to_string(), "delete".to_string());

        Ok(message)
    }

    /// 构建添加反应操作消息
    pub fn build_add_reaction_message(
        req: &AddReactionRequest,
        conversation_id: &str,
    ) -> Result<Message> {
        let now = Utc::now();
        let timestamp = prost_types::Timestamp {
            seconds: now.timestamp(),
            nanos: now.timestamp_subsec_nanos() as i32,
        };

        let operation = MessageOperation {
            operation_type: OperationType::ReactionAdd as i32,
            target_message_id: req.message_id.clone(),
            operator_id: req.user_id.clone(),
            timestamp: Some(timestamp.clone()),
            show_notice: true,
            notice_text: String::new(),
            target_user_id: String::new(),
            operation_data: Some(OperationData::Reaction(
                flare_proto::common::ReactionOperationData {
                    emoji: req.emoji.clone(),
                    action: ReactionAction::Add as i32,
                    count: 0, // 将在处理时更新
                },
            )),
            metadata: std::collections::HashMap::new(),
        };

        let mut message = Message::default();
        message.server_id = format!("op_{}", Uuid::new_v4());
        message.conversation_id = conversation_id.to_string();
        message.sender_id = req.user_id.clone();
        message.message_type = flare_proto::MessageType::Operation as i32;
        message.timestamp = Some(timestamp.clone());
        message.content = Some(MessageContent {
            content: Some(Content::Operation(operation)),
            extensions: Vec::new(),
        });
        message.extra.insert("message_type".to_string(), "operation".to_string());
        message.extra.insert("operation_type".to_string(), "reaction_add".to_string());

        Ok(message)
    }

    /// 构建移除反应操作消息
    pub fn build_remove_reaction_message(
        req: &RemoveReactionRequest,
        conversation_id: &str,
    ) -> Result<Message> {
        let now = Utc::now();
        let timestamp = prost_types::Timestamp {
            seconds: now.timestamp(),
            nanos: now.timestamp_subsec_nanos() as i32,
        };

        let operation = MessageOperation {
            operation_type: OperationType::ReactionRemove as i32,
            target_message_id: req.message_id.clone(),
            operator_id: req.user_id.clone(),
            timestamp: Some(timestamp.clone()),
            show_notice: true,
            notice_text: String::new(),
            target_user_id: String::new(),
            operation_data: Some(OperationData::Reaction(
                flare_proto::common::ReactionOperationData {
                    emoji: req.emoji.clone(),
                    action: ReactionAction::Remove as i32,
                    count: 0, // 将在处理时更新
                },
            )),
            metadata: std::collections::HashMap::new(),
        };

        let mut message = Message::default();
        message.server_id = format!("op_{}", Uuid::new_v4());
        message.conversation_id = conversation_id.to_string();
        message.sender_id = req.user_id.clone();
        message.message_type = flare_proto::MessageType::Operation as i32;
        message.timestamp = Some(timestamp.clone());
        message.content = Some(MessageContent {
            content: Some(Content::Operation(operation)),
            extensions: Vec::new(),
        });
        message.extra.insert("message_type".to_string(), "operation".to_string());
        message.extra.insert("operation_type".to_string(), "reaction_remove".to_string());

        Ok(message)
    }

    /// 构建置顶操作消息
    pub fn build_pin_message(
        req: &PinMessageRequest,
        conversation_id: &str,
    ) -> Result<Message> {
        let now = Utc::now();
        let timestamp = prost_types::Timestamp {
            seconds: now.timestamp(),
            nanos: now.timestamp_subsec_nanos() as i32,
        };

        let operation = MessageOperation {
            operation_type: OperationType::Pin as i32,
            target_message_id: req.message_id.clone(),
            operator_id: req.operator_id.clone(),
            timestamp: Some(timestamp.clone()),
            show_notice: true,
            notice_text: String::new(),
            target_user_id: String::new(),
            operation_data: Some(OperationData::Pin(
                flare_proto::common::PinOperationData {
                    reason: req.reason.clone(),
                    expire_at: req.expire_at.clone(),
                },
            )),
            metadata: std::collections::HashMap::new(),
        };

        let mut message = Message::default();
        message.server_id = format!("op_{}", Uuid::new_v4());
        message.conversation_id = conversation_id.to_string();
        message.sender_id = req.operator_id.clone();
        message.message_type = flare_proto::MessageType::Operation as i32;
        message.timestamp = Some(timestamp.clone());
        message.content = Some(MessageContent {
            content: Some(Content::Operation(operation)),
            extensions: Vec::new(),
        });
        message.extra.insert("message_type".to_string(), "operation".to_string());
        message.extra.insert("operation_type".to_string(), "pin".to_string());

        Ok(message)
    }

    /// 构建取消置顶操作消息
    pub fn build_unpin_message(
        req: &UnpinMessageRequest,
        conversation_id: &str,
    ) -> Result<Message> {
        let now = Utc::now();
        let timestamp = prost_types::Timestamp {
            seconds: now.timestamp(),
            nanos: now.timestamp_subsec_nanos() as i32,
        };

        let operation = MessageOperation {
            operation_type: OperationType::Unpin as i32,
            target_message_id: req.message_id.clone(),
            operator_id: req.operator_id.clone(),
            timestamp: Some(timestamp.clone()),
            show_notice: true,
            notice_text: String::new(),
            target_user_id: String::new(),
            operation_data: None,
            metadata: std::collections::HashMap::new(),
        };

        let mut message = Message::default();
        message.server_id = format!("op_{}", Uuid::new_v4());
        message.conversation_id = conversation_id.to_string();
        message.sender_id = req.operator_id.clone();
        message.message_type = flare_proto::MessageType::Operation as i32;
        message.timestamp = Some(timestamp.clone());
        message.content = Some(MessageContent {
            content: Some(Content::Operation(operation)),
            extensions: Vec::new(),
        });
        message.extra.insert("message_type".to_string(), "operation".to_string());
        message.extra.insert("operation_type".to_string(), "unpin".to_string());

        Ok(message)
    }

    /// 构建标记消息操作
    pub fn build_mark_message(
        req: &MarkMessageRequest,
        conversation_id: &str,
    ) -> Result<Message> {
        let now = Utc::now();
        let timestamp = prost_types::Timestamp {
            seconds: now.timestamp(),
            nanos: now.timestamp_subsec_nanos() as i32,
        };

        let operation = MessageOperation {
            operation_type: OperationType::Mark as i32,
            target_message_id: req.message_id.clone(),
            operator_id: req.user_id.clone(),
            timestamp: Some(timestamp.clone()),
            show_notice: false, // 标记是个人操作，不需要通知
            notice_text: String::new(),
            target_user_id: String::new(),
            operation_data: Some(OperationData::Mark(
                flare_proto::common::MarkOperationData {
                    mark_type: req.mark_type as i32,
                    color: req.color.clone(),
                },
            )),
            metadata: std::collections::HashMap::new(),
        };

        let mut message = Message::default();
        message.server_id = format!("op_{}", Uuid::new_v4());
        message.conversation_id = conversation_id.to_string();
        message.sender_id = req.user_id.clone();
        message.message_type = flare_proto::MessageType::Operation as i32;
        message.timestamp = Some(timestamp.clone());
        message.content = Some(MessageContent {
            content: Some(Content::Operation(operation)),
            extensions: Vec::new(),
        });
        message.extra.insert("message_type".to_string(), "operation".to_string());
        message.extra.insert("operation_type".to_string(), "mark".to_string());

        Ok(message)
    }

    /// 构建取消标记消息操作
    pub fn build_unmark_message(
        req: &UnmarkMessageRequest,
        conversation_id: &str,
    ) -> Result<Message> {
        let now = Utc::now();
        let timestamp = prost_types::Timestamp {
            seconds: now.timestamp(),
            nanos: now.timestamp_subsec_nanos() as i32,
        };

        // mark_type 如果是 0 (Unspecified)，则取消所有标记（使用 -1）
        let mark_type_value = if req.mark_type == 0 {
            -1 // 取消所有标记
        } else {
            req.mark_type
        };

        let operation = MessageOperation {
            operation_type: OperationType::Unmark as i32,
            target_message_id: req.message_id.clone(),
            operator_id: req.user_id.clone(),
            timestamp: Some(timestamp.clone()),
            show_notice: false,
            notice_text: String::new(),
            target_user_id: String::new(),
            operation_data: Some(OperationData::Unmark(
                flare_proto::common::UnmarkOperationData {
                    mark_type: mark_type_value,
                },
            )),
            metadata: std::collections::HashMap::new(),
        };

        let mut message = Message::default();
        message.server_id = format!("op_{}", Uuid::new_v4());
        message.conversation_id = conversation_id.to_string();
        message.sender_id = req.user_id.clone();
        message.message_type = flare_proto::MessageType::Operation as i32;
        message.timestamp = Some(timestamp.clone());
        message.content = Some(MessageContent {
            content: Some(Content::Operation(operation)),
            extensions: Vec::new(),
        });
        message.extra.insert("message_type".to_string(), "operation".to_string());
        message.extra.insert("operation_type".to_string(), "unmark".to_string());

        Ok(message)
    }
}

