//! 消息操作消息构建器

use anyhow::{Context, Result};
use flare_proto::common::{
    Message, MessageContent, MessageType, MessageSource, MessageOperation,
};
use flare_proto::storage::StoreMessage;
use flare_proto::MessageContentExt;
use uuid::Uuid;

use crate::application::commands::{
    AddReactionCommand, DeleteMessageCommand, DeleteType, EditMessageCommand,
    MarkMessageCommand, MessageOperationCommand, PinMessageCommand, RecallMessageCommand,
    RemoveReactionCommand, UnpinMessageCommand,
};

/// 消息操作消息构建器
pub struct MessageOperationBuilder;

impl MessageOperationBuilder {
    /// 构建撤回消息的 StoreMessageRequest
    pub fn build_recall_request(
        cmd: &RecallMessageCommand,
    ) -> Result<StoreMessage> {
        use flare_proto::common::{
            MessageOperation, OperationType,
            message_operation::OperationData, RecallOperationData,
        };

        // 1. 构建 MessageOperation
        let recall_data = RecallOperationData {
            reason: cmd.reason.clone().unwrap_or_default(),
            time_limit_seconds: cmd.time_limit_seconds.unwrap_or(120), // 默认 2 分钟
            allow_admin_recall: false, // 默认不允许管理员撤回
        };

        let operation = MessageOperation {
            operation_type: OperationType::Recall as i32,
            target_message_id: cmd.base.message_id.clone(),
            operator_id: cmd.base.operator_id.clone(),
            timestamp: Some(prost_types::Timestamp {
                seconds: cmd.base.timestamp.timestamp(),
                nanos: cmd.base.timestamp.timestamp_subsec_nanos() as i32,
            }),
            operation_data: Some(OperationData::Recall(recall_data)),
            metadata: std::collections::HashMap::new(),
            notice_text: cmd.reason.clone().unwrap_or_else(|| "对方撤回了一条消息".to_string()),
            show_notice: true,
            target_user_id: String::new(),
        };

        // 2. 使用统一的辅助方法构建操作消息
        Self::build_operation_message(&cmd.base, operation, "OPERATION_TYPE_RECALL")
    }

    /// 构建编辑消息的 StoreMessageRequest
    pub fn build_edit_request(cmd: &EditMessageCommand) -> Result<StoreMessage> {
        use flare_proto::common::{
            MessageOperation, OperationType,
            message_operation::OperationData, EditOperationData,
        };

        // 1. 解析 new_content 为 MessageContent（使用统一的解码方法）
        let new_content = flare_proto::decode_message_content(cmd.new_content.as_slice())
            .context("Failed to decode new_content as MessageContent")?;

        // 2. 构建 MessageOperation
        // 将 MessageContent 序列化为 Vec<u8>（使用统一的编码方法）
        let new_content_bytes = new_content.encode_to_bytes()
            .context("Failed to encode new_content")?;
        
        let edit_data = EditOperationData {
            new_content: new_content_bytes,
            edit_version: 0, // 版本号由 Writer 在持久化时确定
            reason: cmd.reason.clone().unwrap_or_default(),
            show_edited_mark: true,
        };

        let target_message_id = cmd.base.message_id.clone();
        tracing::info!(
            target_message_id = %target_message_id,
            operator_id = %cmd.base.operator_id,
            conversation_id = %cmd.base.conversation_id,
            "🔨 构建编辑操作消息，target_message_id 使用服务端返回的 server_msg_id"
        );
        
        let operation = MessageOperation {
            operation_type: OperationType::Edit as i32,
            target_message_id,
            operator_id: cmd.base.operator_id.clone(),
            timestamp: Some(prost_types::Timestamp {
                seconds: cmd.base.timestamp.timestamp(),
                nanos: cmd.base.timestamp.timestamp_subsec_nanos() as i32,
            }),
            operation_data: Some(OperationData::Edit(edit_data)),
            metadata: std::collections::HashMap::new(),
            notice_text: cmd.reason.clone().unwrap_or_default(),
            show_notice: false,
            target_user_id: String::new(),
        };

        // 3. 使用统一的辅助方法构建操作消息
        let request = Self::build_operation_message(&cmd.base, operation, "OPERATION_TYPE_EDIT")?;
        Ok(request)
    }

    /// 构建删除消息的 StoreMessageRequest
    pub fn build_delete_request(cmd: &DeleteMessageCommand) -> Result<StoreMessage> {
        use flare_proto::common::{
            MessageOperation, OperationType,
            message_operation::OperationData, DeleteOperationData, DeleteType as ProtoDeleteType,
        };

        // 1. 构建 MessageOperation
        let delete_data = DeleteOperationData {
            delete_type: match cmd.delete_type {
                DeleteType::Soft => ProtoDeleteType::Soft as i32,
                DeleteType::Hard => ProtoDeleteType::Hard as i32,
            },
            reason: cmd.reason.clone().unwrap_or_default(),
            notify_others: cmd.notify_others,
        };

        let operation = MessageOperation {
            operation_type: OperationType::Delete as i32,
            target_message_id: cmd.base.message_id.clone(),
            operator_id: cmd.base.operator_id.clone(),
            timestamp: Some(prost_types::Timestamp {
                seconds: cmd.base.timestamp.timestamp(),
                nanos: cmd.base.timestamp.timestamp_subsec_nanos() as i32,
            }),
            operation_data: Some(OperationData::Delete(delete_data)),
            metadata: std::collections::HashMap::new(),
            notice_text: match cmd.delete_type {
                DeleteType::Hard => "消息已删除".to_string(),
                DeleteType::Soft => "消息已隐藏".to_string(),
            },
            show_notice: cmd.notify_others,
            target_user_id: String::new(),
        };

        // 2. 使用统一的辅助方法构建操作消息
        let mut request = Self::build_operation_message(&cmd.base, operation, "OPERATION_TYPE_DELETE")?;
        // 添加删除类型到 attributes
        if let Some(mut msg) = request.message.take() {
            msg.attributes.insert("delete_type".to_string(), format!("{:?}", cmd.delete_type));
            request.message = Some(msg);
        }
        Ok(request)
    }

    /// 构建置顶消息的 StoreMessageRequest
    pub fn build_pin_request(cmd: &PinMessageCommand) -> Result<StoreMessage> {
        use flare_proto::common::{
            MessageOperation, OperationType,
            message_operation::OperationData, PinOperationData,
        };

        let operation = MessageOperation {
            operation_type: OperationType::Pin as i32,
            target_message_id: cmd.base.message_id.clone(),
            operator_id: cmd.base.operator_id.clone(),
            timestamp: Some(prost_types::Timestamp {
                seconds: cmd.base.timestamp.timestamp(),
                nanos: cmd.base.timestamp.timestamp_subsec_nanos() as i32,
            }),
            operation_data: Some(OperationData::Pin(PinOperationData {
                reason: cmd.reason.clone().unwrap_or_default(),
                expire_at: cmd.expire_at.map(|dt| prost_types::Timestamp {
                    seconds: dt.timestamp(),
                    nanos: dt.timestamp_subsec_nanos() as i32,
                }),
            })),
            metadata: std::collections::HashMap::new(),
            notice_text: String::new(),
            show_notice: false,
            target_user_id: String::new(),
        };

        Self::build_operation_message(&cmd.base, operation, "OPERATION_TYPE_PIN")
    }

    /// 构建取消置顶消息的 StoreMessageRequest
    pub fn build_unpin_request(cmd: &UnpinMessageCommand) -> Result<StoreMessage> {
        use flare_proto::common::{
            MessageOperation, OperationType,
        };

        let operation = MessageOperation {
            operation_type: OperationType::Unpin as i32,
            target_message_id: cmd.base.message_id.clone(),
            operator_id: cmd.base.operator_id.clone(),
            timestamp: Some(prost_types::Timestamp {
                seconds: cmd.base.timestamp.timestamp(),
                nanos: cmd.base.timestamp.timestamp_subsec_nanos() as i32,
            }),
            operation_data: None, // Unpin 操作不需要额外的操作数据
            metadata: std::collections::HashMap::new(),
            notice_text: String::new(),
            show_notice: false,
            target_user_id: String::new(),
        };

        Self::build_operation_message(&cmd.base, operation, "OPERATION_TYPE_UNPIN")
    }

    /// 构建添加反应操作的 StoreMessageRequest
    pub fn build_add_reaction_request(cmd: &AddReactionCommand) -> Result<StoreMessage> {
        use flare_proto::common::{
            MessageOperation, OperationType,
            message_operation::OperationData, ReactionOperationData, ReactionAction,
        };

        let operation = MessageOperation {
            operation_type: OperationType::ReactionAdd as i32,
            target_message_id: cmd.base.message_id.clone(),
            operator_id: cmd.base.operator_id.clone(),
            timestamp: Some(prost_types::Timestamp {
                seconds: cmd.base.timestamp.timestamp(),
                nanos: cmd.base.timestamp.timestamp_subsec_nanos() as i32,
            }),
            operation_data: Some(OperationData::Reaction(ReactionOperationData {
                emoji: cmd.emoji.clone(),
                action: ReactionAction::Add as i32,
                count: 1,
            })),
            metadata: std::collections::HashMap::new(),
            notice_text: String::new(),
            show_notice: false,
            target_user_id: String::new(),
        };

        // 使用统一的辅助方法构建操作消息
        Self::build_operation_message(&cmd.base, operation, "OPERATION_TYPE_REACTION_ADD")
    }

    /// 构建移除反应操作的 StoreMessageRequest
    pub fn build_remove_reaction_request(cmd: &RemoveReactionCommand) -> Result<StoreMessage> {
        use flare_proto::common::{
            MessageOperation, OperationType,
            message_operation::OperationData, ReactionOperationData, ReactionAction,
        };

        let operation = MessageOperation {
            operation_type: OperationType::ReactionRemove as i32,
            target_message_id: cmd.base.message_id.clone(),
            operator_id: cmd.base.operator_id.clone(),
            timestamp: Some(prost_types::Timestamp {
                seconds: cmd.base.timestamp.timestamp(),
                nanos: cmd.base.timestamp.timestamp_subsec_nanos() as i32,
            }),
            operation_data: Some(OperationData::Reaction(ReactionOperationData {
                emoji: cmd.emoji.clone(),
                action: ReactionAction::Remove as i32,
                count: 0,
            })),
            metadata: std::collections::HashMap::new(),
            notice_text: String::new(),
            show_notice: false,
            target_user_id: String::new(),
        };

        // 使用统一的辅助方法构建操作消息
        Self::build_operation_message(&cmd.base, operation, "OPERATION_TYPE_REACTION_REMOVE")
    }

    /// 构建标记消息的 StoreMessageRequest
    pub fn build_mark_request(cmd: &MarkMessageCommand) -> Result<StoreMessage> {
        use flare_proto::common::{
            MessageOperation, OperationType,
            message_operation::OperationData, MarkOperationData,
        };

        let operation = MessageOperation {
            operation_type: OperationType::Mark as i32,
            target_message_id: cmd.base.message_id.clone(),
            operator_id: cmd.base.operator_id.clone(),
            timestamp: Some(prost_types::Timestamp {
                seconds: cmd.base.timestamp.timestamp(),
                nanos: cmd.base.timestamp.timestamp_subsec_nanos() as i32,
            }),
            operation_data: Some(OperationData::Mark(MarkOperationData {
                mark_type: cmd.mark_type,
                color: String::new(),
            })),
            metadata: std::collections::HashMap::new(),
            notice_text: String::new(),
            show_notice: false,
            target_user_id: String::new(),
        };

        Self::build_operation_message(&cmd.base, operation, "OPERATION_TYPE_MARK")
    }

    /// 通用方法：构建操作消息的 StoreMessage
    /// 
    /// **架构说明**：
    /// - 所有操作消息统一使用 `MessageType::Operation (302)` 和 `Content::Operation`
    /// - 直接使用 `MessageOperation` 结构，不包装在 `NotificationContent` 中
    /// - 这确保了操作消息的类型安全和架构一致性
    fn build_operation_message(
        base: &MessageOperationCommand,
        operation: MessageOperation,
        operation_type_label: &str,
    ) -> Result<StoreMessage> {
        let operation_id = format!("op-{}", Uuid::new_v4());
        let mut message = Message::default();
        message.server_id = operation_id.clone();
        message.conversation_id = base.conversation_id.clone();
        message.client_msg_id = format!("client-op-{}", Uuid::new_v4());
        message.sender_id = base.operator_id.clone();
        message.source = MessageSource::System as i32;
        message.seq = 0;
        message.timestamp = Some(prost_types::Timestamp {
            seconds: base.timestamp.timestamp(),
            nanos: base.timestamp.timestamp_subsec_nanos() as i32,
        });
        message.conversation_type = 0;
        message.message_type = MessageType::Operation as i32;  // **关键**：使用 Operation 类型 (302)
        message.business_type = "message_operation".to_string();
        message.receiver_id = String::new();
        message.channel_id = base.conversation_id.clone();
        message.content = Some(MessageContent {
            content: Some(
                flare_proto::common::message_content::Content::Operation(operation),  // **关键**：直接使用 Content::Operation
            ),
            extensions: vec![],
        });
        message.attributes = {
            let mut attrs = std::collections::HashMap::new();
            attrs.insert("operation_type".to_string(), operation_type_label.to_string());
            attrs.insert("target_message_id".to_string(), base.message_id.clone());
            attrs
        };
        message.extra = {
            let mut extra = std::collections::HashMap::new();
            extra.insert("is_operation_message".to_string(), "true".to_string());
            extra.insert("operation_type".to_string(), operation_type_label.to_string());
            extra
        };
        message.tags = vec![];

        Ok(StoreMessage {
            conversation_id: base.conversation_id.clone(),
            message: Some(message),
            sync: false,
            tags: std::collections::HashMap::new(),
            metadata: std::collections::HashMap::new(),
        })
    }
}

