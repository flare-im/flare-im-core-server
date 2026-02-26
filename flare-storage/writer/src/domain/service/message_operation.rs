//! 消息操作领域服务 - 处理消息操作（撤回、编辑、删除等）
//!
//! 职责：
//! - 识别操作消息
//! - 提取 MessageOperation
//! - 根据操作类型更新数据库

use anyhow::{Context, Result, anyhow};
use flare_proto::common::{
    Message, MessageOperation, OperationType,
    message_operation::OperationData,
};
use std::sync::Arc;
use tracing::{instrument, warn};

use crate::domain::repository::ArchiveStoreRepository;

/// 消息操作领域服务
pub struct MessageOperationDomainService {
    archive_repo: Option<Arc<dyn ArchiveStoreRepository + Send + Sync>>,
}

impl MessageOperationDomainService {
    pub fn new(archive_repo: Option<Arc<dyn ArchiveStoreRepository + Send + Sync>>) -> Self {
        Self { archive_repo }
    }

    /// 检查消息是否为操作消息
    /// 
    /// 操作消息使用 MessageType::Operation (302) 和 Content::Operation
    pub fn is_operation_message(message: &Message) -> bool {
        let message_type = message.message_type;
        let expected_type = flare_proto::MessageType::Operation as i32;
        
        // 检查消息类型是否为 Operation (302)
        if message_type != expected_type {
            tracing::debug!(
                message_id = %message.server_id,
                message_type = message_type,
                expected_type = expected_type,
                "❌ 消息类型不匹配（期望 Operation=302，实际={})",
                message_type
            );
            return false;
        }
        
        // 检查内容是否为 Content::Operation
        let has_operation_content = message
            .content
            .as_ref()
            .and_then(|c| c.content.as_ref())
            .map(|c| matches!(c, flare_proto::common::message_content::Content::Operation(_)))
            .unwrap_or(false);
        
        if !has_operation_content {
            let content_variant = message
                .content
                .as_ref()
                .and_then(|c| c.content.as_ref())
                .map(|c| match c {
                    flare_proto::common::message_content::Content::Operation(_) => "Operation",
                    flare_proto::common::message_content::Content::Notification(_) => "Notification",
                    flare_proto::common::message_content::Content::Text(_) => "Text",
                    _ => "Other",
                })
                .unwrap_or("None");
            
            tracing::debug!(
                message_id = %message.server_id,
                message_type = message_type,
                content_variant = content_variant,
                "❌ 消息类型是 Operation(302)，但内容不是 Content::Operation（实际内容类型: {})",
                content_variant
            );
        } else {
            tracing::debug!(
                message_id = %message.server_id,
                message_type = message_type,
                "✅ 消息是操作消息（message_type=302 且 content=Operation）"
            );
        }
        
        has_operation_content
    }

    /// 从消息中提取 MessageOperation
    /// 
    /// 操作消息使用 Content::Operation，直接包含 MessageOperation
    pub fn extract_operation_from_message(message: &Message) -> Result<Option<MessageOperation>> {
        if !Self::is_operation_message(message) {
            tracing::debug!(
                message_id = %message.server_id,
                "⚠️ extract_operation_from_message: is_operation_message 返回 false"
            );
            return Ok(None);
        }

        // 直接从 Content::Operation 中提取 MessageOperation
        if let Some(ref content) = message.content {
            if let Some(flare_proto::common::message_content::Content::Operation(operation)) =
                &content.content
            {
                tracing::debug!(
                    message_id = %message.server_id,
                    operation_type = operation.operation_type,
                    "✅ 成功从 Content::Operation 中提取 MessageOperation"
                );
                // Content::Operation 直接包含 MessageOperation，无需反序列化
                return Ok(Some(operation.clone()));
            } else {
                let content_variant = content.content.as_ref().map(|c| match c {
                    flare_proto::common::message_content::Content::Operation(_) => "Operation",
                    flare_proto::common::message_content::Content::Notification(_) => "Notification",
                    _ => "Other",
                }).unwrap_or("None");
                
                tracing::warn!(
                    message_id = %message.server_id,
                    content_variant = content_variant,
                    "⚠️ extract_operation_from_message: 内容不是 Content::Operation（实际: {})",
                    content_variant
                );
            }
        } else {
            tracing::warn!(
                message_id = %message.server_id,
                "⚠️ extract_operation_from_message: message.content 为 None"
            );
        }

        Ok(None)
    }

    /// 处理消息操作（撤回、编辑、删除等）
    #[instrument(skip(self), fields(operation_type = %operation.operation_type))]
    pub async fn process_operation(
        &self,
        operation: MessageOperation,
        message: &Message,
    ) -> Result<()> {
        let archive_repo = self
            .archive_repo
            .as_ref()
            .ok_or_else(|| anyhow!("Archive repository not configured"))?;

        match OperationType::try_from(operation.operation_type) {
            Ok(OperationType::Recall) => {
                self.handle_recall_operation(&operation, archive_repo).await
            }
            Ok(OperationType::Edit) => self.handle_edit_operation(&operation, archive_repo).await,
            Ok(OperationType::Delete) => {
                self.handle_delete_operation(&operation, archive_repo).await
            }
            Ok(OperationType::Read) => {
                self.handle_read_operation(&operation, archive_repo).await
            }
            Ok(OperationType::ReactionAdd) | Ok(OperationType::ReactionRemove) => {
                self.handle_reaction_operation(&operation, archive_repo).await
            }
            Ok(OperationType::Pin) | Ok(OperationType::Unpin) => {
                self.handle_pin_operation(&operation, message, archive_repo).await
            }
            Ok(OperationType::Mark) => {
                self.handle_mark_operation(&operation, message, archive_repo).await
            }
            Ok(OperationType::Unmark) => {
                self.handle_unmark_operation(&operation, message, archive_repo).await
            }
            _ => {
                warn!(
                    operation_type = operation.operation_type,
                    "Unsupported operation type, skipping"
                );
                Ok(())
            }
        }
    }

    /// 处理撤回操作
    #[instrument(skip(self, archive_repo), fields(message_id = %operation.target_message_id))]
    async fn handle_recall_operation(
        &self,
        operation: &MessageOperation,
        archive_repo: &Arc<dyn ArchiveStoreRepository + Send + Sync>,
    ) -> Result<()> {
        let message_id = &operation.target_message_id;

        let recall_reason = match &operation.operation_data {
            Some(OperationData::Recall(recall_data)) => {
                if recall_data.reason.is_empty() {
                    None
                } else {
                    Some(recall_data.reason.as_str())
                }
            }
            _ => None,
        };

        archive_repo
            .update_message_fsm_state(message_id, "RECALLED", recall_reason)
            .await?;

        archive_repo.append_operation(message_id, operation).await?;

        Ok(())
    }

    /// 处理编辑操作
    ///
    /// 功能：
    /// 1. 更新消息内容并保存编辑历史（权限验证已在 Orchestrator 完成）
    /// 2. 追加操作记录
    ///
    /// 注意：Writer 只负责数据持久化，不包含业务逻辑（权限验证、时间限制等）
    /// 所有业务逻辑应在 Orchestrator 层完成，以便快速失败并返回错误给客户端
    #[instrument(skip(self, archive_repo), fields(message_id = %operation.target_message_id))]
    async fn handle_edit_operation(
        &self,
        operation: &MessageOperation,
        archive_repo: &Arc<dyn ArchiveStoreRepository + Send + Sync>,
    ) -> Result<()> {
        let message_id = &operation.target_message_id;
        
        tracing::info!(
            target_message_id = %message_id,
            operator_id = %operation.operator_id,
            operation_type = operation.operation_type,
            "📝 开始处理编辑操作，target_message_id 必须与数据库中的 server_id 匹配"
        );

        // 从 operation_data 中提取编辑后的内容
        let edit_data = match &operation.operation_data {
            Some(OperationData::Edit(data)) => data,
            _ => return Err(anyhow!("Edit operation requires EditOperationData")),
        };

        let new_content_bytes = &edit_data.new_content;

        // 解码 new_content 从 &[u8] 到 MessageContent（使用统一的解码方法）
        let new_content = flare_proto::decode_message_content(new_content_bytes.as_slice())
            .context("Failed to decode new_content as MessageContent")?;

        // 1. 更新消息内容（内部会验证版本号并保存编辑历史）
        let reason = if edit_data.reason.is_empty() {
            None
        } else {
            Some(edit_data.reason.as_str())
        };

        archive_repo
            .update_message_content(
                message_id,
                &new_content,
                edit_data.edit_version,
                &operation.operator_id,
                reason,
            )
            .await?;

        // 2. 追加操作记录
        archive_repo.append_operation(message_id, operation).await?;

        Ok(())
    }

    /// 处理删除操作（硬删除和软删除）
    #[instrument(skip(self, archive_repo), fields(message_id = %operation.target_message_id))]
    async fn handle_delete_operation(
        &self,
        operation: &MessageOperation,
        archive_repo: &Arc<dyn ArchiveStoreRepository + Send + Sync>,
    ) -> Result<()> {
        let message_id = &operation.target_message_id;
        let user_id = &operation.operator_id;

        if let Some(OperationData::Delete(delete_data)) = &operation.operation_data {
            if delete_data.delete_type == flare_proto::common::DeleteType::Hard as i32 {
                // 硬删除：更新消息状态为 DELETED_HARD
                archive_repo
                    .update_message_fsm_state(message_id, "DELETED_HARD", None)
                    .await?;

                archive_repo.append_operation(message_id, operation).await?;
            } else {
                // 软删除：更新 message_visibility 表
                archive_repo
                    .update_message_visibility(message_id, user_id, "DELETED")
                    .await?;

                archive_repo.append_operation(message_id, operation).await?;
            }
        } else {
            return Err(anyhow!("Delete operation requires DeleteOperationData"));
        }

        Ok(())
    }

    /// 处理已读操作
    #[instrument(skip(self, archive_repo), fields(message_id = %operation.target_message_id))]
    async fn handle_read_operation(
        &self,
        operation: &MessageOperation,
        archive_repo: &Arc<dyn ArchiveStoreRepository + Send + Sync>,
    ) -> Result<()> {
        let message_id = &operation.target_message_id;
        let user_id = &operation.operator_id;

        archive_repo.record_message_read(message_id, user_id).await?;
        archive_repo.append_operation(message_id, operation).await?;

        Ok(())
    }

    /// 处理反应操作
    #[instrument(skip(self, archive_repo), fields(message_id = %operation.target_message_id))]
    async fn handle_reaction_operation(
        &self,
        operation: &MessageOperation,
        archive_repo: &Arc<dyn ArchiveStoreRepository + Send + Sync>,
    ) -> Result<()> {
        let message_id = &operation.target_message_id;
        let user_id = &operation.operator_id;

        if let Some(OperationData::Reaction(reaction_data)) = &operation.operation_data {
            let add = operation.operation_type == OperationType::ReactionAdd as i32;
            archive_repo
                .upsert_message_reaction(message_id, &reaction_data.emoji, user_id, add)
                .await?;
        } else {
            return Err(anyhow!("Reaction operation requires ReactionOperationData"));
        }

        archive_repo.append_operation(message_id, operation).await?;

        Ok(())
    }

    /// 处理置顶操作
    #[instrument(skip(self, archive_repo), fields(message_id = %operation.target_message_id))]
    async fn handle_pin_operation(
        &self,
        operation: &MessageOperation,
        message: &Message,
        archive_repo: &Arc<dyn ArchiveStoreRepository + Send + Sync>,
    ) -> Result<()> {
        let message_id = &operation.target_message_id;
        let user_id = &operation.operator_id;
        let conversation_id = &message.conversation_id;

        let pin = operation.operation_type == OperationType::Pin as i32;

        if let Some(OperationData::Pin(pin_data)) = &operation.operation_data {
            let expire_at = pin_data
                .expire_at
                .as_ref()
                .and_then(|ts| {
                    flare_im_core::utils::timestamp_to_datetime(ts)
                });

            let reason = if pin_data.reason.is_empty() {
                None
            } else {
                Some(pin_data.reason.as_str())
            };

            archive_repo
                .pin_message(
                    message_id,
                    conversation_id,
                    user_id,
                    pin,
                    expire_at,
                    reason,
                )
                .await?;
        } else {
            return Err(anyhow!("Pin operation requires PinOperationData"));
        }

        archive_repo.append_operation(message_id, operation).await?;

        Ok(())
    }

    /// 处理标记操作
    #[instrument(skip(self, archive_repo), fields(message_id = %operation.target_message_id))]
    async fn handle_mark_operation(
        &self,
        operation: &MessageOperation,
        message: &Message,
        archive_repo: &Arc<dyn ArchiveStoreRepository + Send + Sync>,
    ) -> Result<()> {
        let message_id = &operation.target_message_id;
        let user_id = &operation.operator_id;
        let conversation_id = &message.conversation_id;

        if let Some(OperationData::Mark(mark_data)) = &operation.operation_data {
            let mark_type = match mark_data.mark_type {
                x if x == flare_proto::common::MarkType::Important as i32 => "IMPORTANT",
                x if x == flare_proto::common::MarkType::Todo as i32 => "TODO",
                x if x == flare_proto::common::MarkType::Done as i32 => "DONE",
                x if x == flare_proto::common::MarkType::Custom as i32 => "CUSTOM",
                _ => return Err(anyhow!("Invalid mark type")),
            };

            let color = if mark_data.color.is_empty() {
                None
            } else {
                Some(mark_data.color.as_str())
            };

            archive_repo
                .mark_message(
                    message_id,
                    conversation_id,
                    user_id,
                    mark_type,
                    color,
                    true,
                )
                .await?;
        } else {
            return Err(anyhow!("Mark operation requires MarkOperationData"));
        }

        archive_repo.append_operation(message_id, operation).await?;

        Ok(())
    }

    /// 处理取消标记操作
    #[instrument(skip(self, archive_repo), fields(message_id = %operation.target_message_id))]
    async fn handle_unmark_operation(
        &self,
        operation: &MessageOperation,
        message: &Message,
        archive_repo: &Arc<dyn ArchiveStoreRepository + Send + Sync>,
    ) -> Result<()> {
        let message_id = &operation.target_message_id;
        let user_id = &operation.operator_id;
        let conversation_id = &message.conversation_id;

        if let Some(OperationData::Unmark(unmark_data)) = &operation.operation_data {
            let opt_mark_type = match unmark_data.mark_type {
                x if x == flare_proto::common::MarkType::Important as i32 => Some("IMPORTANT"),
                x if x == flare_proto::common::MarkType::Todo as i32 => Some("TODO"),
                x if x == flare_proto::common::MarkType::Done as i32 => Some("DONE"),
                x if x == flare_proto::common::MarkType::Custom as i32 => Some("CUSTOM"),
                _ => None, // 未指定类型：取消该用户对该消息的所有标记
            };

            if let Some(mark_type) = opt_mark_type {
                archive_repo
                    .mark_message(
                        message_id,
                        conversation_id,
                        user_id,
                        mark_type,
                        None,
                        false,
                    )
                    .await?;
            } else {
                for mt in ["IMPORTANT", "TODO", "DONE", "CUSTOM"] {
                    let _ = archive_repo
                        .mark_message(
                            message_id,
                            conversation_id,
                            user_id,
                            mt,
                            None,
                            false,
                        )
                        .await;
                }
            }
        } else {
            return Err(anyhow!("Unmark operation requires UnmarkOperationData"));
        }

        archive_repo.append_operation(message_id, operation).await?;
        Ok(())
    }
}
