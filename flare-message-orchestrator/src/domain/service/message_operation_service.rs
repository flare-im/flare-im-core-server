//! 消息操作服务（Message Operation Service）
//!
//! 负责处理消息操作命令，执行FSM状态迁移，发布领域事件

use chrono::Utc;
use std::sync::Arc;
use tracing::instrument;

use crate::error::{FlareError, Result, MessageOperationErrorBuilder};

use crate::application::commands::{
    AddReactionCommand, DeleteMessageCommand, DeleteType, EditMessageCommand,
    MarkMessageCommand, PinMessageCommand, ReadMessageCommand,
    RecallMessageCommand, RemoveReactionCommand, UnmarkMessageCommand,
    UnpinMessageCommand,
};
use crate::domain::event::{
    MessageDeletedEvent, MessageEditedEvent, MessageFavoritedEvent, MessagePinnedEvent,
    MessageReadEvent, MessageRecalledEvent, MessageReactionAddedEvent,
    MessageReactionRemovedEvent, MessageUnfavoritedEvent, MessageUnpinnedEvent,
    MessageOperationEvent,
};
use crate::domain::model::{Message, MessageFsmState};
use crate::domain::repository::WalRepository;
use crate::infrastructure::persistence::wal_repository_impl::WalRepositoryItem;
use crate::domain::service::message_operation_builder::MessageOperationBuilder;
use flare_proto::MessageContentExt;

/// 消息仓储接口（用于查询和保存消息）
#[async_trait::async_trait]
pub trait MessageRepository: Send + Sync {
    /// 根据消息ID查询消息
    async fn find_by_id(&self, message_id: &str) -> Result<Option<Message>>;

    /// 保存消息
    async fn save(&self, message: &Message) -> Result<()>;
}

/// 事件发布器接口
#[async_trait::async_trait]
pub trait EventPublisher: Send + Sync {
    /// 发布消息撤回事件
    async fn publish_recalled(&self, event: &MessageRecalledEvent) -> crate::error::Result<()>;

    /// 发布消息编辑事件
    async fn publish_edited(&self, event: &MessageEditedEvent) -> crate::error::Result<()>;

    /// 发布消息删除事件
    async fn publish_deleted(&self, event: &MessageDeletedEvent) -> crate::error::Result<()>;

    /// 发布消息已读事件
    async fn publish_read(&self, event: &MessageReadEvent) -> crate::error::Result<()>;

    /// 发布消息反应添加事件
    async fn publish_reaction_added(&self, event: &MessageReactionAddedEvent) -> crate::error::Result<()>;

    /// 发布消息反应移除事件
    async fn publish_reaction_removed(&self, event: &MessageReactionRemovedEvent) -> crate::error::Result<()>;

    /// 发布消息置顶事件
    async fn publish_pinned(&self, event: &MessagePinnedEvent) -> crate::error::Result<()>;

    /// 发布消息取消置顶事件
    async fn publish_unpinned(&self, event: &MessageUnpinnedEvent) -> crate::error::Result<()>;

    /// 发布消息收藏事件
    async fn publish_favorited(&self, event: &MessageFavoritedEvent) -> crate::error::Result<()>;

    /// 发布消息取消收藏事件
    async fn publish_unfavorited(&self, event: &MessageUnfavoritedEvent) -> crate::error::Result<()>;
}

/// 消息操作服务
pub struct MessageOperationService {
    message_repo: Arc<dyn MessageRepository>,
    event_publisher: Arc<dyn EventPublisher>,
    kafka_publisher: Arc<dyn crate::domain::repository::message_publisher::MessageEventPublisher>,
    wal_repository: Option<Arc<crate::infrastructure::persistence::wal_repository_impl::WalRepositoryItem>>,
}

impl MessageOperationService {
    pub fn new(
        message_repo: Arc<dyn MessageRepository>,
        event_publisher: Arc<dyn EventPublisher>,
        kafka_publisher: Arc<dyn crate::domain::repository::message_publisher::MessageEventPublisher>,
        wal_repository: Option<Arc<crate::infrastructure::persistence::wal_repository_impl::WalRepositoryItem>>,
    ) -> Self {
        Self {
            message_repo,
            event_publisher,
            kafka_publisher,
            wal_repository,
        }
    }

    #[instrument(skip(self), fields(message_id = %cmd.base.message_id, operator_id = %cmd.base.operator_id))]
    pub async fn handle_recall(&self, cmd: RecallMessageCommand) -> Result<()> {
        // 1. 查询原消息（用于权限验证和快速失败）
        // 策略：先查 Reader（已持久化的消息），如果查不到，再查 WAL（刚发送但未持久化的消息）
        let mut original_message = self
            .message_repo
            .find_by_id(&cmd.base.message_id)
            .await?;

        // 如果 Reader 查询不到，尝试从 WAL 查询
        if original_message.is_none() {
            tracing::debug!(
                message_id = %cmd.base.message_id,
                "Message not found in Reader, trying WAL fallback"
            );
            if let Some(wal_repo) = &self.wal_repository {
                match wal_repo.find_by_message_id(&cmd.base.message_id).await {
                    Ok(Some(proto_message)) => {
                        // 将 Proto Message 转换为 Domain Message (简化转换用于校验)
                        let timestamp = proto_message.timestamp
                            .map(|ts| {
                                chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
                                    .unwrap_or_else(Utc::now)
                            })
                            .unwrap_or_else(Utc::now);

                        original_message = Some(Message {
                            server_id: proto_message.server_id.clone(),
                            conversation_id: proto_message.conversation_id.clone(),
                            sender_id: proto_message.sender_id.clone(),
                            receiver_id: proto_message.receiver_id.clone(),
                            content: vec![], // 校验不需要内容
                            timestamp,
                            fsm_state: MessageFsmState::Sent, // 假设是 Sent，具体状态校验在下面
                            fsm_state_changed_at: timestamp,
                            edit_version: 0,
                            edit_history: vec![],
                            extra: proto_message.extra,
                            updated_at: timestamp,
                        });
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!("Failed to query WAL: {}", e);
                    }
                }
            }
        }

        let message = original_message.ok_or_else(|| MessageOperationErrorBuilder::message_not_found(&cmd.base.message_id))?;

        // 2. 校验权限：只有发送者可以撤回
        if message.sender_id != cmd.base.operator_id {
            return Err(MessageOperationErrorBuilder::permission_denied(
                "recall", 
                &cmd.base.operator_id
            ));
        }

        // 3. 校验时间限制 (默认 2 分钟)
        let time_limit = chrono::Duration::seconds(120);
        if Utc::now() - message.timestamp > time_limit {
             return Err(MessageOperationErrorBuilder::recall_timeout(&cmd.base.message_id));
        }

        // 4. 构建操作消息并发布到 Kafka
        let store_request = MessageOperationBuilder::build_recall_request(&cmd)
            .map_err(|e| FlareError::system(format!("Failed to build recall request: {}", e)))?;
        
        self.kafka_publisher
            .publish_operation(store_request)
            .await
            .map_err(|e| FlareError::system(format!("Failed to publish recall operation to Kafka: {}", e)))?;

        // 5. 发布领域事件
        let event = MessageRecalledEvent {
            base: MessageOperationEvent {
                message_id: cmd.base.message_id.clone(),
                conversation_id: cmd.base.conversation_id.clone(),
                operator_id: cmd.base.operator_id.clone(),
                timestamp: cmd.base.timestamp,
                tenant_id: cmd.base.tenant_id.clone(),
            },
            reason: cmd.reason.clone(),
            new_state: MessageFsmState::Recalled,
        };
        self.event_publisher.publish_recalled(&event).await?;

        // 6. 发送推送通知 (Sync)
        // 构造 Proto Message 用于推送
        use flare_proto::common::{Message as ProtoMessage, MessageStatus};
        use flare_proto::push::{PushMessageRequest, PushOptions};

        let mut proto_msg = ProtoMessage::default();
        proto_msg.server_id = message.server_id.clone();
        proto_msg.conversation_id = message.conversation_id.clone();
        proto_msg.sender_id = message.sender_id.clone();
        proto_msg.receiver_id = message.receiver_id.clone();
        // 撤回后的消息内容通常为空或特定提示，这里保持为空，客户端根据 status 判断
        proto_msg.status = MessageStatus::Recalled as i32;
        proto_msg.is_recalled = true;
        proto_msg.recalled_at = Some(prost_types::Timestamp::from(std::time::SystemTime::now()));
        proto_msg.recall_reason = cmd.reason.clone().unwrap_or_default();
        proto_msg.extra = message.extra.clone();
        
        // 确定接收者 ID
        let mut user_ids = Vec::new();
        if !message.receiver_id.is_empty() {
             user_ids.push(message.receiver_id.clone());
        }
        // 注意：如果是群聊，user_ids 为空，Push Worker 会处理

        let push_req = PushMessageRequest {
            user_ids,
            message: Some(proto_msg),
            options: Some(PushOptions {
                persist_if_offline: true, // 撤回必须持久化通知
                ..Default::default()
            }),
            template_id: String::new(),
            template_data: Default::default(),
        };

        self.kafka_publisher
            .publish_push(push_req)
            .await
            .map_err(|e| FlareError::system(format!("Failed to publish push notification: {}", e)))?;

        Ok(())
    }

    #[instrument(skip(self), fields(message_id = %cmd.base.message_id, operator_id = %cmd.base.operator_id))]
    pub async fn handle_edit(&self, cmd: EditMessageCommand) -> Result<()> {
        // 1. 查询原消息（用于权限验证和快速失败）
        // 策略：先查 Reader（已持久化的消息），如果查不到，再查 WAL（刚发送但未持久化的消息）
        let mut original_message = self
            .message_repo
            .find_by_id(&cmd.base.message_id)
            .await?;

        // 如果 Reader 查询不到，尝试从 WAL 查询（解决时序问题：消息刚发送但未持久化）
        if original_message.is_none() {
            tracing::debug!(
                message_id = %cmd.base.message_id,
                "Message not found in Reader, trying WAL fallback"
            );
            if let Some(wal_repo) = &self.wal_repository {
                match wal_repo.find_by_message_id(&cmd.base.message_id).await {
                    Ok(Some(proto_message)) => {
                        tracing::info!(
                            message_id = %cmd.base.message_id,
                            "✅ Found message in WAL, using for permission validation"
                        );
                    // 将 Proto Message 转换为 Domain Message
                    use crate::domain::model::MessageFsmState;
                    use chrono::{DateTime, Utc};
                    
                    let fsm_state = if proto_message.is_recalled {
                        MessageFsmState::Recalled
                    } else if proto_message.status == flare_proto::common::MessageStatus::DeletedHard as i32 {
                        MessageFsmState::DeletedHard
                    } else {
                        MessageFsmState::from_str(
                            proto_message.extra.get("message_fsm_state")
                                .map(|s| s.as_str())
                                .unwrap_or("SENT")
                        ).unwrap_or(MessageFsmState::Sent)
                    };

                    let timestamp = proto_message.timestamp
                        .map(|ts| {
                            DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
                                .unwrap_or_else(Utc::now)
                        })
                        .unwrap_or_else(Utc::now);

                    let content_bytes = proto_message.content
                        .as_ref()
                        .and_then(|c| c.encode_to_bytes().ok())
                        .unwrap_or_default();

                    original_message = Some(Message {
                        server_id: proto_message.server_id.clone(),
                        conversation_id: proto_message.conversation_id.clone(),
                        sender_id: proto_message.sender_id.clone(),
                        receiver_id: proto_message.receiver_id.clone(),
                        content: content_bytes,
                        timestamp,
                        fsm_state,
                        fsm_state_changed_at: timestamp,
                        edit_version: proto_message.extra.get("current_edit_version")
                            .and_then(|v| v.parse::<i32>().ok())
                            .unwrap_or(0),
                        edit_history: vec![],
                        extra: proto_message.extra,
                        updated_at: timestamp,
                    });
                    }
                    Ok(None) => {
                        tracing::debug!(
                            message_id = %cmd.base.message_id,
                            "Message not found in WAL either"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            message_id = %cmd.base.message_id,
                            error = %e,
                            "Failed to query WAL for message"
                        );
                    }
                }
            } else {
                tracing::debug!(
                    message_id = %cmd.base.message_id,
                    "WAL repository not configured, cannot use fallback"
                );
            }
        }

        // 如果 Reader 和 WAL 都查询不到，可能是时序问题（消息刚发送但未持久化）
        // 允许操作继续，由 Storage Writer 处理验证和持久化
        let (original_message, skip_permission_check) = match original_message {
            Some(msg) => (msg, false),
            None => {
                let wal_configured = self.wal_repository.is_some();
                if !wal_configured {
                    tracing::error!(
                        message_id = %cmd.base.message_id,
                        "WAL not configured (wal_hash_key is None). Cannot validate edit permissions. Please configure WAL or wait for message to be persisted."
                    );
                    return Err(FlareError::system(
                        "Message not found and WAL not configured. Cannot validate edit permissions. Please configure WAL (MESSAGE_ORCHESTRATOR_WAL_HASH_KEY) or wait for message to be persisted."
                    ));
                } else {
                    tracing::warn!(
                        message_id = %cmd.base.message_id,
                        "Message not found in Reader or WAL. This may be a timing issue (message just sent but not yet persisted). Allowing operation to proceed - Storage Writer will handle validation."
                    );
                    use crate::domain::model::{Message, MessageFsmState};
                    use chrono::Utc;
                    let temp_message = Message {
                        server_id: cmd.base.message_id.clone(),
                        conversation_id: cmd.base.conversation_id.clone(),
                        sender_id: cmd.base.operator_id.clone(),
                        receiver_id: String::new(),
                        content: vec![],
                        timestamp: Utc::now(),
                        fsm_state: MessageFsmState::Sent,
                        fsm_state_changed_at: Utc::now(),
                        edit_version: 0,
                        extra: std::collections::HashMap::new(),
                        edit_history: vec![],
                        updated_at: Utc::now(),
                    };
                    (temp_message, true)
                }
            }
        };

        // 2. 验证权限（只有发送者可以编辑）
        if !skip_permission_check && original_message.sender_id != cmd.base.operator_id {
            return Err(MessageOperationErrorBuilder::permission_denied(
                "edit", 
                &cmd.base.operator_id
            ));
        }

        // 2.1. 如果命令中没有 conversation_id，从查询到的消息中获取
        let mut cmd = cmd;
        if cmd.base.conversation_id.is_empty() {
            cmd.base.conversation_id = original_message.conversation_id.clone();
        }

        // 2.2. 确保使用服务端返回的 server_msg_id（从查询到的消息中获取）
        // 如果查询到的消息的 server_id 与命令中的 message_id 不同，使用查询到的 server_id
        if original_message.server_id != cmd.base.message_id {
            tracing::info!(
                command_message_id = %cmd.base.message_id,
                actual_server_id = %original_message.server_id,
                "Using actual server_id from queried message instead of command message_id"
            );
            cmd.base.message_id = original_message.server_id.clone();
        }

        // 3. 构建操作消息并发布到 Kafka（权限已验证，Writer 只负责写入）
        let store_request = MessageOperationBuilder::build_edit_request(&cmd)
            .map_err(|e| FlareError::system(format!("Failed to build edit request: {}", e)))?;
        
        self.kafka_publisher
            .publish_operation(store_request)
            .await
            .map_err(|e| FlareError::system(format!("Failed to publish edit operation to Kafka: {}", e)))?;

        // 构建新的编辑历史
        use crate::domain::model::message_fsm::EditHistoryEntry;
        let mut new_edit_history = original_message.edit_history.clone();
        let new_edit_version = original_message.edit_version + 1;
        
        new_edit_history.push(EditHistoryEntry {
            edit_version: new_edit_version,
            content_encoded: original_message.content.clone(), // 保存旧内容
            edited_at: Utc::now(),
            editor_id: cmd.base.operator_id.clone(),
            reason: cmd.reason.clone(),
        });

        // 4. 发布领域事件（用于推送通知）
        let event = MessageEditedEvent {
            base: MessageOperationEvent {
                message_id: cmd.base.message_id.clone(),
                conversation_id: cmd.base.conversation_id.clone(),
                operator_id: cmd.base.operator_id.clone(),
                timestamp: cmd.base.timestamp,
                tenant_id: cmd.base.tenant_id.clone(),
            },
            edit_version: new_edit_version,
            new_state: MessageFsmState::Edited,
            reason: cmd.reason.clone(),
            new_content: cmd.new_content.clone(),
            edit_history: new_edit_history.clone(),
        };
        self.event_publisher.publish_edited(&event).await?;

        // 5. 发布推送通知（PushMessageRequest）
        // 构造完整的 Proto Message，包含最新的 content 和 edit_history
        use flare_proto::common::{Message as ProtoMessage, MessageStatus};
        use flare_proto::common::EditHistory as ProtoEditHistory;
        use flare_proto::push::{PushMessageRequest, PushOptions};
        
        // 转换 edit_history
        let proto_edit_history: Vec<ProtoEditHistory> = new_edit_history.iter().map(|e| {
             // Decode content bytes to MessageContent for Proto
             let content = flare_proto::decode_message_content(&e.content_encoded).unwrap_or_default();
             ProtoEditHistory {
                 edit_version: e.edit_version,
                 content: Some(content),
                 edited_at: Some(prost_types::Timestamp::from(std::time::SystemTime::from(e.edited_at))),
                 editor_id: e.editor_id.clone(),
                 reason: e.reason.clone().unwrap_or_default(),
                 show_edited_mark: true,
             }
        }).collect();

        // 构造 Proto Message
        let new_content_proto = flare_proto::decode_message_content(&cmd.new_content).unwrap_or_default();
        
        // **关键修复**：更新 extra 中的 content_text，确保推送通知包含最新的文本预览
        let mut extra = original_message.extra.clone();
        if let Some(flare_proto::common::message_content::Content::Text(text)) = &new_content_proto.content {
            extra.insert("content_text".to_string(), text.text.clone());
        }

        let mut proto_msg = ProtoMessage::default();
        proto_msg.server_id = original_message.server_id.clone();
        proto_msg.conversation_id = original_message.conversation_id.clone();
        proto_msg.sender_id = original_message.sender_id.clone();
        proto_msg.receiver_id = original_message.receiver_id.clone();
        proto_msg.content = Some(new_content_proto);
        proto_msg.edit_history = proto_edit_history;
        proto_msg.current_edit_version = new_edit_version;
        proto_msg.last_edited_at = Some(prost_types::Timestamp::from(std::time::SystemTime::now()));
        proto_msg.status = MessageStatus::Sent as i32; // 默认为 SENT
        proto_msg.extra = extra; // 设置更新后的 extra

        // 确定接收者 ID
        let mut user_ids = Vec::new();
        // 单聊：添加接收者
        if !original_message.receiver_id.is_empty() {
             user_ids.push(original_message.receiver_id.clone());
        }
        // 群聊：user_ids 为空，由 Push Worker 查询成员

        let push_req = PushMessageRequest {
            user_ids,
            message: Some(proto_msg),
            options: Some(PushOptions {
                persist_if_offline: true, // 编辑操作需要持久化通知
                ..Default::default()
            }),
            template_id: String::new(),
            template_data: Default::default(),
        };

        self.kafka_publisher
            .publish_push(push_req)
            .await
            .map_err(|e| FlareError::system(format!("Failed to publish push notification: {}", e)))?;

        Ok(())
    }

    #[instrument(skip(self), fields(message_id = %cmd.base.message_id, operator_id = %cmd.base.operator_id))]
    pub async fn handle_delete(&self, cmd: DeleteMessageCommand) -> Result<()> {
        // **关键修复**：删除操作（硬删除和软删除）都应该发布到操作队列
                let store_request = MessageOperationBuilder::build_delete_request(&cmd)
                    .map_err(|e| FlareError::system(format!("Failed to build delete request: {}", e)))?;
                
                self.kafka_publisher
            .publish_operation(store_request)
                    .await
                    .map_err(|e| FlareError::system(format!("Failed to publish delete operation to Kafka: {}", e)))?;

                let event = MessageDeletedEvent {
                    base: MessageOperationEvent {
                        message_id: cmd.base.message_id.clone(),
                        conversation_id: cmd.base.conversation_id.clone(),
                        operator_id: cmd.base.operator_id.clone(),
                        timestamp: cmd.base.timestamp,
                        tenant_id: cmd.base.tenant_id.clone(),
                    },
            delete_type: match cmd.delete_type {
                DeleteType::Hard => "HARD",
                DeleteType::Soft => "SOFT",
            }
            .to_string(),
            new_state: match cmd.delete_type {
                DeleteType::Hard => Some(MessageFsmState::DeletedHard),
                DeleteType::Soft => None,
            },
                    target_user_id: Some(String::new()),
                };
                self.event_publisher.publish_deleted(&event).await?;

        Ok(())
    }

    #[instrument(skip(self), fields(operator_id = %cmd.base.operator_id))]
    pub async fn handle_read(&self, cmd: ReadMessageCommand) -> Result<()> {
        let event = MessageReadEvent {
            base: MessageOperationEvent {
                message_id: cmd.base.message_id.clone(),
                conversation_id: cmd.base.conversation_id.clone(),
                operator_id: cmd.base.operator_id.clone(),
                timestamp: cmd.base.timestamp,
                tenant_id: cmd.base.tenant_id.clone(),
            },
            message_ids: cmd.message_ids.clone(),
            read_at: cmd.read_at.unwrap_or_else(|| Utc::now()),
        };
        self.event_publisher.publish_read(&event).await?;

        Ok(())
    }

    #[instrument(skip(self), fields(message_id = %cmd.base.message_id, emoji = %cmd.emoji))]
    pub async fn handle_add_reaction(&self, cmd: AddReactionCommand) -> Result<i32> {
        // 1. 查询消息以获取当前反应计数（如果需要）
        // 注意：反应计数应该由读模型维护，这里返回占位值
        // 实际计数应该在查询时从 message_reactions 表统计
        
        // 2. 构建操作消息并发布到 Kafka（storage-writer 会保存到 message_reactions 表）
        let store_request = MessageOperationBuilder::build_add_reaction_request(&cmd)
            .map_err(|e| FlareError::system(format!("Failed to build add reaction request: {}", e)))?;
        
        self.kafka_publisher
            .publish_operation(store_request)
            .await
            .map_err(|e| FlareError::system(format!("Failed to publish add reaction operation to Kafka: {}", e)))?;

        // 3. 发布领域事件
        let event = MessageReactionAddedEvent {
            base: MessageOperationEvent {
                message_id: cmd.base.message_id.clone(),
                conversation_id: cmd.base.conversation_id.clone(),
                operator_id: cmd.base.operator_id.clone(),
                timestamp: cmd.base.timestamp,
                tenant_id: cmd.base.tenant_id.clone(),
            },
            emoji: cmd.emoji.clone(),
            count: 0, // 由读模型计算
        };
        self.event_publisher.publish_reaction_added(&event).await?;

        // 返回占位计数（实际计数应该从读模型查询）
        Ok(0)
    }

    #[instrument(skip(self), fields(message_id = %cmd.base.message_id, emoji = %cmd.emoji))]
    pub async fn handle_remove_reaction(&self, cmd: RemoveReactionCommand) -> Result<i32> {
        // 1. 查询消息以获取当前反应计数（如果需要）
        // 注意：反应计数应该由读模型维护，这里返回占位值
        // 实际计数应该在查询时从 message_reactions 表统计
        
        // 2. 构建操作消息并发布到 Kafka（storage-writer 会保存到 message_reactions 表）
        let store_request = MessageOperationBuilder::build_remove_reaction_request(&cmd)
            .map_err(|e| FlareError::system(format!("Failed to build remove reaction request: {}", e)))?;
        
        self.kafka_publisher
            .publish_operation(store_request)
            .await
            .map_err(|e| FlareError::system(format!("Failed to publish remove reaction operation to Kafka: {}", e)))?;

        // 3. 发布领域事件
        let event = MessageReactionRemovedEvent {
            base: MessageOperationEvent {
                message_id: cmd.base.message_id.clone(),
                conversation_id: cmd.base.conversation_id.clone(),
                operator_id: cmd.base.operator_id.clone(),
                timestamp: cmd.base.timestamp,
                tenant_id: cmd.base.tenant_id.clone(),
            },
            emoji: cmd.emoji.clone(),
            count: 0, // 由读模型计算
        };
        self.event_publisher.publish_reaction_removed(&event).await?;

        // 返回占位计数（实际计数应该从读模型查询）
        Ok(0)
    }

    #[instrument(skip(self), fields(message_id = %cmd.base.message_id))]
    pub async fn handle_pin(&self, cmd: PinMessageCommand) -> Result<()> {
        // 1. 构建操作消息并发布到 Kafka（storage-writer 会保存到 pinned_messages 表）
        let store_request = MessageOperationBuilder::build_pin_request(&cmd)
            .map_err(|e| FlareError::system(format!("Failed to build pin request: {}", e)))?;
        
        self.kafka_publisher
            .publish_operation(store_request)
            .await
            .map_err(|e| FlareError::system(format!("Failed to publish pin operation to Kafka: {}", e)))?;

        // 2. 发布领域事件
        let event = MessagePinnedEvent {
            base: MessageOperationEvent {
                message_id: cmd.base.message_id.clone(),
                conversation_id: cmd.base.conversation_id.clone(),
                operator_id: cmd.base.operator_id.clone(),
                timestamp: cmd.base.timestamp,
                tenant_id: cmd.base.tenant_id.clone(),
            },
        };
        self.event_publisher.publish_pinned(&event).await?;

        Ok(())
    }

    #[instrument(skip(self), fields(message_id = %cmd.base.message_id))]
    pub async fn handle_unpin(&self, cmd: UnpinMessageCommand) -> Result<()> {
        // 1. 构建操作消息并发布到 Kafka（storage-writer 会保存到 pinned_messages 表）
        let store_request = MessageOperationBuilder::build_unpin_request(&cmd)
            .map_err(|e| FlareError::system(format!("Failed to build unpin request: {}", e)))?;
        
        self.kafka_publisher
            .publish_operation(store_request)
            .await
            .map_err(|e| FlareError::system(format!("Failed to publish unpin operation to Kafka: {}", e)))?;

        // 2. 发布领域事件
        let event = MessageUnpinnedEvent {
            base: MessageOperationEvent {
                message_id: cmd.base.message_id.clone(),
                conversation_id: cmd.base.conversation_id.clone(),
                operator_id: cmd.base.operator_id.clone(),
                timestamp: cmd.base.timestamp,
                tenant_id: cmd.base.tenant_id.clone(),
            },
        };
        self.event_publisher.publish_unpinned(&event).await?;

        Ok(())
    }

    // Favorite/Unfavorite 功能暂未实现
    // #[instrument(skip(self), fields(message_id = %cmd.base.message_id))]
    // pub async fn handle_favorite(&self, cmd: FavoriteMessageCommand) -> Result<()> {
    //     let event = MessageFavoritedEvent {
    //         base: MessageOperationEvent {
    //             message_id: cmd.base.message_id.clone(),
    //             conversation_id: cmd.base.conversation_id.clone(),
    //             operator_id: cmd.base.operator_id.clone(),
    //             timestamp: cmd.base.timestamp,
    //             tenant_id: cmd.base.tenant_id.clone(),
    //         },
    //         tags: cmd.tags.clone(),
    //     };
    //     self.event_publisher.publish_favorited(&event).await?;
    //
    //     Ok(())
    // }

    // #[instrument(skip(self), fields(message_id = %cmd.base.message_id))]
    // pub async fn handle_unfavorite(&self, cmd: UnfavoriteMessageCommand) -> Result<()> {
    //     let event = MessageUnfavoritedEvent {
    //         base: MessageOperationEvent {
    //             message_id: cmd.base.message_id.clone(),
    //             conversation_id: cmd.base.conversation_id.clone(),
    //             operator_id: cmd.base.operator_id.clone(),
    //             timestamp: cmd.base.timestamp,
    //             tenant_id: cmd.base.tenant_id.clone(),
    //         },
    //     };
    //     self.event_publisher.publish_unfavorited(&event).await?;
    //
    //     Ok(())
    // }

    #[instrument(skip(self), fields(message_id = %cmd.base.message_id, mark_type = %cmd.mark_type))]
    pub async fn handle_mark(&self, cmd: MarkMessageCommand) -> Result<()> {
        // 1. 构建操作消息并发布到 Kafka（storage-writer 会保存到 marked_messages 表）
        let store_request = MessageOperationBuilder::build_mark_request(&cmd)
            .map_err(|e| FlareError::system(format!("Failed to build mark request: {}", e)))?;
        
        self.kafka_publisher
            .publish_operation(store_request)
            .await
            .map_err(|e| FlareError::system(format!("Failed to publish mark operation to Kafka: {}", e)))?;

        // 2. 发布领域事件（如果需要）
        // 注意：目前 EventPublisher trait 中没有 publish_marked 方法
        // 如果需要，可以在 EventPublisher trait 中添加

        Ok(())
    }

    /// 取消标记消息（业务逻辑）
    #[instrument(skip(self), fields(message_id = %cmd.base.message_id, user_id = %cmd.user_id))]
    pub async fn handle_unmark(&self, cmd: UnmarkMessageCommand) -> Result<()> {
        // 1. 查询消息的当前标记信息
        let _message = self
            .message_repo
            .find_by_id(&cmd.base.message_id)
            .await?
            .ok_or_else(|| MessageOperationErrorBuilder::message_not_found(&cmd.base.message_id))?;

        // 2. 构建操作消息并发布到 Kafka
        // 注意：取消标记是用户维度的操作，不需要 Kafka 推送，但需要持久化到数据库
        // TODO: 添加 MessageOperationBuilder::build_unmark_request
        // let store_request = MessageOperationBuilder::build_unmark_request(&cmd)
        //     .context("Failed to build unmark request")?;
        
        // self.kafka_publisher
        //     .publish_operation(store_request)
        //     .await
        //     .context("Failed to publish unmark operation to Kafka")?;

        // 注意：目前取消标记操作的属性更新逻辑在 handler 中直接调用 Storage Reader
        // 未来应该通过 MessageOperationBuilder 构建操作消息，发布到 Kafka，由 Storage Writer 处理
        // 这样可以保证一致性，并支持事件驱动架构

        Ok(())
    }
}

