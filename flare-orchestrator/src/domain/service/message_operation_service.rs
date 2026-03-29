//! 消息操作领域服务（Message BC）：撤回/编辑/删除/已读/反应/置顶/标记等。
//!
//! 对应 proto 统一入口 ExecuteEventRequest → 本服务执行 FSM 状态迁移并发布领域事件至 Kafka，
//! Storage Writer / Push Server 消费后写库与在线推送。
//! 通过 to_system_err_with 统一错误映射，方法保持 ≤50 行。

use chrono::Utc;
use flare_server_core::context::Ctx;
use std::sync::Arc;
use tracing::instrument;

use crate::error::{ErrorCode, FlareError, MessageOperationErrorBuilder, Result};

use crate::application::commands::{
    AddReactionCommand, DeleteMessageCommand, DeleteType, EditMessageCommand, MarkMessageCommand,
    MessageOperationCommand, PinMessageCommand, ReadMessageCommand, RecallMessageCommand,
    RemoveReactionCommand, UnmarkMessageCommand, UnpinMessageCommand,
};
use crate::domain::event::{
    MessageDeletedEvent, MessageEditedEvent, MessageOperationDomainEvent, MessageOperationEvent,
    MessagePinnedEvent, MessageReactionAddedEvent, MessageReactionRemovedEvent, MessageReadEvent,
    MessageRecalledEvent, MessageUnpinnedEvent,
};
use crate::domain::model::{Message, MessageFsmState};
use crate::domain::repository::WalRepository;
use crate::domain::service::event_builder::EventBuilder;
use crate::domain::service::operation_event_dispatcher::OperationEventDispatcher;
use crate::domain::service::sequence_allocator::SequenceAllocator;

/// 会话内一页消息的 `server_id`（时间倒序，与 Storage Reader 一致），供编排层「读到某条」等使用。
#[derive(Debug, Clone)]
pub struct ConversationServerIdsPage {
    pub server_ids: Vec<String>,
    pub next_cursor: String,
    pub has_more: bool,
}

/// 消息仓储接口：单条解析、持久化占位、按会话分页 ID（读路径落在基础设施，不经 application queries）。
pub trait MessageRepository: Send + Sync {
    /// 根据消息ID查询消息
    async fn find_by_id(&self, ctx: &Ctx, message_id: &str) -> Result<Option<Message>>;

    /// 保存消息
    async fn save(&self, ctx: &Ctx, message: &Message) -> Result<()>;

    /// 按会话分页拉取 `server_id`（时间倒序）。
    async fn page_server_ids_in_conversation(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        limit: i32,
        cursor: Option<&str>,
    ) -> Result<ConversationServerIdsPage>;
}

/// 消息操作服务（事件驱动：产生领域事件后由 OperationEventDispatcher 统一派发 Kafka + Push）
pub struct MessageOperationService<R: MessageRepository, D: OperationEventDispatcher> {
    message_repo: Arc<R>,
    dispatcher: Arc<D>,
    wal_repository:
        Option<Arc<crate::infrastructure::persistence::wal_repository_impl::WalRepositoryItem>>,
    sequence_allocator: Arc<SequenceAllocator>,
}

impl<R: MessageRepository, D: OperationEventDispatcher> MessageOperationService<R, D> {
    pub fn new(
        message_repo: Arc<R>,
        dispatcher: Arc<D>,
        wal_repository: Option<
            Arc<crate::infrastructure::persistence::wal_repository_impl::WalRepositoryItem>,
        >,
        sequence_allocator: Arc<SequenceAllocator>,
    ) -> Self {
        Self {
            message_repo,
            dispatcher,
            wal_repository,
            sequence_allocator,
        }
    }

    /// 从命令基类构建领域事件基类（Builder 模式：可选覆盖 message_id，如删除多条）
    fn base_event(
        base: &MessageOperationCommand,
        message_id_override: Option<&str>,
    ) -> MessageOperationEvent {
        MessageOperationEvent {
            message_id: message_id_override.unwrap_or(&base.message_id).to_string(),
            conversation_id: base.conversation_id.clone(),
            operator_id: base.operator_id.clone(),
            timestamp: base.timestamp,
            tenant_id: base.tenant_id.clone(),
        }
    }

    /// 为会话分配流 seq（事件化模型：操作与用户消息共用同一 seq 空间）
    async fn allocate_stream_seq(&self, conversation_id: &str, tenant_id: &str) -> u64 {
        match self
            .sequence_allocator
            .allocate_seq(conversation_id, tenant_id)
            .await
        {
            Ok(seq) => seq,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    conversation_id = %conversation_id,
                    "Sequence allocation failed, using degraded seq"
                );
                self.sequence_allocator
                    .allocate_seq_degraded()
                    .unwrap_or_else(|_| {
                        // 如果降级也失败,使用当前时间戳作为临时序列号
                        use std::time::{SystemTime, UNIX_EPOCH};
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs() as u64
                    })
            }
        }
    }

    /// 解析操作所需原消息：WAL 优先，再 Storage Reader。
    /// 理由：新消息先写 WAL 再异步落库，先查 WAL 可降低延迟、减少对 Reader 的读压力。
    #[instrument(skip(self), fields(message_id = %message_id))]
    pub async fn resolve_message_for_operation(
        &self,
        ctx: &Ctx,
        message_id: &str,
    ) -> Result<Option<Message>> {
        // 1. 优先从 WAL 读（刚发送未落库的消息、本地延迟最低）
        if let Some(wal_repo) = &self.wal_repository {
            match wal_repo.find_by_message_id(message_id).await {
                Ok(Some(proto)) => {
                    tracing::debug!(message_id = %message_id, "Resolved message from WAL");
                    return Ok(Some(Self::proto_to_domain_message(proto)));
                }
                Ok(None) => {}
                Err(e) => tracing::warn!(message_id = %message_id, error = %e, "WAL lookup failed"),
            }
        }
        // 2. 回退到 Storage Reader（已持久化或 WAL 未命中）
        let msg = self.message_repo.find_by_id(ctx, message_id).await?;
        if msg.is_some() {
            tracing::debug!(message_id = %message_id, "Resolved message from Storage Reader");
        }
        Ok(msg)
    }

    /// 按会话分页拉取 `server_id`，委托仓储（通常走 Storage Reader）。
    pub async fn page_server_ids_in_conversation(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        limit: i32,
        cursor: Option<&str>,
    ) -> Result<ConversationServerIdsPage> {
        self.message_repo
            .page_server_ids_in_conversation(ctx, conversation_id, limit, cursor)
            .await
    }

    /// 将 Proto Message 转为领域 Message（供 WAL 解析结果使用）
    fn proto_to_domain_message(proto: flare_proto::common::Message) -> Message {
        use chrono::DateTime;

        let fsm_state = if proto.status == flare_proto::common::MessageStatus::Recalled as i32 {
            MessageFsmState::Recalled
        } else if proto.status == flare_proto::common::MessageStatus::DeletedHard as i32 {
            MessageFsmState::DeletedHard
        } else {
            MessageFsmState::from_str(
                proto
                    .extra
                    .get("message_fsm_state")
                    .map(|s| s.as_str())
                    .unwrap_or("SENT"),
            )
            .unwrap_or(MessageFsmState::Sent)
        };

        let created_at_dt = proto
            .timestamp
            .as_ref()
            .and_then(|ts| DateTime::from_timestamp(ts.seconds, ts.nanos as u32))
            .unwrap_or_else(Utc::now);

        let content_bytes = proto.content.clone();

        Message {
            server_id: proto.server_id,
            conversation_id: proto.conversation_id,
            sender_id: proto.sender_id,
            channel_id: proto.channel_id.clone(),
            content: content_bytes,
            timestamp: created_at_dt,
            fsm_state,
            fsm_state_changed_at: created_at_dt,
            edit_version: proto
                .extra
                .get("current_edit_version")
                .and_then(|v| v.parse::<i32>().ok())
                .unwrap_or(0),
            edit_history: vec![],
            extra: proto.extra,
            updated_at: created_at_dt,
        }
    }

    fn is_editable_structured_content(content: &[u8]) -> bool {
        let Ok(decoded) = flare_proto::decode_message_content(content) else {
            return false;
        };
        use flare_proto::common::message_content::Content;
        matches!(
            decoded.content,
            Some(Content::Text(_))
                | Some(Content::Card(_))
                | Some(Content::LinkCard(_))
                | Some(Content::Custom(_))
                | Some(Content::Notification(_))
                | Some(Content::Vote(_))
                | Some(Content::Task(_))
                | Some(Content::Schedule(_))
                | Some(Content::Announcement(_))
                | Some(Content::Thread(_))
        )
    }

    #[instrument(skip(self), fields(message_id = %cmd.base.message_id, operator_id = %cmd.base.operator_id))]
    pub async fn handle_recall(&self, ctx: &Ctx, cmd: RecallMessageCommand) -> Result<()> {
        let message_opt = self
            .resolve_message_for_operation(ctx, &cmd.base.message_id)
            .await?;
        let message = match &message_opt {
            Some(m) => m.clone(),
            None => {
                if cmd.base.conversation_id.is_empty() {
                    return Err(MessageOperationErrorBuilder::message_not_found(
                        &cmd.base.message_id,
                    ));
                }
                if self.wal_repository.is_none() {
                    return Err(MessageOperationErrorBuilder::message_not_found(
                        &cmd.base.message_id,
                    ));
                }
                tracing::warn!(
                    message_id = %cmd.base.message_id,
                    "Message not in WAL nor Reader; proceeding with operator as sender (recall)"
                );
                Message {
                    server_id: cmd.base.message_id.clone(),
                    conversation_id: cmd.base.conversation_id.clone(),
                    sender_id: cmd.base.operator_id.clone(),
                    channel_id: String::new(),
                    content: vec![],
                    timestamp: Utc::now(),
                    fsm_state: MessageFsmState::Sent,
                    fsm_state_changed_at: Utc::now(),
                    edit_version: 0,
                    edit_history: vec![],
                    extra: std::collections::HashMap::new(),
                    updated_at: Utc::now(),
                }
            }
        };

        if message.sender_id != cmd.base.operator_id && !cmd.allow_admin_override {
            return Err(MessageOperationErrorBuilder::permission_denied(
                "recall",
                &cmd.base.operator_id,
            ));
        }

        // 分配流 seq，构建 proto Event 与领域事件，统一派发（Kafka 操作流 + Push）
        let seq = self
            .allocate_stream_seq(&cmd.base.conversation_id, &cmd.base.tenant_id)
            .await;
        let proto_event = EventBuilder::recall(&cmd, seq);
        let domain_event = MessageRecalledEvent {
            base: Self::base_event(&cmd.base, None),
            reason: cmd.reason.clone(),
            new_state: MessageFsmState::Recalled,
        };
        self.dispatcher
            .dispatch(
                ctx,
                proto_event,
                MessageOperationDomainEvent::Recalled(domain_event),
            )
            .await?;
        Ok(())
    }

    #[instrument(skip(self), fields(message_id = %cmd.base.message_id, operator_id = %cmd.base.operator_id))]
    pub async fn handle_edit(&self, ctx: &Ctx, cmd: EditMessageCommand) -> Result<()> {
        // 1. 解析原消息：WAL 优先，再 Storage Reader
        let original_message = self
            .resolve_message_for_operation(ctx, &cmd.base.message_id)
            .await?;

        // 若配置了 WAL 且两边都查不到，则允许继续（跳过权限校验，由下游保证幂等）
        let (original_message, skip_permission_check) = match original_message {
            Some(msg) => (msg, false),
            None => {
                if self.wal_repository.is_none() {
                    return Err(FlareError::system(
                        "Message not found and WAL not configured. Cannot validate edit permissions. Please configure WAL (MESSAGE_ORCHESTRATOR_WAL_HASH_KEY) or wait for message to be persisted.",
                    ));
                }
                tracing::warn!(
                    message_id = %cmd.base.message_id,
                    "Message not in WAL nor Reader; proceeding without permission check (WAL configured)"
                );
                (
                    Message {
                        server_id: cmd.base.message_id.clone(),
                        conversation_id: cmd.base.conversation_id.clone(),
                        sender_id: cmd.base.operator_id.clone(),
                        channel_id: String::new(),
                        content: vec![],
                        timestamp: Utc::now(),
                        fsm_state: MessageFsmState::Sent,
                        fsm_state_changed_at: Utc::now(),
                        edit_version: 0,
                        edit_history: vec![],
                        extra: std::collections::HashMap::new(),
                        updated_at: Utc::now(),
                    },
                    true,
                )
            }
        };

        // 2. 验证权限（发送者或管理员可编辑）
        if !skip_permission_check
            && original_message.sender_id != cmd.base.operator_id
            && !cmd.allow_admin_override
        {
            return Err(MessageOperationErrorBuilder::permission_denied(
                "edit",
                &cmd.base.operator_id,
            ));
        }

        if !Self::is_editable_structured_content(&cmd.new_content) {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "Only text or structured message content is editable",
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

        // 3. 分配流 seq，构建 proto Event 与领域事件，统一派发
        use crate::domain::model::message_fsm::EditHistoryEntry;
        let mut new_edit_history = original_message.edit_history.clone();
        let new_edit_version = original_message.edit_version + 1;
        new_edit_history.push(EditHistoryEntry {
            edit_version: new_edit_version,
            content_encoded: original_message.content.clone(),
            edited_at: Utc::now(),
            editor_id: cmd.base.operator_id.clone(),
            reason: cmd.reason.clone(),
        });

        let seq = self
            .allocate_stream_seq(&cmd.base.conversation_id, &cmd.base.tenant_id)
            .await;
        let proto_event = EventBuilder::edit(&cmd, seq);
        let domain_event = MessageEditedEvent {
            base: Self::base_event(&cmd.base, None),
            edit_version: new_edit_version,
            new_state: MessageFsmState::Edited,
            reason: cmd.reason.clone(),
            new_content: cmd.new_content.clone(),
            edit_history: new_edit_history.clone(),
        };
        self.dispatcher
            .dispatch(
                ctx,
                proto_event,
                MessageOperationDomainEvent::Edited(domain_event),
            )
            .await?;
        Ok(())
    }

    #[instrument(skip(self), fields(message_id = %cmd.base.message_id, operator_id = %cmd.base.operator_id))]
    pub async fn handle_delete(&self, ctx: &Ctx, cmd: DeleteMessageCommand) -> Result<()> {
        if matches!(cmd.delete_type, DeleteType::Hard) && !cmd.allow_admin_override {
            return Err(MessageOperationErrorBuilder::permission_denied(
                "delete_hard",
                &cmd.base.operator_id,
            ));
        }

        // 1. 解析原消息：WAL 优先，再 Storage Reader；若未查到但命令带 conversation_id（如刚发未落库），仍允许派发删除事件
        let _original = self
            .resolve_message_for_operation(ctx, &cmd.base.message_id)
            .await?;
        if _original.is_none() && cmd.base.conversation_id.is_empty() {
            return Err(MessageOperationErrorBuilder::message_not_found(
                &cmd.base.message_id,
            ));
        }

        // 2. 对每条 message_id 派发删除事件（Kafka 操作流 + Push，每条独立 seq）
        let ids = if cmd.message_ids.is_empty() {
            vec![cmd.base.message_id.clone()]
        } else {
            cmd.message_ids.clone()
        };
        let delete_type_str = match cmd.delete_type {
            DeleteType::Hard => "HARD",
            DeleteType::Soft => "SOFT",
        };
        let new_state = match cmd.delete_type {
            DeleteType::Hard => Some(MessageFsmState::DeletedHard),
            DeleteType::Soft => Some(MessageFsmState::DeletedSoft),
        };
        for msg_id in &ids {
            let seq = self
                .allocate_stream_seq(&cmd.base.conversation_id, &cmd.base.tenant_id)
                .await;
            let proto_event = EventBuilder::delete_one(msg_id, &cmd, seq);
            let domain_event = MessageDeletedEvent {
                base: Self::base_event(&cmd.base, Some(msg_id)),
                delete_type: delete_type_str.to_string(),
                new_state: new_state.clone(),
                target_user_id: Some(String::new()),
            };
            self.dispatcher
                .dispatch(
                    ctx,
                    proto_event,
                    MessageOperationDomainEvent::Deleted(domain_event),
                )
                .await?;
        }
        Ok(())
    }

    #[instrument(skip(self), fields(operator_id = %cmd.base.operator_id))]
    pub async fn handle_read(&self, ctx: &Ctx, cmd: ReadMessageCommand) -> Result<()> {
        let seq = self
            .allocate_stream_seq(&cmd.base.conversation_id, &cmd.base.tenant_id)
            .await;
        let proto_event = EventBuilder::read(&cmd, seq);
        let domain_event = MessageReadEvent {
            base: Self::base_event(&cmd.base, None),
            message_ids: cmd.message_ids.clone(),
            read_at: cmd.read_at.unwrap_or_else(Utc::now),
        };
        self.dispatcher
            .dispatch(
                ctx,
                proto_event,
                MessageOperationDomainEvent::Read(domain_event),
            )
            .await?;
        Ok(())
    }

    #[instrument(skip(self), fields(message_id = %cmd.base.message_id, emoji = %cmd.emoji))]
    pub async fn handle_add_reaction(&self, ctx: &Ctx, cmd: AddReactionCommand) -> Result<i32> {
        // 1. 查询消息以获取当前反应计数（如果需要）
        // 注意：反应计数应该由读模型维护，这里返回占位值
        // 实际计数应该在查询时从 message_reactions 表统计

        let seq = self
            .allocate_stream_seq(&cmd.base.conversation_id, &cmd.base.tenant_id)
            .await;
        let proto_event = EventBuilder::reaction_add(&cmd, seq);
        let domain_event = MessageReactionAddedEvent {
            base: Self::base_event(&cmd.base, None),
            emoji: cmd.emoji.clone(),
            count: 0,
        };
        self.dispatcher
            .dispatch(
                ctx,
                proto_event,
                MessageOperationDomainEvent::ReactionAdded(domain_event),
            )
            .await?;
        Ok(0)
    }

    #[instrument(skip(self), fields(message_id = %cmd.base.message_id, emoji = %cmd.emoji))]
    pub async fn handle_remove_reaction(
        &self,
        ctx: &Ctx,
        cmd: RemoveReactionCommand,
    ) -> Result<i32> {
        // 1. 查询消息以获取当前反应计数（如果需要）
        // 注意：反应计数应该由读模型维护，这里返回占位值
        // 实际计数应该在查询时从 message_reactions 表统计

        let seq = self
            .allocate_stream_seq(&cmd.base.conversation_id, &cmd.base.tenant_id)
            .await;
        let proto_event = EventBuilder::reaction_remove(&cmd, seq);
        let domain_event = MessageReactionRemovedEvent {
            base: Self::base_event(&cmd.base, None),
            emoji: cmd.emoji.clone(),
            count: 0,
        };
        self.dispatcher
            .dispatch(
                ctx,
                proto_event,
                MessageOperationDomainEvent::ReactionRemoved(domain_event),
            )
            .await?;
        Ok(0)
    }

    #[instrument(skip(self), fields(message_id = %cmd.base.message_id))]
    pub async fn handle_pin(&self, ctx: &Ctx, cmd: PinMessageCommand) -> Result<()> {
        let seq = self
            .allocate_stream_seq(&cmd.base.conversation_id, &cmd.base.tenant_id)
            .await;
        let proto_event = EventBuilder::pin(&cmd, seq);
        let domain_event = MessagePinnedEvent {
            base: Self::base_event(&cmd.base, None),
        };
        self.dispatcher
            .dispatch(
                ctx,
                proto_event,
                MessageOperationDomainEvent::Pinned(domain_event),
            )
            .await?;
        Ok(())
    }

    #[instrument(skip(self), fields(message_id = %cmd.base.message_id))]
    pub async fn handle_unpin(&self, ctx: &Ctx, cmd: UnpinMessageCommand) -> Result<()> {
        let seq = self
            .allocate_stream_seq(&cmd.base.conversation_id, &cmd.base.tenant_id)
            .await;
        let proto_event = EventBuilder::unpin(&cmd, seq);
        let domain_event = MessageUnpinnedEvent {
            base: Self::base_event(&cmd.base, None),
        };
        self.dispatcher
            .dispatch(
                ctx,
                proto_event,
                MessageOperationDomainEvent::Unpinned(domain_event),
            )
            .await?;
        Ok(())
    }

    #[instrument(skip(self), fields(message_id = %cmd.base.message_id, mark_type = %cmd.mark_type))]
    pub async fn handle_mark(&self, ctx: &Ctx, cmd: MarkMessageCommand) -> Result<()> {
        let seq = self
            .allocate_stream_seq(&cmd.base.conversation_id, &cmd.base.tenant_id)
            .await;
        let proto_event = EventBuilder::mark(&cmd, seq);
        self.dispatcher
            .dispatch_event_only(ctx, proto_event)
            .await?;
        Ok(())
    }

    /// 取消标记消息（业务逻辑）
    #[instrument(skip(self), fields(message_id = %cmd.base.message_id, user_id = %cmd.user_id))]
    pub async fn handle_unmark(&self, ctx: &Ctx, cmd: UnmarkMessageCommand) -> Result<()> {
        let _message = self
            .resolve_message_for_operation(ctx, &cmd.base.message_id)
            .await?;
        if _message.is_none() && cmd.base.conversation_id.is_empty() {
            return Err(MessageOperationErrorBuilder::message_not_found(
                &cmd.base.message_id,
            ));
        }

        let seq = self
            .allocate_stream_seq(&cmd.base.conversation_id, &cmd.base.tenant_id)
            .await;
        let proto_event = EventBuilder::unmark(&cmd, seq);
        self.dispatcher
            .dispatch_event_only(ctx, proto_event)
            .await?;

        Ok(())
    }
}
