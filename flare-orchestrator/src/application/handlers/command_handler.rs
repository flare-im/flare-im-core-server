//! 命令处理器（编排层）- 轻量级，只负责编排领域服务和记录指标

use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use flare_im_core::metrics::MessageOrchestratorMetrics;
use flare_server_core::context::{Context, ContextExt, Ctx};
use tracing::instrument;

use crate::application::commands::{
    AddReactionCommand, BatchMarkMessageReadCommand, BatchSendMessageCommand,
    BatchStoreMessageCommand, DeleteMessageCommand, EditMessageCommand,
    HandleTemporaryMessageCommand, MarkConversationReadCommand, MarkMessageCommand,
    PinMessageCommand, ReadMessageCommand, RecallMessageCommand, RemoveReactionCommand,
    SendMessageCommand, SendSystemMessageCommand, StoreMessageCommand, UnmarkMessageCommand,
    UnpinMessageCommand,
};
use crate::domain::repository::ConversationRepositoryItem;
use crate::domain::repository::conversation_repository::ConversationRepository;
use crate::domain::service::ConversationServerIdsPage;
use crate::domain::service::MessageDomainService;
use crate::domain::service::SystemMessageService;
use crate::domain::service::message_operation_service::MessageOperationService;
use crate::error::{ErrorCode, FlareError, Result as OrchestratorResult};
use crate::domain::service::message_temporary_service::MessageTemporaryService;
use crate::infrastructure::messaging::operation_dispatcher_impl::OperationEventDispatcherImpl;
use crate::infrastructure::persistence::message_repository_kind::MessageRepositoryKind;

/// 与 wire 一致的具体 `MessageOperationService`（避免 Handler 上开放泛型）
pub type MessageOperationServiceImpl =
    MessageOperationService<MessageRepositoryKind, OperationEventDispatcherImpl>;

/// 消息命令处理器（编排层）
pub struct MessageCommandHandler {
    domain_service: Arc<MessageDomainService>,
    operation_service: Arc<MessageOperationServiceImpl>,
    temporary_service: Option<Arc<MessageTemporaryService>>,
    conversation_repository: Option<Arc<ConversationRepositoryItem>>,
    metrics: Arc<MessageOrchestratorMetrics>,
}

impl MessageCommandHandler {
    pub fn new(
        domain_service: Arc<MessageDomainService>,
        operation_service: Arc<MessageOperationServiceImpl>,
        temporary_service: Option<Arc<MessageTemporaryService>>,
        conversation_repository: Option<Arc<ConversationRepositoryItem>>,
        metrics: Arc<MessageOrchestratorMetrics>,
    ) -> Self {
        Self {
            domain_service,
            operation_service,
            temporary_service,
            conversation_repository,
            metrics,
        }
    }

    /// 处理存储消息命令
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        tenant_id = %ctx.tenant_id().unwrap_or("0"),
    ))]
    pub async fn handle_store_message(
        &self,
        ctx: &Ctx,
        command: StoreMessageCommand,
    ) -> Result<(String, u64)> {
        ctx.ensure_not_cancelled()?;
        let start = Instant::now();
        let _tenant_id = ctx.tenant_id().unwrap_or("0").to_string();
        let message_type = match command.request.message_type {
            0 => "normal",
            _ => "notification",
        }
        .to_string();
        let tenant_id = ctx.tenant_id().unwrap_or("0").to_string();

        let result = self
            .domain_service
            .orchestrate_message_storage(ctx, command.request, true)
            .await;

        // 记录指标
        let duration = start.elapsed();
        self.metrics
            .messages_sent_duration_seconds
            .observe(duration.as_secs_f64());

        if result.is_ok() {
            self.metrics
                .messages_sent_total
                .with_label_values(&[message_type, tenant_id])
                .inc();
        }

        result
    }

    /// 处理存储消息命令（跳过 PreSend Hook）
    /// Context 由 gRPC 中间件从请求自动提取并传入，调用其他 gRPC 与 Kafka 时自动透传。
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        tenant_id = %ctx.tenant_id().unwrap_or("0"),
    ))]
    pub async fn handle_store_message_without_pre_hook(
        &self,
        ctx: &Ctx,
        command: StoreMessageCommand,
    ) -> Result<(String, u64)> {
        ctx.ensure_not_cancelled()?;
        let start = Instant::now();

        let tenant_id = ctx.tenant_id().unwrap_or("unknown").to_string();
        let message_type = match command.request.message_type {
            0 => "normal",
            _ => "notification",
        }
        .to_string();
        let result = self
            .domain_service
            .orchestrate_message_storage(ctx, command.request, false)
            .await;

        let duration = start.elapsed();
        self.metrics
            .messages_sent_duration_seconds
            .observe(duration.as_secs_f64());

        if result.is_ok() {
            self.metrics
                .messages_sent_total
                .with_label_values(&[&message_type, &tenant_id])
                .inc();
        }

        result
    }

    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        tenant_id = %ctx.tenant_id().unwrap_or("0"),
    ))]
    pub async fn handle_send_system_message(
        &self,
        ctx: &Ctx,
        command: SendSystemMessageCommand,
    ) -> Result<String> {
        let message = SystemMessageService::prepare(
            ctx,
            &command.conversation_id,
            command.message,
            &command.system_message_type,
        )?;

        let (message_id, _seq) = self
            .handle_store_message_without_pre_hook(ctx, StoreMessageCommand { request: message })
            .await?;

        Ok(message_id)
    }

    /// 处理批量存储消息命令
    #[instrument(skip(self), fields(batch_size = command.requests.len()))]
    pub async fn handle_batch_store_message(
        &self,
        ctx: &Ctx,
        command: BatchStoreMessageCommand,
    ) -> Result<Vec<String>> {
        let mut message_ids = Vec::new();
        for request in command.requests {
            let request_ctx: Ctx = if let Some(tenant_id_value) = request.extra.get("x-tenant-id") {
                Arc::new(Context::root().with_tenant_id(tenant_id_value.clone()))
            } else {
                ctx.clone()
            };
            match self
                .domain_service
                .orchestrate_message_storage(&request_ctx, request, true)
                .await
            {
                Ok((message_id, _seq)) => message_ids.push(message_id),
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to store message in batch");
                    // 继续处理其他消息
                }
            }
        }
        Ok(message_ids)
    }

    /// 处理撤回消息命令
    #[instrument(skip(self), fields(message_id = %cmd.base.message_id))]
    pub async fn handle_recall_message(&self, ctx: &Ctx, cmd: RecallMessageCommand) -> Result<()> {
        self.operation_service
            .handle_recall(ctx, cmd)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// 处理编辑消息命令
    #[instrument(skip(self), fields(message_id = %cmd.base.message_id))]
    pub async fn handle_edit_message(&self, ctx: &Ctx, cmd: EditMessageCommand) -> Result<()> {
        self.operation_service
            .handle_edit(ctx, cmd)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// 处理删除消息命令
    #[instrument(skip(self), fields(message_id = %cmd.base.message_id))]
    pub async fn handle_delete_message(&self, ctx: &Ctx, cmd: DeleteMessageCommand) -> Result<()> {
        self.operation_service
            .handle_delete(ctx, cmd)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// 处理标记已读命令
    #[instrument(skip(self), fields(message_id = %cmd.base.message_id))]
    pub async fn handle_read_message(&self, ctx: &Ctx, cmd: ReadMessageCommand) -> Result<()> {
        self.operation_service
            .handle_read(ctx, cmd)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// 处理添加反应命令
    #[instrument(skip(self), fields(message_id = %cmd.base.message_id, emoji = %cmd.emoji))]
    pub async fn handle_add_reaction(&self, ctx: &Ctx, cmd: AddReactionCommand) -> Result<i32> {
        self.operation_service
            .handle_add_reaction(ctx, cmd)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// 处理移除反应命令
    #[instrument(skip(self), fields(message_id = %cmd.base.message_id, emoji = %cmd.emoji))]
    pub async fn handle_remove_reaction(
        &self,
        ctx: &Ctx,
        cmd: RemoveReactionCommand,
    ) -> Result<i32> {
        self.operation_service
            .handle_remove_reaction(ctx, cmd)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// 处理置顶消息命令
    #[instrument(skip(self), fields(message_id = %cmd.base.message_id))]
    pub async fn handle_pin_message(&self, ctx: &Ctx, cmd: PinMessageCommand) -> Result<()> {
        self.operation_service
            .handle_pin(ctx, cmd)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// 处理取消置顶消息命令
    #[instrument(skip(self), fields(message_id = %cmd.base.message_id))]
    pub async fn handle_unpin_message(&self, ctx: &Ctx, cmd: UnpinMessageCommand) -> Result<()> {
        self.operation_service
            .handle_unpin(ctx, cmd)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// 处理标记消息命令
    #[instrument(skip(self), fields(message_id = %cmd.base.message_id))]
    pub async fn handle_mark_message(&self, ctx: &Ctx, cmd: MarkMessageCommand) -> Result<()> {
        self.operation_service
            .handle_mark(ctx, cmd)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// 处理取消标记消息命令
    #[instrument(skip(self), fields(message_id = %cmd.base.message_id))]
    pub async fn handle_unmark_message(&self, ctx: &Ctx, cmd: UnmarkMessageCommand) -> Result<()> {
        self.operation_service
            .handle_unmark(ctx, cmd)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// 处理批量标记消息已读命令
    #[instrument(skip(self), fields(conversation_id = %cmd.conversation_id, user_id = %cmd.user_id))]
    pub async fn handle_batch_mark_message_read(
        &self,
        ctx: &Ctx,
        cmd: BatchMarkMessageReadCommand,
    ) -> Result<i32> {
        use crate::application::commands::ReadMessageCommand;

        if cmd.message_ids.is_empty() {
            let read_cmd = ReadMessageCommand {
                base: crate::application::commands::MessageOperationCommand {
                    message_id: String::new(),
                    operator_id: cmd.user_id.clone(),
                    timestamp: cmd.read_at.unwrap_or_else(|| chrono::Utc::now()),
                    tenant_id: cmd.tenant_id.clone(),
                    conversation_id: cmd.conversation_id.clone(),
                },
                message_ids: vec![],
                read_at: cmd.read_at,
                burn_after_read: false,
            };
            self.handle_read_message(ctx, read_cmd).await?;
            return Ok(1);
        }

        let mut processed_count = 0;
        for message_id in &cmd.message_ids {
            let read_cmd = ReadMessageCommand {
                base: crate::application::commands::MessageOperationCommand {
                    message_id: message_id.clone(),
                    operator_id: cmd.user_id.clone(),
                    timestamp: cmd.read_at.unwrap_or_else(|| chrono::Utc::now()),
                    tenant_id: cmd.tenant_id.clone(),
                    conversation_id: cmd.conversation_id.clone(),
                },
                message_ids: vec![message_id.clone()],
                read_at: cmd.read_at,
                burn_after_read: false,
            };
            match self.handle_read_message(ctx, read_cmd).await {
                Ok(()) => processed_count += 1,
                Err(e) => {
                    tracing::warn!(message_id = %message_id, error = %e, "Failed to mark message as read in batch");
                }
            }
        }
        Ok(processed_count)
    }

    /// 处理标记会话已读命令（调用 Conversation 服务 MarkConversationAsRead RPC，更新未读数）
    #[instrument(skip(self), fields(conversation_id = %cmd.conversation_id, user_id = %cmd.user_id))]
    pub async fn handle_mark_conversation_read(
        &self,
        cmd: MarkConversationReadCommand,
    ) -> Result<()> {
        if let Some(ref repo) = self.conversation_repository {
            let ctx = Arc::new(
                Context::root()
                    .with_tenant_id(cmd.tenant_id.as_str())
                    .with_user_id(cmd.user_id.as_str()),
            );
            repo.mark_conversation_as_read(ctx.as_ref(), &cmd.conversation_id, 0)
                .await?;
        } else {
            tracing::debug!(
                conversation_id = %cmd.conversation_id,
                "Conversation client not configured, skip MarkConversationAsRead RPC"
            );
        }
        Ok(())
    }

    /// 处理临时消息命令（只推送，不持久化）
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        message_id = %cmd.message.server_id
    ))]
    pub async fn handle_temporary_message(
        &self,
        ctx: &Ctx,
        cmd: HandleTemporaryMessageCommand,
    ) -> Result<()> {
        ctx.ensure_not_cancelled()?;

        let temporary_service = self
            .temporary_service
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Temporary message service not configured"))?;

        temporary_service
            .handle_temporary_message(ctx, &cmd.message)
            .await
    }

    /// 处理发送消息命令（包含消息类别判断和路由）
    ///
    /// 根据消息类别路由到不同处理流程：
    /// - Temporary: 临时消息（只推送，不持久化）
    /// - Operation: 操作消息（通过 NotificationContent 传递）
    /// - Normal/Notification: 普通消息（存储编排）
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        message_id = %cmd.message.server_id,
        conversation_id = %cmd.conversation_id
    ))]
    pub async fn handle_send_message(
        &self,
        ctx: &Ctx,
        cmd: SendMessageCommand,
    ) -> Result<(String, u64)> {
        ctx.ensure_not_cancelled()?;

        use crate::domain::model::message_kind::MessageProfile;

        let mut message = cmd.message.clone();

        // 判断消息类别（使用 MessageProfile）
        let profile = MessageProfile::ensure(&mut message);
        let category = profile.category();

        tracing::info!(
            message_id = %message.server_id,
            message_type = message.message_type,
            category = ?category,
            conversation_id = %message.conversation_id,
            sender_id = %message.sender_id,
            "🔍 处理发送消息，判断消息类别：message_type={}, category={:?}", message.message_type, category
        );

        // 根据消息类别路由到不同处理流程（领域操作已统一走 Event 流 + gRPC 操作 API）
        match category {
            crate::domain::model::message_kind::MessageCategory::Temporary => {
                // 临时消息：只推送，不持久化
                let temp_cmd = HandleTemporaryMessageCommand {
                    message: message.clone(),
                };
                self.handle_temporary_message(ctx, temp_cmd).await?;

                // 临时消息返回消息ID和seq=0
                Ok((message.server_id, 0))
            }
            crate::domain::model::message_kind::MessageCategory::Operation | _ => {
                // 操作类与普通/通知消息：统一按普通消息处理（存储+推送）；recall/edit/delete 等走 gRPC 操作接口并发布 Event
                self.handle_normal_message(ctx, cmd).await
            }
        }
    }

    /// 处理普通消息（内部方法）
    async fn handle_normal_message(
        &self,
        ctx: &Ctx,
        cmd: SendMessageCommand,
    ) -> Result<(String, u64)> {
        ctx.ensure_not_cancelled()?;
        // 验证单聊消息必须包含 receiver_id，除非是群聊
        if cmd.message.conversation_type == flare_proto::common::ConversationType::Single as i32 {
            if cmd.message.channel_id.is_empty() {
                // 如果是单聊且没有 channel_id，尝试从 conversation_id 或 attributes 中推断
                // 这里暂时保持严格检查，因为单聊必须明确接收者
                // 但为了兼容某些客户端行为（如未正确设置 receiver_id），我们可以记录警告并尝试继续（如果业务允许）
                // 目前为了保证数据完整性，仍然报错，但错误信息更明确
                return Err(anyhow::anyhow!(
                    "Single chat message must provide channel_id (receiver). message_id={}, conversation_id={}, sender_id={}",
                    cmd.message.server_id,
                    cmd.message.conversation_id,
                    cmd.message.sender_id
                ));
            }
        } else if cmd.message.conversation_type
            == flare_proto::common::ConversationType::Group as i32
        {
            // 群聊消息不需要 channel_id 表示对方，可为空或为群 ID
            if cmd.message.channel_id.is_empty() {
                // 对于群聊，channel_id 可为群 ID 或空
                // 这里不做强制检查，依靠后续逻辑处理
            }
        }

        // 将 SendMessageCommand 转为 Message（envelope 写入 extra；tenant 从 Context 传递）
        let mut msg = cmd.message;
        if msg.conversation_id.is_empty() {
            msg.conversation_id = cmd.conversation_id.clone();
        }
        msg.extra.insert(
            flare_im_core::abstractions::storage_payload::EXTRA_KEY_SYNC.to_string(),
            cmd.sync.to_string(),
        );
        if let Ok(tags_json) =
            serde_json::to_string(&std::collections::HashMap::<String, String>::new())
        {
            msg.extra.insert(
                flare_im_core::abstractions::storage_payload::EXTRA_KEY_TAGS.to_string(),
                tags_json,
            );
        }
        self.handle_store_message(ctx, StoreMessageCommand { request: msg })
            .await
    }

    /// 处理批量发送消息命令
    ///
    /// 返回成功和失败的结果
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        batch_size = cmd.requests.len()
    ))]
    pub async fn handle_batch_send_message(
        &self,
        ctx: &Ctx,
        cmd: BatchSendMessageCommand,
    ) -> Result<(Vec<(String, u64)>, Vec<String>)> {
        ctx.ensure_not_cancelled()?;
        let mut successes = Vec::new();
        let mut failures = Vec::new();

        for send_req in cmd.requests {
            let message = match send_req.message {
                Some(msg) => msg,
                None => {
                    failures.push("message is required".to_string());
                    continue;
                }
            };

            let send_cmd = SendMessageCommand {
                message,
                conversation_id: send_req.conversation_id.clone(),
                sync: send_req.sync,
            };

            match self.handle_send_message(ctx, send_cmd).await {
                Ok((message_id, seq)) => {
                    successes.push((message_id, seq));
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to send message in batch");
                    failures.push(e.to_string());
                }
            }
        }

        Ok((successes, failures))
    }

    /// 解析 `message_id` → `(conversation_id, server_msg_id)`，供操作编排（WAL / Reader / 客户端 fallback）。
    pub async fn resolve_message_ids_for_operation(
        &self,
        ctx: &Ctx,
        message_id: &str,
        fallback_conversation_id: Option<&str>,
    ) -> OrchestratorResult<(String, String)> {
        match self
            .operation_service
            .resolve_message_for_operation(ctx, message_id)
            .await
        {
            Ok(Some(msg)) => {
                let server_msg_id = if msg.server_id.is_empty() {
                    message_id.to_string()
                } else {
                    msg.server_id
                };
                Ok((msg.conversation_id, server_msg_id))
            }
            Ok(None) => {
                if let Some(fallback) = fallback_conversation_id.filter(|s| !s.trim().is_empty()) {
                    tracing::warn!(
                        message_id = %message_id,
                        conversation_id = %fallback,
                        "Message not resolved; using fallback conversation_id"
                    );
                    Ok((fallback.trim().to_string(), message_id.to_string()))
                } else {
                    Err(FlareError::localized(
                        ErrorCode::MessageNotFound,
                        format!("Message not found: {}", message_id),
                    ))
                }
            }
            Err(e) => Err(e),
        }
    }

    /// 按会话分页拉取 `server_id`（时间倒序），供「读到某条」等编排。
    pub async fn page_conversation_server_ids(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        limit: i32,
        cursor: Option<&str>,
    ) -> OrchestratorResult<ConversationServerIdsPage> {
        self.operation_service
            .page_server_ids_in_conversation(ctx, conversation_id, limit, cursor)
            .await
    }
}
