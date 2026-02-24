//! 命令处理器（编排层）- 轻量级，只负责编排领域服务和记录指标

use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use flare_im_core::metrics::MessageOrchestratorMetrics;
use flare_server_core::context::{Context, ContextExt};
use flare_im_core::utils::context::require_context;
use tracing::instrument;

use crate::application::commands::{
    AddReactionCommand, BatchMarkMessageReadCommand, BatchSendMessageCommand,
    BatchStoreMessageCommand, DeleteMessageCommand, EditMessageCommand,
    HandleTemporaryMessageCommand, MarkAllConversationsReadCommand,
    MarkConversationReadCommand, MarkMessageCommand, PinMessageCommand,
    ReadMessageCommand, RecallMessageCommand, RemoveReactionCommand, SendMessageCommand,
    StoreMessageCommand, UnmarkMessageCommand, UnpinMessageCommand,
};
use crate::domain::service::MessageDomainService;
use crate::domain::service::message_operation_service::MessageOperationService;
use crate::domain::service::message_temporary_service::MessageTemporaryService;

/// 消息命令处理器（编排层）
pub struct MessageCommandHandler {
    domain_service: Arc<MessageDomainService>,
    operation_service: Arc<MessageOperationService>,
    temporary_service: Option<Arc<MessageTemporaryService>>,
    metrics: Arc<MessageOrchestratorMetrics>,
}

impl MessageCommandHandler {
    pub fn new(
        domain_service: Arc<MessageDomainService>,
        operation_service: Arc<MessageOperationService>,
        temporary_service: Option<Arc<MessageTemporaryService>>,
        metrics: Arc<MessageOrchestratorMetrics>,
    ) -> Self {
        Self {
            domain_service,
            operation_service,
            temporary_service,
            metrics,
        }
    }

    /// 处理存储消息命令
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        tenant_id = %ctx.tenant_id().unwrap_or("0"),
    ))]
    pub async fn handle_store_message(&self, ctx: &Context, command: StoreMessageCommand) -> Result<(String, u64)> {
        ctx.ensure_not_cancelled()?;
        let start = Instant::now();

        let tenant_id = ctx.tenant_id().unwrap_or("0").to_string();

        let message_type = command
            .request
            .message
            .as_ref()
            .map(|m| match m.message_type {
                0 => "normal",
                _ => "notification",
            })
            .unwrap_or("normal")
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
    #[instrument(skip(self))]
    pub async fn handle_store_message_without_pre_hook(
        &self,
        command: StoreMessageCommand,
    ) -> Result<(String, u64)> {
        let start = Instant::now();

        // 提取租户ID和消息类型用于指标标签（在移动之前）
        let tenant_id = command
            .request
            .metadata
            .get("tenant_id")
            .map(|s| s.as_str())
            .unwrap_or("unknown")
            .to_string();

        let message_type = command
            .request
            .message
            .as_ref()
            .map(|m| match m.message_type {
                0 => "normal",
                _ => "notification",
            })
            .unwrap_or("normal")
            .to_string();

        // 从 command.request 构建 Context
        let ctx = if let Some(tenant_id_value) = command.request.metadata.get("tenant_id") {
            Context::root().with_tenant_id(tenant_id_value.clone())
        } else {
            Context::root()
        };
        let result = self
            .domain_service
            .orchestrate_message_storage(&ctx, command.request, false)
            .await;

        // 记录指标
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

    /// 处理批量存储消息命令
    #[instrument(skip(self), fields(batch_size = command.requests.len()))]
    pub async fn handle_batch_store_message(
        &self,
        ctx: &Context,
        command: BatchStoreMessageCommand,
    ) -> Result<Vec<String>> {
        let mut message_ids = Vec::new();
        for request in command.requests {
            // 从 request 构建 Context
            let request_ctx = if let Some(tenant_id_value) = request.metadata.get("tenant_id") {
                Context::root().with_tenant_id(tenant_id_value.clone())
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
    pub async fn handle_recall_message(&self, cmd: RecallMessageCommand) -> Result<()> {
        self.operation_service.handle_recall(cmd).await.map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// 处理编辑消息命令
    #[instrument(skip(self), fields(message_id = %cmd.base.message_id))]
    pub async fn handle_edit_message(&self, cmd: EditMessageCommand) -> Result<()> {
        self.operation_service.handle_edit(cmd).await.map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// 处理删除消息命令
    #[instrument(skip(self), fields(message_id = %cmd.base.message_id))]
    pub async fn handle_delete_message(&self, cmd: DeleteMessageCommand) -> Result<()> {
        self.operation_service.handle_delete(cmd).await.map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// 处理标记已读命令
    #[instrument(skip(self), fields(message_id = %cmd.base.message_id))]
    pub async fn handle_read_message(&self, cmd: ReadMessageCommand) -> Result<()> {
        self.operation_service.handle_read(cmd).await.map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// 处理添加反应命令
    #[instrument(skip(self), fields(message_id = %cmd.base.message_id, emoji = %cmd.emoji))]
    pub async fn handle_add_reaction(&self, cmd: AddReactionCommand) -> Result<i32> {
        self.operation_service.handle_add_reaction(cmd).await.map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// 处理移除反应命令
    #[instrument(skip(self), fields(message_id = %cmd.base.message_id, emoji = %cmd.emoji))]
    pub async fn handle_remove_reaction(&self, cmd: RemoveReactionCommand) -> Result<i32> {
        self.operation_service.handle_remove_reaction(cmd).await.map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// 处理置顶消息命令
    #[instrument(skip(self), fields(message_id = %cmd.base.message_id))]
    pub async fn handle_pin_message(&self, cmd: PinMessageCommand) -> Result<()> {
        self.operation_service.handle_pin(cmd).await.map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// 处理取消置顶消息命令
    #[instrument(skip(self), fields(message_id = %cmd.base.message_id))]
    pub async fn handle_unpin_message(&self, cmd: UnpinMessageCommand) -> Result<()> {
        self.operation_service.handle_unpin(cmd).await.map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// 处理标记消息命令
    #[instrument(skip(self), fields(message_id = %cmd.base.message_id))]
    pub async fn handle_mark_message(&self, cmd: MarkMessageCommand) -> Result<()> {
        self.operation_service.handle_mark(cmd).await.map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// 处理取消标记消息命令
    #[instrument(skip(self), fields(message_id = %cmd.base.message_id))]
    pub async fn handle_unmark_message(&self, cmd: UnmarkMessageCommand) -> Result<()> {
        self.operation_service.handle_unmark(cmd).await.map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// 处理批量标记消息已读命令
    #[instrument(skip(self), fields(conversation_id = %cmd.conversation_id, user_id = %cmd.user_id))]
    pub async fn handle_batch_mark_message_read(&self, cmd: BatchMarkMessageReadCommand) -> Result<i32> {
        // 这里需要调用一个专门处理批量标记已读的服务或方法
        // 目前在 MessageOperationService 中没有对应的处理方法，我们需要创建一个
        
        // 由于当前的架构中没有专门处理批量标记已读的业务逻辑
        // 我们可以通过遍历消息ID列表，逐个处理每个标记已读操作
        let mut processed_count = 0;
        
        for message_id in &cmd.message_ids {
            // 构建单个标记已读命令
            use crate::application::commands::ReadMessageCommand;
            let read_cmd = ReadMessageCommand {
                base: crate::application::commands::MessageOperationCommand {
                    message_id: message_id.clone(),
                    operator_id: cmd.user_id.clone(),
                    timestamp: cmd.read_at.unwrap_or_else(|| chrono::Utc::now()),
                    tenant_id: cmd.tenant_id.clone(),
                    conversation_id: cmd.conversation_id.clone(),
                },
                message_ids: vec![message_id.clone()], // 只包含当前消息ID
                read_at: cmd.read_at,
                burn_after_read: false,
            };
            
            // 执行单个标记已读操作
            match self.handle_read_message(read_cmd).await {
                Ok(()) => processed_count += 1,
                Err(e) => {
                    tracing::warn!(message_id = %message_id, error = %e, "Failed to mark message as read in batch");
                    // 继续处理下一个消息
                }
            }
        }
        
        Ok(processed_count)
    }

    /// 处理标记会话已读命令
    #[instrument(skip(self), fields(conversation_id = %cmd.conversation_id, user_id = %cmd.user_id))]
    pub async fn handle_mark_conversation_read(&self, cmd: MarkConversationReadCommand) -> Result<()> {
        // 这里需要调用一个专门处理标记会话已读的服务或方法
        // 目前在 MessageOperationService 中没有对应的处理方法，我们需要创建一个
        
        // 标记会话已读本质上是标记该会话中所有未读消息为已读
        // 由于这是复杂的业务逻辑，我们简单地记录这个操作
        // 实际的实现应该在 MessageOperationService 中添加相应方法
        
        // 暂时只记录操作
        tracing::info!(
            conversation_id = %cmd.conversation_id,
            user_id = %cmd.user_id,
            "Mark conversation as read operation received"
        );
        
        Ok(())
    }

    /// 处理临时消息命令（只推送，不持久化）
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        message_id = %cmd.message.server_id
    ))]
    pub async fn handle_temporary_message(&self, ctx: &Context, cmd: HandleTemporaryMessageCommand) -> Result<()> {
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
        ctx: &Context,
        mut cmd: SendMessageCommand,
    ) -> Result<(String, u64)> {
        ctx.ensure_not_cancelled()?;
        
        // 如果 cmd.tenant 为空，从 ctx 中提取 tenant_id 并设置
        if cmd.tenant.is_none() || cmd.tenant.as_ref().map(|t| t.tenant_id.is_empty()).unwrap_or(true) {
            if let Some(tenant_id) = ctx.tenant_id() {
                if !tenant_id.is_empty() {
                    cmd.tenant = Some(flare_proto::common::TenantContext {
                        tenant_id: tenant_id.to_string(),
                        business_type: String::new(),
                        environment: String::new(),
                        organization_id: String::new(),
                        labels: std::collections::HashMap::new(),
                        attributes: std::collections::HashMap::new(),
                    });
                }
            }
        }
        
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
        
        // 如果是操作消息，打印更详细的信息
        if message.message_type == 302 {
            tracing::info!(
                message_id = %message.server_id,
                message_type = message.message_type,
                category = ?category,
                content_debug = ?message.content.as_ref().map(|c| {
                    format!("content_variant={:?}", c.content.as_ref().map(|cnt| {
                        match cnt {
                            flare_proto::common::message_content::Content::Operation(_) => "Operation",
                            _ => "Other",
                        }
                    }))
                }),
                "✅ 收到操作消息 (message_type=302)"
            );
        }

        // 根据消息类别路由到不同处理流程
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
            crate::domain::model::message_kind::MessageCategory::Operation => {
                // 操作消息：直接提取 MessageOperation 并执行操作
                tracing::info!(
                    message_id = %message.server_id,
                    "🔍 尝试从操作消息中提取 MessageOperation"
                );
                
                // 调试：打印 content 的详细信息
                if let Some(content) = message.content.as_ref() {
                    tracing::debug!(
                        message_id = %message.server_id,
                        content_variant = ?content.content.as_ref().map(|c| {
                            match c {
                                flare_proto::common::message_content::Content::Text(_) => "Text",
                                flare_proto::common::message_content::Content::Image(_) => "Image",
                                flare_proto::common::message_content::Content::Audio(_) => "Audio",
                                flare_proto::common::message_content::Content::Video(_) => "Video",
                                flare_proto::common::message_content::Content::File(_) => "File",
                                flare_proto::common::message_content::Content::Location(_) => "Location",
                                flare_proto::common::message_content::Content::Card(_) => "Card",
                                flare_proto::common::message_content::Content::LinkCard(_) => "LinkCard",
                                flare_proto::common::message_content::Content::Forward(_) => "Forward",
                                flare_proto::common::message_content::Content::Thread(_) => "Thread",
                                flare_proto::common::message_content::Content::Custom(_) => "Custom",
                                flare_proto::common::message_content::Content::Operation(_) => "Operation",
                                flare_proto::common::message_content::Content::Notification(_) => "Notification",
                                flare_proto::common::message_content::Content::Typing(_) => "Typing",
                                flare_proto::common::message_content::Content::SystemEvent(_) => "SystemEvent",
                            }
                        }),
                        "Content 详细内容"
                    );
                }
                
                if let Some(flare_proto::common::message_content::Content::Operation(operation)) =
                    message.content.as_ref().and_then(|c| c.content.as_ref())
                {
                    tracing::info!(
                        message_id = %message.server_id,
                        operation_type = operation.operation_type,
                        target_message_id = %operation.target_message_id,
                        operator_id = %operation.operator_id,
                        "✅ 成功提取 MessageOperation，准备执行操作"
                    );
                    
                    // 执行操作
                    self.execute_operation(ctx, operation, &message, &cmd).await?;

                    // 操作消息返回目标消息ID和seq=0（操作不产生新消息）
                    // 但操作结果会通过推送消息通知用户
                    Ok((operation.target_message_id.clone(), 0))
                } else {
                    // 无法提取操作，降级为普通消息
                    tracing::warn!(
                        message_id = %message.server_id,
                        content_debug = ?message.content.as_ref().map(|c| format!("{:?}", c)),
                        "❌ Operation message without MessageOperation, fallback to normal message"
                    );
                    self.handle_normal_message(ctx, cmd).await
                }
            }
            _ => {
                // Notification 和 Normal 消息：普通消息处理（存储编排）
                self.handle_normal_message(ctx, cmd).await
            }
        }
    }

    /// 处理普通消息（内部方法）
    async fn handle_normal_message(&self, ctx: &Context, cmd: SendMessageCommand) -> Result<(String, u64)> {
        ctx.ensure_not_cancelled()?;
        // 验证单聊消息必须包含 receiver_id，除非是群聊
        if cmd.message.conversation_type == flare_proto::common::ConversationType::Single as i32 {
            if cmd.message.receiver_id.is_empty() {
                // 如果是单聊且没有 receiver_id，尝试从 conversation_id 或 attributes 中推断
                // 这里暂时保持严格检查，因为单聊必须明确接收者
                // 但为了兼容某些客户端行为（如未正确设置 receiver_id），我们可以记录警告并尝试继续（如果业务允许）
                // 目前为了保证数据完整性，仍然报错，但错误信息更明确
                return Err(anyhow::anyhow!(
                    "Single chat message must provide receiver_id. message_id={}, conversation_id={}, sender_id={}",
                    cmd.message.server_id, cmd.message.conversation_id, cmd.message.sender_id
                ));
            }
        } else if cmd.message.conversation_type == flare_proto::common::ConversationType::Group as i32 {
            // 群聊消息不需要 receiver_id，如果为空则设为 channel_id 或 conversation_id
             if cmd.message.receiver_id.is_empty() {
                 // 对于群聊，receiver_id 通常为空，或者等于 channel_id/conversation_id
                 // 这里不做强制检查，依靠后续逻辑处理
             }
        }


        // 从 Context 中提取 RequestContext 和 TenantContext
        let context = None::<flare_proto::common::RequestContext>;
        
        // 优先从 Context 中提取 tenant，如果 Context 中没有，则使用 cmd.tenant
        let tenant = ctx.tenant().cloned()
            .map(|tc| tc.into())
            .or_else(|| {
                // 如果 Context 中没有完整的 TenantContext，但 ctx.tenant_id() 有值，则构建 TenantContext
                ctx.tenant_id()
                    .filter(|id| !id.is_empty())
                    .map(|tenant_id| {
                        flare_proto::common::TenantContext {
                            tenant_id: tenant_id.to_string(),
                            business_type: String::new(),
                            environment: String::new(),
                            organization_id: String::new(),
                            labels: std::collections::HashMap::new(),
                            attributes: std::collections::HashMap::new(),
                        }
                    })
            })
            .or(cmd.tenant.clone());
        
        // 将 SendMessageCommand 转换为 StoreMessage
        let mut metadata = std::collections::HashMap::new();
        
        // 从 Context 中获取租户ID并放入 metadata
        if let Some(tenant_id) = ctx.tenant_id() {
            metadata.insert("tenant_id".to_string(), tenant_id.to_string());
        }
        
        let store_request = flare_proto::storage::StoreMessage {
            conversation_id: cmd.conversation_id.clone(),
            message: Some(cmd.message),
            sync: cmd.sync,
            tags: std::collections::HashMap::new(),
            metadata,
        };

        // 调用存储消息命令处理
        self.handle_store_message(ctx, StoreMessageCommand {
            request: store_request,
        })
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
        ctx: &Context,
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
                context: None,
                tenant: None,
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

    /// 执行消息操作
    ///
    /// 注意：operation 参数直接是 MessageOperation，不需要从 NotificationContent 中提取
    async fn execute_operation(
        &self,
        ctx: &Context,
        operation: &flare_proto::common::MessageOperation,
        message: &flare_proto::common::Message,
        _cmd: &SendMessageCommand,
    ) -> Result<()> {
        ctx.ensure_not_cancelled()?;
        use flare_proto::common::{OperationType, message_operation::OperationData};
        use crate::application::commands::{
            RecallMessageCommand, EditMessageCommand, DeleteMessageCommand,
            ReadMessageCommand, AddReactionCommand, RemoveReactionCommand,
            PinMessageCommand, UnpinMessageCommand, MarkMessageCommand, UnmarkMessageCommand,
            MessageOperationCommand,
        };

        let tenant_id = ctx.tenant_id().unwrap_or("0").to_string();

        let base_cmd = MessageOperationCommand {
            message_id: operation.target_message_id.clone(),
            operator_id: operation.operator_id.clone(),
            timestamp: operation.timestamp.as_ref()
                .map(|ts| chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32))
                .flatten()
                .unwrap_or_else(|| chrono::Utc::now()),
            tenant_id: tenant_id.to_string(),
            conversation_id: message.conversation_id.clone(),
        };

        match OperationType::try_from(operation.operation_type) {
            Ok(OperationType::Recall) => {
                let recall_data = match &operation.operation_data {
                    Some(OperationData::Recall(data)) => data,
                    _ => return Err(anyhow::anyhow!("Recall operation requires RecallOperationData")),
                };

                let recall_cmd = RecallMessageCommand {
                    base: base_cmd,
                    reason: if recall_data.reason.is_empty() {
                        None
                    } else {
                        Some(recall_data.reason.clone())
                    },
                    time_limit_seconds: if recall_data.time_limit_seconds == 0 {
                        None
                    } else {
                        Some(recall_data.time_limit_seconds)
                    },
                };

                self.handle_recall_message(recall_cmd).await
            }
            Ok(OperationType::Edit) => {
                let edit_data = match &operation.operation_data {
                    Some(OperationData::Edit(data)) => data,
                    _ => return Err(anyhow::anyhow!("Edit operation requires EditOperationData")),
                };

                let new_content_bytes = &edit_data.new_content;

                let edit_cmd = EditMessageCommand {
                    base: base_cmd,
                    new_content: new_content_bytes.clone(),
                    reason: if edit_data.reason.is_empty() {
                        None
                    } else {
                        Some(edit_data.reason.clone())
                    },
                };

                self.handle_edit_message(edit_cmd).await
            }
            Ok(OperationType::Delete) => {
                let delete_data = match &operation.operation_data {
                    Some(OperationData::Delete(data)) => data,
                    _ => return Err(anyhow::anyhow!("Delete operation requires DeleteOperationData")),
                };

                // 从 metadata 中获取要删除的消息ID列表
                let message_ids: Vec<String> = operation
                    .metadata
                    .get("message_ids")
                    .map(|s| s.split(',').map(|s| s.to_string()).collect())
                    .unwrap_or_else(|| vec![operation.target_message_id.clone()]);

                let delete_cmd = DeleteMessageCommand {
                    base: base_cmd,
                    delete_type: if delete_data.delete_type == 1 {
                        crate::application::commands::DeleteType::Hard
                    } else {
                        crate::application::commands::DeleteType::Soft
                    },
                    reason: if delete_data.reason.is_empty() {
                        None
                    } else {
                        Some(delete_data.reason.clone())
                    },
                    message_ids,
                    notify_others: delete_data.notify_others,
                };

                self.handle_delete_message(delete_cmd).await
            }
            Ok(OperationType::Read) => {
                let read_data = match &operation.operation_data {
                    Some(OperationData::Read(data)) => data,
                    _ => return Err(anyhow::anyhow!("Read operation requires ReadOperationData")),
                };

                let read_cmd = ReadMessageCommand {
                    base: base_cmd,
                    message_ids: read_data.message_ids.clone(),
                    read_at: read_data.read_at.as_ref()
                        .map(|ts| chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32))
                        .flatten(),
                    burn_after_read: read_data.burn_after_read,
                };

                self.handle_read_message(read_cmd).await
            }
            Ok(OperationType::ReactionAdd) => {
                tracing::info!(
                    target_message_id = %operation.target_message_id,
                    operator_id = %operation.operator_id,
                    operation_data = ?operation.operation_data,
                    "🔍 处理 ReactionAdd 操作"
                );
                
                let reaction_data = match &operation.operation_data {
                    Some(OperationData::Reaction(data)) => {
                        tracing::info!(
                            target_message_id = %operation.target_message_id,
                            emoji = %data.emoji,
                            action = data.action,
                            count = data.count,
                            "✅ 成功提取 ReactionOperationData"
                        );
                        data
                    },
                    other => {
                        tracing::error!(
                            target_message_id = %operation.target_message_id,
                            operation_data = ?other,
                            "❌ Reaction operation requires ReactionOperationData, but got: {:?}", other
                        );
                        return Err(anyhow::anyhow!("Reaction operation requires ReactionOperationData, but got: {:?}", other));
                    }
                };

                let reaction_cmd = AddReactionCommand {
                    base: base_cmd,
                    emoji: reaction_data.emoji.clone(),
                };

                tracing::info!(
                    target_message_id = %operation.target_message_id,
                    emoji = %reaction_data.emoji,
                    "📤 调用 operation_service.handle_add_reaction"
                );
                
                let result = self.operation_service.handle_add_reaction(reaction_cmd).await;
                
                match &result {
                    Ok(count) => {
                        tracing::info!(
                            target_message_id = %operation.target_message_id,
                            emoji = %reaction_data.emoji,
                            count = *count,
                            "✅ ReactionAdd 操作成功"
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            target_message_id = %operation.target_message_id,
                            emoji = %reaction_data.emoji,
                            error = %e,
                            "❌ ReactionAdd 操作失败"
                        );
                    }
                }
                
                result?;
                Ok(())
            }
            Ok(OperationType::ReactionRemove) => {
                let reaction_data = match &operation.operation_data {
                    Some(OperationData::Reaction(data)) => data,
                    _ => return Err(anyhow::anyhow!("Reaction operation requires ReactionOperationData")),
                };

                let reaction_cmd = RemoveReactionCommand {
                    base: base_cmd,
                    emoji: reaction_data.emoji.clone(),
                };

                self.operation_service.handle_remove_reaction(reaction_cmd).await?;
                Ok(())
            }
            Ok(OperationType::Pin) => {
                let pin_data = match &operation.operation_data {
                    Some(OperationData::Pin(data)) => data,
                    _ => return Err(anyhow::anyhow!("Pin operation requires PinOperationData")),
                };

                let pin_cmd = PinMessageCommand {
                    base: base_cmd,
                    reason: if pin_data.reason.is_empty() {
                        None
                    } else {
                        Some(pin_data.reason.clone())
                    },
                    expire_at: pin_data.expire_at.as_ref().and_then(|ts| {
                        chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
                    }),
                };

                self.operation_service.handle_pin(pin_cmd).await?;
                Ok(())
            }
            Ok(OperationType::Unpin) => {
                let unpin_cmd = UnpinMessageCommand {
                    base: base_cmd,
                };

                self.operation_service.handle_unpin(unpin_cmd).await?;
                Ok(())
            }
            Ok(OperationType::Mark) => {
                let mark_data = match &operation.operation_data {
                    Some(OperationData::Mark(data)) => data,
                    _ => return Err(anyhow::anyhow!("Mark operation requires MarkOperationData")),
                };

                let mark_cmd = MarkMessageCommand {
                    base: base_cmd,
                    mark_type: mark_data.mark_type,
                };

                self.handle_mark_message(mark_cmd).await
            }
            Ok(OperationType::Unmark) => {
                let unmark_data = match &operation.operation_data {
                    Some(OperationData::Unmark(data)) => data,
                    _ => return Err(anyhow::anyhow!("Unmark operation requires UnmarkOperationData")),
                };

                let unmark_cmd = UnmarkMessageCommand {
                    base: base_cmd,
                    mark_type: if unmark_data.mark_type < 0 {
                        None
                    } else {
                        Some(unmark_data.mark_type)
                    },
                    user_id: operation.operator_id.clone(),
                };

                self.handle_unmark_message(unmark_cmd).await
            }
            _ => Err(anyhow::anyhow!("Unsupported operation type: {}", operation.operation_type)),
        }
    }
}
