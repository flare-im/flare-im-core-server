//! 消息处理器（编排层）- 负责编排领域服务
//!
//! ## 核心职责
//! 1. 消息校验（调用 MessageDomainService）
//! 2. Hook 执行（调用 ExtensionOrchestrator）
//! 3. 领域操作编排（调用 MessageDomainService）
//! 4. 会话确保（调用 ConversationEnsureService）
//!
//! ## 设计原则
//! - 编排层：不包含业务逻辑，只负责流程编排
//! - 依赖注入：通过构造函数注入所有依赖
//! - CQRS：Command Handler 负责写操作

use std::sync::Arc;

use flare_im_core::Ctx;
use tracing::instrument;

use crate::application::commands::{SendMessageCommand, SendSystemMessageCommand};
use crate::application::extension::ExtensionOrchestrator;
use crate::domain::PersistenceMode;
use crate::domain::service::{
    ConversationEnsureService, MessageDomainService, build_conversation_ensure_request_from_message,
};
use crate::error::Result;

/// 消息处理器（编排层）
pub struct MessageHandler {
    /// 消息领域服务
    message_domain_service: Arc<MessageDomainService>,
    /// 扩展编排器（统一 Hook / Plugin 执行策略）
    extension_orchestrator: Arc<ExtensionOrchestrator>,
    /// 会话确保服务
    conversation_ensure_service: Arc<ConversationEnsureService>,
}

impl MessageHandler {
    pub fn new(
        message_domain_service: Arc<MessageDomainService>,
        extension_orchestrator: Arc<ExtensionOrchestrator>,
        conversation_ensure_service: Arc<ConversationEnsureService>,
    ) -> Self {
        Self {
            message_domain_service,
            extension_orchestrator,
            conversation_ensure_service,
        }
    }

    /// 处理发送消息命令
    ///
    /// # 编排流程
    /// 1. 校验消息
    /// 2. 执行 PreSend Hook
    /// 3. 准备消息并分配序列号
    /// 4. 写入 WAL
    /// 5. 确保会话存在
    /// 6. 消息装饰
    /// 7. 推送消息
    /// 8. 执行 PostSend Hook
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        conversation_id = %cmd.conversation_id,
    ))]
    pub async fn handle_send_message(
        &self,
        ctx: &Ctx,
        cmd: SendMessageCommand,
    ) -> Result<(String, u64)> {
        let tenant_id = ctx.tenant_id().unwrap_or("0").to_string();

        // 1. 校验消息
        self.message_domain_service
            .validate_message(ctx, &tenant_id, &cmd.message)
            .await?;

        // 2. 执行 PreSend Hook（经统一扩展编排器）
        let hook_context = self
            .extension_orchestrator
            .execute_pre_send(ctx, cmd.message, true)
            .await?;

        // 3. 准备消息并分配序列号
        let (mut submission, profile) = self
            .message_domain_service
            .prepare_and_allocate_seq(ctx, &tenant_id, hook_context.message)
            .await?;

        // 4. 写入 WAL
        self.message_domain_service
            .write_wal_if_needed(&submission, &profile)
            .await?;

        // 5. 确保会话存在
        self.conversation_ensure_service
            .ensure_conversation(
                ctx,
                &build_conversation_ensure_request_from_message(&submission.message, &tenant_id),
            )
            .await?;

        // 6. 消息装饰
        submission.message = self
            .message_domain_service
            .decorate_message(submission.message.clone())
            .await?;

        // 7. 推送消息
        let persistence_mode = if profile.is_temporary() {
            PersistenceMode::ForcePushOnly
        } else {
            PersistenceMode::Auto
        };
        self.message_domain_service
            .push_message(ctx, &submission, &profile, persistence_mode)
            .await?;

        // 8. 执行 PostSend Hook（经统一扩展编排器）
        self.extension_orchestrator
            .execute_post_send(ctx, &submission, &hook_context.hook_context)
            .await?;

        Ok((submission.message_id, submission.message.seq))
    }

    /// 发送系统消息
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        conversation_id = %cmd.conversation_id,
    ))]
    pub async fn send_system_message(
        &self,
        ctx: &Ctx,
        cmd: SendSystemMessageCommand,
    ) -> Result<String> {
        let send_cmd = SendMessageCommand {
            message: cmd.message,
            conversation_id: cmd.conversation_id,
            sync: false,
        };
        let (message_id, _) = self.handle_send_message(ctx, send_cmd).await?;
        Ok(message_id)
    }

    #[instrument(skip(self, ctx))]
    pub async fn batch_send_message(
        &self,
        ctx: &Ctx,
        messages: Vec<SendMessageCommand>,
    ) -> Result<Vec<Result<(String, u64)>>> {
        let _ = (ctx, messages);
        Ok(vec![])
    }

    #[instrument(skip(self, ctx))]
    pub async fn send_ack(&self, ctx: &Ctx, message_id: &str) -> Result<()> {
        let _ = (ctx, message_id);
        Ok(())
    }

    #[instrument(skip(self, ctx))]
    pub async fn send_custom_data(&self, ctx: &Ctx, data: Vec<u8>) -> Result<()> {
        let _ = (ctx, data);
        Ok(())
    }
}
