//! 消息处理器（编排层）- 负责编排领域服务
//!
//! ## 核心职责
//! 1. 消息校验（调用 MessageDomainService）
//! 2. Hook 执行（调用 HookExecutionService）
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
use crate::domain::PersistenceMode;
use crate::domain::service::{
    MessageDomainService, 
    HookExecutionService,
    ConversationEnsureService,
    build_conversation_ensure_request_from_message,
};
use crate::error::Result;

/// 消息处理器（编排层）
pub struct MessageHandler {
    /// 消息领域服务
    message_domain_service: Arc<MessageDomainService>,
    /// Hook 执行服务
    hook_execution_service: Arc<HookExecutionService>,
    /// 会话确保服务
    conversation_ensure_service: Arc<ConversationEnsureService>,
}

impl MessageHandler {
    pub fn new(
        message_domain_service: Arc<MessageDomainService>,
        hook_execution_service: Arc<HookExecutionService>,
        conversation_ensure_service: Arc<ConversationEnsureService>,
    ) -> Self {
        Self {
            message_domain_service,
            hook_execution_service,
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
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `cmd`: 发送消息命令
    ///
    /// # 返回
    /// - `Ok((message_id, seq))`: 消息 ID 和序列号
    /// - `Err`: 错误
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
        
        // 2. 执行 PreSend Hook
        let hook_context = self.hook_execution_service
            .execute_pre_send(ctx, cmd.message, true)
            .await?;
        
        // 3. 准备消息并分配序列号
        let (mut submission, profile) = self.message_domain_service
            .prepare_and_allocate_seq(ctx, &tenant_id, hook_context.message)
            .await?;
        
        // 4. 写入 WAL
        self.message_domain_service
            .write_wal_if_needed(&submission, &profile)
            .await?;
        
        // 5. 确保会话存在
        self.conversation_ensure_service
            .ensure_conversation(ctx, &build_conversation_ensure_request_from_message(&submission.message, &tenant_id))
            .await?;
        
        // 6. 消息装饰
        submission.message = self.message_domain_service
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
        
        // 8. 执行 PostSend Hook
        self.hook_execution_service
            .execute_post_send(&hook_context.hook_context, &submission)
            .await?;
        
        Ok((submission.message_id, submission.message.seq))
    }

    /// 发送系统消息
    ///
    /// # 编排流程
    /// 1. 准备系统消息内容
    /// 2. 调用消息发送流程
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `cmd`: 发送系统消息命令
    ///
    /// # 返回
    /// - `Ok(message_id)`: 消息 ID
    /// - `Err`: 错误
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
        // 构建发送消息命令
        let send_cmd = SendMessageCommand {
            message: cmd.message,
            conversation_id: cmd.conversation_id,
            sync: false,
        };
        
        // 调用消息发送流程
        let (message_id, _) = self.handle_send_message(ctx, send_cmd).await?;
        
        Ok(message_id)
    }

    /// 批量发送消息
    ///
    /// TODO: 实现批量发送消息逻辑
    /// - 需要批量校验消息
    /// - 批量执行 PreSend Hook
    /// - 批量分配序列号
    /// - 批量写入 WAL
    /// - 批量推送消息
    #[instrument(skip(self, ctx))]
    pub async fn batch_send_message(
        &self,
        ctx: &Ctx,
        messages: Vec<SendMessageCommand>,
    ) -> Result<Vec<Result<(String, u64)>>> {
        // TODO: 实现批量发送消息
        // 1. 批量校验消息
        // 2. 批量执行 PreSend Hook
        // 3. 批量准备消息并分配序列号
        // 4. 批量写入 WAL
        // 5. 批量确保会话存在
        // 6. 批量推送消息
        // 7. 批量执行 PostSend Hook
        let _ = (ctx, messages);
        Ok(vec![])
    }

    /// 发送 ACK
    ///
    /// TODO: 实现 ACK 逻辑
    /// - 处理客户端 ACK
    /// - 更新消息状态
    #[instrument(skip(self, ctx))]
    pub async fn send_ack(&self, ctx: &Ctx, message_id: &str) -> Result<()> {
        // TODO: 实现 ACK 逻辑
        // 1. 验证 ACK 有效性
        // 2. 更新消息状态
        // 3. 触发相关事件
        let _ = (ctx, message_id);
        Ok(())
    }

    /// 发送自定义数据
    ///
    /// TODO: 实现自定义数据发送逻辑
    /// - 用于扩展功能
    #[instrument(skip(self, ctx))]
    pub async fn send_custom_data(&self, ctx: &Ctx, data: Vec<u8>) -> Result<()> {
        // TODO: 实现自定义数据发送
        // 1. 解析自定义数据
        // 2. 执行相应逻辑
        let _ = (ctx, data);
        Ok(())
    }
}
