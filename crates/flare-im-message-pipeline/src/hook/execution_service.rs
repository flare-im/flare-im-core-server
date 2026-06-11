//! Hook 执行领域服务
//!
//! ## 核心职责
//! 1. PreSend Hook 执行
//! 2. PostSend Hook 执行
//! 3. Hook 上下文构建
//!
//! ## 设计原则
//! - 单一职责：只负责 Hook 的执行
//! - 依赖注入：通过构造函数注入 HookDispatcher
//! - 纯领域逻辑：不依赖外部基础设施

use std::sync::Arc;

use flare_im_contracts::Ctx;
use flare_im_hooks::hooks::HookDispatcher;
use flare_proto::common::Message;
use flare_server_core::error::AnyhowContext;
use flare_server_core::error::ErrorCode;
use flare_server_core::flare_err;
use tracing::instrument;

use super::SubmittedMessage;
use super::builder::{
    apply_draft_to_request, build_draft_from_request, build_hook_context,
    build_hook_context_from_ctx, build_message_record, draft_from_submission, merge_context,
};
use flare_server_core::error::Result;

/// Hook 执行结果
pub struct HookExecutionContext {
    /// 更新后的消息
    pub message: Message,
    /// Hook 上下文（用于 PostSend）
    pub hook_context: Ctx,
}

/// Hook 执行领域服务
pub struct HookExecutionService {
    /// Hook 分发器
    hooks: Arc<HookDispatcher>,
    /// 默认租户 ID
    default_tenant_id: Option<String>,
}

impl HookExecutionService {
    pub fn new(hooks: Arc<HookDispatcher>, default_tenant_id: Option<String>) -> Self {
        Self {
            hooks,
            default_tenant_id,
        }
    }

    /// 执行 PreSend Hook
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `message`: 原始消息
    /// - `execute_pre_send`: 是否执行 PreSend Hook
    ///
    /// # 返回
    /// - `Ok(HookExecutionContext)`: 包含更新后的消息和 Hook 上下文
    /// - `Err`: 错误
    #[instrument(skip(self), fields(
        conversation_id = %message.conversation_id,
        message_type = message.message_type,
    ))]
    pub async fn execute_pre_send(
        &self,
        ctx: &Ctx,
        mut message: Message,
        execute_pre_send: bool,
    ) -> Result<HookExecutionContext> {
        // 构建原始 Hook 上下文
        let original_context = build_hook_context_from_ctx(ctx, &message);

        // 构建 Draft
        let mut draft = build_draft_from_request(&message)
            .with_context(|| "Failed to build draft from request")?;

        // 执行 PreSend Hook
        if execute_pre_send {
            self.hooks
                .pre_send(&original_context, &mut draft)
                .await
                .with_context(|| "PreSend hook failed")?;

            // 应用 Draft 到消息
            apply_draft_to_request(&mut message, &draft);
        }

        // 构建更新后的上下文
        let updated_context = build_hook_context(&message, self.default_tenant_id.as_ref());
        let hook_context = merge_context(&original_context, updated_context);

        Ok(HookExecutionContext {
            message,
            hook_context,
        })
    }

    /// 执行 PostSend Hook
    ///
    /// # 参数
    /// - `hook_context`: Hook 上下文（来自 PreSend）
    /// - `submission`: 消息提交
    ///
    /// # 返回
    /// - `Ok(())`: 成功
    /// - `Err`: 错误
    #[instrument(skip(self, submission), fields(
        conversation_id = %submission.message().conversation_id,
        message_id = %submission.message_id(),
    ))]
    pub async fn execute_post_send<S>(&self, hook_context: &Ctx, submission: &S) -> Result<()>
    where
        S: SubmittedMessage + ?Sized,
    {
        // 构建消息记录
        let record = build_message_record(submission, submission.message());

        // 构建 Draft
        let post_draft =
            draft_from_submission(submission).context("Failed to build draft from submission")?;

        // 执行 PostSend Hook
        self.hooks
            .post_send(hook_context, &record, &post_draft)
            .await
            .map_err(|e| {
                flare_err!(
                    ErrorCode::InternalError,
                    &format!("PostSend hook failed: {}", e)
                )
            })
    }
}
