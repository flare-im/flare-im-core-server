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
    // The message body and the caller context must never be recorded on the span.
    #[instrument(skip_all, fields(
        conversation_id = %message.conversation_id,
        client_msg_id = %message.client_msg_id,
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
            // Keep the hook's structured error. Its code, reason and details reach
            // the sender's ACK, which is how a client tells a policy denial apart
            // from an infrastructure failure; wrapping it would flatten it into a
            // generic internal error.
            if let Err(error) = self.hooks.pre_send(&original_context, &mut draft).await {
                tracing::debug!(%error, "PreSend hook refused the message");
                return Err(error);
            }

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
    #[instrument(skip_all, fields(
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use flare_im_hooks::hooks::{
        HookMetadata, HookRegistry, HookSelector, MessageDraft, PreSendDecision, PreSendHook,
    };
    use flare_server_core::context::Context;
    use flare_server_core::error::{ErrorBuilder, FlareError};

    struct PolicyDenial;

    #[async_trait]
    impl PreSendHook for PolicyDenial {
        async fn handle(&self, _ctx: &Ctx, _draft: &mut MessageDraft) -> PreSendDecision {
            PreSendDecision::Reject {
                error: ErrorBuilder::new(ErrorCode::OperationFailed, "pre-send hook rejected")
                    .details("allow=false")
                    .build_error(),
            }
        }
    }

    #[tokio::test]
    async fn a_hook_denial_reaches_the_caller_with_its_reason_and_details() {
        let registry = HookRegistry::new();
        registry
            .register_pre_send(
                HookMetadata::default().with_name("policy"),
                HookSelector::default(),
                Arc::new(PolicyDenial),
            )
            .await;
        let service = HookExecutionService::new(Arc::new(HookDispatcher::new(registry)), None);
        let ctx: Ctx = Arc::new(Context::with_request_id("req").with_tenant_id("tenant-a"));
        let error = match service
            .execute_pre_send(&ctx, Message::default(), true)
            .await
        {
            Ok(_) => panic!("denied message was accepted"),
            Err(error) => error,
        };
        match error {
            FlareError::Localized {
                code,
                reason,
                details,
                ..
            } => {
                assert_eq!(code, ErrorCode::OperationFailed);
                assert_eq!(reason, "pre-send hook rejected");
                assert_eq!(details.as_deref(), Some("allow=false"));
            }
            other => panic!("hook denial was flattened: {other}"),
        }
    }

    /// 放行的 Hook 改写的 metadata 原样成为消息 attributes：Hook 剥掉的客户端属性不能
    /// 留下来，Hook 写入的属性会随消息持久化。Social 的好友纪元围栏依赖这一点——
    /// PreSend 剥掉客户端自带的纪元、写入真实纪元，重新加好友时 Core 按它清除旧消息。
    struct StampEpoch;

    #[async_trait]
    impl PreSendHook for StampEpoch {
        async fn handle(&self, _ctx: &Ctx, draft: &mut MessageDraft) -> PreSendDecision {
            draft.metadata.remove("flare.social.friendship_epoch");
            draft
                .metadata
                .insert("flare.social.friendship_epoch".into(), "3".into());
            PreSendDecision::Continue
        }
    }

    #[tokio::test]
    async fn attributes_written_or_stripped_by_an_allowing_hook_reach_the_message() {
        let registry = HookRegistry::new();
        registry
            .register_pre_send(
                HookMetadata::default().with_name("epoch"),
                HookSelector::default(),
                Arc::new(StampEpoch),
            )
            .await;
        let service = HookExecutionService::new(Arc::new(HookDispatcher::new(registry)), None);
        let ctx: Ctx = Arc::new(Context::with_request_id("req").with_tenant_id("tenant-a"));
        let mut message = Message::default();
        message
            .attributes
            .insert("flare.social.friendship_epoch".into(), "999".into());
        message
            .attributes
            .insert("client_tag".into(), "kept".into());
        let out = service
            .execute_pre_send(&ctx, message, true)
            .await
            .expect("allowed by the hook");
        assert_eq!(
            out.message
                .attributes
                .get("flare.social.friendship_epoch")
                .map(String::as_str),
            Some("3")
        );
        assert_eq!(
            out.message.attributes.get("client_tag").map(String::as_str),
            Some("kept")
        );
    }
}
