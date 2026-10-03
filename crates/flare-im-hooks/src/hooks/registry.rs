use std::sync::Arc;

use crate::Ctx;
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError, Result};
use once_cell::sync::OnceCell;
use tokio::sync::RwLock;

use super::selector::HookSelector;
use super::types::{
    ConversationLifecycleEvent, ConversationLifecycleHook, ConversationMemberEvent,
    ConversationMemberHook, DeliveryEvent, DeliveryHook, HookDecision, HookKind, HookMetadata,
    HookOutcome, MessageDraft, MessageReactionEvent, MessageReactionHook, MessageReadEvent,
    MessageReadHook, MessageRecord, PostSendHook, PreSendDecision, PreSendHook, RecallEvent,
    RecallHook,
};

#[derive(Debug)]
struct RegistryEntry<T: ?Sized> {
    metadata: HookMetadata,
    selector: HookSelector,
    handler: Arc<T>,
}

impl<T: ?Sized> RegistryEntry<T> {
    fn new(metadata: HookMetadata, selector: HookSelector, handler: Arc<T>) -> Self {
        Self {
            metadata,
            selector,
            handler,
        }
    }
}

impl<T: ?Sized> Clone for RegistryEntry<T> {
    fn clone(&self) -> Self {
        Self {
            metadata: self.metadata.clone(),
            selector: self.selector.clone(),
            handler: Arc::clone(&self.handler),
        }
    }
}

/// Hook 执行计划
#[derive(Clone)]
pub struct PreSendPlan {
    metadata: HookMetadata,
    handler: Arc<dyn PreSendHook>,
}

impl PreSendPlan {
    pub fn metadata(&self) -> &HookMetadata {
        &self.metadata
    }

    pub async fn execute(&self, ctx: &Ctx, draft: &mut MessageDraft) -> PreSendDecision {
        let fut = self.handler.handle(ctx, draft);
        match tokio::time::timeout(self.metadata.timeout, fut).await {
            Ok(decision) => match decision {
                PreSendDecision::Continue => PreSendDecision::Continue,
                PreSendDecision::Reject { error } => {
                    let error = annotate(error, &self.metadata);
                    if !self.metadata.require_success
                        && should_fail_open_on_hook_unavailable(&error)
                    {
                        tracing::warn!(
                            hook = %self.metadata.name,
                            reason = %error,
                            "hook pre-send unavailable, degrade to no-hook path"
                        );
                        PreSendDecision::Continue
                    } else {
                        PreSendDecision::Reject { error }
                    }
                }
            },
            Err(_) => {
                // 超时与不可达同属「Hook 不可用」：回可重试的 ServiceUnavailable（摄入侧 gRPC
                // UNAVAILABLE，HTTP 发送 503），与 gRPC 适配器对传输失败的应答一致。若回
                // OperationTimeout，摄入侧映射为 DEADLINE_EXCEEDED，网关回 500，调用方无从判断能否重试。
                let err =
                    ErrorBuilder::new(ErrorCode::ServiceUnavailable, "pre-send hook timed out")
                        .details(format!("hook={}", self.metadata.name))
                        .build_error();
                if self.metadata.require_success {
                    PreSendDecision::Reject { error: err }
                } else {
                    tracing::warn!(hook = %self.metadata.name, "pre-send hook timeout ignored");
                    PreSendDecision::Continue
                }
            }
        }
    }
}

fn annotate(err: FlareError, metadata: &HookMetadata) -> FlareError {
    if let Some(localized) = err.as_localized()
        && localized.details.is_none()
    {
        tracing::trace!(
            hook = %metadata.name,
            "hook error returned without details"
        );
    }
    err
}

#[derive(Default)]
pub struct HookRegistry {
    pre_send: RwLock<Vec<RegistryEntry<dyn PreSendHook>>>,
    post_send: RwLock<Vec<RegistryEntry<dyn PostSendHook>>>,
    delivery: RwLock<Vec<RegistryEntry<dyn DeliveryHook>>>,
    recall: RwLock<Vec<RegistryEntry<dyn RecallHook>>>,
    message_read: RwLock<Vec<RegistryEntry<dyn MessageReadHook>>>,
    message_reaction: RwLock<Vec<RegistryEntry<dyn MessageReactionHook>>>,
    conversation_lifecycle: RwLock<Vec<RegistryEntry<dyn ConversationLifecycleHook>>>,
    conversation_member: RwLock<Vec<RegistryEntry<dyn ConversationMemberHook>>>,
}

impl HookRegistry {
    pub fn builder() -> HookRegistryBuilder {
        HookRegistryBuilder::default()
    }

    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub async fn register_pre_send(
        &self,
        metadata: HookMetadata,
        selector: HookSelector,
        handler: Arc<dyn PreSendHook>,
    ) {
        let mut guard = self.pre_send.write().await;
        guard.push(RegistryEntry::new(
            metadata.with_kind(HookKind::PreSend),
            selector,
            handler,
        ));
        guard.sort_by_key(|a| a.metadata.priority);
    }

    pub async fn register_post_send(
        &self,
        metadata: HookMetadata,
        selector: HookSelector,
        handler: Arc<dyn PostSendHook>,
    ) {
        let mut guard = self.post_send.write().await;
        guard.push(RegistryEntry::new(
            metadata.with_kind(HookKind::PostSend),
            selector,
            handler,
        ));
        guard.sort_by_key(|a| a.metadata.priority);
    }

    pub async fn register_delivery(
        &self,
        metadata: HookMetadata,
        selector: HookSelector,
        handler: Arc<dyn DeliveryHook>,
    ) {
        let mut guard = self.delivery.write().await;
        guard.push(RegistryEntry::new(
            metadata.with_kind(HookKind::Delivery),
            selector,
            handler,
        ));
        guard.sort_by_key(|a| a.metadata.priority);
    }

    pub async fn register_recall(
        &self,
        metadata: HookMetadata,
        selector: HookSelector,
        handler: Arc<dyn RecallHook>,
    ) {
        let mut guard = self.recall.write().await;
        guard.push(RegistryEntry::new(
            metadata.with_kind(HookKind::Recall),
            selector,
            handler,
        ));
        guard.sort_by_key(|a| a.metadata.priority);
    }

    pub async fn register_message_read(
        &self,
        metadata: HookMetadata,
        selector: HookSelector,
        handler: Arc<dyn MessageReadHook>,
    ) {
        let mut guard = self.message_read.write().await;
        guard.push(RegistryEntry::new(
            metadata.with_kind(HookKind::MessageRead),
            selector,
            handler,
        ));
        guard.sort_by_key(|a| a.metadata.priority);
    }

    pub async fn register_message_reaction(
        &self,
        metadata: HookMetadata,
        selector: HookSelector,
        handler: Arc<dyn MessageReactionHook>,
    ) {
        let mut guard = self.message_reaction.write().await;
        guard.push(RegistryEntry::new(
            metadata.with_kind(HookKind::MessageReaction),
            selector,
            handler,
        ));
        guard.sort_by_key(|a| a.metadata.priority);
    }

    pub async fn register_conversation_lifecycle(
        &self,
        metadata: HookMetadata,
        selector: HookSelector,
        handler: Arc<dyn ConversationLifecycleHook>,
    ) {
        let mut guard = self.conversation_lifecycle.write().await;
        guard.push(RegistryEntry::new(
            metadata.with_kind(HookKind::ConversationLifecycle),
            selector,
            handler,
        ));
        guard.sort_by_key(|a| a.metadata.priority);
    }

    pub async fn register_conversation_member(
        &self,
        metadata: HookMetadata,
        selector: HookSelector,
        handler: Arc<dyn ConversationMemberHook>,
    ) {
        let mut guard = self.conversation_member.write().await;
        guard.push(RegistryEntry::new(
            metadata.with_kind(HookKind::ConversationMember),
            selector,
            handler,
        ));
        guard.sort_by_key(|a| a.metadata.priority);
    }

    pub async fn plan_pre_send(&self, ctx: &Ctx) -> Vec<PreSendPlan> {
        let guard = self.pre_send.read().await;
        guard
            .iter()
            .filter(|entry| entry.selector.matches(ctx))
            .map(|entry| PreSendPlan {
                metadata: entry.metadata.clone(),
                handler: Arc::clone(&entry.handler),
            })
            .collect()
    }

    pub async fn execute_pre_send(&self, ctx: &Ctx, draft: &mut MessageDraft) -> Result<()> {
        for plan in self.plan_pre_send(ctx).await {
            match plan.execute(ctx, draft).await {
                PreSendDecision::Continue => continue,
                PreSendDecision::Reject { error } => return Err(error),
            }
        }
        Ok(())
    }

    pub async fn execute_post_send(
        &self,
        ctx: &Ctx,
        record: &MessageRecord,
        draft: &MessageDraft,
    ) -> Result<()> {
        let guard = self.post_send.read().await;
        for entry in guard.iter().filter(|entry| entry.selector.matches(ctx)) {
            let fut = entry.handler.handle(ctx, record, draft);
            let outcome = tokio::time::timeout(entry.metadata.timeout, fut).await;
            let outcome = match outcome {
                Ok(result) => result,
                Err(_) => timeout_outcome(&entry.metadata, "post-send hook timed out")?,
            };
            if let Err(err) = outcome.into_result(&entry.metadata) {
                handle_hook_error_or_degrade(&entry.metadata, "post_send", err)?;
            }
        }
        Ok(())
    }

    pub async fn execute_delivery(&self, ctx: &Ctx, event: &DeliveryEvent) -> Result<()> {
        let guard = self.delivery.read().await;
        for entry in guard.iter().filter(|entry| entry.selector.matches(ctx)) {
            let fut = entry.handler.handle(ctx, event);
            let outcome = tokio::time::timeout(entry.metadata.timeout, fut).await;
            let outcome = match outcome {
                Ok(result) => result,
                Err(_) => timeout_outcome(&entry.metadata, "delivery hook timed out")?,
            };
            if let Err(err) = outcome.into_result(&entry.metadata) {
                handle_hook_error_or_degrade(&entry.metadata, "delivery", err)?;
            }
        }
        Ok(())
    }

    pub async fn execute_recall(&self, ctx: &Ctx, event: &RecallEvent) -> Result<()> {
        let guard = self.recall.read().await;
        for entry in guard.iter().filter(|entry| entry.selector.matches(ctx)) {
            let fut = entry.handler.handle(ctx, event);
            let outcome = tokio::time::timeout(entry.metadata.timeout, fut).await;
            let outcome = match outcome {
                Ok(result) => result,
                Err(_) => timeout_outcome(&entry.metadata, "recall hook timed out")?,
            };
            if let Err(err) = outcome.into_result(&entry.metadata) {
                handle_hook_error_or_degrade(&entry.metadata, "recall", err)?;
            }
        }
        Ok(())
    }

    pub async fn execute_message_read(&self, ctx: &Ctx, event: &MessageReadEvent) -> Result<()> {
        let guard = self.message_read.read().await;
        for entry in guard.iter().filter(|entry| entry.selector.matches(ctx)) {
            let fut = entry.handler.handle(ctx, event);
            let outcome = tokio::time::timeout(entry.metadata.timeout, fut).await;
            let outcome = match outcome {
                Ok(result) => result,
                Err(_) => timeout_outcome(&entry.metadata, "message-read hook timed out")?,
            };
            if let Err(err) = outcome.into_result(&entry.metadata) {
                handle_hook_error_or_degrade(&entry.metadata, "message_read", err)?;
            }
        }
        Ok(())
    }

    pub async fn execute_message_reaction(
        &self,
        ctx: &Ctx,
        event: &MessageReactionEvent,
    ) -> Result<()> {
        let guard = self.message_reaction.read().await;
        for entry in guard.iter().filter(|entry| entry.selector.matches(ctx)) {
            let fut = entry.handler.handle(ctx, event);
            let decision = tokio::time::timeout(entry.metadata.timeout, fut).await;
            let decision = match decision {
                Ok(result) => result,
                Err(_) => timeout_decision(&entry.metadata, "message-reaction hook timed out")?,
            };
            if let Err(err) = decision.into_result() {
                handle_hook_error_or_degrade(&entry.metadata, "message_reaction", err)?;
            }
        }
        Ok(())
    }

    pub async fn execute_conversation_lifecycle(
        &self,
        ctx: &Ctx,
        event: &ConversationLifecycleEvent,
    ) -> Result<()> {
        let guard = self.conversation_lifecycle.read().await;
        for entry in guard.iter().filter(|entry| entry.selector.matches(ctx)) {
            let fut = entry.handler.handle(ctx, event);
            let outcome = tokio::time::timeout(entry.metadata.timeout, fut).await;
            let outcome = match outcome {
                Ok(result) => result,
                Err(_) => {
                    timeout_outcome(&entry.metadata, "conversation-lifecycle hook timed out")?
                }
            };
            if let Err(err) = outcome.into_result(&entry.metadata) {
                handle_hook_error_or_degrade(&entry.metadata, "conversation_lifecycle", err)?;
            }
        }
        Ok(())
    }

    pub async fn execute_conversation_member(
        &self,
        ctx: &Ctx,
        event: &ConversationMemberEvent,
    ) -> Result<()> {
        let guard = self.conversation_member.read().await;
        for entry in guard.iter().filter(|entry| entry.selector.matches(ctx)) {
            let fut = entry.handler.handle(ctx, event);
            let decision = tokio::time::timeout(entry.metadata.timeout, fut).await;
            let decision = match decision {
                Ok(result) => result,
                Err(_) => timeout_decision(&entry.metadata, "conversation-member hook timed out")?,
            };
            if let Err(err) = decision.into_result() {
                handle_hook_error_or_degrade(&entry.metadata, "conversation_member", err)?;
            }
        }
        Ok(())
    }
}

fn handle_hook_error_or_degrade(
    metadata: &HookMetadata,
    phase: &str,
    err: FlareError,
) -> Result<()> {
    if should_fail_open_on_hook_unavailable(&err) {
        tracing::warn!(
            hook = %metadata.name,
            phase,
            reason = %err,
            "hook unavailable, degrade to no-hook path"
        );
        return Ok(());
    }
    Err(err)
}

fn should_fail_open_on_hook_unavailable(err: &FlareError) -> bool {
    matches!(
        err.code(),
        Some(
            ErrorCode::ConnectionFailed
                | ErrorCode::ConnectionTimeout
                | ErrorCode::ConnectionClosed
                | ErrorCode::ConnectionRefused
                | ErrorCode::ServiceUnavailable
                | ErrorCode::ResourceExhausted
                | ErrorCode::NetworkError
                | ErrorCode::NetworkTimeout
                | ErrorCode::NetworkUnreachable
                | ErrorCode::NetworkConnectionLost
                | ErrorCode::OperationTimeout
                | ErrorCode::HttpBadGateway
                | ErrorCode::HttpServiceUnavailable
                | ErrorCode::HttpGatewayTimeout
                | ErrorCode::HttpRequestTimeout
        )
    )
}

/// 必需的事件类 Hook（PreSend 之外的各类）超时：与不可达同属「Hook 不可用」，回可重试的
/// `ServiceUnavailable`，与 PreSend 的口径一致（W12-T）。这些 Hook 跑在 MQ 消费者上时，消费者按
/// `is_retryable()` 交给重投，按既有重试与退避预算用尽后才进死信；以前回 `OperationTimeout`
/// （不可重试），一次暂时的超时就直接进死信（W13-D）。同步发送路径上的 PostSend 失败由
/// `HookExecutionService` 收成 `InternalError`、摄入在受理之后只记日志，应答不受影响。
fn required_timeout_error(metadata: &HookMetadata, message: &str) -> FlareError {
    metadata.build_error(ErrorCode::ServiceUnavailable, message)
}

fn timeout_outcome(metadata: &HookMetadata, message: &str) -> Result<HookOutcome> {
    if metadata.require_success {
        Err(required_timeout_error(metadata, message))
    } else {
        tracing::warn!(hook = %metadata.name, "{message} ignored");
        Ok(HookOutcome::Completed)
    }
}

fn timeout_decision(metadata: &HookMetadata, message: &str) -> Result<HookDecision> {
    if metadata.require_success {
        Err(required_timeout_error(metadata, message))
    } else {
        tracing::warn!(hook = %metadata.name, "{message} ignored");
        Ok(HookDecision::Allow)
    }
}

#[derive(Default)]
pub struct HookRegistryBuilder {
    registry: Option<Arc<HookRegistry>>,
}

impl HookRegistryBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_registry(mut self, registry: Arc<HookRegistry>) -> Self {
        self.registry = Some(registry);
        self
    }

    pub fn build(self) -> Arc<HookRegistry> {
        #[allow(clippy::unwrap_or_default)] // HookRegistry::new() returns Arc<Self>, not Self
        self.registry.unwrap_or_else(HookRegistry::new)
    }
}

static GLOBAL_REGISTRY: OnceCell<Arc<HookRegistry>> = OnceCell::new();

pub struct GlobalHookRegistry;

impl GlobalHookRegistry {
    pub fn init(registry: Arc<HookRegistry>) -> Arc<HookRegistry> {
        GLOBAL_REGISTRY.get_or_init(|| registry).clone()
    }

    pub fn get() -> Arc<HookRegistry> {
        GLOBAL_REGISTRY
            .get()
            .cloned()
            .unwrap_or_else(|| GLOBAL_REGISTRY.get_or_init(HookRegistry::new).clone())
    }
}

#[cfg(test)]
mod tests {
    use super::HookRegistry;
    use crate::Ctx;
    use crate::hooks::selector::HookSelector;
    use crate::hooks::{
        ConversationLifecycleEvent, ConversationLifecycleEventKind, ConversationLifecycleHook,
        ConversationMemberChangeKind, ConversationMemberEvent, ConversationMemberHook,
        DeliveryEvent, DeliveryHook, HookDecision, HookMetadata, HookOutcome, MessageDraft,
        MessageReactionEvent, MessageReactionHook, MessageReadEvent, MessageReadHook,
        MessageRecord, PostSendHook, PreSendDecision, PreSendHook, RecallEvent, RecallHook,
    };
    use async_trait::async_trait;
    use flare_server_core::context::Context;
    use flare_server_core::error::{ErrorBuilder, ErrorCode};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::SystemTime;
    use tokio::time::Duration;

    struct RejectServiceUnavailablePreSend;

    #[async_trait]
    impl PreSendHook for RejectServiceUnavailablePreSend {
        async fn handle(&self, _ctx: &Ctx, _draft: &mut MessageDraft) -> PreSendDecision {
            PreSendDecision::Reject {
                error: ErrorBuilder::new(ErrorCode::ServiceUnavailable, "hook down").build_error(),
            }
        }
    }

    struct RejectBusinessPreSend;

    #[async_trait]
    impl PreSendHook for RejectBusinessPreSend {
        async fn handle(&self, _ctx: &Ctx, _draft: &mut MessageDraft) -> PreSendDecision {
            PreSendDecision::Reject {
                error: ErrorBuilder::new(ErrorCode::OperationFailed, "business reject")
                    .build_error(),
            }
        }
    }

    struct FailedUnavailableDelivery;

    #[async_trait]
    impl DeliveryHook for FailedUnavailableDelivery {
        async fn handle(&self, _ctx: &Ctx, _event: &DeliveryEvent) -> crate::hooks::HookOutcome {
            crate::hooks::HookOutcome::Failed(
                ErrorBuilder::new(ErrorCode::ServiceUnavailable, "delivery hook down")
                    .build_error(),
            )
        }
    }

    struct SlowPreSendHook;

    #[async_trait]
    impl PreSendHook for SlowPreSendHook {
        async fn handle(&self, _ctx: &Ctx, _draft: &mut MessageDraft) -> PreSendDecision {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn optional_pre_send_unavailable_hook_degrades_to_continue() {
        let registry = HookRegistry::new();
        registry
            .register_pre_send(
                HookMetadata::default()
                    .with_name("pre-send-unavailable")
                    .with_require_success(false),
                HookSelector::default(),
                Arc::new(RejectServiceUnavailablePreSend),
            )
            .await;

        let mut draft = MessageDraft::new(vec![1, 2, 3]);
        let ctx: Ctx = Arc::new(Context::with_request_id("req-pre-send-unavailable"));
        let result = registry.execute_pre_send(&ctx, &mut draft).await;
        assert!(result.is_ok(), "unavailable hook should degrade");
    }

    #[tokio::test]
    async fn pre_send_business_reject_still_blocks() {
        let registry = HookRegistry::new();
        registry
            .register_pre_send(
                HookMetadata::default().with_name("pre-send-business-reject"),
                HookSelector::default(),
                Arc::new(RejectBusinessPreSend),
            )
            .await;

        let mut draft = MessageDraft::new(vec![1, 2, 3]);
        let ctx: Ctx = Arc::new(Context::with_request_id("req-pre-send-business"));
        let result = registry.execute_pre_send(&ctx, &mut draft).await;
        assert!(result.is_err(), "business reject should remain fail-closed");
    }

    #[tokio::test]
    async fn optional_pre_send_timeout_degrades_to_continue() {
        let registry = HookRegistry::new();
        registry
            .register_pre_send(
                HookMetadata::default()
                    .with_name("pre-send-timeout")
                    .with_require_success(false)
                    .with_timeout(Duration::from_millis(1)),
                HookSelector::default(),
                Arc::new(SlowPreSendHook),
            )
            .await;

        let mut draft = MessageDraft::new(vec![1, 2, 3]);
        let ctx: Ctx = Arc::new(Context::with_request_id("req-pre-send-timeout"));
        let result = registry.execute_pre_send(&ctx, &mut draft).await;
        assert!(result.is_ok(), "hook timeout should degrade");
    }

    #[tokio::test]
    async fn required_pre_send_unavailable_hook_rejects() {
        let registry = HookRegistry::new();
        registry
            .register_pre_send(
                HookMetadata::default().with_require_success(true),
                HookSelector::default(),
                Arc::new(RejectServiceUnavailablePreSend),
            )
            .await;
        let ctx: Ctx = Arc::new(Context::with_request_id("required-unavailable"));
        let result = registry
            .execute_pre_send(&ctx, &mut MessageDraft::new(vec![]))
            .await;
        assert_eq!(
            result.unwrap_err().code(),
            Some(ErrorCode::ServiceUnavailable)
        );
    }

    #[tokio::test]
    async fn pre_send_connection_errors_obey_required_policy() {
        struct RejectConnection(ErrorCode);
        #[async_trait]
        impl PreSendHook for RejectConnection {
            async fn handle(&self, _ctx: &Ctx, _draft: &mut MessageDraft) -> PreSendDecision {
                PreSendDecision::Reject {
                    error: ErrorBuilder::new(self.0, "connection unavailable").build_error(),
                }
            }
        }
        for code in [
            ErrorCode::ConnectionRefused,
            ErrorCode::ConnectionClosed,
            ErrorCode::ConnectionTimeout,
        ] {
            for required in [true, false] {
                let registry = HookRegistry::new();
                registry
                    .register_pre_send(
                        HookMetadata::default().with_require_success(required),
                        HookSelector::default(),
                        Arc::new(RejectConnection(code)),
                    )
                    .await;
                let ctx: Ctx = Arc::new(Context::with_request_id("connection-error"));
                let result = registry
                    .execute_pre_send(&ctx, &mut MessageDraft::new(vec![]))
                    .await;
                if required {
                    assert_eq!(result.unwrap_err().code(), Some(code));
                } else {
                    assert!(result.is_ok());
                }
            }
        }
    }

    /// 必需的 PreSend Hook 超时与不可达同属「Hook 不可用」：回可重试的 `ServiceUnavailable`
    /// （摄入侧 gRPC `UNAVAILABLE`，HTTP 发送 503），不是 `OperationTimeout`（`DEADLINE_EXCEEDED`，
    /// 以前网关回 500 `MESSAGE_SEND_FAILED`），也不带策略拒绝标记。
    #[tokio::test]
    async fn required_pre_send_timeout_rejects_as_retryable_unavailable() {
        let registry = HookRegistry::new();
        registry
            .register_pre_send(
                HookMetadata::default()
                    .with_name("slow-policy")
                    .with_require_success(true)
                    .with_timeout(Duration::from_millis(1)),
                HookSelector::default(),
                Arc::new(SlowPreSendHook),
            )
            .await;
        let ctx: Ctx = Arc::new(Context::with_request_id("required-timeout"));
        let error = registry
            .execute_pre_send(&ctx, &mut MessageDraft::new(vec![]))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable), "{error}");
        assert!(error.is_retryable(), "{error}");
        assert_eq!(error.reason(), "pre-send hook timed out");
        assert!(!crate::hooks::is_pre_send_rejection(&error), "{error}");
    }

    #[tokio::test]
    async fn optional_pre_send_business_reject_still_blocks() {
        let registry = HookRegistry::new();
        registry
            .register_pre_send(
                HookMetadata::default().with_require_success(false),
                HookSelector::default(),
                Arc::new(RejectBusinessPreSend),
            )
            .await;
        let ctx: Ctx = Arc::new(Context::with_request_id("optional-business-reject"));
        let result = registry
            .execute_pre_send(&ctx, &mut MessageDraft::new(vec![]))
            .await;
        assert_eq!(result.unwrap_err().code(), Some(ErrorCode::OperationFailed));
    }

    #[tokio::test]
    async fn delivery_unavailable_hook_degrades_to_continue() {
        let registry = HookRegistry::new();
        registry
            .register_delivery(
                HookMetadata::default().with_name("delivery-unavailable"),
                HookSelector::default(),
                Arc::new(FailedUnavailableDelivery),
            )
            .await;

        let ctx: Ctx = Arc::new(Context::with_request_id("req-delivery-unavailable"));
        let event = DeliveryEvent {
            message_id: "m1".to_string(),
            user_id: "u1".to_string(),
            channel: "push".to_string(),
            delivered_at: SystemTime::now(),
            metadata: std::collections::HashMap::new(),
        };
        let result = registry.execute_delivery(&ctx, &event).await;
        assert!(result.is_ok(), "delivery unavailable should degrade");
    }

    #[tokio::test]
    async fn delivery_business_failure_still_blocks() {
        struct FailedBusinessDelivery;
        #[async_trait]
        impl DeliveryHook for FailedBusinessDelivery {
            async fn handle(
                &self,
                _ctx: &Ctx,
                _event: &DeliveryEvent,
            ) -> crate::hooks::HookOutcome {
                crate::hooks::HookOutcome::Failed(
                    ErrorBuilder::new(ErrorCode::OperationFailed, "business failure").build_error(),
                )
            }
        }

        let registry = HookRegistry::new();
        registry
            .register_delivery(
                HookMetadata::default().with_name("delivery-business-fail"),
                HookSelector::default(),
                Arc::new(FailedBusinessDelivery),
            )
            .await;

        let ctx: Ctx = Arc::new(Context::with_request_id("req-delivery-business-fail"));
        let event = DeliveryEvent {
            message_id: "m2".to_string(),
            user_id: "u2".to_string(),
            channel: "push".to_string(),
            delivered_at: SystemTime::now(),
            metadata: std::collections::HashMap::new(),
        };
        let result = registry.execute_delivery(&ctx, &event).await;
        assert!(
            result.is_err(),
            "business failure should remain fail-closed"
        );
    }

    /// 永不返回的事件类 Hook：模拟被暂停、只接连接不回应答的扩展。
    struct HangingEventHook;

    #[async_trait]
    impl PostSendHook for HangingEventHook {
        async fn handle(
            &self,
            _ctx: &Ctx,
            _record: &MessageRecord,
            _draft: &MessageDraft,
        ) -> HookOutcome {
            std::future::pending().await
        }
    }

    #[async_trait]
    impl DeliveryHook for HangingEventHook {
        async fn handle(&self, _ctx: &Ctx, _event: &DeliveryEvent) -> HookOutcome {
            std::future::pending().await
        }
    }

    #[async_trait]
    impl RecallHook for HangingEventHook {
        async fn handle(&self, _ctx: &Ctx, _event: &RecallEvent) -> HookOutcome {
            std::future::pending().await
        }
    }

    #[async_trait]
    impl MessageReadHook for HangingEventHook {
        async fn handle(&self, _ctx: &Ctx, _event: &MessageReadEvent) -> HookOutcome {
            std::future::pending().await
        }
    }

    #[async_trait]
    impl MessageReactionHook for HangingEventHook {
        async fn handle(&self, _ctx: &Ctx, _event: &MessageReactionEvent) -> HookDecision {
            std::future::pending().await
        }
    }

    #[async_trait]
    impl ConversationLifecycleHook for HangingEventHook {
        async fn handle(&self, _ctx: &Ctx, _event: &ConversationLifecycleEvent) -> HookOutcome {
            std::future::pending().await
        }
    }

    #[async_trait]
    impl ConversationMemberHook for HangingEventHook {
        async fn handle(&self, _ctx: &Ctx, _event: &ConversationMemberEvent) -> HookDecision {
            std::future::pending().await
        }
    }

    /// 每种事件类 Hook 各注册一个永不返回的实例（1 ms 超时），逐一执行，返回（阶段名，结果）。
    async fn run_hanging_event_hooks(
        required: bool,
    ) -> Vec<(&'static str, flare_server_core::error::Result<()>)> {
        let registry = HookRegistry::new();
        let hook = Arc::new(HangingEventHook);
        let metadata = || {
            HookMetadata::default()
                .with_name("hanging")
                .with_require_success(required)
                .with_timeout(Duration::from_millis(1))
        };
        let selector = HookSelector::default;
        registry
            .register_post_send(metadata(), selector(), hook.clone())
            .await;
        registry
            .register_delivery(metadata(), selector(), hook.clone())
            .await;
        registry
            .register_recall(metadata(), selector(), hook.clone())
            .await;
        registry
            .register_message_read(metadata(), selector(), hook.clone())
            .await;
        registry
            .register_message_reaction(metadata(), selector(), hook.clone())
            .await;
        registry
            .register_conversation_lifecycle(metadata(), selector(), hook.clone())
            .await;
        registry
            .register_conversation_member(metadata(), selector(), hook)
            .await;

        let ctx: Ctx = Arc::new(Context::with_request_id("req-hanging").with_tenant_id("t1"));
        let now = SystemTime::now();
        let record = MessageRecord {
            message_id: "m1".into(),
            client_message_id: None,
            conversation_id: "c1".into(),
            sender_id: "u1".into(),
            conversation_type: None,
            message_type: None,
            persisted_at: now,
            metadata: HashMap::new(),
        };
        vec![
            (
                "post-send",
                registry
                    .execute_post_send(&ctx, &record, &MessageDraft::new(vec![]))
                    .await,
            ),
            (
                "delivery",
                registry
                    .execute_delivery(
                        &ctx,
                        &DeliveryEvent {
                            message_id: "m1".into(),
                            user_id: "u2".into(),
                            channel: "push".into(),
                            delivered_at: now,
                            metadata: HashMap::new(),
                        },
                    )
                    .await,
            ),
            (
                "recall",
                registry
                    .execute_recall(
                        &ctx,
                        &RecallEvent {
                            message_id: "m1".into(),
                            conversation_id: Some("c1".into()),
                            operator_id: "u1".into(),
                            recalled_at: now,
                            metadata: HashMap::new(),
                        },
                    )
                    .await,
            ),
            (
                "message-read",
                registry
                    .execute_message_read(
                        &ctx,
                        &MessageReadEvent {
                            conversation_id: "c1".into(),
                            message_id: "m1".into(),
                            reader_user_id: "u2".into(),
                            device_id: None,
                            read_at: now,
                            metadata: HashMap::new(),
                        },
                    )
                    .await,
            ),
            (
                "message-reaction",
                registry
                    .execute_message_reaction(
                        &ctx,
                        &MessageReactionEvent {
                            conversation_id: "c1".into(),
                            message_id: "m1".into(),
                            actor_user_id: "u2".into(),
                            reaction_key: "like".into(),
                            added: true,
                            occurred_at: now,
                            metadata: HashMap::new(),
                        },
                    )
                    .await,
            ),
            (
                "conversation-lifecycle",
                registry
                    .execute_conversation_lifecycle(
                        &ctx,
                        &ConversationLifecycleEvent {
                            conversation_id: "c1".into(),
                            conversation_type: None,
                            event: ConversationLifecycleEventKind::Created,
                            operator_user_id: None,
                            participant_version: None,
                            occurred_at: now,
                            metadata: HashMap::new(),
                        },
                    )
                    .await,
            ),
            (
                "conversation-member",
                registry
                    .execute_conversation_member(
                        &ctx,
                        &ConversationMemberEvent {
                            conversation_id: "c1".into(),
                            tenant_id: Some("t1".into()),
                            change: ConversationMemberChangeKind::Joined,
                            operator_user_id: None,
                            affected_user_id: "u2".into(),
                            previous_role: None,
                            new_role: None,
                            participant_version: None,
                            occurred_at: now,
                            metadata: HashMap::new(),
                        },
                    )
                    .await,
            ),
        ]
    }

    /// 必需的事件类 Hook（post_send、delivery、recall、已读、表态、会话生命周期，以及共用同一
    /// 超时分支的会话成员）超时与不可达同属「Hook 不可用」：回可重试的 `ServiceUnavailable`，
    /// 与 PreSend（W12-T）一致。以前回 `OperationTimeout`，`is_retryable()` 为否（W13-D）。
    #[tokio::test]
    async fn required_event_hook_timeouts_are_retryable_unavailable() {
        let results = run_hanging_event_hooks(true).await;
        assert_eq!(results.len(), 7);
        for (phase, result) in results {
            let error = result.expect_err(phase);
            assert_eq!(
                error.code(),
                Some(ErrorCode::ServiceUnavailable),
                "{phase}: {error}"
            );
            assert!(error.is_retryable(), "{phase}: {error}");
            assert_eq!(error.reason(), format!("{phase} hook timed out"));
        }
    }

    /// MQ 消费者路径：推送类消费者（`flare-push` 的 `push_consumer`、`online_consumer`）按
    /// `FlareError::is_retryable()` 决定重投还是进死信。必需 Hook 的一次暂时超时在既有重试与退避
    /// 预算内重投，预算用尽（`max_retries`）后才进死信；以前第一次失败就进死信（W13-D）。
    /// 可选 Hook 的超时照旧只记日志、放行。
    #[tokio::test]
    async fn a_consumer_retries_required_hook_timeouts_before_dead_lettering() {
        use flare_server_core::mq::consumer::{
            ConsumerError, FailureAction, RetryBackoff, RetryPolicy,
        };

        let max_retries = 3;
        let policy = RetryPolicy::new(max_retries, true).with_backoff(RetryBackoff::new(
            Duration::from_millis(500),
            Duration::from_secs(30),
        ));
        for (phase, result) in run_hanging_event_hooks(true).await {
            let error = result.expect_err(phase);
            let consumer_error = if error.is_retryable() {
                ConsumerError::Retry(error.to_string())
            } else {
                ConsumerError::DeadLetter(error.to_string())
            };
            for retry_count in 0..max_retries {
                assert_eq!(
                    policy.action_for_error(&consumer_error, retry_count),
                    FailureAction::Retry,
                    "{phase}: attempt {retry_count} should be redelivered: {error}"
                );
            }
            assert_eq!(
                policy.action_for_error(&consumer_error, max_retries),
                FailureAction::DeadLetter,
                "{phase}: an exhausted budget dead-letters"
            );
        }

        for (phase, result) in run_hanging_event_hooks(false).await {
            assert!(
                result.is_ok(),
                "{phase}: an optional hook timeout is ignored"
            );
        }
    }
}
