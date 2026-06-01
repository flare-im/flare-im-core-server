use std::sync::Arc;

use crate::Ctx;
use crate::error::{ErrorBuilder, ErrorCode, FlareError, Result};
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
                    if should_fail_open_on_hook_unavailable(&error) {
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
                let err = ErrorBuilder::new(ErrorCode::OperationTimeout, "pre-send hook timed out")
                    .details(format!("hook={}", self.metadata.name))
                    .build_error();
                if should_fail_open_on_hook_unavailable(&err) {
                    tracing::warn!(
                        hook = %self.metadata.name,
                        "hook pre-send timeout, degrade to no-hook path"
                    );
                    PreSendDecision::Continue
                } else if self.metadata.require_success {
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
        guard.sort_by(|a, b| a.metadata.priority.cmp(&b.metadata.priority));
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
        guard.sort_by(|a, b| a.metadata.priority.cmp(&b.metadata.priority));
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
        guard.sort_by(|a, b| a.metadata.priority.cmp(&b.metadata.priority));
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
        guard.sort_by(|a, b| a.metadata.priority.cmp(&b.metadata.priority));
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
        guard.sort_by(|a, b| a.metadata.priority.cmp(&b.metadata.priority));
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
        guard.sort_by(|a, b| a.metadata.priority.cmp(&b.metadata.priority));
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
        guard.sort_by(|a, b| a.metadata.priority.cmp(&b.metadata.priority));
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
        guard.sort_by(|a, b| a.metadata.priority.cmp(&b.metadata.priority));
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
                Err(_) => {
                    if entry.metadata.require_success {
                        return Err(entry
                            .metadata
                            .build_error(ErrorCode::OperationTimeout, "post-send hook timed out"));
                    } else {
                        tracing::warn!(
                            hook = %entry.metadata.name,
                            "post-send hook timeout ignored"
                        );
                        HookOutcome::Completed
                    }
                }
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
                Err(_) => {
                    if entry.metadata.require_success {
                        return Err(entry
                            .metadata
                            .build_error(ErrorCode::OperationTimeout, "delivery hook timed out"));
                    } else {
                        tracing::warn!(
                            hook = %entry.metadata.name,
                            "delivery hook timeout ignored"
                        );
                        HookOutcome::Completed
                    }
                }
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
                Err(_) => {
                    if entry.metadata.require_success {
                        return Err(entry
                            .metadata
                            .build_error(ErrorCode::OperationTimeout, "recall hook timed out"));
                    } else {
                        tracing::warn!(
                            hook = %entry.metadata.name,
                            "recall hook timeout ignored"
                        );
                        HookOutcome::Completed
                    }
                }
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

fn timeout_outcome(metadata: &HookMetadata, message: &str) -> Result<HookOutcome> {
    if metadata.require_success {
        Err(metadata.build_error(ErrorCode::OperationTimeout, message))
    } else {
        tracing::warn!(hook = %metadata.name, "{message} ignored");
        Ok(HookOutcome::Completed)
    }
}

fn timeout_decision(metadata: &HookMetadata, message: &str) -> Result<HookDecision> {
    if metadata.require_success {
        Err(metadata.build_error(ErrorCode::OperationTimeout, message))
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
    use crate::error::{ErrorBuilder, ErrorCode};
    use crate::hooks::selector::HookSelector;
    use crate::hooks::{
        DeliveryEvent, DeliveryHook, HookMetadata, MessageDraft, PreSendDecision, PreSendHook,
    };
    use async_trait::async_trait;
    use flare_server_core::context::Context;
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
            tokio::time::sleep(Duration::from_millis(20)).await;
            PreSendDecision::Continue
        }
    }

    #[tokio::test]
    async fn pre_send_unavailable_hook_degrades_to_continue() {
        let registry = HookRegistry::new();
        registry
            .register_pre_send(
                HookMetadata::default().with_name("pre-send-unavailable"),
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
    async fn pre_send_timeout_degrades_to_continue() {
        let registry = HookRegistry::new();
        registry
            .register_pre_send(
                HookMetadata::default()
                    .with_name("pre-send-timeout")
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
}
