use std::sync::Arc;

use crate::Ctx;
use crate::error::{ErrorBuilder, ErrorCode, FlareError, Result};
use once_cell::sync::OnceCell;
use tokio::sync::RwLock;

use super::selector::HookSelector;
use super::types::{
    DeliveryEvent, DeliveryHook, HookKind, HookMetadata, HookOutcome, MessageDraft, MessageRecord,
    PostSendHook, PreSendDecision, PreSendHook, RecallEvent, RecallHook,
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
                PreSendDecision::Reject { error } => PreSendDecision::Reject {
                    error: annotate(error, &self.metadata),
                },
            },
            Err(_) => {
                let err = ErrorBuilder::new(ErrorCode::OperationTimeout, "pre-send hook timed out")
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
            outcome.into_result(&entry.metadata)?;
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
            outcome.into_result(&entry.metadata)?;
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
            outcome.into_result(&entry.metadata)?;
        }
        Ok(())
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
