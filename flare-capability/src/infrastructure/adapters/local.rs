//! # Local Plugin适配器
//!
//! 提供基于本地插件的Hook传输适配器实现。

use std::collections::HashMap;
use std::sync::Arc;

use flare_im_hooks::{
    DeliveryEvent, DeliveryHook, MessageDraft, MessageRecord, PostSendHook, PreSendDecision,
    PreSendHook, RecallEvent, RecallHook,
};
use flare_server_core::context::{Context, Ctx};

use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};

/// Local Plugin适配器
pub struct LocalHookAdapter {
    pre_send_hooks: HashMap<String, Arc<dyn PreSendHook>>,
    post_send_hooks: HashMap<String, Arc<dyn PostSendHook>>,
    delivery_hooks: HashMap<String, Arc<dyn DeliveryHook>>,
    recall_hooks: HashMap<String, Arc<dyn RecallHook>>,
}

impl LocalHookAdapter {
    /// 创建Local Plugin适配器
    pub fn new(_target: String) -> Result<Self> {
        Ok(Self {
            pre_send_hooks: HashMap::new(),
            post_send_hooks: HashMap::new(),
            delivery_hooks: HashMap::new(),
            recall_hooks: HashMap::new(),
        })
    }

    /// 注册PreSend Hook
    pub fn register_pre_send(&mut self, name: String, hook: Arc<dyn PreSendHook>) {
        self.pre_send_hooks.insert(name, hook);
    }

    /// 注册PostSend Hook
    pub fn register_post_send(&mut self, name: String, hook: Arc<dyn PostSendHook>) {
        self.post_send_hooks.insert(name, hook);
    }

    /// 注册Delivery Hook
    pub fn register_delivery(&mut self, name: String, hook: Arc<dyn DeliveryHook>) {
        self.delivery_hooks.insert(name, hook);
    }

    /// 注册Recall Hook
    pub fn register_recall(&mut self, name: String, hook: Arc<dyn RecallHook>) {
        self.recall_hooks.insert(name, hook);
    }

    /// 执行PreSend Hook
    pub async fn pre_send(
        &self,
        target: &str,
        ctx: &Context,
        draft: &mut MessageDraft,
    ) -> Result<PreSendDecision> {
        let hook = self.pre_send_hooks.get(target).ok_or_else(|| {
            ErrorBuilder::new(
                ErrorCode::InternalError,
                format!("local PreSend hook not registered for target `{target}`"),
            )
            .build_error()
        })?;

        // 将 &Context 包装为 &Ctx (&Arc<Context>)
        let ctx_arc: Ctx = Arc::new(ctx.clone());
        Ok(hook.handle(&ctx_arc, draft).await)
    }

    /// 执行PostSend Hook
    pub async fn post_send(
        &self,
        target: &str,
        ctx: &Context,
        record: &MessageRecord,
        draft: &MessageDraft,
    ) -> Result<()> {
        let hook = self.post_send_hooks.get(target).ok_or_else(|| {
            ErrorBuilder::new(
                ErrorCode::InternalError,
                format!("local PostSend hook not registered for target `{target}`"),
            )
            .build_error()
        })?;

        let ctx_arc: Ctx = Arc::new(ctx.clone());
        let outcome = hook.handle(&ctx_arc, record, draft).await;
        if outcome.is_completed() {
            Ok(())
        } else {
            Err(ErrorBuilder::new(
                ErrorCode::OperationFailed,
                "local PostSend hook did not complete successfully",
            )
            .build_error())
        }
    }

    /// 执行Delivery Hook
    pub async fn delivery(&self, target: &str, ctx: &Context, event: &DeliveryEvent) -> Result<()> {
        let hook = self.delivery_hooks.get(target).ok_or_else(|| {
            ErrorBuilder::new(
                ErrorCode::InternalError,
                format!("local Delivery hook not registered for target `{target}`"),
            )
            .build_error()
        })?;

        let ctx_arc: Ctx = Arc::new(ctx.clone());
        let outcome = hook.handle(&ctx_arc, event).await;
        if outcome.is_completed() {
            Ok(())
        } else {
            Err(ErrorBuilder::new(
                ErrorCode::OperationFailed,
                "local Delivery hook did not complete successfully",
            )
            .build_error())
        }
    }

    /// 执行Recall Hook
    pub async fn recall(
        &self,
        target: &str,
        ctx: &Context,
        event: &RecallEvent,
    ) -> Result<PreSendDecision> {
        let hook = self.recall_hooks.get(target).ok_or_else(|| {
            ErrorBuilder::new(
                ErrorCode::InternalError,
                format!("local Recall hook not registered for target `{target}`"),
            )
            .build_error()
        })?;

        let ctx_arc: Ctx = Arc::new(ctx.clone());
        let outcome = hook.handle(&ctx_arc, event).await;
        if outcome.is_completed() {
            Ok(PreSendDecision::Continue)
        } else {
            let error = ErrorBuilder::new(
                ErrorCode::OperationFailed,
                "local Recall hook did not complete successfully",
            )
            .build_error();
            Ok(PreSendDecision::Reject { error })
        }
    }
}
