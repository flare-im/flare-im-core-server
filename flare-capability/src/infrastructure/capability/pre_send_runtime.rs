//! Guard Pipeline 默认运行时。

use std::sync::Arc;

use async_trait::async_trait;
use flare_core_base::context::Ctx;
use tokio::sync::RwLock;

use crate::domain::capability::{
    GuardDecision, PreSendEvaluateInput, PreSendGuard, PreSendGuardPipeline, Result,
};

#[derive(Clone, Default)]
pub struct PreSendGuardRuntime {
    guards: Arc<RwLock<Vec<Arc<dyn PreSendGuard>>>>,
}

impl PreSendGuardRuntime {
    pub fn new() -> Self {
        Self {
            guards: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn register(&self, guard: Arc<dyn PreSendGuard>) {
        self.guards.write().await.push(guard);
    }
}

#[async_trait]
impl PreSendGuardPipeline for PreSendGuardRuntime {
    async fn evaluate(&self, ctx: &Ctx, input: &PreSendEvaluateInput) -> Result<GuardDecision> {
        let chain = self.guards.read().await;
        if chain.is_empty() {
            return Ok(GuardDecision::Allow);
        }
        for g in chain.iter() {
            match g.evaluate(ctx, input).await? {
                GuardDecision::Allow => {}
                GuardDecision::Reject(r) => return Ok(GuardDecision::Reject(r)),
            }
        }
        Ok(GuardDecision::Allow)
    }
}
