//! Resolver 运行时。

use std::sync::Arc;

use flare_core_base::context::Ctx;
use tokio::sync::RwLock;

use crate::domain::capability::{
    CapabilityError, RecipientResolveRequest, RecipientResolveResult, RecipientResolver, Result,
};

#[derive(Clone, Default)]
pub struct RecipientResolverRuntime {
    resolvers: Arc<RwLock<Vec<Arc<dyn RecipientResolver>>>>,
}

impl RecipientResolverRuntime {
    pub fn new() -> Self {
        Self {
            resolvers: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn register(&self, resolver: Arc<dyn RecipientResolver>) {
        self.resolvers.write().await.push(resolver);
    }

    pub async fn resolve(
        &self,
        ctx: &Ctx,
        req: &RecipientResolveRequest,
    ) -> Result<RecipientResolveResult> {
        let r = self.resolvers.read().await;
        for entry in r.iter() {
            if entry.supports(&req.conversation_kind, req.trigger) {
                return entry.resolve(ctx, req).await;
            }
        }
        Err(CapabilityError::NotRegistered(format!(
            "no recipient resolver for {:?} / {:?}",
            req.conversation_kind, req.trigger
        )))
    }

    pub async fn resolve_with(
        &self,
        ctx: &Ctx,
        req: &RecipientResolveRequest,
        preferred_id: Option<&str>,
    ) -> Result<RecipientResolveResult> {
        let r = self.resolvers.read().await;
        if let Some(pid) = preferred_id {
            for entry in r.iter() {
                if entry.id() == pid && entry.supports(&req.conversation_kind, req.trigger) {
                    return entry.resolve(ctx, req).await;
                }
            }
            return Err(CapabilityError::NotRegistered(format!(
                "resolver id {pid} not available or not supported"
            )));
        }
        drop(r);
        self.resolve(ctx, req).await
    }
}
