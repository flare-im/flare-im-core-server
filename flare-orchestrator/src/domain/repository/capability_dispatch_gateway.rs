use async_trait::async_trait;
use flare_im_core::Ctx;
use serde_json::Value;

use crate::error::Result;

/// 能力分发端口（domain 仅依赖抽象，不感知 gRPC 细节）。
#[async_trait]
pub trait CapabilityDispatchGateway: Send + Sync {
    async fn dispatch_json(
        &self,
        ctx: &Ctx,
        capability_id: &str,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        request_id: String,
        payload: Value,
    ) -> Result<Value>;
}
