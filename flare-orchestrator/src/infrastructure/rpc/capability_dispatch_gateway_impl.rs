use std::sync::Arc;

use async_trait::async_trait;
use flare_grpc_proto::capability::DispatchCapabilityRequest;
use flare_im_core::Ctx;
use flare_server_core::flare_err;
use serde_json::Value;

use crate::domain::repository::CapabilityDispatchGateway;
use crate::error::{ErrorCode, Result};

use super::CapabilityDispatchClient;

/// 基于 `CapabilityDispatchClient` 的领域端口实现。
pub struct CapabilityDispatchGatewayImpl {
    client: Arc<CapabilityDispatchClient>,
}

impl CapabilityDispatchGatewayImpl {
    pub fn new(client: Arc<CapabilityDispatchClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl CapabilityDispatchGateway for CapabilityDispatchGatewayImpl {
    async fn dispatch_json(
        &self,
        ctx: &Ctx,
        capability_id: &str,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        request_id: String,
        payload: Value,
    ) -> Result<Value> {
        let req = DispatchCapabilityRequest {
            capability_id: capability_id.to_string(),
            tenant_id: tenant_id.to_string(),
            user_id: user_id.to_string(),
            conversation_id: conversation_id.to_string(),
            payload_json: payload.to_string(),
            request_id,
        };
        let out = self.client.dispatch(ctx.as_ref(), req).await?;
        if out.result_json.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&out.result_json).map_err(|e| {
            flare_err!(
                ErrorCode::InternalError,
                &format!("capability result_json invalid: {e}")
            )
        })
    }
}
