mod grpc;
mod webhook;

use std::collections::HashMap;
use std::sync::Arc;

use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};

use super::config::{HookDefinition, HookFactory, HookTransportConfig};
use super::selector::HookSelector;
use super::types::PreSendHook;

pub use grpc::GrpcHookFactory;
pub(in crate::hooks) use grpc::test_pre_send as test_grpc_pre_send;
pub use webhook::{
    WEBHOOK_SIGNATURE_HEADER, WEBHOOK_SIGNATURE_TOLERANCE_SECS, WebhookHookFactory,
    WebhookSignatureError, is_reserved_webhook_header, sign_webhook_request,
    verify_webhook_signature, webhook_signature,
};

/// 默认的 Hook 工厂，支持 gRPC / WebHook / 本地实现
pub struct DefaultHookFactory {
    grpc: GrpcHookFactory,
    webhook: WebhookHookFactory,
    pre_send_locals: HashMap<String, Arc<dyn PreSendHook>>,
}

impl DefaultHookFactory {
    pub fn new() -> Result<Self> {
        Ok(Self {
            grpc: GrpcHookFactory::new(),
            webhook: WebhookHookFactory::new()?,
            pre_send_locals: HashMap::new(),
        })
    }

    pub fn register_pre_send_local<S: Into<String>>(
        &mut self,
        name: S,
        hook: Arc<dyn PreSendHook>,
    ) {
        self.pre_send_locals.insert(name.into(), hook);
    }
}

impl HookFactory for DefaultHookFactory {
    fn build_pre_send(
        &self,
        def: &HookDefinition,
        selector: &HookSelector,
    ) -> Result<Arc<dyn PreSendHook>> {
        match &def.transport {
            HookTransportConfig::Grpc {
                endpoint, metadata, ..
            } => {
                let mut merged = def.metadata.clone();
                merged.extend(metadata.clone());
                Ok(self.grpc.build_pre_send(merged, endpoint.clone()))
            }
            HookTransportConfig::Webhook {
                endpoint,
                secret,
                headers,
            } => Ok(self
                .webhook
                .build_pre_send(def, endpoint, secret.clone(), headers.clone())),
            HookTransportConfig::Local { target } => {
                self.pre_send_locals.get(target).cloned().ok_or_else(|| {
                    ErrorBuilder::new(
                        ErrorCode::ConfigurationError,
                        "local pre-send hook not found",
                    )
                    .details(format!("hook={}, selector={selector:?}", def.name))
                    .build_error()
                })
            }
        }
    }
}
