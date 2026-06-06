mod grpc;
mod webhook;

use std::collections::HashMap;
use std::sync::Arc;

use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};

use super::config::{HookDefinition, HookFactory, HookTransportConfig};
use super::selector::HookSelector;
use super::types::{DeliveryHook, PostSendHook, PreSendHook, RecallHook};

pub use grpc::GrpcHookFactory;
pub use webhook::WebhookHookFactory;

/// 默认的 Hook 工厂，支持 gRPC / WebHook / 本地实现
pub struct DefaultHookFactory {
    grpc: GrpcHookFactory,
    webhook: WebhookHookFactory,
    pre_send_locals: HashMap<String, Arc<dyn PreSendHook>>,
    post_send_locals: HashMap<String, Arc<dyn PostSendHook>>,
    delivery_locals: HashMap<String, Arc<dyn DeliveryHook>>,
    recall_locals: HashMap<String, Arc<dyn RecallHook>>,
}

impl DefaultHookFactory {
    pub fn new() -> Result<Self> {
        Ok(Self {
            grpc: GrpcHookFactory::new(),
            webhook: WebhookHookFactory::new()?,
            pre_send_locals: HashMap::new(),
            post_send_locals: HashMap::new(),
            delivery_locals: HashMap::new(),
            recall_locals: HashMap::new(),
        })
    }

    pub fn register_pre_send_local<S: Into<String>>(
        &mut self,
        name: S,
        hook: Arc<dyn PreSendHook>,
    ) {
        self.pre_send_locals.insert(name.into(), hook);
    }

    pub fn register_post_send_local<S: Into<String>>(
        &mut self,
        name: S,
        hook: Arc<dyn PostSendHook>,
    ) {
        self.post_send_locals.insert(name.into(), hook);
    }

    pub fn register_delivery_local<S: Into<String>>(
        &mut self,
        name: S,
        hook: Arc<dyn DeliveryHook>,
    ) {
        self.delivery_locals.insert(name.into(), hook);
    }

    pub fn register_recall_local<S: Into<String>>(&mut self, name: S, hook: Arc<dyn RecallHook>) {
        self.recall_locals.insert(name.into(), hook);
    }
}

impl HookFactory for DefaultHookFactory {
    fn build_pre_send(
        &self,
        def: &HookDefinition,
        selector: &HookSelector,
    ) -> Result<Option<Arc<dyn PreSendHook>>> {
        match &def.transport {
            HookTransportConfig::Grpc {
                endpoint, metadata, ..
            } => {
                let mut merged = def.metadata.clone();
                merged.extend(metadata.clone());
                Ok(Some(self.grpc.build_pre_send(merged, endpoint.clone())))
            }
            HookTransportConfig::Webhook {
                endpoint,
                secret,
                headers,
            } => Ok(Some(self.webhook.build_pre_send(
                def,
                endpoint,
                secret.clone(),
                headers.clone(),
            ))),
            HookTransportConfig::Local { target } => {
                let hook = self.pre_send_locals.get(target).cloned().ok_or_else(|| {
                    ErrorBuilder::new(
                        ErrorCode::ConfigurationError,
                        "local pre-send hook not found",
                    )
                    .details(format!("hook={}, selector={selector:?}", def.name))
                    .build_error()
                })?;
                Ok(Some(hook))
            }
        }
    }

    fn build_post_send(
        &self,
        def: &HookDefinition,
        _selector: &HookSelector,
    ) -> Result<Option<Arc<dyn PostSendHook>>> {
        match &def.transport {
            HookTransportConfig::Grpc {
                endpoint, metadata, ..
            } => {
                let mut merged = def.metadata.clone();
                merged.extend(metadata.clone());
                Ok(Some(self.grpc.build_post_send(merged, endpoint.clone())))
            }
            HookTransportConfig::Webhook {
                endpoint,
                secret,
                headers,
            } => Ok(Some(self.webhook.build_post_send(
                def,
                endpoint,
                secret.clone(),
                headers.clone(),
            ))),
            HookTransportConfig::Local { target } => {
                let hook = self.post_send_locals.get(target).cloned().ok_or_else(|| {
                    ErrorBuilder::new(
                        ErrorCode::ConfigurationError,
                        "local post-send hook not found",
                    )
                    .details(format!("hook={}", def.name))
                    .build_error()
                })?;
                Ok(Some(hook))
            }
        }
    }

    fn build_delivery(
        &self,
        def: &HookDefinition,
        _selector: &HookSelector,
    ) -> Result<Option<Arc<dyn DeliveryHook>>> {
        match &def.transport {
            HookTransportConfig::Grpc {
                endpoint, metadata, ..
            } => {
                let mut merged = def.metadata.clone();
                merged.extend(metadata.clone());
                Ok(Some(self.grpc.build_delivery(merged, endpoint.clone())))
            }
            HookTransportConfig::Webhook {
                endpoint,
                secret,
                headers,
            } => Ok(Some(self.webhook.build_delivery(
                def,
                endpoint,
                secret.clone(),
                headers.clone(),
            ))),
            HookTransportConfig::Local { target } => {
                let hook = self.delivery_locals.get(target).cloned().ok_or_else(|| {
                    ErrorBuilder::new(
                        ErrorCode::ConfigurationError,
                        "local delivery hook not found",
                    )
                    .details(format!("hook={}", def.name))
                    .build_error()
                })?;
                Ok(Some(hook))
            }
        }
    }

    fn build_recall(
        &self,
        def: &HookDefinition,
        _selector: &HookSelector,
    ) -> Result<Option<Arc<dyn RecallHook>>> {
        match &def.transport {
            HookTransportConfig::Grpc {
                endpoint, metadata, ..
            } => {
                let mut merged = def.metadata.clone();
                merged.extend(metadata.clone());
                Ok(Some(self.grpc.build_recall(merged, endpoint.clone())))
            }
            HookTransportConfig::Webhook {
                endpoint,
                secret,
                headers,
            } => Ok(Some(self.webhook.build_recall(
                def,
                endpoint,
                secret.clone(),
                headers.clone(),
            ))),
            HookTransportConfig::Local { target } => {
                let hook = self.recall_locals.get(target).cloned().ok_or_else(|| {
                    ErrorBuilder::new(ErrorCode::ConfigurationError, "local recall hook not found")
                        .details(format!("hook={}", def.name))
                        .build_error()
                })?;
                Ok(Some(hook))
            }
        }
    }
}
