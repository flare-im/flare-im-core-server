//! # Hook适配器工厂
//!
//! 提供Hook适配器的创建和管理

use std::sync::Arc;
use tokio::sync::Mutex;

use anyhow::{Context, Result};

use crate::domain::model::{HookTransportConfig, LoadBalanceStrategy};
use crate::infrastructure::adapters::grpc::GrpcHookAdapter;
use crate::infrastructure::adapters::local::LocalHookAdapter;
use crate::infrastructure::adapters::webhook::WebhookHookAdapter;

pub mod conversion;
pub mod grpc;
pub mod hook_context_data;
pub mod local;
pub mod webhook;

/// Hook适配器工厂
pub struct HookAdapterFactory {
    /// 服务注册发现（可选，用于服务发现模式）
    /// 使用新的统一服务发现接口
    service_client: Option<Arc<Mutex<flare_server_core::ServiceClient>>>,
}

impl HookAdapterFactory {
    pub fn new() -> Self {
        Self {
            service_client: None,
        }
    }

    /// 设置服务注册发现
    pub fn with_service_client(
        mut self,
        client: Option<Arc<Mutex<flare_server_core::ServiceClient>>>,
    ) -> Self {
        self.service_client = client;
        self
    }

    /// 根据传输配置创建适配器
    ///
    /// 优先级：service_name + registry > endpoint（直接地址）
    pub async fn create_adapter(
        &self,
        transport: &HookTransportConfig,
    ) -> Result<Arc<dyn HookAdapter>> {
        match transport {
            HookTransportConfig::Grpc {
                endpoint,
                service_name,
                registry_type: _,
                namespace: _,
                load_balance,
                metadata,
            } => {
                // 优先级1: 服务发现模式（推荐，生产环境）
                if let Some(service_name) = service_name {
                    if let Some(service_client) = &self.service_client {
                        let strategy = load_balance.unwrap_or(LoadBalanceStrategy::RoundRobin);
                        let adapter = GrpcHookAdapter::new_from_service_client(
                            service_client.clone(),
                            service_name.clone(),
                            strategy,
                            metadata.clone(),
                        )
                        .await
                        .context("Failed to create gRPC adapter from service discovery")?;
                        return Ok(Arc::new(adapter));
                    } else {
                        // 如果没有注册中心但配置了 service_name，给出警告并使用 endpoint fallback
                        tracing::warn!(
                            service_name = %service_name,
                            "Service client not available, falling back to endpoint mode"
                        );
                    }
                }

                // 优先级2: 直接地址模式（fallback/外部系统/开发测试）
                if let Some(endpoint) = endpoint {
                    let adapter =
                        GrpcHookAdapter::new_from_endpoint(endpoint.clone(), metadata.clone())
                            .await
                            .context("Failed to create gRPC adapter from endpoint")?;
                    return Ok(Arc::new(adapter));
                }

                Err(anyhow::anyhow!(
                    "Either service_name (with registry) or endpoint must be provided for gRPC transport"
                ))
            }
            HookTransportConfig::Webhook {
                endpoint,
                secret,
                headers,
            } => {
                // WebHook 必须使用直接地址
                let adapter =
                    WebhookHookAdapter::new(endpoint.clone(), secret.clone(), headers.clone())
                        .await
                        .context("Failed to create WebHook adapter")?;
                Ok(Arc::new(adapter))
            }
            HookTransportConfig::Local { target } => {
                let adapter = LocalHookAdapter::new(target.clone())
                    .context("Failed to create Local Plugin adapter")?;
                Ok(Arc::new(adapter))
            }
        }
    }
}

/// Hook适配器接口
///
/// 使用 async-trait 宏以支持 trait 对象（dyn HookAdapter）
#[async_trait::async_trait]
pub trait HookAdapter: Send + Sync {
    /// 执行PreSend Hook
    async fn pre_send(
        &self,
        ctx: &flare_server_core::context::Context,
        draft: &mut flare_im_core::MessageDraft,
    ) -> Result<flare_im_core::PreSendDecision>;

    /// 执行PostSend Hook
    async fn post_send(
        &self,
        ctx: &flare_server_core::context::Context,
        record: &flare_im_core::MessageRecord,
        draft: &flare_im_core::MessageDraft,
    ) -> Result<()>;

    /// 执行Delivery Hook
    async fn delivery(
        &self,
        ctx: &flare_server_core::context::Context,
        event: &flare_im_core::DeliveryEvent,
    ) -> Result<()>;

    /// 执行Recall Hook
    async fn recall(
        &self,
        ctx: &flare_server_core::context::Context,
        event: &flare_im_core::RecallEvent,
    ) -> Result<flare_im_core::PreSendDecision>;
}

#[async_trait::async_trait]
impl HookAdapter for GrpcHookAdapter {
    async fn pre_send(
        &self,
        ctx: &flare_server_core::context::Context,
        draft: &mut flare_im_core::MessageDraft,
    ) -> Result<flare_im_core::PreSendDecision> {
        GrpcHookAdapter::pre_send(self, ctx, draft).await
    }
    async fn post_send(
        &self,
        ctx: &flare_server_core::context::Context,
        record: &flare_im_core::MessageRecord,
        draft: &flare_im_core::MessageDraft,
    ) -> Result<()> {
        GrpcHookAdapter::post_send(self, ctx, record, draft).await
    }

    async fn delivery(
        &self,
        ctx: &flare_server_core::context::Context,
        event: &flare_im_core::DeliveryEvent,
    ) -> Result<()> {
        GrpcHookAdapter::delivery(self, ctx, event).await
    }

    async fn recall(
        &self,
        ctx: &flare_server_core::context::Context,
        event: &flare_im_core::RecallEvent,
    ) -> Result<flare_im_core::PreSendDecision> {
        GrpcHookAdapter::recall(self, ctx, event).await
    }
}

#[async_trait::async_trait]
impl HookAdapter for WebhookHookAdapter {
    async fn pre_send(
        &self,
        _ctx: &flare_server_core::context::Context,
        _draft: &mut flare_im_core::MessageDraft,
    ) -> Result<flare_im_core::PreSendDecision> {
        Ok(flare_im_core::PreSendDecision::Continue)
    }

    async fn post_send(
        &self,
        _ctx: &flare_server_core::context::Context,
        _record: &flare_im_core::MessageRecord,
        _draft: &flare_im_core::MessageDraft,
    ) -> Result<()> {
        Ok(())
    }

    async fn delivery(
        &self,
        _ctx: &flare_server_core::context::Context,
        _event: &flare_im_core::DeliveryEvent,
    ) -> Result<()> {
        Ok(())
    }

    async fn recall(
        &self,
        _ctx: &flare_server_core::context::Context,
        _event: &flare_im_core::RecallEvent,
    ) -> Result<flare_im_core::PreSendDecision> {
        Ok(flare_im_core::PreSendDecision::Continue)
    }
}
#[async_trait::async_trait]
impl HookAdapter for LocalHookAdapter {
    async fn pre_send(
        &self,
        ctx: &flare_server_core::context::Context,
        draft: &mut flare_im_core::MessageDraft,
    ) -> Result<flare_im_core::PreSendDecision> {
        LocalHookAdapter::pre_send(self, "", ctx, draft).await
    }

    async fn post_send(
        &self,
        ctx: &flare_server_core::context::Context,
        record: &flare_im_core::MessageRecord,
        draft: &flare_im_core::MessageDraft,
    ) -> Result<()> {
        LocalHookAdapter::post_send(self, "", ctx, record, draft).await
    }

    async fn delivery(
        &self,
        ctx: &flare_server_core::context::Context,
        event: &flare_im_core::DeliveryEvent,
    ) -> Result<()> {
        LocalHookAdapter::delivery(self, "", ctx, event).await
    }

    async fn recall(
        &self,
        ctx: &flare_server_core::context::Context,
        event: &flare_im_core::RecallEvent,
    ) -> Result<flare_im_core::PreSendDecision> {
        LocalHookAdapter::recall(self, "", ctx, event).await
    }
}
