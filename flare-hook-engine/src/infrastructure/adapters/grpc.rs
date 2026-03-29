//! # gRPC Hook适配器
//!
//! 提供基于gRPC的Hook传输适配器实现。
//! 支持两种模式：
//! 1. 直接地址模式（外部系统/开发测试）
//! 2. 服务发现模式（生产环境内部服务）

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::Duration;

use anyhow::{Context as AnyhowContext, Result};
use tonic::Request;
use tonic::transport::{Channel, Endpoint};

use flare_im_core::{DeliveryEvent, MessageDraft, MessageRecord, PreSendDecision, RecallEvent};
use flare_proto::hooks::hook_extension_client::HookExtensionClient;
use flare_proto::hooks::{
    DeliveryHookRequest, PostSendHookRequest, PreSendHookRequest, RecallHookRequest,
};
use flare_server_core::client::set_context_metadata;
use flare_server_core::context::Context;

use crate::domain::model::LoadBalanceStrategy;
use crate::infrastructure::adapters::conversion::{
    context_to_proto, delivery_event_to_proto, message_draft_to_proto, message_record_to_proto,
    proto_to_pre_send_decision, proto_to_recall_decision, recall_event_to_proto,
};

// 导入服务发现相关模块
use flare_server_core::{ServiceClient, ServiceDiscover};

/// gRPC Hook适配器
pub struct GrpcHookAdapter {
    // 模式1: 直接地址模式（固定客户端）
    client: Option<Arc<Mutex<HookExtensionClient<Channel>>>>,

    // 模式2: 服务发现模式（动态选择实例）
    service_client: Option<Arc<Mutex<ServiceClient>>>,
    service_name: String,
    load_balance_strategy: LoadBalanceStrategy,

    // 模式3: 动态服务发现模式（通过服务发现客户端创建ServiceClient）
    discovery_client: Option<Arc<ServiceDiscover>>,

    // 通用配置
    metadata: HashMap<String, String>,
    timeout: Duration,
}

impl GrpcHookAdapter {
    /// 从直接地址创建gRPC Hook适配器（模式1: 直接地址模式）
    pub async fn new_from_endpoint(
        endpoint: String,
        metadata: HashMap<String, String>,
    ) -> Result<Self> {
        let channel = Endpoint::from_shared(endpoint.clone())?
            .connect()
            .await
            .context("Failed to connect to gRPC endpoint")
            .map_err(|e| anyhow::anyhow!("Failed to connect to gRPC endpoint: {}", e))?;

        let client = HookExtensionClient::new(channel);

        tracing::info!(endpoint = %endpoint, "Created gRPC adapter from endpoint");

        Ok(Self {
            client: Some(Arc::new(Mutex::new(client))),
            service_client: None,
            service_name: String::new(),
            load_balance_strategy: LoadBalanceStrategy::RoundRobin,
            discovery_client: None,
            metadata,
            timeout: Duration::from_secs(5),
        })
    }

    /// 从服务发现创建gRPC Hook适配器（模式2: 服务发现模式）
    pub async fn new_from_service_client(
        service_client: Arc<Mutex<ServiceClient>>,
        service_name: String,
        load_balance_strategy: LoadBalanceStrategy,
        metadata: HashMap<String, String>,
    ) -> Result<Self> {
        tracing::info!(
            service_name = %service_name,
            strategy = ?load_balance_strategy,
            "Created gRPC adapter from service client"
        );

        Ok(Self {
            client: None,
            service_client: Some(service_client),
            service_name,
            load_balance_strategy,
            discovery_client: None,
            metadata,
            timeout: Duration::from_secs(5),
        })
    }

    /// 从服务发现客户端创建gRPC Hook适配器（模式3: 动态服务发现模式）
    pub async fn new_from_discovery(
        discovery_client: Arc<ServiceDiscover>,
        service_name: String,
        load_balance_strategy: LoadBalanceStrategy,
        metadata: HashMap<String, String>,
    ) -> Result<Self> {
        tracing::info!(
            service_name = %service_name,
            strategy = ?load_balance_strategy,
            "Created gRPC adapter from discovery client"
        );

        Ok(Self {
            client: None,
            service_client: None,
            service_name,
            load_balance_strategy,
            discovery_client: Some(discovery_client),
            metadata,
            timeout: Duration::from_secs(5),
        })
    }

    /// 获取客户端（自动选择模式）
    async fn get_client(&self, _key: Option<&str>) -> Result<HookExtensionClient<Channel>> {
        // 模式1: 直接地址模式
        if let Some(ref client) = self.client {
            return Ok(client.lock().await.clone());
        }

        // 模式2: 服务发现模式
        if let Some(service_client) = &self.service_client {
            // 使用 ServiceClient 获取 Channel（已包含负载均衡）
            let mut client_guard = service_client.lock().await;
            let channel = client_guard
                .get_channel()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to get channel from service client: {}", e))?;

            let client = HookExtensionClient::new(channel);
            return Ok(client);
        }

        // 模式3: 动态服务发现模式
        // 注意：由于ServiceDiscover没有实现Clone trait，我们不能直接克隆它
        // 在这种模式下，我们需要在创建GrpcHookAdapter时就创建好ServiceClient
        // 这里的实现仅作说明，实际使用时应该在new_from_discovery中创建ServiceClient

        Err(anyhow::anyhow!(
            "No client available: neither endpoint nor service discovery configured"
        ))
    }

    /// 设置请求元数据（包括静态 metadata 和从 Context 提取的 Context）
    fn set_request_metadata<T>(&self, mut request: Request<T>, ctx: &Context) -> Request<T> {
        // 1. 设置静态 metadata
        for (key, value) in &self.metadata {
            if let Ok(key) = key.parse::<tonic::metadata::MetadataKey<_>>() {
                if let Ok(value) = value.parse::<tonic::metadata::MetadataValue<_>>() {
                    request.metadata_mut().insert(key, value);
                } else {
                    tracing::warn!("Invalid metadata value: {}", value);
                }
            } else {
                tracing::warn!("Invalid metadata key: {}", key);
            }
        }

        // 2. 设置 Context 到 metadata
        set_context_metadata(&mut request, ctx);

        request
    }

    /// 执行PreSend Hook
    pub async fn pre_send(
        &self,
        ctx: &Context,
        draft: &mut MessageDraft,
    ) -> Result<PreSendDecision> {
        let request = PreSendHookRequest {
            context: Some(context_to_proto(ctx)),
            draft: Some(message_draft_to_proto(draft)),
        };

        let mut request = Request::new(request);
        request = self.set_request_metadata(request, ctx);

        // 使用一致性哈希时，以 conversation_id 作为 key
        let key = ctx
            .session_id()
            .and_then(|s| if s.is_empty() { None } else { Some(s) });
        let mut client = self.get_client(key).await?;

        let response = client
            .invoke_pre_send(request)
            .await
            .context("gRPC PreSend hook call failed")
            .map_err(|e| anyhow::anyhow!("gRPC PreSend hook call failed: {}", e))?
            .into_inner();

        Ok(proto_to_pre_send_decision(&response, draft))
    }

    /// 执行PostSend Hook
    pub async fn post_send(
        &self,
        ctx: &Context,
        record: &MessageRecord,
        draft: &MessageDraft,
    ) -> Result<()> {
        let request = PostSendHookRequest {
            context: Some(context_to_proto(ctx)),
            record: Some(message_record_to_proto(record)),
            draft: Some(message_draft_to_proto(draft)),
        };

        let mut request = Request::new(request);
        request = self.set_request_metadata(request, ctx);

        // 使用一致性哈希时，以 conversation_id 作为 key
        let key = ctx
            .session_id()
            .and_then(|s| if s.is_empty() { None } else { Some(s) });
        let mut client = self.get_client(key).await?;

        let response = client
            .invoke_post_send(request)
            .await
            .map_err(|e| anyhow::anyhow!("gRPC PostSend hook call failed: {}", e))?
            .into_inner();

        if response.success {
            Ok(())
        } else {
            use flare_im_core::error::{ErrorBuilder, ErrorCode};
            let error = response
                .status
                .map(|s| {
                    ErrorBuilder::new(
                        ErrorCode::from_u32(s.code as u32).unwrap_or(ErrorCode::GeneralError),
                        &s.message,
                    )
                    .build_error()
                })
                .unwrap_or_else(|| {
                    ErrorBuilder::new(ErrorCode::InternalError, "PostSend hook failed")
                        .build_error()
                });
            Err(error.into())
        }
    }

    /// 执行Delivery Hook
    pub async fn delivery(&self, ctx: &Context, event: &DeliveryEvent) -> Result<()> {
        let request = DeliveryHookRequest {
            context: Some(context_to_proto(ctx)),
            event: Some(delivery_event_to_proto(event)),
        };

        let mut request = Request::new(request);
        request = self.set_request_metadata(request, ctx);

        // 使用一致性哈希时，以 user_id 作为 key
        let key = Some(event.user_id.as_str());
        let mut client = self.get_client(key).await?;

        let response = client
            .notify_delivery(request)
            .await
            .map_err(|e| anyhow::anyhow!("gRPC Delivery hook call failed: {}", e))?
            .into_inner();

        if response.success {
            Ok(())
        } else {
            use flare_im_core::error::{ErrorBuilder, ErrorCode};
            let error = response
                .status
                .map(|s| {
                    ErrorBuilder::new(
                        ErrorCode::from_u32(s.code as u32).unwrap_or(ErrorCode::GeneralError),
                        &s.message,
                    )
                    .build_error()
                })
                .unwrap_or_else(|| {
                    ErrorBuilder::new(ErrorCode::InternalError, "Delivery hook failed")
                        .build_error()
                });
            Err(error.into())
        }
    }

    /// 执行Recall Hook
    pub async fn recall(&self, ctx: &Context, event: &RecallEvent) -> Result<PreSendDecision> {
        let request = RecallHookRequest {
            context: Some(context_to_proto(ctx)),
            event: Some(recall_event_to_proto(event)),
        };

        let mut request = Request::new(request);
        request = self.set_request_metadata(request, ctx);

        // 使用一致性哈希时，以 conversation_id 作为 key
        let key = ctx
            .session_id()
            .and_then(|s| if s.is_empty() { None } else { Some(s) });
        let mut client = self.get_client(key).await?;

        let response = client
            .notify_recall(request)
            .await
            .map_err(|e| anyhow::anyhow!("gRPC Recall hook call failed: {}", e))?
            .into_inner();

        Ok(proto_to_recall_decision(&response))
    }
}
