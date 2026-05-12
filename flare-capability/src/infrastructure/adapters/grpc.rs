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

use prost::Message;
use tonic::Request;
use tonic::transport::{Channel, Endpoint};

use flare_grpc_proto::capability::hook_plugin_client::HookPluginClient;
use flare_grpc_proto::capability::{
    DeliveryHookRequest, DeliveryHookResponse, GenericRequest, PostSendHookRequest,
    PostSendHookResponse, PreSendHookRequest, PreSendHookResponse, RecallHookRequest,
    RecallHookResponse,
};
use flare_im_core::{DeliveryEvent, MessageDraft, MessageRecord, PreSendDecision, RecallEvent};
use flare_server_core::client::set_context_metadata;
use flare_server_core::context::Context;

use crate::domain::model::LoadBalanceStrategy;
use crate::infrastructure::adapters::conversion::{
    context_to_proto, delivery_event_to_proto, message_draft_to_proto, message_record_to_proto,
    proto_to_pre_send_decision, proto_to_recall_decision, recall_event_to_proto,
};

// 导入服务发现相关模块
use flare_server_core::{ServiceClient, ServiceDiscover};

use crate::error::{ErrorBuilder, ErrorCode, Result, map_infra_error};

/// gRPC Hook适配器
#[allow(dead_code)]
pub struct GrpcHookAdapter {
    // 模式1: 直接地址模式（固定客户端）
    client: Option<Arc<Mutex<HookPluginClient<Channel>>>>,

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
        let channel = Endpoint::from_shared(endpoint.clone())
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::InvalidParameter,
                    "invalid gRPC hook endpoint URI",
                )
            })?
            .connect()
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::NetworkError,
                    "failed to connect to gRPC hook endpoint",
                )
            })?;

        let client = HookPluginClient::new(channel);

        tracing::trace!(endpoint = %endpoint, "Created gRPC adapter from endpoint");

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
        tracing::trace!(
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
        tracing::trace!(
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
    async fn get_client(&self, _key: Option<&str>) -> Result<HookPluginClient<Channel>> {
        // 模式1: 直接地址模式
        if let Some(ref client) = self.client {
            return Ok(client.lock().await.clone());
        }

        // 模式2: 服务发现模式
        if let Some(service_client) = &self.service_client {
            // 使用 ServiceClient 获取 Channel（已包含负载均衡）
            let mut client_guard = service_client.lock().await;
            let channel = client_guard.get_channel().await.map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::InternalError,
                    "failed to get gRPC channel from service client",
                )
            })?;

            let client = HookPluginClient::new(channel);
            return Ok(client);
        }

        // 模式3: 动态服务发现模式
        // 注意：由于ServiceDiscover没有实现Clone trait，我们不能直接克隆它
        // 在这种模式下，我们需要在创建GrpcHookAdapter时就创建好ServiceClient
        // 这里的实现仅作说明，实际使用时应该在new_from_discovery中创建ServiceClient

        Err(ErrorBuilder::new(
            ErrorCode::InternalError,
            "no gRPC hook client: neither endpoint nor service discovery configured",
        )
        .build_error())
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

    async fn call_hook<Mres: Message + Default>(
        &self,
        ctx: &Context,
        operation: &'static str,
        request_type_url: &str,
        inner: &impl Message,
        key: Option<&str>,
    ) -> Result<Mres> {
        let any = prost_types::Any {
            type_url: request_type_url.to_string(),
            value: inner.encode_to_vec(),
        };
        let mut req = Request::new(GenericRequest {
            operation: operation.to_string(),
            metadata: HashMap::new(),
            payload: Some(any),
            request_id: uuid::Uuid::new_v4().to_string(),
        });
        req = self.set_request_metadata(req, ctx);
        let mut client = self.get_client(key).await?;
        let envelope = client
            .call(req)
            .await
            .map_err(|e| {
                map_infra_error(e, ErrorCode::InternalError, "gRPC HookPlugin.Call failed")
            })?
            .into_inner();
        if !envelope.ok {
            return Err(ErrorBuilder::new(
                ErrorCode::InternalError,
                envelope.error_message.clone(),
            )
            .details(envelope.error_code.clone())
            .build_error());
        }
        let payload = envelope.payload.ok_or_else(|| {
            ErrorBuilder::new(
                ErrorCode::InternalError,
                "HookPlugin.Call returned empty payload",
            )
            .build_error()
        })?;
        Mres::decode(payload.value.as_slice()).map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::InternalError,
                "failed to decode HookPlugin.Call response",
            )
        })
    }

    /// 执行PreSend Hook
    pub async fn pre_send(
        &self,
        ctx: &Context,
        draft: &mut MessageDraft,
    ) -> Result<PreSendDecision> {
        let inner = PreSendHookRequest {
            context: Some(context_to_proto(ctx)),
            draft: Some(message_draft_to_proto(draft)),
        };
        let key = ctx
            .session_id()
            .and_then(|s| if s.is_empty() { None } else { Some(s) });
        let response: PreSendHookResponse = self
            .call_hook(
                ctx,
                "flare.hook.v1.pre_send",
                "type.googleapis.com/flare.capability.v1.PreSendHookRequest",
                &inner,
                key,
            )
            .await?;

        Ok(proto_to_pre_send_decision(&response, draft))
    }

    /// 执行PostSend Hook
    pub async fn post_send(
        &self,
        ctx: &Context,
        record: &MessageRecord,
        draft: &MessageDraft,
    ) -> Result<()> {
        let inner = PostSendHookRequest {
            context: Some(context_to_proto(ctx)),
            record: Some(message_record_to_proto(record)),
            draft: Some(message_draft_to_proto(draft)),
        };
        let key = ctx
            .session_id()
            .and_then(|s| if s.is_empty() { None } else { Some(s) });
        let response: PostSendHookResponse = self
            .call_hook(
                ctx,
                "flare.hook.v1.post_send",
                "type.googleapis.com/flare.capability.v1.PostSendHookRequest",
                &inner,
                key,
            )
            .await?;

        if response.success {
            Ok(())
        } else {
            Err(ErrorBuilder::new(
                ErrorCode::InternalError,
                "remote PostSend hook reported failure",
            )
            .build_error())
        }
    }

    /// 执行Delivery Hook
    pub async fn delivery(&self, ctx: &Context, event: &DeliveryEvent) -> Result<()> {
        let inner = DeliveryHookRequest {
            context: Some(context_to_proto(ctx)),
            event: Some(delivery_event_to_proto(event)),
        };
        let key = Some(event.user_id.as_str());
        let response: DeliveryHookResponse = self
            .call_hook(
                ctx,
                "flare.hook.v1.delivery",
                "type.googleapis.com/flare.capability.v1.DeliveryHookRequest",
                &inner,
                key,
            )
            .await?;

        if response.success {
            Ok(())
        } else {
            Err(ErrorBuilder::new(
                ErrorCode::InternalError,
                "remote Delivery hook reported failure",
            )
            .build_error())
        }
    }

    /// 执行Recall Hook
    pub async fn recall(&self, ctx: &Context, event: &RecallEvent) -> Result<PreSendDecision> {
        let inner = RecallHookRequest {
            context: Some(context_to_proto(ctx)),
            event: Some(recall_event_to_proto(event)),
        };
        let key = ctx
            .session_id()
            .and_then(|s| if s.is_empty() { None } else { Some(s) });
        let response: RecallHookResponse = self
            .call_hook(
                ctx,
                "flare.hook.v1.recall",
                "type.googleapis.com/flare.capability.v1.RecallHookRequest",
                &inner,
                key,
            )
            .await?;

        Ok(proto_to_recall_decision(&response))
    }
}
