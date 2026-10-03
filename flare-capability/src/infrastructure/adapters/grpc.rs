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
use tonic::transport::Channel;

use flare_grpc_proto::capability::hook_plugin_client::HookPluginClient;
use flare_grpc_proto::capability::{
    DeliveryHookRequest, DeliveryHookResponse, GenericRequest, PostSendHookRequest,
    PostSendHookResponse, PreSendHookRequest, PreSendHookResponse, RecallHookRequest,
    RecallHookResponse,
};
use flare_im_hooks::{DeliveryEvent, MessageDraft, MessageRecord, PreSendDecision, RecallEvent};
use flare_server_core::client::{forward_signed_context, set_context_metadata};
use flare_server_core::context::Context;
use flare_server_core::grpc::middleware::signed_context::VerifiedContext;

use crate::domain::model::LoadBalanceStrategy;
use crate::infrastructure::adapters::conversion::{
    context_to_proto, delivery_event_to_proto, message_draft_to_proto, message_record_to_proto,
    proto_to_pre_send_decision, proto_to_recall_decision, recall_event_to_proto,
};

// 导入服务发现相关模块
use flare_im_service_kit::discovery::{
    invalidate_static_channel_on_transport_failure, resolve_grpc_channel,
};
use flare_server_core::{ServiceClient, ServiceDiscover};

use flare_server_core::error::{ErrorBuilder, ErrorCode, Result, map_infra_error};

fn unsigned_hook_context() -> flare_server_core::error::FlareError {
    ErrorBuilder::new(
        ErrorCode::AuthenticationFailed,
        "gRPC hook requires a signed hook context to forward",
    )
    .build_error()
}

/// gRPC Hook适配器
#[allow(dead_code)]
pub struct GrpcHookAdapter {
    // 模式1: 直接地址模式（`http(s)://` 静态地址，通道取自 service-kit 的静态通道缓存）
    endpoint: Option<String>,

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
    ///
    /// 通道取自 service-kit 的静态通道（建连上限 2 s 覆盖名字解析与握手、HTTP/2 探活，见
    /// `flare_im_service_kit::discovery::StaticChannelPolicy`），按地址缓存、构造时不连下游；
    /// 调用遇到传输层失败时丢弃该地址的通道，下一次调用重新解析、新建连接。以前每次构造都
    /// 立即建一条没有上限、没有探活的新连接。
    pub async fn new_from_endpoint(
        endpoint: String,
        metadata: HashMap<String, String>,
    ) -> Result<Self> {
        resolve_grpc_channel(&endpoint).await.map_err(|e| {
            map_infra_error(
                std::io::Error::new(std::io::ErrorKind::InvalidInput, e),
                ErrorCode::InvalidParameter,
                "invalid gRPC hook endpoint URI",
            )
        })?;

        tracing::trace!(endpoint = %endpoint, "Created gRPC adapter from endpoint");

        Ok(Self {
            endpoint: Some(endpoint),
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
            endpoint: None,
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
            endpoint: None,
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
        if let Some(endpoint) = self.endpoint.as_deref() {
            let channel = resolve_grpc_channel(endpoint).await.map_err(|e| {
                map_infra_error(
                    std::io::Error::other(e),
                    ErrorCode::ServiceUnavailable,
                    "failed to resolve gRPC hook endpoint",
                )
            })?;
            return Ok(HookPluginClient::new(channel));
        }

        // 模式2: 服务发现模式
        if let Some(service_client) = &self.service_client {
            // 使用 ServiceClient 获取 Channel（已包含负载均衡）
            let mut client_guard = service_client.lock().await;
            let channel = flare_im_service_kit::discovery::get_discovered_channel_with_timeout(
                &self.service_name,
                &mut client_guard,
            )
            .await
            .map_err(|e| {
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

    /// 设置请求元数据：静态 metadata、Context 的元数据，最后原样转发调用方的签名令牌
    /// （`x-flare-ctx`，同 Message Ingest 的 gRPC Hook 适配器）。
    ///
    /// 下游插件（如 Social 的 HookPlugin）只认签名身份，以前这里只带裸元数据，要求签名的插件
    /// 一律拒绝。上下文不是由验过签的令牌建的（HookPlugin 之外的调用）时不发出调用：没有可
    /// 转发的签名身份。静态 metadata 里同名的 `x-flare-ctx` 会被签名令牌覆盖。
    fn set_request_metadata<T>(
        &self,
        mut request: Request<T>,
        ctx: &Context,
    ) -> Result<Request<T>> {
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

        // 3. 原样转发签名令牌
        match forward_signed_context(&mut request, ctx) {
            Ok(true) => Ok(request),
            Ok(false) => Err(unsigned_hook_context()),
            Err(status) => Err(map_infra_error(
                status,
                ErrorCode::AuthenticationFailed,
                "failed to forward the signed hook context",
            )),
        }
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
        // 外层请求号取签名里的请求号：要求签名的插件核对它与令牌一致。
        let request_id = ctx
            .get_data::<VerifiedContext>()
            .and_then(VerifiedContext::claims)
            .map(|claims| claims.request_id.clone())
            .ok_or_else(unsigned_hook_context)?;
        let mut req = Request::new(GenericRequest {
            operation: operation.to_string(),
            metadata: HashMap::new(),
            payload: Some(any),
            request_id,
        });
        req = self.set_request_metadata(req, ctx)?;
        let mut client = self.get_client(key).await?;
        let envelope = match client.call(req).await {
            Ok(response) => response.into_inner(),
            Err(status) => {
                // 传输层失败（连不上、连接被断、ping 超时）时丢弃该地址的静态通道，下一次调用
                // 重新解析、新建连接；对端回的状态不动缓存。
                if let Some(endpoint) = self.endpoint.as_deref()
                    && invalidate_static_channel_on_transport_failure(endpoint, &status)
                {
                    tracing::debug!(
                        endpoint,
                        code = ?status.code(),
                        "hook channel dropped after transport failure"
                    );
                }
                return Err(map_infra_error(
                    status,
                    ErrorCode::InternalError,
                    "gRPC HookPlugin.Call failed",
                ));
            }
        };
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
        let key = ctx.session_id().filter(|&s| !s.is_empty());
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
        let key = ctx.session_id().filter(|&s| !s.is_empty());
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
        let key = ctx.session_id().filter(|&s| !s.is_empty());
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

#[cfg(test)]
mod static_channel_tests {
    use super::*;
    use crate::interface::grpc::hooks::test_support::{cell_keys, signed_ctx};
    use flare_grpc_proto::capability::GenericResponse;
    use flare_grpc_proto::capability::hook_plugin_server::{HookPlugin, HookPluginServer};
    use flare_im_service_kit::discovery::is_static_channel_cached;
    use flare_server_core::SignedActorType;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tonic::{Response, Status};

    struct AllowAll;

    #[tonic::async_trait]
    impl HookPlugin for AllowAll {
        async fn call(
            &self,
            request: Request<GenericRequest>,
        ) -> std::result::Result<Response<GenericResponse>, Status> {
            let response = PreSendHookResponse {
                allow: true,
                ..Default::default()
            };
            Ok(Response::new(GenericResponse {
                ok: true,
                request_id: request.into_inner().request_id,
                payload: Some(prost_types::Any {
                    type_url: "type.googleapis.com/flare.capability.v1.PreSendHookResponse"
                        .to_string(),
                    value: response.encode_to_vec(),
                }),
                error_code: String::new(),
                error_message: String::new(),
            }))
        }
    }

    /// 直接地址模式走 service-kit 的静态通道（建连上限、HTTP/2 探活），传输层失败后丢弃
    /// 该地址的通道。以前每次构造都立即建一条没有上限、没有探活的新连接。
    #[tokio::test]
    async fn endpoint_mode_uses_the_static_channel_and_drops_it_after_transport_failure() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, stop_rx) = oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(HookPluginServer::new(AllowAll))
                .serve_with_incoming_shutdown(
                    tonic::transport::server::TcpIncoming::from(listener),
                    async {
                        let _ = stop_rx.await;
                    },
                )
                .await
                .unwrap();
        });
        let endpoint = format!("http://{addr}");
        let adapter = GrpcHookAdapter::new_from_endpoint(endpoint.clone(), HashMap::new())
            .await
            .expect("endpoint adapter");
        assert!(
            is_static_channel_cached(&endpoint),
            "endpoint mode must use the service-kit static channel"
        );

        let keys = cell_keys(93);
        let ctx = signed_ctx(&keys, SignedActorType::User, "u-static", "tenant-a");
        let mut draft = MessageDraft::new(b"hello".to_vec());
        let decision = adapter.pre_send(&ctx, &mut draft).await.expect("allowed");
        assert!(matches!(decision, PreSendDecision::Continue));
        assert!(is_static_channel_cached(&endpoint));

        let _ = stop.send(());
        let _ = tokio::time::timeout(Duration::from_secs(5), server).await;
        assert!(adapter.pre_send(&ctx, &mut draft).await.is_err());
        assert!(
            !is_static_channel_cached(&endpoint),
            "transport failure must drop the cached static channel"
        );
    }

    #[tokio::test]
    async fn an_invalid_endpoint_is_rejected_at_construction() {
        assert!(
            GrpcHookAdapter::new_from_endpoint("http://bad host:1".to_string(), HashMap::new())
                .await
                .is_err()
        );
    }
}

#[cfg(test)]
mod signed_forwarding_tests {
    //! Capability 自带的 gRPC Hook 适配器原样转发调用方的签名令牌（W11-H 余项）：要求签名的
    //! 下游插件（同 Social 的 HookPlugin：验签，外层与上下文的请求号、租户、操作者都须与签名
    //! 一致）能收下；没有签名身份的上下文不发出调用。
    use super::*;
    use crate::interface::grpc::hooks::test_support::{CellKeys, cell_keys, signed_ctx};
    use flare_grpc_proto::capability::GenericResponse;
    use flare_grpc_proto::capability::hook_plugin_server::{HookPlugin, HookPluginServer};
    use flare_server_core::SignedActorType;
    use flare_server_core::grpc::middleware::signed_context::verify_signed_context_metadata;
    use std::sync::Mutex;
    use tokio::net::TcpListener;
    use tonic::{Response, Status};

    /// 只收签名调用的插件：验签失败或身份不一致回 UNAUTHENTICATED，记下每次收下的调用。
    struct SignedOnly {
        keys: CellKeys,
        accepted: Arc<Mutex<Vec<(String, String)>>>,
        calls: Arc<Mutex<usize>>,
    }

    #[tonic::async_trait]
    impl HookPlugin for SignedOnly {
        async fn call(
            &self,
            request: Request<GenericRequest>,
        ) -> std::result::Result<Response<GenericResponse>, Status> {
            *self.calls.lock().unwrap() += 1;
            let ctx = verify_signed_context_metadata(request.metadata(), &self.keys.verifier)?;
            let claims = ctx
                .get_data::<VerifiedContext>()
                .and_then(VerifiedContext::claims)
                .cloned()
                .ok_or_else(|| Status::unauthenticated("signed hook context required"))?;
            let outer = request.into_inner();
            let inner = PreSendHookRequest::decode(outer.payload.unwrap().value.as_slice())
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            let hook = inner.context.unwrap_or_default();
            if outer.request_id != claims.request_id
                || hook.request_id != claims.request_id
                || hook.tenant_id != claims.tenant_id
                || hook.operator_user_id != claims.actor_id
            {
                return Err(Status::unauthenticated("hook identity mismatch"));
            }
            self.accepted
                .lock()
                .unwrap()
                .push((claims.tenant_id.clone(), claims.actor_id.clone()));
            let response = PreSendHookResponse {
                allow: true,
                ..Default::default()
            };
            Ok(Response::new(GenericResponse {
                ok: true,
                request_id: outer.request_id,
                payload: Some(prost_types::Any {
                    type_url: "type.googleapis.com/flare.capability.v1.PreSendHookResponse"
                        .to_string(),
                    value: response.encode_to_vec(),
                }),
                error_code: String::new(),
                error_message: String::new(),
            }))
        }
    }

    #[tokio::test]
    async fn the_callers_signed_context_reaches_a_plugin_that_requires_it() {
        let accepted = Arc::new(Mutex::new(Vec::new()));
        let calls = Arc::new(Mutex::new(0));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(HookPluginServer::new(SignedOnly {
                    keys: cell_keys(94),
                    accepted: Arc::clone(&accepted),
                    calls: Arc::clone(&calls),
                }))
                .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener)),
        );
        // 静态 metadata 想冒充签名令牌也没用：转发的令牌最后写入。
        let adapter = GrpcHookAdapter::new_from_endpoint(
            format!("http://{addr}"),
            HashMap::from([("x-flare-ctx".to_string(), "forged".to_string())]),
        )
        .await
        .unwrap();

        let keys = cell_keys(94);
        let ctx = signed_ctx(&keys, SignedActorType::User, "u-forward", "tenant-a")
            .as_ref()
            .clone()
            .with_session_id("conv-1");
        let mut draft = MessageDraft::new(b"hello".to_vec());
        let decision = adapter.pre_send(&ctx, &mut draft).await.expect("accepted");
        assert!(matches!(decision, PreSendDecision::Continue));
        assert_eq!(
            accepted.lock().unwrap().as_slice(),
            [("tenant-a".to_string(), "u-forward".to_string())]
        );

        // 没有签名身份的上下文（只有裸租户、用户）不发出调用。
        let unsigned = Context::with_request_id("req-unsigned")
            .with_tenant_id("tenant-a")
            .with_user_id("u-forward");
        let error = adapter.pre_send(&unsigned, &mut draft).await.unwrap_err();
        assert_eq!(
            error.code(),
            Some(ErrorCode::AuthenticationFailed),
            "{error}"
        );
        assert_eq!(*calls.lock().unwrap(), 1);
    }
}
