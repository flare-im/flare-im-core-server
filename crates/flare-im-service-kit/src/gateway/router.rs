//! Gateway Router（跨地区路由组件）
//!
//! 根据gateway_id路由到对应的Access Gateway，支持单地区/多地区自适应部署。
//!
//! 位于 `flare-im-core/src/gateway/router.rs`（IM 核心共享库），
//! 被 Push Server、Push Worker 和 API Gateway 复用。
//!
//! ## 使用流程
//!
//! 1. **查询在线状态**：调用方（Push Server/API Gateway）先查询 Signaling Online 服务获取用户的 `gateway_id`
//! 2. **路由推送**：调用 Gateway Router 的 `route_push_message` 方法，传入 `gateway_id` 和推送请求
//! 3. **服务发现**：Gateway Router 通过服务发现获取对应 `gateway_id` 的 Access Gateway 地址
//! 4. **推送消息**：Gateway Router 调用 Access Gateway 的 `PushMessage` 接口推送消息

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use flare_server_core::error::{AnyhowContext, ErrorBuilder, ErrorCode, FlareError, Result};

use flare_grpc_proto::access_gateway::access_gateway_client::AccessGatewayClient;
use flare_grpc_proto::access_gateway::{
    DeliverToConversationRequest, PushAckRequest, PushAckResponse, PushCapabilityRequest,
    PushCustomRequest, PushEventRequest, PushMessageRequest, PushNotificationRequest,
    PushNotificationResponse, PushResponse, RelayRealtimeControlRequest,
};
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tracing::{debug, info, warn};

use crate::discovery::{StaticChannelPolicy, is_transport_failure, static_grpc_endpoint};
use crate::{ServiceClient, ServiceDiscover};

/// Access Gateway 暂时不可达：连不上、连接被断、调用超时、网关在停机或重启（W6-S2 F4：core
/// 停机窗口里 `tcp connect error` 被当成不可重试，推送直接进了 DLQ）。按 `ServiceUnavailable`
/// 报出，push-worker 据此 Nack 交给 broker 重投（JetStream 有投递次数上限）。必须用
/// `localized`：只有它带 `ErrorCode`，`system` 的 `is_retryable()` 恒为 false。
fn gateway_unavailable(message: String) -> FlareError {
    FlareError::localized(ErrorCode::ServiceUnavailable, message)
}

/// 网关调用的失败是否暂时性的：传输层失败，或网关回的是「稍后重试」类状态。
fn is_transient_gateway_status(status: &tonic::Status) -> bool {
    use tonic::Code;
    is_transport_failure(status)
        || matches!(
            status.code(),
            Code::Unavailable
                | Code::Cancelled
                | Code::DeadlineExceeded
                | Code::ResourceExhausted
                | Code::Aborted
        )
}

/// 网关调用失败的错误：暂时性的可重试，网关明确拒绝（参数、鉴权等）仍不可重试。
fn gateway_call_failure(message: String, status: &tonic::Status) -> FlareError {
    if is_transient_gateway_status(status) {
        gateway_unavailable(message)
    } else {
        FlareError::system(message)
    }
}

/// Gateway Router 配置
#[derive(Debug, Clone)]
pub struct GatewayRouterConfig {
    /// 连接池最大大小（每个gateway_id一个连接）
    pub connection_pool_size: usize,
    /// 连接超时时间（毫秒）
    pub connection_timeout_ms: u64,
    /// 连接空闲超时时间（毫秒，超过此时间未使用则关闭连接）
    pub connection_idle_timeout_ms: u64,
    /// 部署模式：single_region | multi_region
    pub deployment_mode: String,
    /// 本地网关 ID（多地区部署时使用）
    pub local_gateway_id: Option<String>,
    /// Access Gateway 服务名（用于服务发现）
    pub access_gateway_service: String,
    /// 无注册中心、且未注入 ServiceClient/ServiceDiscover 时，直连的 gRPC 基址（本地单实例开发）
    /// 环境变量 `ACCESS_GATEWAY_GRPC_ENDPOINT` 可由各服务 wire 写入；亦见 [`crate::discovery::build_gateway_router_from_app_config`]。
    pub static_fallback_endpoint: Option<String>,
}

impl Default for GatewayRouterConfig {
    fn default() -> Self {
        use crate::service_names::ACCESS_GATEWAY;
        Self {
            connection_pool_size: 100, // 支持最多100个不同的gateway_id
            connection_timeout_ms: 5000,
            connection_idle_timeout_ms: 300_000, // 5分钟空闲超时
            deployment_mode: "single_region".to_string(),
            local_gateway_id: None,
            access_gateway_service: ACCESS_GATEWAY.to_string(),
            static_fallback_endpoint: None,
        }
    }
}

/// Gateway Router trait
pub trait GatewayRouterTrait: Send + Sync {
    /// 路由推送请求到Access Gateway
    async fn route_push_message(
        &self,
        gateway_id: &str,
        request: PushMessageRequest,
    ) -> Result<PushResponse>;

    async fn route_push_event(
        &self,
        gateway_id: &str,
        request: PushEventRequest,
    ) -> Result<PushResponse>;

    async fn route_push_notification(
        &self,
        gateway_id: &str,
        request: PushNotificationRequest,
    ) -> Result<PushNotificationResponse>;

    async fn route_push_ack(
        &self,
        gateway_id: &str,
        request: PushAckRequest,
    ) -> Result<PushAckResponse>;

    async fn route_push_custom(
        &self,
        gateway_id: &str,
        request: PushCustomRequest,
    ) -> Result<PushResponse>;
}

/// 连接池条目（包含客户端和最后使用时间）
struct ConnectionPoolEntry {
    client: AccessGatewayClient<Channel>,
    last_used: Instant,
}

/// Gateway Router实现
pub struct GatewayRouter {
    config: GatewayRouterConfig,
    /// 连接池（gateway_id -> entry）
    connection_pool: Arc<RwLock<HashMap<String, ConnectionPoolEntry>>>,
    /// ServiceClient（通过 wire 注入，可选，用于负载均衡场景）
    service_client: Option<Arc<tokio::sync::Mutex<ServiceClient>>>,
    /// ServiceDiscover（用于根据 gateway_id 获取特定实例）
    service_discover: Option<Arc<ServiceDiscover>>,
}

impl GatewayRouter {
    /// 创建Gateway Router（使用服务名称，内部创建服务发现）
    pub fn new(config: GatewayRouterConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            connection_pool: Arc::new(RwLock::new(HashMap::new())),
            service_client: None,
            service_discover: None,
        })
    }

    /// 使用 ServiceClient 创建Gateway Router（推荐，通过 wire 注入）
    pub fn with_service_client(
        config: GatewayRouterConfig,
        service_client: ServiceClient,
    ) -> Arc<Self> {
        Arc::new(Self {
            config,
            connection_pool: Arc::new(RwLock::new(HashMap::new())),
            service_client: Some(Arc::new(tokio::sync::Mutex::new(service_client))),
            service_discover: None, // 目前不保存 ServiceDiscover，使用 ServiceClient 的负载均衡
        })
    }

    /// 使用 ServiceClient 和 ServiceDiscover 创建Gateway Router（支持按 gateway_id 过滤实例）
    pub fn with_service_client_and_discover(
        config: GatewayRouterConfig,
        service_client: ServiceClient,
        service_discover: ServiceDiscover,
    ) -> Arc<Self> {
        Arc::new(Self {
            config,
            connection_pool: Arc::new(RwLock::new(HashMap::new())),
            service_client: Some(Arc::new(tokio::sync::Mutex::new(service_client))),
            service_discover: Some(Arc::new(service_discover)),
        })
    }

    /// 判断是否为本地网关
    fn is_local_gateway(&self, gateway_id: &str) -> bool {
        // 使用逻辑表达式替代嵌套 if，提高可读性
        self.config
            .local_gateway_id
            .as_ref()
            .map(|local_id| local_id == gateway_id)
            .unwrap_or(false)
            || self.config.deployment_mode == "single_region"
    }

    /// 清理空闲连接
    async fn cleanup_idle_connections(&self) {
        let idle_timeout = Duration::from_millis(self.config.connection_idle_timeout_ms);
        let now = Instant::now();

        let mut pool = self.connection_pool.write().await;
        let before = pool.len();

        pool.retain(|_, entry| now.duration_since(entry.last_used) < idle_timeout);

        let after = pool.len();
        if before > after {
            debug!(
                removed = before - after,
                remaining = after,
                "Cleaned up idle gateway connections"
            );
        }
    }

    async fn connect_channel(&self, uri: &str, target: impl Into<String>) -> Result<Channel> {
        let target = target.into();
        let endpoint = static_grpc_endpoint(uri, StaticChannelPolicy::DEFAULT)
            .map_err(FlareError::system)
            .with_context(|| format!("Invalid URI for {}: {}", target, uri))?;
        let timeout_duration = Duration::from_millis(self.config.connection_timeout_ms);

        tokio::time::timeout(timeout_duration, endpoint.connect())
            .await
            .map_err(|_| {
                gateway_unavailable(format!(
                    "Timeout connecting to {} at {} (timeout: {}ms)",
                    target,
                    uri,
                    timeout_duration.as_millis()
                ))
            })?
            .map_err(|e| {
                gateway_unavailable(format!("Failed to connect to {} at {}: {}", target, uri, e))
            })
    }

    /// 获取或创建Access Gateway客户端
    async fn get_or_create_client(&self, gateway_id: &str) -> Result<AccessGatewayClient<Channel>> {
        // 先检查连接池
        {
            let mut pool = self.connection_pool.write().await;
            if let Some(entry) = pool.get_mut(gateway_id) {
                // 更新最后使用时间
                entry.last_used = Instant::now();
                return Ok(entry.client.clone());
            }

            // 检查连接池大小限制
            if pool.len() >= self.config.connection_pool_size {
                // 清理空闲连接
                drop(pool);
                self.cleanup_idle_connections().await;

                // 再次检查
                let mut pool = self.connection_pool.write().await;
                if pool.len() >= self.config.connection_pool_size {
                    // 如果还是满的，移除最旧的连接
                    let oldest = pool
                        .iter()
                        .min_by_key(|(_, entry)| entry.last_used)
                        .map(|(id, _)| id.clone());
                    if let Some(oldest_id) = oldest {
                        pool.remove(&oldest_id);
                        debug!(
                            removed_gateway_id = %oldest_id,
                            "Removed oldest connection to make room for new connection"
                        );
                    }
                }
            }
        }

        // 使用服务发现获取特定 gateway_id 的 Channel
        // 优先使用 ServiceDiscover 根据 instance_id 过滤实例，如果不可用则回退到 ServiceClient 的负载均衡
        let channel = if let Some(ref service_discover) = self.service_discover {
            // 使用 ServiceDiscover 获取所有实例，然后根据 instance_id == gateway_id 筛选
            let instances = service_discover.get_instances().await;

            // 使用 find_map 简化查找逻辑
            let target_instance = instances.iter().find(|inst| inst.instance_id == gateway_id);

            match target_instance {
                Some(instance) => {
                    // 根据实例地址直接创建 channel
                    let uri = instance.to_grpc_uri();
                    self.connect_channel(&uri, format!("gateway {}", gateway_id))
                        .await?
                }
                None => {
                    if let Some(ref uri) = self.config.static_fallback_endpoint {
                        warn!(
                            gateway_id = %gateway_id,
                            fallback = %uri,
                            available_instances = ?instances
                                .iter()
                                .map(|i| i.instance_id.clone())
                                .collect::<Vec<_>>(),
                            "Gateway instance not found in service discovery; using static Access Gateway fallback"
                        );
                        self.connect_channel(uri, "static Access Gateway fallback")
                            .await?
                    } else {
                        // ServiceUnavailable 而不是 system：网关实例查不到是**瞬时可恢复**
                        // 状态——滚动重启时 push-worker 可能比网关早起来几秒，此时网关
                        // 还没完成 consul 注册。system 不在 is_retryable 列表里，于是
                        // push-worker 把它判成 non-retryable、消息直接进 DLQ 丢掉。
                        // 实测 r16 部署时因此丢了 2 条（push-worker 早启动 1 秒）。
                        // ServiceUnavailable 会走 Nack → broker 重投，网关注册完成后自然成功。
                        // ⚠️ 必须用 localized：FlareError 只有 Localized 变体带 ErrorCode，
                        // 其余变体 code() 返回 None → is_retryable() 恒 false。
                        // 用 ::system 构造的话，无论语义如何都会被判成不可重试。
                        return Err(flare_server_core::error::FlareError::localized(
                            flare_server_core::error::ErrorCode::ServiceUnavailable,
                            format!(
                                "Gateway instance not found: gateway_id={}. Available instances: {}",
                                gateway_id,
                                instances
                                    .iter()
                                    .map(|i| i.instance_id.clone())
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            ),
                        ));
                    }
                }
            }
        } else if let Some(ref service_client_arc) = self.service_client {
            // 回退到 ServiceClient 的负载均衡（不保证是特定的 gateway_id）
            warn!(
                gateway_id = %gateway_id,
                "ServiceDiscover not available, using ServiceClient load balancing (may not match specific gateway_id)"
            );
            let mut service_client = service_client_arc.lock().await;
            let timeout_duration = Duration::from_millis(self.config.connection_timeout_ms);

            tokio::time::timeout(timeout_duration, service_client.get_channel())
                .await
                .map_err(|_| {
                    gateway_unavailable(format!(
                        "Timeout waiting for service discovery to get channel for gateway {} (timeout: {}ms)",
                        gateway_id,
                        timeout_duration.as_millis()
                    ))
                })?
                .map_err(|e| {
                    gateway_unavailable(format!(
                        "Failed to get channel from service discovery: {}",
                        e
                    ))
                })?
        } else if let Some(ref uri) = self.config.static_fallback_endpoint {
            self.connect_channel(uri, "static Access Gateway fallback")
                .await?
        } else {
            return Err(flare_server_core::error::FlareError::system(
                "Neither ServiceDiscover nor ServiceClient is available, and static_fallback_endpoint is unset. Inject discovery via with_service_client() / with_service_client_and_discover(), or set GatewayRouterConfig.static_fallback_endpoint (e.g. ACCESS_GATEWAY_GRPC_ENDPOINT in dev)."
                    .to_string(),
            ));
        };

        let client = AccessGatewayClient::new(channel);

        // 存入连接池
        {
            let mut pool = self.connection_pool.write().await;
            pool.insert(
                gateway_id.to_string(),
                ConnectionPoolEntry {
                    client: client.clone(),
                    last_used: Instant::now(),
                },
            );
        }

        info!(
            gateway_id = %gateway_id,
            service_name = %self.config.access_gateway_service,
            "Created new gateway connection via service discovery"
        );

        Ok(client)
    }

    /// 轻量信令跨节点直转：把一条已聚合的 RealtimeControl 帧**广播给其余网关节点**，各节点再发给
    /// 自己的本地订阅者。发起节点已经转给过自己的连接了，所以这里跳过 `skip_gateway_id`。
    ///
    /// best-effort，而且比消息更松：typing / presence 是有损的瞬时信号，单节点失败只记 trace，
    /// 既不重试也不回报发送方——重试一条已经过时的「正在输入」没有意义。
    pub async fn broadcast_relay_realtime_control(
        &self,
        request: RelayRealtimeControlRequest,
        skip_gateway_id: Option<&str>,
        signed_token: &str,
    ) {
        let gateway_ids: Vec<String> = match self.service_discover {
            Some(ref sd) => peer_gateway_ids(
                sd.get_instances()
                    .await
                    .into_iter()
                    .map(|inst| inst.instance_id),
                skip_gateway_id,
            ),
            None => Vec::new(),
        };
        // 没有服务发现就是单实例部署：本节点的直转已经覆盖了全部在线订阅者，没有对等节点要广播。
        if gateway_ids.is_empty() {
            return;
        }
        for gateway_id in gateway_ids {
            match self.get_or_create_client(&gateway_id).await {
                Ok(mut client) => {
                    let mut signed_request = tonic::Request::new(request.clone());
                    if flare_server_core::set_signed_context_token(
                        &mut signed_request,
                        signed_token,
                    )
                    .is_err()
                    {
                        tracing::warn!("realtime relay not sent: invalid signed context header");
                        return;
                    }
                    if let Err(e) = client.relay_realtime_control(signed_request).await {
                        tracing::trace!(
                            gateway_id = %gateway_id,
                            error = %e,
                            "relay_realtime_control to gateway failed (ephemeral, ignored)"
                        );
                    }
                }
                Err(e) => tracing::trace!(
                    gateway_id = %gateway_id,
                    error = %e,
                    "relay_realtime_control: get gateway client failed (ephemeral, ignored)"
                ),
            }
        }
    }

    /// 无服务发现实例：单端点（get_or_create_client 内部回退 static_fallback_endpoint）。
    /// 请求已带好签名身份。
    async fn deliver_via_static_fallback(
        &self,
        request: tonic::Request<DeliverToConversationRequest>,
    ) -> Result<()> {
        let mut client = self.get_or_create_client("__static_fallback__").await?;
        client.deliver_to_conversation(request).await.map_err(|e| {
            gateway_call_failure(format!("deliver_to_conversation failed: {e}"), &e)
        })?;
        Ok(())
    }

    /// 统一读扩散：把会话消息**广播到所有网关节点**，各节点按本地会话订阅过滤投递。
    /// 上游（push-worker）无需解析收件人。best-effort：单节点失败不影响其他节点（在线投递为软实时，
    /// 离线成员靠版本号增量拉兜底）。无服务发现实例时走静态 fallback 单端点。
    pub async fn broadcast_deliver_to_conversation(
        &self,
        ctx: &flare_server_core::context::Ctx,
        request: DeliverToConversationRequest,
    ) -> Result<()> {
        // Reject unscoped work before opening a channel. Gateway conversation
        // subscriptions are tenant-scoped; a fresh bare Request loses that scope.
        let request = push_producer_request(ctx, request)?;
        let gateway_ids: Vec<String> = match self.service_discover {
            Some(ref sd) => sd
                .get_instances()
                .await
                .into_iter()
                .map(|inst| inst.instance_id)
                .collect(),
            None => Vec::new(),
        };

        if gateway_ids.is_empty() {
            return self.deliver_via_static_fallback(request).await;
        }

        for gateway_id in gateway_ids {
            match self.get_or_create_client(&gateway_id).await {
                Ok(mut client) => {
                    if let Err(e) = client
                        .deliver_to_conversation(push_producer_request(
                            ctx,
                            request.get_ref().clone(),
                        )?)
                        .await
                    {
                        warn!(
                            gateway_id = %gateway_id,
                            error = %e,
                            "broadcast deliver_to_conversation to gateway failed (continuing)"
                        );
                    }
                }
                Err(e) => warn!(
                    gateway_id = %gateway_id,
                    error = %e,
                    "broadcast deliver_to_conversation: get gateway client failed (continuing)"
                ),
            }
        }
        Ok(())
    }
}

impl GatewayRouter {
    /// A plugin realtime packet to **every** gateway node; each writes it to the listed users' own
    /// connections in the producer's tenant, or does nothing. Recipients are few (at most 32), so
    /// no per-user Online lookup sits on the latency path, and a connection the Online index has
    /// not seen yet still gets the packet. Best effort: a failed node is logged and skipped (at
    /// most once, never retried).
    pub async fn broadcast_push_capability(
        &self,
        ctx: &flare_server_core::context::Ctx,
        request: PushCapabilityRequest,
    ) -> Result<()> {
        // Refuse unscoped work before opening a channel.
        let first = push_producer_request(ctx, request.clone())?;
        let gateway_ids: Vec<String> = match self.service_discover {
            Some(ref sd) => sd
                .get_instances()
                .await
                .into_iter()
                .map(|inst| inst.instance_id)
                .collect(),
            None => Vec::new(),
        };
        if gateway_ids.is_empty() {
            let mut client = self.get_or_create_client("__static_fallback__").await?;
            client
                .push_capability(first)
                .await
                .map_err(|e| gateway_call_failure(format!("push_capability failed: {e}"), &e))?;
            return Ok(());
        }
        let calls = gateway_ids.into_iter().map(|gateway_id| {
            let request = request.clone();
            async move {
                let outcome =
                    match self.get_or_create_client(&gateway_id).await {
                        Ok(mut client) => {
                            let mut call = push_producer_request(ctx, request)?;
                            call.set_timeout(Duration::from_secs(1));
                            client.push_capability(call).await.map(|_| ()).map_err(|e| {
                                gateway_call_failure(format!("push_capability: {e}"), &e)
                            })
                        }
                        Err(e) => Err(e),
                    };
                if let Err(error) = outcome {
                    warn!(
                        gateway_id = %gateway_id,
                        error = %error,
                        "broadcast push_capability to gateway failed (continuing)"
                    );
                }
                Ok::<_, FlareError>(())
            }
        });
        for result in futures::future::join_all(calls).await {
            result?;
        }
        Ok(())
    }
}

impl GatewayRouterTrait for GatewayRouter {
    async fn route_push_message(
        &self,
        gateway_id: &str,
        request: PushMessageRequest,
    ) -> Result<PushResponse> {
        info!(
            gateway_id = %gateway_id,
            user_count = request.user_ids.len(),
            user_ids = ?request.user_ids,
            "Routing push message to gateway"
        );

        // 判断是否为本地网关
        let is_local = self.is_local_gateway(gateway_id);

        // 使用 guard clause 减少嵌套
        if is_local {
            debug!(
                gateway_id = %gateway_id,
                "Local gateway, direct call"
            );
        } else {
            info!(
                gateway_id = %gateway_id,
                "Remote gateway, cross-region call"
            );
        }

        // 获取或创建客户端
        let mut client = match self.get_or_create_client(gateway_id).await {
            Ok(c) => {
                debug!(
                    gateway_id = %gateway_id,
                    "Successfully got gateway client"
                );
                c
            }
            Err(e) => {
                warn!(
                    error = %e,
                    gateway_id = %gateway_id,
                    "Failed to get gateway client"
                );
                return Err(e);
            }
        };

        // 调用Access Gateway推送接口（添加超时保护）
        debug!(
            gateway_id = %gateway_id,
            user_count = request.user_ids.len(),
            user_ids = ?request.user_ids,
            "Calling Access Gateway push_message"
        );

        // 优化超时时间：单聊消息推送应该很快，设置3秒超时
        let timeout_duration = Duration::from_secs(3); // 3秒超时
        let response = match tokio::time::timeout(
            timeout_duration,
            client.push_message(tonic::Request::new(request.clone())),
        )
        .await
        {
            Ok(Ok(resp)) => {
                let response = resp.into_inner();

                let (pushed_device_count, offline_users) =
                    push_response_delivery_summary(&response);

                if !offline_users.is_empty() {
                    warn!(
                        gateway_id = %gateway_id,
                        pushed_device_count,
                        offline_user_count = offline_users.len(),
                        offline_users = ?offline_users,
                        "Some users have no online delivery (offline pending)"
                    );
                    if pushed_device_count == 0 {
                        return Err(users_offline_error(offline_users));
                    }
                }

                info!(
                    gateway_id = %gateway_id,
                    user_count = response.user_results.len(),
                    "Successfully pushed message to Access Gateway"
                );
                response
            }
            Ok(Err(e)) => {
                warn!(
                    error = %e,
                    gateway_id = %gateway_id,
                    "Failed to call Access Gateway push_message"
                );
                return Err(gateway_call_failure(
                    format!("Failed to call access gateway: {}", e),
                    &e,
                ));
            }
            Err(_) => {
                warn!(
                    gateway_id = %gateway_id,
                    timeout_secs = timeout_duration.as_secs(),
                    "Timeout calling Access Gateway push_message"
                );
                return Err(gateway_unavailable(format!(
                    "Timeout calling access gateway push_message (timeout: {}s)",
                    timeout_duration.as_secs()
                )));
            }
        };

        Ok(response)
    }

    async fn route_push_event(
        &self,
        gateway_id: &str,
        request: PushEventRequest,
    ) -> Result<PushResponse> {
        let mut client = self.get_or_create_client(gateway_id).await?;
        let timeout_duration = Duration::from_secs(3);
        match tokio::time::timeout(
            timeout_duration,
            client.push_event(tonic::Request::new(request)),
        )
        .await
        {
            Ok(Ok(resp)) => {
                let response = resp.into_inner();
                let (pushed_device_count, offline_users) =
                    push_response_delivery_summary(&response);

                if !offline_users.is_empty() {
                    warn!(
                        gateway_id = %gateway_id,
                        pushed_device_count,
                        offline_user_count = offline_users.len(),
                        offline_users = ?offline_users,
                        "Some users have no online event delivery (offline pending)"
                    );
                    if pushed_device_count == 0 {
                        return Err(users_offline_error(offline_users));
                    }
                }

                Ok(response)
            }
            Ok(Err(e)) => Err(gateway_call_failure(format!("push_event: {}", e), &e)),
            Err(_) => Err(gateway_unavailable(format!(
                "Timeout access gateway push_event ({}s)",
                timeout_duration.as_secs()
            ))),
        }
    }

    async fn route_push_notification(
        &self,
        gateway_id: &str,
        request: PushNotificationRequest,
    ) -> Result<PushNotificationResponse> {
        let mut client = self.get_or_create_client(gateway_id).await?;
        let timeout_duration = Duration::from_secs(3);
        match tokio::time::timeout(
            timeout_duration,
            client.push_notification(tonic::Request::new(request)),
        )
        .await
        {
            Ok(Ok(resp)) => Ok(resp.into_inner()),
            Ok(Err(e)) => Err(gateway_call_failure(
                format!("push_notification: {}", e),
                &e,
            )),
            Err(_) => Err(gateway_unavailable(format!(
                "Timeout access gateway push_notification ({}s)",
                timeout_duration.as_secs()
            ))),
        }
    }

    async fn route_push_ack(
        &self,
        gateway_id: &str,
        request: PushAckRequest,
    ) -> Result<PushAckResponse> {
        let mut client = self.get_or_create_client(gateway_id).await?;
        let timeout_duration = Duration::from_secs(3);
        match tokio::time::timeout(
            timeout_duration,
            client.push_ack(tonic::Request::new(request)),
        )
        .await
        {
            Ok(Ok(resp)) => Ok(resp.into_inner()),
            Ok(Err(e)) => Err(gateway_call_failure(format!("push_ack: {}", e), &e)),
            Err(_) => Err(gateway_unavailable(format!(
                "Timeout access gateway push_ack ({}s)",
                timeout_duration.as_secs()
            ))),
        }
    }

    async fn route_push_custom(
        &self,
        gateway_id: &str,
        request: PushCustomRequest,
    ) -> Result<PushResponse> {
        let mut client = self.get_or_create_client(gateway_id).await?;
        let timeout_duration = Duration::from_secs(3);
        match tokio::time::timeout(
            timeout_duration,
            client.push_custom(tonic::Request::new(request)),
        )
        .await
        {
            Ok(Ok(resp)) => Ok(resp.into_inner()),
            Ok(Err(e)) => Err(gateway_call_failure(format!("push_custom: {}", e), &e)),
            Err(_) => Err(gateway_unavailable(format!(
                "Timeout access gateway push_custom ({}s)",
                timeout_duration.as_secs()
            ))),
        }
    }
}

fn push_response_delivery_summary(response: &PushResponse) -> (usize, Vec<String>) {
    let mut pushed_device_count = 0usize;
    let mut offline_users = Vec::new();
    for user_result in &response.user_results {
        if let Some(push_result) = user_result.result.as_ref() {
            pushed_device_count =
                pushed_device_count.saturating_add(push_result.pushed_device_count as usize);
            if push_result.pushed_device_count == 0 && push_result.offline_pending_count > 0 {
                offline_users.push(user_result.user_id.clone());
            }
        }
    }
    (pushed_device_count, offline_users)
}

fn users_offline_error(user_ids: Vec<String>) -> FlareError {
    ErrorBuilder::new(
        ErrorCode::UserOffline,
        "users have no online delivery and require online state refresh",
    )
    .details(format!("offline_users={user_ids:?}"))
    .build_error()
}

#[cfg(test)]
mod tests {
    #[test]
    fn conversation_delivery_rejects_unsigned_tenant_metadata() {
        use super::*;
        use flare_server_core::context::Context;
        for tenant in ["tenant-a", "tenant-b"] {
            let ctx = Arc::new(Context::with_request_id("delivery-request").with_tenant_id(tenant));
            assert!(
                push_producer_request(
                    &ctx,
                    DeliverToConversationRequest {
                        conversation_id: "same-conversation".into(),
                        ..Default::default()
                    },
                )
                .is_err(),
                "raw tenant metadata must not become a gateway push identity"
            );
        }
        assert!(
            push_producer_request(
                &Arc::new(Context::root()),
                DeliverToConversationRequest::default()
            )
            .is_err()
        );
        assert!(
            push_producer_request(
                &Arc::new(Context::root().with_tenant_id("")),
                PushCapabilityRequest::default()
            )
            .is_err()
        );
    }

    /// 「网关实例未找到」必须是**可重试**错误。
    ///
    /// 滚动重启时 push-worker 可能比网关早起来几秒，此时网关还没完成 consul
    /// 注册。若这个错误被判成不可重试，push-worker 会把消息直接丢进 DLQ——
    /// 而根因只是"网关还没起来"，等几秒就好。实测 r16 部署时因此丢了 2 条消息。
    ///
    /// ⚠️ FlareError 只有 Localized 变体带 ErrorCode，其余 code() 返回 None、
    /// is_retryable() 恒 false。所以构造时必须用 localized 而不是 system。
    #[test]
    fn gateway_not_found_is_retryable() {
        use flare_server_core::error::{ErrorCode, FlareError};

        let err = FlareError::localized(
            ErrorCode::ServiceUnavailable,
            "Gateway instance not found: gateway_id=x",
        );
        assert!(
            err.is_retryable(),
            "网关未就绪被判成不可重试，消息会被直接丢进 DLQ"
        );

        let legacy = FlareError::system("Gateway instance not found: gateway_id=x");
        assert!(
            !legacy.is_retryable(),
            "system 变体不带 ErrorCode，预期不可重试（说明为何必须改用 localized）"
        );
    }
}

/// `request` carrying the verified push producer token of `ctx` (`flare-push-worker`, the
/// context's own tenant): the gateway admits Push RPCs only under that identity.
fn push_producer_request<T>(
    ctx: &flare_server_core::context::Ctx,
    request: T,
) -> Result<tonic::Request<T>> {
    use flare_server_core::auth::SignedActorType;
    use flare_server_core::grpc::middleware::signed_context::VerifiedContext;

    let verified = ctx.get_data::<VerifiedContext>().ok_or_else(|| {
        ErrorBuilder::new(
            ErrorCode::PermissionDenied,
            "verified push producer context required",
        )
        .build_error()
    })?;
    let claims = verified.claims().ok_or_else(|| {
        ErrorBuilder::new(
            ErrorCode::PermissionDenied,
            "signed push producer context required",
        )
        .build_error()
    })?;
    if claims.actor_type != SignedActorType::Service
        || claims.actor_id != "flare-push-worker"
        || ctx.tenant_id() != Some(claims.tenant_id.as_str())
        || !flare_im_contracts::domain::tenant::is_valid_tenant_id(&claims.tenant_id)
        || flare_im_contracts::utils::normalize_tenant_id(&claims.tenant_id) != claims.tenant_id
    {
        return Err(ErrorBuilder::new(
            ErrorCode::PermissionDenied,
            "push producer identity mismatch",
        )
        .build_error());
    }
    let token = verified.token().ok_or_else(|| {
        ErrorBuilder::new(
            ErrorCode::PermissionDenied,
            "signed push producer token required",
        )
        .build_error()
    })?;
    let mut outbound = tonic::Request::new(request);
    flare_server_core::set_signed_context_token(&mut outbound, token).map_err(|_| {
        ErrorBuilder::new(ErrorCode::PermissionDenied, "invalid push producer token").build_error()
    })?;
    Ok(outbound)
}

/// 广播目标：服务发现给出的实例去掉自己。
///
/// 发起节点已经转给过自己的本地订阅者，再收一遍就是**重复投递**——同一条「正在输入」在同一台设备上
/// 出现两次。单独提出来是为了让这一条能被断言：循环体是管道，这里才是判断。
fn peer_gateway_ids(
    instances: impl Iterator<Item = String>,
    skip_gateway_id: Option<&str>,
) -> Vec<String> {
    instances
        .filter(|id| !id.is_empty() && Some(id.as_str()) != skip_gateway_id)
        .collect()
}

#[cfg(test)]
mod realtime_broadcast_tests {
    use super::peer_gateway_ids;

    #[test]
    fn the_originating_node_is_not_one_of_its_own_peers() {
        let instances = ["gw-a", "gw-b", "gw-c"].map(String::from);
        assert_eq!(
            peer_gateway_ids(instances.into_iter(), Some("gw-b")),
            vec!["gw-a".to_string(), "gw-c".to_string()],
            "broadcasting to itself would deliver the same signal twice on its own connections"
        );
    }

    #[test]
    fn without_a_local_id_every_instance_is_a_peer() {
        let instances = ["gw-a", "gw-b"].map(String::from);
        assert_eq!(
            peer_gateway_ids(instances.into_iter(), None),
            vec!["gw-a".to_string(), "gw-b".to_string()]
        );
    }

    #[test]
    fn a_single_instance_deployment_has_no_peers() {
        let instances = ["gw-a".to_string()];
        assert!(peer_gateway_ids(instances.into_iter(), Some("gw-a")).is_empty());
    }

    #[test]
    fn an_instance_without_an_id_is_not_a_target() {
        let instances = ["", "gw-b"].map(String::from);
        assert_eq!(
            peer_gateway_ids(instances.into_iter(), Some("gw-a")),
            vec!["gw-b".to_string()]
        );
    }
}

#[cfg(test)]
mod delivery_retry_tests {
    //! Access Gateway 停机、重启窗口里的推送失败必须可重试（W6-S2 F4），否则 push-worker
    //! 把它送进 DLQ，在线投递就此丢掉。

    use super::*;
    use flare_grpc_proto::access_gateway::access_gateway_server::{
        AccessGateway, AccessGatewayServer,
    };
    use flare_grpc_proto::access_gateway::{GetUserConnectionsRequest, GetUserConnectionsResponse};
    use tokio::sync::oneshot;
    use tonic::{Request, Response, Status};

    struct FakeGateway;

    #[tonic::async_trait]
    impl AccessGateway for FakeGateway {
        async fn push_message(
            &self,
            _request: Request<PushMessageRequest>,
        ) -> std::result::Result<Response<PushResponse>, Status> {
            Err(Status::unimplemented("not used"))
        }
        async fn push_event(
            &self,
            _request: Request<PushEventRequest>,
        ) -> std::result::Result<Response<PushResponse>, Status> {
            Err(Status::unimplemented("not used"))
        }
        async fn push_notification(
            &self,
            _request: Request<PushNotificationRequest>,
        ) -> std::result::Result<Response<PushNotificationResponse>, Status> {
            Ok(Response::new(PushNotificationResponse::default()))
        }
        async fn push_ack(
            &self,
            _request: Request<PushAckRequest>,
        ) -> std::result::Result<Response<PushAckResponse>, Status> {
            Err(Status::invalid_argument("rejected by the gateway"))
        }
        async fn push_custom(
            &self,
            _request: Request<PushCustomRequest>,
        ) -> std::result::Result<Response<PushResponse>, Status> {
            Err(Status::unimplemented("not used"))
        }
        async fn push_capability(
            &self,
            _request: Request<PushCapabilityRequest>,
        ) -> std::result::Result<Response<PushResponse>, Status> {
            Err(Status::unimplemented("not used"))
        }
        async fn deliver_to_conversation(
            &self,
            _request: Request<DeliverToConversationRequest>,
        ) -> std::result::Result<Response<PushResponse>, Status> {
            Ok(Response::new(PushResponse::default()))
        }
        async fn get_user_connections(
            &self,
            _request: Request<GetUserConnectionsRequest>,
        ) -> std::result::Result<Response<GetUserConnectionsResponse>, Status> {
            Err(Status::unimplemented("not used"))
        }
        async fn relay_realtime_control(
            &self,
            _request: Request<RelayRealtimeControlRequest>,
        ) -> std::result::Result<Response<PushResponse>, Status> {
            Err(Status::unimplemented("not used"))
        }
    }

    async fn start_gateway() -> (String, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, stop_rx) = oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(AccessGatewayServer::new(FakeGateway))
                .serve_with_incoming_shutdown(
                    tonic::transport::server::TcpIncoming::from(listener),
                    async {
                        let _ = stop_rx.await;
                    },
                )
                .await
                .unwrap();
        });
        (format!("http://{addr}"), stop, task)
    }

    fn static_router(endpoint: String) -> Arc<GatewayRouter> {
        GatewayRouter::new(GatewayRouterConfig {
            connection_timeout_ms: 1_000,
            static_fallback_endpoint: Some(endpoint),
            ..Default::default()
        })
    }

    async fn closed_endpoint() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        format!("http://{}", listener.local_addr().unwrap())
    }

    #[tokio::test]
    async fn conversation_delivery_to_a_stopped_gateway_is_retryable() {
        let (endpoint, stop, task) = start_gateway().await;
        let router = static_router(endpoint);
        router
            .deliver_via_static_fallback(Request::new(DeliverToConversationRequest::default()))
            .await
            .expect("gateway is up");

        // 网关停机：池里的通道还在，这次调用在传输层失败。
        let _ = stop.send(());
        let _ = task.await;
        let error = router
            .deliver_via_static_fallback(Request::new(DeliverToConversationRequest::default()))
            .await
            .expect_err("gateway is down");
        println!("stopped gateway: {error}");
        assert!(error.is_retryable(), "{error}");
        assert!(error.to_string().contains("deliver_to_conversation failed"));
    }

    #[tokio::test]
    async fn a_gateway_that_never_answers_the_connect_is_retryable() {
        let router = static_router(closed_endpoint().await);
        let error = router
            .deliver_via_static_fallback(Request::new(DeliverToConversationRequest::default()))
            .await
            .expect_err("nothing listens");
        println!("closed port: {error}");
        assert!(error.is_retryable(), "{error}");

        let error = router
            .route_push_notification("gw-1", PushNotificationRequest::default())
            .await
            .expect_err("nothing listens");
        assert!(error.is_retryable(), "{error}");
    }

    #[tokio::test]
    async fn a_gateway_rejection_stays_non_retryable() {
        let (endpoint, stop, task) = start_gateway().await;
        let router = static_router(endpoint);
        router
            .route_push_notification("gw-1", PushNotificationRequest::default())
            .await
            .expect("gateway is up");
        let error = router
            .route_push_ack("gw-1", PushAckRequest::default())
            .await
            .expect_err("the fake gateway rejects acks");
        assert!(!error.is_retryable(), "{error}");
        let _ = stop.send(());
        let _ = task.await;
    }

    #[test]
    fn transient_gateway_statuses() {
        for status in [
            Status::unavailable("shutting down"),
            Status::cancelled("connection reset"),
            Status::deadline_exceeded("slow"),
            Status::resource_exhausted("busy"),
            Status::aborted("retry"),
        ] {
            assert!(is_transient_gateway_status(&status), "{status:?}");
        }
        for status in [
            Status::invalid_argument("bad"),
            Status::permission_denied("no"),
            Status::unauthenticated("no"),
            Status::internal("bug"),
        ] {
            assert!(!is_transient_gateway_status(&status), "{status:?}");
        }
    }
}
