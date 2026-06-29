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
    DeliverToConversationRequest, PushAckRequest, PushAckResponse, PushCustomRequest,
    PushEventRequest, PushMessageRequest, PushNotificationRequest, PushNotificationResponse,
    PushResponse,
};
use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, info, warn};

use crate::{ServiceClient, ServiceDiscover};

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
        let endpoint = Endpoint::from_shared(uri.to_string())
            .with_context(|| format!("Invalid URI for {}: {}", target, uri))?;
        let timeout_duration = Duration::from_millis(self.config.connection_timeout_ms);

        tokio::time::timeout(timeout_duration, endpoint.connect())
            .await
            .map_err(|_| {
                flare_server_core::error::FlareError::system(format!(
                    "Timeout connecting to {} at {} (timeout: {}ms)",
                    target,
                    uri,
                    timeout_duration.as_millis()
                ))
            })?
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!(
                    "Failed to connect to {} at {}: {}",
                    target, uri, e
                ))
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
                        return Err(flare_server_core::error::FlareError::system(format!(
                            "Gateway instance not found: gateway_id={}. Available instances: {}",
                            gateway_id,
                            instances
                                .iter()
                                .map(|i| i.instance_id.clone())
                                .collect::<Vec<_>>()
                                .join(", ")
                        )));
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
                    flare_server_core::error::FlareError::system(format!(
                        "Timeout waiting for service discovery to get channel for gateway {} (timeout: {}ms)",
                        gateway_id,
                        timeout_duration.as_millis()
                    ))
                })?
                .map_err(|e| {
                    flare_server_core::error::FlareError::system(format!(
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

    /// 统一读扩散：把会话消息**广播到所有网关节点**，各节点按本地会话订阅过滤投递。
    /// 上游（push-worker）无需解析收件人。best-effort：单节点失败不影响其他节点（在线投递为软实时，
    /// 离线成员靠版本号增量拉兜底）。无服务发现实例时走静态 fallback 单端点。
    pub async fn broadcast_deliver_to_conversation(
        &self,
        request: DeliverToConversationRequest,
    ) -> Result<()> {
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
            // 无服务发现实例：单端点（get_or_create_client 内部回退 static_fallback_endpoint）。
            let mut client = self.get_or_create_client("__static_fallback__").await?;
            client
                .deliver_to_conversation(tonic::Request::new(request))
                .await
                .map_err(|e| {
                    flare_server_core::error::FlareError::system(format!(
                        "deliver_to_conversation failed: {e}"
                    ))
                })?;
            return Ok(());
        }

        for gateway_id in gateway_ids {
            match self.get_or_create_client(&gateway_id).await {
                Ok(mut client) => {
                    if let Err(e) = client
                        .deliver_to_conversation(tonic::Request::new(request.clone()))
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
                return Err(flare_server_core::error::FlareError::system(format!(
                    "Failed to call access gateway: {}",
                    e
                )));
            }
            Err(_) => {
                warn!(
                    gateway_id = %gateway_id,
                    timeout_secs = timeout_duration.as_secs(),
                    "Timeout calling Access Gateway push_message"
                );
                return Err(flare_server_core::error::FlareError::system(format!(
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
            Ok(Err(e)) => Err(flare_server_core::error::FlareError::system(format!(
                "push_event: {}",
                e
            ))),
            Err(_) => Err(flare_server_core::error::FlareError::system(format!(
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
            Ok(Err(e)) => Err(flare_server_core::error::FlareError::system(format!(
                "push_notification: {}",
                e
            ))),
            Err(_) => Err(flare_server_core::error::FlareError::system(format!(
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
            Ok(Err(e)) => Err(flare_server_core::error::FlareError::system(format!(
                "push_ack: {}",
                e
            ))),
            Err(_) => Err(flare_server_core::error::FlareError::system(format!(
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
            Ok(Err(e)) => Err(flare_server_core::error::FlareError::system(format!(
                "push_custom: {}",
                e
            ))),
            Err(_) => Err(flare_server_core::error::FlareError::system(format!(
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
