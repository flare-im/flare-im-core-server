//! Gateway Router（跨地区路由组件）
//!
//! 根据gateway_id路由到对应的Access Gateway，支持单地区/多地区自适应部署。
//!
//! 位于 `flare-im-core/src/gateway/router.rs`（IM 核心共享库），
//! 被 Push Server、Push Worker 和 Core Gateway 复用。
//!
//! ## 使用流程
//!
//! 1. **查询在线状态**：调用方（Push Server/Core Gateway）先查询 Signaling Online 服务获取用户的 `gateway_id`
//! 2. **路由推送**：调用 Gateway Router 的 `route_push_message` 方法，传入 `gateway_id` 和推送请求
//! 3. **服务发现**：Gateway Router 通过服务发现获取对应 `gateway_id` 的 Access Gateway 地址
//! 4. **推送消息**：Gateway Router 调用 Access Gateway 的 `PushMessage` 接口推送消息

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context as AnyhowContext, Result};

use flare_proto::access_gateway::{
    PushAckRequest, PushAckResponse, PushCustomRequest, PushEventRequest, PushMessageRequest,
    PushNotificationRequest, PushNotificationResponse, PushResponse,
    access_gateway_client::AccessGatewayClient,
};
use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, info, warn};

use flare_server_core::discovery::{ServiceClient, discover::ServiceDiscover};

/// Gateway Router 错误类型
#[derive(Debug, thiserror::Error)]
pub enum GatewayRouterError {
    /// 用户离线错误（需要重新查询在线状态）
    #[error("Users offline: {0:?}")]
    UsersOffline(Vec<String>),
    /// 其他错误
    #[error(transparent)]
    Other(#[from] anyhow::Error),
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
                    let endpoint = Endpoint::from_shared(uri).with_context(|| {
                        format!(
                            "Invalid URI for gateway {}: {}",
                            gateway_id, instance.address
                        )
                    })?;

                    let timeout_duration = Duration::from_millis(self.config.connection_timeout_ms);
                    tokio::time::timeout(timeout_duration, endpoint.connect())
                        .await
                        .map_err(|_| {
                            anyhow::anyhow!(
                                "Timeout connecting to gateway {} at {} (timeout: {}ms)",
                                gateway_id,
                                instance.address,
                                timeout_duration.as_millis()
                            )
                        })?
                        .map_err(|e| {
                            anyhow::anyhow!(
                                "Failed to connect to gateway {} at {}: {}",
                                gateway_id,
                                instance.address,
                                e
                            )
                        })?
                }
                None => {
                    return Err(anyhow::anyhow!(
                        "Gateway instance not found: gateway_id={}. Available instances: {}",
                        gateway_id,
                        instances
                            .iter()
                            .map(|i| i.instance_id.clone())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
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
                    anyhow::anyhow!(
                        "Timeout waiting for service discovery to get channel for gateway {} (timeout: {}ms)",
                        gateway_id,
                        timeout_duration.as_millis()
                    )
                })?
                .map_err(|e| {
                    anyhow::anyhow!("Failed to get channel from service discovery: {}", e)
                })?
        } else if let Some(ref uri) = self.config.static_fallback_endpoint {
            let endpoint = Endpoint::from_shared(uri.clone()).with_context(|| {
                format!(
                    "Invalid static_fallback_endpoint for Access Gateway: {}",
                    uri
                )
            })?;
            let timeout_duration = Duration::from_millis(self.config.connection_timeout_ms);
            tokio::time::timeout(timeout_duration, endpoint.connect())
                .await
                .map_err(|_| {
                    anyhow::anyhow!(
                        "Timeout connecting to static Access Gateway {} (timeout: {}ms)",
                        uri,
                        timeout_duration.as_millis()
                    )
                })?
                .map_err(|e| {
                    anyhow::anyhow!("Failed to connect to static Access Gateway {}: {}", uri, e)
                })?
        } else {
            return Err(anyhow::anyhow!(
                "Neither ServiceDiscover nor ServiceClient is available, and static_fallback_endpoint is unset. Inject discovery via with_service_client() / with_service_client_and_discover(), or set GatewayRouterConfig.static_fallback_endpoint (e.g. ACCESS_GATEWAY_GRPC_ENDPOINT in dev)."
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

                // 按用户明细推断「全未触达在线设备且存在离线队列」视为需重查在线态
                let mut offline_users = Vec::new();
                for ur in &response.user_results {
                    if let Some(pr) = ur.result.as_ref() {
                        if pr.pushed_device_count == 0 && pr.offline_pending_count > 0 {
                            offline_users.push(ur.user_id.clone());
                        }
                    }
                }

                if !offline_users.is_empty() {
                    warn!(
                        gateway_id = %gateway_id,
                        offline_user_count = offline_users.len(),
                        offline_users = ?offline_users,
                        "Some users have no online delivery (offline pending), caller may re-query online"
                    );
                    return Err(GatewayRouterError::UsersOffline(offline_users).into());
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
                return Err(anyhow::anyhow!("Failed to call access gateway: {}", e));
            }
            Err(_) => {
                warn!(
                    gateway_id = %gateway_id,
                    timeout_secs = timeout_duration.as_secs(),
                    "Timeout calling Access Gateway push_message"
                );
                return Err(anyhow::anyhow!(
                    "Timeout calling access gateway push_message (timeout: {}s)",
                    timeout_duration.as_secs()
                ));
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
            Ok(Ok(resp)) => Ok(resp.into_inner()),
            Ok(Err(e)) => Err(anyhow::anyhow!("push_event: {}", e)),
            Err(_) => Err(anyhow::anyhow!(
                "Timeout access gateway push_event ({}s)",
                timeout_duration.as_secs()
            )),
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
            Ok(Err(e)) => Err(anyhow::anyhow!("push_notification: {}", e)),
            Err(_) => Err(anyhow::anyhow!(
                "Timeout access gateway push_notification ({}s)",
                timeout_duration.as_secs()
            )),
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
            Ok(Err(e)) => Err(anyhow::anyhow!("push_ack: {}", e)),
            Err(_) => Err(anyhow::anyhow!(
                "Timeout access gateway push_ack ({}s)",
                timeout_duration.as_secs()
            )),
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
            Ok(Err(e)) => Err(anyhow::anyhow!("push_custom: {}", e)),
            Err(_) => Err(anyhow::anyhow!(
                "Timeout access gateway push_custom ({}s)",
                timeout_duration.as_secs()
            )),
        }
    }
}
