//! 消息转发服务
//!
//! 负责将消息转发到对应的业务系统

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use flare_grpc_proto::message::message_event_service_client::MessageEventServiceClient;
use flare_grpc_proto::message::message_send_service_client::MessageSendServiceClient;
use flare_grpc_proto::message::{ExecuteEventRequest, SendMessageRequest, SendMessageResponse};
use flare_proto::common::{CustomData, Event as ProtoEvent, Message, MessageSource};
use prost::Message as ProstMessage;
use tokio::sync::RwLock;
use tonic::transport::Channel;

use flare_server_core::context::Context as ServerContext;
use flare_server_core::error::{ErrorBuilder, ErrorCode, map_infra_error};
use tracing::{debug, info};

use crate::domain::repository::{DefaultRouteRepository, RouteRepository};
use flare_im_contracts::service_names::{MESSAGE_INGEST, MESSAGE_ORCHESTRATOR};
use flare_im_contracts::utils::normalize_tenant_id;
use flare_im_service_kit::ServiceClient;
use flare_im_service_kit::config::ServicesConfig;
use flare_im_service_kit::discovery::{
    invalidate_static_channel_on_transport_failure, is_transport_failure, resolve_grpc_channel,
};
use flare_server_core::error::Result;
use flare_server_core::{SignedActorType, VerifiedContext, set_signed_context_token};

/// SVID 常量定义
pub mod svid {
    /// IM 业务系统 SVID（默认）
    pub const IM: &str = "svid.im";
}

/// Resolve only the two built-in IM routes in a single Core cell without a registry.
/// The target host is never read from a route or request, and an absent configured
/// gRPC port is not replaced with a process-wide or service default port.
fn static_im_endpoint(services: &ServicesConfig, service_name: &str) -> Option<String> {
    let server = match service_name {
        MESSAGE_INGEST => services.message_ingest.as_ref()?.runtime.server.as_ref()?,
        MESSAGE_ORCHESTRATOR => services
            .message_orchestrator
            .as_ref()?
            .runtime
            .server
            .as_ref()?,
        _ => return None,
    };
    let port = server.port.filter(|port| *port != 0)?;
    Some(format!("http://127.0.0.1:{port}"))
}

/// 同一业务端点上的消息与事件 gRPC 客户端。
#[derive(Clone)]
struct BusinessGrpcClients {
    send: MessageSendServiceClient<Channel>,
    event: MessageEventServiceClient<Channel>,
    /// 走静态地址时的地址（service-kit 静态通道缓存的键）；服务发现得到的通道为 `None`。
    static_authority: Option<String>,
}

impl BusinessGrpcClients {
    fn from_channel(channel: Channel) -> Self {
        Self {
            send: MessageSendServiceClient::new(channel.clone()),
            event: MessageEventServiceClient::new(channel),
            static_authority: None,
        }
    }

    /// 静态地址的客户端：通道取自 service-kit 的静态通道（建连上限 2 s、HTTP/2 探活，见
    /// [`flare_im_service_kit::discovery::StaticChannelPolicy`]），构造时不连下游。
    async fn from_static_authority(authority: &str) -> Result<Self> {
        let channel = resolve_grpc_channel(authority).await.map_err(|e| {
            map_infra_error(
                std::io::Error::new(std::io::ErrorKind::InvalidInput, e),
                ErrorCode::NetworkError,
                "Invalid static business endpoint",
            )
        })?;
        let mut clients = Self::from_channel(channel);
        clients.static_authority = Some(authority.to_string());
        Ok(clients)
    }
}

/// 下游回的失败按什么级别记日志。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SendFailureLogLevel {
    /// 可归因于调用方的业务结果（策略拒绝、参数不符、对象不存在），不是故障。
    Info,
    /// 可能是调用方造成，也可能掩盖本侧故障：`UNAUTHENTICATED`、`PERMISSION_DENIED` 可能是
    /// 内部签名上下文或租户配置出错，限流、重复与越界需要留意。
    Warn,
    /// 下游或传输故障。
    Error,
}

/// 只把 `FAILED_PRECONDITION`、`INVALID_ARGUMENT`、`NOT_FOUND` 降为 INFO；签名、权限类
/// 状态码记 WARN，免得内部签名上下文或租户配置故障被当成业务拒绝埋掉。
fn send_failure_log_level(code: tonic::Code) -> SendFailureLogLevel {
    match code {
        tonic::Code::FailedPrecondition | tonic::Code::InvalidArgument | tonic::Code::NotFound => {
            SendFailureLogLevel::Info
        }
        tonic::Code::Unauthenticated
        | tonic::Code::PermissionDenied
        | tonic::Code::AlreadyExists
        | tonic::Code::ResourceExhausted
        | tonic::Code::OutOfRange => SendFailureLogLevel::Warn,
        _ => SendFailureLogLevel::Error,
    }
}

/// 缓存的客户端条目
struct CachedClient {
    /// 客户端组
    clients: BusinessGrpcClients,
    /// 最后使用时间
    last_used: Instant,
}

impl CachedClient {
    fn new(clients: BusinessGrpcClients) -> Self {
        Self {
            clients,
            last_used: Instant::now(),
        }
    }

    fn update_last_used(&mut self) {
        self.last_used = Instant::now();
    }

    /// 检查客户端是否应该被清理（超过最大空闲时间）
    fn should_cleanup(&self, max_idle: Duration) -> bool {
        self.last_used.elapsed() > max_idle
    }
}

/// 发现失败缓存（用于短时熔断，避免 Consul 被失败重试风暴打爆）
struct CachedDiscoveryFailure {
    /// 失败信息（用于日志）
    reason: String,
    /// 冷却截止时间
    retry_after: Instant,
}

/// 消息转发服务
pub struct MessageForwarder {
    /// 业务系统客户端缓存（key: "{svid}:{endpoint}"，value: CachedClient）
    /// 使用 RwLock 提高并发读性能（读多写少场景）
    business_clients: Arc<RwLock<HashMap<String, CachedClient>>>,
    /// 默认租户ID
    default_tenant_id: String,
    /// 最大缓存客户端数量
    max_cache_size: usize,
    /// 客户端最大空闲时间（超过此时间未使用会被清理）
    max_idle_duration: Duration,
    /// 发现失败短时缓存（key: "{svid}:{endpoint}"）
    discovery_failures: Arc<RwLock<HashMap<String, CachedDiscoveryFailure>>>,
    /// 发现失败后的冷却时长（避免连续失败时创建大量 discover 任务）
    discovery_failure_cooldown: Duration,
}

impl MessageForwarder {
    /// 创建新的消息转发服务
    pub fn new(default_tenant_id: String) -> Self {
        Self {
            business_clients: Arc::new(RwLock::new(HashMap::new())),
            default_tenant_id,
            max_cache_size: 100,                         // 最多缓存 100 个客户端
            max_idle_duration: Duration::from_secs(300), // 5 分钟空闲后清理
            discovery_failures: Arc::new(RwLock::new(HashMap::new())),
            discovery_failure_cooldown: Duration::from_secs(3),
        }
    }

    /// 生成缓存键
    fn cache_key(svid: &str, endpoint: &str) -> String {
        format!("{}:{}", svid, endpoint)
    }

    /// 清理过期和空闲的客户端
    async fn cleanup_expired_clients(&self) {
        let mut clients = self.business_clients.write().await;
        let initial_size = clients.len();

        clients.retain(|_key, cached_client| !cached_client.should_cleanup(self.max_idle_duration));

        let removed = initial_size - clients.len();
        if removed > 0 {
            debug!(
                removed_count = removed,
                remaining_count = clients.len(),
                "Cleaned up expired clients from cache"
            );
        }
    }

    /// 检查发现失败冷却是否生效
    async fn check_discovery_cooldown(&self, cache_key: &str) -> Option<(Duration, String)> {
        let failures = self.discovery_failures.read().await;
        let failure = failures.get(cache_key)?;
        if failure.retry_after > Instant::now() {
            Some((
                failure
                    .retry_after
                    .saturating_duration_since(Instant::now()),
                failure.reason.clone(),
            ))
        } else {
            None
        }
    }

    /// 记录发现失败（触发短时冷却）
    async fn mark_discovery_failure(&self, cache_key: &str, reason: String) {
        let mut failures = self.discovery_failures.write().await;
        failures.insert(
            cache_key.to_string(),
            CachedDiscoveryFailure {
                reason,
                retry_after: Instant::now() + self.discovery_failure_cooldown,
            },
        );
    }

    /// 清理发现失败状态
    async fn clear_discovery_failure(&self, cache_key: &str) {
        let mut failures = self.discovery_failures.write().await;
        failures.remove(cache_key);
    }

    /// 从缓存获取客户端，如果不存在或已失效则创建新的
    async fn get_or_create_cached_client(
        &self,
        svid: &str,
        endpoint: &str,
    ) -> Result<BusinessGrpcClients> {
        let cache_key = Self::cache_key(svid, endpoint);

        // 快速路径：尝试从缓存读取（需要写锁以更新 last_used）
        {
            let mut clients = self.business_clients.write().await;
            if let Some(cached) = clients.get_mut(&cache_key) {
                // 检查是否需要清理
                if !cached.should_cleanup(self.max_idle_duration) {
                    // 更新最后使用时间
                    cached.update_last_used();
                    // 克隆客户端（tonic 的客户端是轻量级的，内部使用 Arc）
                    return Ok(cached.clients.clone());
                }
            }
        }

        // 熔断：最近刚失败过时快速失败，避免持续新建 discover 导致服务发现雪崩
        if let Some((remaining, reason)) = self.check_discovery_cooldown(&cache_key).await {
            return Err(map_infra_error(
                std::io::Error::other(format!(
                    "Service discovery is cooling down for key={cache_key}, remaining={}ms, last_error={reason}",
                    remaining.as_millis()
                )),
                ErrorCode::ServiceUnavailable,
                format!(
                    "Service discovery temporarily unavailable for {} (cooldown={}ms)",
                    endpoint,
                    remaining.as_millis()
                ),
            ));
        }

        // 慢速路径：需要创建新客户端（写锁）
        let new_clients = match self.create_business_clients(endpoint, svid).await {
            Ok(clients) => {
                self.clear_discovery_failure(&cache_key).await;
                clients
            }
            Err(e) => {
                self.mark_discovery_failure(&cache_key, e.to_string()).await;
                return Err(e);
            }
        };

        // 检查缓存大小，必要时清理
        let mut clients = self.business_clients.write().await;

        // 如果缓存已满，清理过期客户端
        if clients.len() >= self.max_cache_size {
            drop(clients); // 释放写锁，允许其他操作
            self.cleanup_expired_clients().await;
            let mut clients = self.business_clients.write().await;

            // 如果清理后还是满的，移除最旧的客户端
            if clients.len() >= self.max_cache_size {
                let oldest_key = clients
                    .iter()
                    .min_by_key(|(_, cached)| cached.last_used)
                    .map(|(key, _)| key.clone());

                if let Some(key) = oldest_key {
                    clients.remove(&key);
                    debug!(removed_key = %key, "Removed oldest client from cache");
                }
            }

            // 插入新客户端
            clients.insert(cache_key, CachedClient::new(new_clients.clone()));
            Ok(new_clients)
        } else {
            // 缓存未满，直接插入
            clients.insert(cache_key, CachedClient::new(new_clients.clone()));
            Ok(new_clients)
        }
    }

    /// 对业务端点的调用失败后调用：失败出在传输层（连不上、连接被断、HTTP/2 ping 超时）时，
    /// 丢弃该端点缓存的客户端，静态地址同时丢弃 service-kit 的静态通道，下一次调用重新解析、
    /// 新建连接；对端回的 gRPC 状态（包括业务拒绝）不动缓存。返回是否丢弃了。
    async fn forget_clients_after_failure(
        &self,
        svid: &str,
        endpoint: &str,
        clients: &BusinessGrpcClients,
        status: &tonic::Status,
    ) -> bool {
        if !is_transport_failure(status) {
            return false;
        }
        self.business_clients
            .write()
            .await
            .remove(&Self::cache_key(svid, endpoint));
        if let Some(authority) = clients.static_authority.as_deref() {
            invalidate_static_channel_on_transport_failure(authority, status);
        }
        debug!(
            svid,
            endpoint,
            code = ?status.code(),
            "business service client dropped after transport failure"
        );
        true
    }

    /// 创建业务系统客户端组（内部方法，不缓存）
    async fn create_business_clients(
        &self,
        endpoint: &str,
        svid: &str,
    ) -> Result<BusinessGrpcClients> {
        // 特殊处理：如果 SVID 是 svid.im，使用调用方指定的内部 IM 服务名
        if svid == svid::IM {
            use flare_im_service_kit::config::app_config;
            use flare_im_service_kit::discovery::create_discover_from_registry_config_with_filters;

            let im_service = endpoint.to_string();
            let app_config = app_config();

            // 使用 svid.im 作为过滤条件
            let mut tag_filters = std::collections::HashMap::new();
            tag_filters.insert("svid".to_string(), svid::IM.to_string());

            debug!(
                service = %im_service,
                svid = svid::IM,
                "Creating service discover for svid.im"
            );

            let discover = if let Some(registry_config) = &app_config.core.registry {
                create_discover_from_registry_config_with_filters(
                    registry_config,
                    &im_service,
                    Some(tag_filters),
                )
                .await
                .map_err(|e| {
                    map_infra_error(
                        e,
                        ErrorCode::NetworkError,
                        format!(
                            "Failed to create service discover for {} with SVID filter (svid={})",
                            im_service,
                            svid::IM
                        ),
                    )
                })?
            } else {
                let static_endpoint = static_im_endpoint(&app_config.services, &im_service)
                    .ok_or_else(|| {
                        map_infra_error(
                            std::io::Error::new(
                                std::io::ErrorKind::NotFound,
                                "Static IM service endpoint unavailable",
                            ),
                            ErrorCode::NetworkError,
                            format!(
                                "Static IM service endpoint unavailable or unsupported for {}",
                                im_service
                            ),
                        )
                    })?;
                return BusinessGrpcClients::from_static_authority(&static_endpoint).await;
            };

            let mut service_client = ServiceClient::new(discover);
            // 添加超时保护，避免服务发现阻塞过长时间
            let channel = tokio::time::timeout(
                std::time::Duration::from_secs(3), // 3秒超时
                service_client.get_channel(),
            )
            .await
            .map_err(|_| map_infra_error(
                std::io::Error::new(std::io::ErrorKind::TimedOut, "Timeout waiting for service discovery"),
                ErrorCode::NetworkError,
                format!("Timeout waiting for service discovery to get channel for {} (svid={}) (3s)", im_service, svid::IM)
            ))?
            .map_err(|e| map_infra_error(e, ErrorCode::NetworkError, format!("Failed to get channel from service discovery for {} (svid={})", im_service, svid::IM)))?;

            return Ok(BusinessGrpcClients::from_channel(channel));
        }

        // 其他业务系统：判断 endpoint 是服务名还是 URL
        let channel = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
            // 直接 URL
            return BusinessGrpcClients::from_static_authority(endpoint).await;
        } else if endpoint.contains(':') && !endpoint.contains('.') {
            // host:port 格式
            return BusinessGrpcClients::from_static_authority(&format!("http://{endpoint}")).await;
        } else {
            // 服务名（通过服务发现，支持 SVID 过滤）
            use flare_im_service_kit::config::app_config;
            use flare_im_service_kit::discovery::create_discover_from_registry_config_with_filters;

            let app_config = app_config();

            // 根据 SVID 过滤服务实例（如果提供了 SVID）
            let tag_filters = if !svid.is_empty() {
                let mut filters = std::collections::HashMap::new();
                filters.insert("svid".to_string(), svid.to_string());
                Some(filters)
            } else {
                None
            };

            let discover = if let Some(registry_config) = &app_config.core.registry {
                if tag_filters.is_some() {
                    info!(
                        service = %endpoint,
                        svid = %svid,
                        "Creating service discover with SVID filter"
                    );
                    create_discover_from_registry_config_with_filters(
                        registry_config,
                        endpoint,
                        tag_filters,
                    )
                    .await
                    .map_err(|e| map_infra_error(e, ErrorCode::NetworkError, format!("Failed to create service discover for {} with SVID filter (svid={})", endpoint, svid)))?
                } else {
                    // 没有 SVID，使用普通的服务发现
                    let discover_result =
                        flare_im_service_kit::discovery::create_discover(endpoint)
                            .await
                            .map_err(|e| {
                                map_infra_error(
                                    e,
                                    ErrorCode::NetworkError,
                                    format!("Failed to create service discover for {}", endpoint),
                                )
                            })?;

                    discover_result.ok_or_else(|| {
                        map_infra_error(
                            std::io::Error::new(
                                std::io::ErrorKind::NotFound,
                                "Service discovery not configured",
                            ),
                            ErrorCode::NetworkError,
                            format!("Service discovery not configured for {}", endpoint),
                        )
                    })?
                }
            } else {
                return Err(map_infra_error(
                    std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "Service discovery not configured",
                    ),
                    ErrorCode::NetworkError,
                    format!("Service discovery not configured for {}", endpoint),
                ));
            };

            let mut service_client = ServiceClient::new(discover);
            // 添加超时保护，避免服务发现阻塞过长时间
            tokio::time::timeout(
                std::time::Duration::from_secs(3), // 3秒超时
                service_client.get_channel(),
            )
            .await
            .map_err(|_| map_infra_error(
                std::io::Error::new(std::io::ErrorKind::TimedOut, "Timeout waiting for service discovery"),
                ErrorCode::NetworkError,
                format!("Timeout waiting for service discovery to get channel for {} (svid={}) (3s)", endpoint, svid)
            ))?
            .map_err(|e| map_infra_error(e, ErrorCode::NetworkError, format!("Failed to get channel from service discovery for {} (svid={})", endpoint, svid)))?
        };

        Ok(BusinessGrpcClients::from_channel(channel))
    }

    /// 根据 endpoint 和 SVID 获取或创建业务系统客户端（带缓存）
    ///
    /// endpoint 可以是：
    /// - 服务名（通过服务发现解析，支持 SVID 过滤）
    /// - gRPC URL（http://host:port 或 https://host:port）
    /// - host:port 格式
    ///
    /// # 参数
    /// * `endpoint` - 服务端点（服务名、URL 或 host:port）
    /// * `svid` - SVID（用于服务发现时的标签过滤）
    async fn get_business_clients(
        &self,
        endpoint: &str,
        svid: &str,
    ) -> Result<BusinessGrpcClients> {
        // 从缓存获取或创建客户端组
        self.get_or_create_cached_client(svid, endpoint).await
    }

    /// 转发消息到业务系统
    ///
    /// 返回 (端点, 响应数据) 元组
    pub async fn forward_message(
        &self,
        ctx: &ServerContext,
        svid: &str,
        message: Message,
        route_repository: Arc<DefaultRouteRepository>,
    ) -> Result<(String, Vec<u8>)> {
        let origin = ctx.get_data::<VerifiedContext>().ok_or_else(|| {
            ErrorBuilder::new(
                ErrorCode::PermissionDenied,
                "verified message origin required",
            )
            .build_error()
        })?;
        let claims = origin.claims().ok_or_else(|| {
            ErrorBuilder::new(
                ErrorCode::PermissionDenied,
                "signed message origin required",
            )
            .build_error()
        })?;
        if claims.actor_type != SignedActorType::User
            || ctx.tenant_id() != Some(claims.tenant_id.as_str())
            || message.sender_id != claims.actor_id
            || message.source != MessageSource::User as i32
        {
            return Err(
                ErrorBuilder::new(ErrorCode::PermissionDenied, "message sender mismatch")
                    .build_error(),
            );
        }
        // 提取或使用默认 SVID
        let resolved_svid = if svid.is_empty() { svid::IM } else { svid };
        debug!(svid = %resolved_svid, "Forwarding message to business system");

        // 对于 svid.im，直接使用服务发现，不需要 RouteRepository
        let endpoint = if resolved_svid == svid::IM {
            use flare_im_contracts::service_names::{MESSAGE_INGEST, get_service_name};
            get_service_name(MESSAGE_INGEST)
        } else {
            // 其他 SVID：从路由仓储解析端点
            use crate::domain::model::Svid;
            let svid_obj = Svid::new(resolved_svid.to_string())
                .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Invalid SVID"))?;

            match route_repository.find_by_svid(svid_obj.as_str()).await {
                Ok(Some(route)) => route.endpoint().as_str().to_string(),
                Ok(None) => {
                    return Err(map_infra_error(
                        std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "Business service not found",
                        ),
                        ErrorCode::DatabaseError,
                        format!("Business service not found for SVID {}", resolved_svid),
                    ));
                }
                Err(e) => {
                    return Err(map_infra_error(
                        e,
                        ErrorCode::DatabaseError,
                        format!("Failed to resolve route for SVID {}", resolved_svid),
                    ));
                }
            }
        };

        // 获取或创建业务系统客户端（带缓存，使用 SVID 过滤）
        let mut business_clients = self.get_business_clients(&endpoint, resolved_svid).await?;

        // 保存消息信息用于错误日志
        let message_id = message.server_id.clone();
        let message_type = message.message_type;
        let conversation_id_for_log = message.conversation_id.clone();

        // 提取 conversation_id（以 message 为准；Context 通过 metadata 传递）
        let conversation_id = if !message.conversation_id.is_empty() {
            message.conversation_id.clone()
        } else {
            String::new()
        };

        // 记录消息信息（用于调试）
        debug!(
            message_id = %message_id,
            message_type = message_type,
            conversation_id = %conversation_id_for_log,
            sender_id = %message.sender_id,
            svid = %resolved_svid,
            endpoint = %endpoint,
            "Forwarding message to business service"
        );

        // 构造转发请求
        let request = SendMessageRequest {
            conversation_id,
            message: Some(message),
            sync: false,
            svid: resolved_svid.to_string(),
        };

        let mut grpc_request = tonic::Request::new(request);
        let token = origin.token().ok_or_else(|| {
            ErrorBuilder::new(
                ErrorCode::PermissionDenied,
                "signed message origin required",
            )
            .build_error()
        })?;
        set_signed_context_token(&mut grpc_request, token).map_err(|_| {
            ErrorBuilder::new(
                ErrorCode::PermissionDenied,
                "invalid signed message context",
            )
            .build_error()
        })?;

        let response = match business_clients.send.send_message(grpc_request).await {
            Ok(resp) => resp,
            Err(e) => {
                match send_failure_log_level(e.code()) {
                    SendFailureLogLevel::Info => info!(
                        message_id = %message_id,
                        message_type = message_type,
                        conversation_id = %conversation_id_for_log,
                        svid = %resolved_svid,
                        endpoint = %endpoint,
                        error_code = ?e.code(),
                        error_message = %e.message(),
                        "Business service rejected the message"
                    ),
                    SendFailureLogLevel::Warn => tracing::warn!(
                        message_id = %message_id,
                        message_type = message_type,
                        conversation_id = %conversation_id_for_log,
                        svid = %resolved_svid,
                        endpoint = %endpoint,
                        error_code = ?e.code(),
                        error_message = %e.message(),
                        "Business service refused the message (check signing context and tenant configuration if unexpected)"
                    ),
                    SendFailureLogLevel::Error => {
                        tracing::error!(
                            message_id = %message_id,
                            message_type = message_type,
                            conversation_id = %conversation_id_for_log,
                            svid = %resolved_svid,
                            endpoint = %endpoint,
                            error = %e,
                            error_code = ?e.code(),
                            error_message = %e.message(),
                            "Failed to send message to business service"
                        );
                        self.forget_clients_after_failure(
                            resolved_svid,
                            &endpoint,
                            &business_clients,
                            &e,
                        )
                        .await;
                    }
                }
                if let Some(detail) = flare_server_core::error::grpc::decode_error_detail(&e) {
                    let code = ErrorCode::from_u32(detail.code.max(0) as u32)
                        .unwrap_or(ErrorCode::GeneralError);
                    return Err(ErrorBuilder::new(code, detail.reason)
                        .details(detail.message)
                        .build_error());
                }
                return Err(map_infra_error(
                    std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        e.message().to_string(),
                    ),
                    ErrorCode::NetworkError,
                    format!(
                        "Failed to send message to business service {} (svid={}): {} (code: {:?})",
                        endpoint,
                        resolved_svid,
                        e.message(),
                        e.code()
                    ),
                ));
            }
        };

        let response_inner = response.into_inner();

        // 序列化响应
        let mut response_bytes = Vec::new();
        SendMessageResponse::encode(&response_inner, &mut response_bytes).map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::NetworkError,
                "Failed to encode SendMessageResponse",
            )
        })?;

        info!(
            svid = %resolved_svid,
            endpoint = %endpoint,
            "Message forwarded to business service"
        );
        Ok((endpoint, response_bytes))
    }

    /// 转发 DATA 通道 `CustomData`（当前无统一编排 RPC 时返回空响应，供网关走「无回包」语义）。
    pub async fn forward_custom_data(
        &self,
        _ctx: &ServerContext,
        _svid: &str,
        data: CustomData,
        _route_repository: Arc<DefaultRouteRepository>,
    ) -> Result<(String, Vec<u8>)> {
        tracing::warn!(
            r#type = %data.r#type,
            "CustomData uplink accepted; downstream orchestrator RPC not wired — returning empty response_data"
        );
        Ok(("unwired-custom-data".to_string(), Vec::new()))
    }

    /// 转发事件到业务系统（ExecuteEvent）
    ///
    /// 返回 (端点, 响应数据) 元组
    pub async fn forward_event(
        &self,
        ctx: &ServerContext,
        svid: &str,
        event: &ProtoEvent,
        route_repository: Arc<DefaultRouteRepository>,
    ) -> Result<(String, Vec<u8>)> {
        let resolved_svid = if svid.is_empty() { svid::IM } else { svid };
        let endpoint = if resolved_svid == svid::IM {
            use flare_im_contracts::service_names::{MESSAGE_ORCHESTRATOR, get_service_name};
            get_service_name(MESSAGE_ORCHESTRATOR)
        } else {
            use crate::domain::model::Svid;
            let svid_obj = Svid::new(resolved_svid.to_string())
                .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Invalid SVID"))?;
            match route_repository.find_by_svid(svid_obj.as_str()).await {
                Ok(Some(route)) => route.endpoint().as_str().to_string(),
                Ok(None) => {
                    return Err(map_infra_error(
                        std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "Business service not found",
                        ),
                        ErrorCode::DatabaseError,
                        format!("Business service not found for SVID {}", resolved_svid),
                    ));
                }
                Err(e) => {
                    return Err(map_infra_error(
                        e,
                        ErrorCode::DatabaseError,
                        format!("Failed to resolve route for SVID {}", resolved_svid),
                    ));
                }
            }
        };

        let mut business_clients = self.get_business_clients(&endpoint, resolved_svid).await?;

        let forwarding_ctx = ctx.with_tenant_id(
            ctx.tenant_id()
                .filter(|tenant_id| !tenant_id.trim().is_empty())
                .map(normalize_tenant_id)
                .unwrap_or_else(|| normalize_tenant_id(&self.default_tenant_id)),
        );

        let exec_req = ExecuteEventRequest {
            svid: resolved_svid.to_string(),
            event: Some(event.clone()),
        };
        let mut grpc_request = tonic::Request::new(exec_req);
        flare_server_core::grpc::utils::encode_context_to_metadata(
            grpc_request.metadata_mut(),
            &forwarding_ctx,
        );

        let response = match business_clients.event.execute_event(grpc_request).await {
            Ok(response) => response,
            Err(e) => {
                self.forget_clients_after_failure(resolved_svid, &endpoint, &business_clients, &e)
                    .await;
                return Err(map_infra_error(
                    std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        e.message().to_string(),
                    ),
                    ErrorCode::NetworkError,
                    format!(
                        "ExecuteEvent failed (svid={}): {} ({:?})",
                        resolved_svid,
                        e.message(),
                        e.code()
                    ),
                ));
            }
        };
        // ExecuteEvent returns google.protobuf.Empty, no response data to encode
        response.into_inner();
        info!(
            svid = %resolved_svid,
            endpoint = %endpoint,
            "Event forwarded to business service"
        );
        Ok((endpoint, Vec::new()))
    }
}

#[cfg(test)]
mod static_im_endpoint_tests {
    use super::*;
    use flare_im_service_kit::config::{
        MessageIngestServiceConfig, MessageOrchestratorServiceConfig, ServiceEndpointConfig,
    };

    #[test]
    fn fixed_im_service_names_use_only_their_configured_local_grpc_ports() {
        let mut services = ServicesConfig::default();
        let mut ingest = MessageIngestServiceConfig::default();
        ingest.runtime.server = Some(ServiceEndpointConfig {
            address: Some("untrusted.example".to_string()),
            port: Some(50182),
        });
        services.message_ingest = Some(ingest);

        let mut orchestrator = MessageOrchestratorServiceConfig::default();
        orchestrator.runtime.server = Some(ServiceEndpointConfig {
            address: Some("0.0.0.0".to_string()),
            port: Some(50181),
        });
        services.message_orchestrator = Some(orchestrator);

        assert_eq!(
            static_im_endpoint(&services, MESSAGE_INGEST).as_deref(),
            Some("http://127.0.0.1:50182")
        );
        assert_eq!(
            static_im_endpoint(&services, MESSAGE_ORCHESTRATOR).as_deref(),
            Some("http://127.0.0.1:50181")
        );
        assert_eq!(
            static_im_endpoint(&services, "http://evil.example:1234"),
            None
        );
        assert_eq!(static_im_endpoint(&services, "flare-admin-gateway"), None);
    }

    #[test]
    fn missing_or_zero_configured_port_fails_closed() {
        let mut services = ServicesConfig::default();
        assert_eq!(static_im_endpoint(&services, MESSAGE_INGEST), None);

        let mut ingest = MessageIngestServiceConfig::default();
        ingest.runtime.server = Some(ServiceEndpointConfig {
            address: Some("127.0.0.1".to_string()),
            port: None,
        });
        services.message_ingest = Some(ingest);
        assert_eq!(static_im_endpoint(&services, MESSAGE_INGEST), None);

        services
            .message_ingest
            .as_mut()
            .unwrap()
            .runtime
            .server
            .as_mut()
            .unwrap()
            .port = Some(0);
        assert_eq!(static_im_endpoint(&services, MESSAGE_INGEST), None);
    }
}

#[cfg(test)]
mod business_channel_tests {
    use super::*;
    use flare_grpc_proto::message::message_send_service_server::{
        MessageSendService, MessageSendServiceServer,
    };
    use flare_grpc_proto::message::{
        BatchSendMessageRequest, BatchSendMessageResponse, SendAckRequest, SendAckResponse,
        SendCustomDataRequest, SendCustomDataResponse, SendSystemMessageRequest,
        SendSystemMessageResponse,
    };
    use flare_im_service_kit::discovery::is_static_channel_cached;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tonic::{Request, Response, Status};

    struct FakeIngest;

    #[tonic::async_trait]
    impl MessageSendService for FakeIngest {
        async fn send_message(
            &self,
            _request: Request<SendMessageRequest>,
        ) -> std::result::Result<Response<SendMessageResponse>, Status> {
            Err(Status::failed_precondition("pre-send hook rejected"))
        }
        async fn batch_send_message(
            &self,
            _request: Request<BatchSendMessageRequest>,
        ) -> std::result::Result<Response<BatchSendMessageResponse>, Status> {
            Err(Status::unimplemented("batch"))
        }
        async fn send_system_message(
            &self,
            _request: Request<SendSystemMessageRequest>,
        ) -> std::result::Result<Response<SendSystemMessageResponse>, Status> {
            Err(Status::unimplemented("system"))
        }
        async fn send_ack(
            &self,
            _request: Request<SendAckRequest>,
        ) -> std::result::Result<Response<SendAckResponse>, Status> {
            Err(Status::unimplemented("ack"))
        }
        async fn send_custom_data(
            &self,
            _request: Request<SendCustomDataRequest>,
        ) -> std::result::Result<Response<SendCustomDataResponse>, Status> {
            Err(Status::unimplemented("custom data"))
        }
    }

    async fn start_fake_ingest() -> (
        std::net::SocketAddr,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, stop_rx) = oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(MessageSendServiceServer::new(FakeIngest))
                .serve_with_incoming_shutdown(
                    tonic::transport::server::TcpIncoming::from(listener),
                    async {
                        let _ = stop_rx.await;
                    },
                )
                .await
                .unwrap();
        });
        (addr, stop, task)
    }

    async fn send(clients: &mut BusinessGrpcClients) -> std::result::Result<(), Status> {
        clients
            .send
            .send_message(Request::new(SendMessageRequest::default()))
            .await
            .map(|_| ())
    }

    /// 静态业务端点走 service-kit 的静态通道（建连上限、HTTP/2 探活），传输层失败后丢弃
    /// 转发器缓存的客户端与静态通道；对端回的拒绝不丢。以前用没有探活的立即建连通道，缓存
    /// 5 分钟，下游换地址后只能等缓存过期。
    #[tokio::test]
    async fn static_business_clients_are_dropped_after_transport_failure_only() {
        let forwarder = MessageForwarder::new("0".to_string());
        let (addr, stop, task) = start_fake_ingest().await;
        let authority = format!("http://{addr}");
        let svid = "svid.test";

        let mut clients = forwarder
            .get_or_create_cached_client(svid, &authority)
            .await
            .expect("static business clients");
        assert_eq!(
            clients.static_authority.as_deref(),
            Some(authority.as_str())
        );
        assert!(
            is_static_channel_cached(&authority),
            "static business endpoint must use the service-kit static channel"
        );

        // 对端回的业务拒绝：不是传输层失败，缓存保留。
        let rejected = send(&mut clients).await.expect_err("fake ingest rejects");
        assert_eq!(rejected.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            send_failure_log_level(rejected.code()),
            SendFailureLogLevel::Info
        );
        assert!(
            !forwarder
                .forget_clients_after_failure(svid, &authority, &clients, &rejected)
                .await
        );
        assert!(is_static_channel_cached(&authority));
        assert!(
            forwarder
                .business_clients
                .read()
                .await
                .contains_key(&MessageForwarder::cache_key(svid, &authority))
        );

        // 下游停止：传输层失败，转发器缓存与静态通道一起丢弃。
        let _ = stop.send(());
        let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
        let failed = send(&mut clients).await.expect_err("stopped ingest");
        assert!(is_transport_failure(&failed), "{failed:?}");
        assert_eq!(
            send_failure_log_level(failed.code()),
            SendFailureLogLevel::Error,
            "{failed:?}"
        );
        assert!(
            forwarder
                .forget_clients_after_failure(svid, &authority, &clients, &failed)
                .await
        );
        assert!(!is_static_channel_cached(&authority));
        assert!(
            !forwarder
                .business_clients
                .read()
                .await
                .contains_key(&MessageForwarder::cache_key(svid, &authority))
        );
    }

    #[test]
    fn only_caller_attributable_statuses_count_as_business_rejections() {
        for code in [
            tonic::Code::FailedPrecondition,
            tonic::Code::InvalidArgument,
            tonic::Code::NotFound,
        ] {
            assert_eq!(
                send_failure_log_level(code),
                SendFailureLogLevel::Info,
                "{code:?}"
            );
        }
        for code in [
            tonic::Code::Unknown,
            tonic::Code::Internal,
            tonic::Code::Unavailable,
            tonic::Code::DeadlineExceeded,
        ] {
            assert_eq!(
                send_failure_log_level(code),
                SendFailureLogLevel::Error,
                "{code:?}"
            );
        }
    }

    /// ingest 回的 `UNAUTHENTICATED`、`PERMISSION_DENIED` 可能是内部签名上下文或租户配置
    /// 故障，不降为 INFO（W8-C 复审：以前与策略拒绝一起记 INFO）。
    #[test]
    fn auth_and_permission_statuses_stay_visible() {
        for code in [
            tonic::Code::Unauthenticated,
            tonic::Code::PermissionDenied,
            tonic::Code::AlreadyExists,
            tonic::Code::ResourceExhausted,
            tonic::Code::OutOfRange,
        ] {
            assert_eq!(
                send_failure_log_level(code),
                SendFailureLogLevel::Warn,
                "{code:?}"
            );
        }
    }
}
