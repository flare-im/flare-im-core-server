//! 消息转发服务
//!
//! 负责将消息转发到对应的业务系统

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use flare_grpc_proto::message::message_send_service_client::MessageSendServiceClient;
use flare_grpc_proto::message::{ExecuteEventRequest, SendMessageRequest, SendMessageResponse};
use flare_proto::common::{CustomData, Event as ProtoEvent, Message};
use prost::Message as ProstMessage;
use tokio::sync::RwLock;
use tonic::transport::Channel;

use flare_im_core::error::{ErrorCode, map_infra_error};
use flare_server_core::context::Context as ServerContext;
use tracing::{debug, info};

use crate::domain::repository::{DefaultRouteRepository, RouteRepository};
use crate::error::Result;
use flare_im_core::ServiceClient;

/// SVID 常量定义
pub mod svid {
    /// IM 业务系统 SVID（默认）
    pub const IM: &str = "svid.im";
}

/// 缓存的客户端条目
struct CachedClient {
    /// 客户端
    client: MessageSendServiceClient<Channel>,
    /// 最后使用时间
    last_used: Instant,
}

impl CachedClient {
    fn new(client: MessageSendServiceClient<Channel>) -> Self {
        Self {
            client,
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
}

impl MessageForwarder {
    /// 创建新的消息转发服务
    pub fn new(default_tenant_id: String) -> Self {
        Self {
            business_clients: Arc::new(RwLock::new(HashMap::new())),
            default_tenant_id,
            max_cache_size: 100,                         // 最多缓存 100 个客户端
            max_idle_duration: Duration::from_secs(300), // 5 分钟空闲后清理
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

    /// 从缓存获取客户端，如果不存在或已失效则创建新的
    async fn get_or_create_cached_client(
        &self,
        svid: &str,
        endpoint: &str,
    ) -> Result<MessageSendServiceClient<Channel>> {
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
                    return Ok(cached.client.clone());
                }
            }
        }

        // 慢速路径：需要创建新客户端（写锁）
        let new_client = self.create_business_client(endpoint, svid).await?;

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
            clients.insert(cache_key, CachedClient::new(new_client.clone()));
            Ok(new_client)
        } else {
            // 缓存未满，直接插入
            clients.insert(cache_key, CachedClient::new(new_client.clone()));
            Ok(new_client)
        }
    }

    /// 创建业务系统客户端（内部方法，不缓存）
    async fn create_business_client(
        &self,
        endpoint: &str,
        svid: &str,
    ) -> Result<MessageSendServiceClient<Channel>> {
        // 特殊处理：如果 SVID 是 svid.im，直接使用 MESSAGE_ORCHESTRATOR 服务名
        if svid == svid::IM {
            use flare_im_core::config::app_config;
            use flare_im_core::discovery::create_discover_from_registry_config_with_filters;
            use flare_im_core::service_names::{MESSAGE_ORCHESTRATOR, get_service_name};

            let message_orchestrator_service = get_service_name(MESSAGE_ORCHESTRATOR);
            let app_config = app_config();

            // 使用 svid.im 作为过滤条件
            let mut tag_filters = std::collections::HashMap::new();
            tag_filters.insert("svid".to_string(), svid::IM.to_string());

            debug!(
                service = %message_orchestrator_service,
                svid = svid::IM,
                "Creating service discover for svid.im with MESSAGE_ORCHESTRATOR"
            );

            let discover = if let Some(registry_config) = &app_config.core.registry {
                create_discover_from_registry_config_with_filters(
                    registry_config,
                    &message_orchestrator_service,
                    Some(tag_filters),
                )
                .await
                .map_err(|e| {
                    map_infra_error(
                        e,
                        ErrorCode::NetworkError,
                        &format!(
                            "Failed to create service discover for {} with SVID filter (svid={})",
                            message_orchestrator_service,
                            svid::IM
                        ),
                    )
                })?
            } else {
                return Err(map_infra_error(
                    std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "Service discovery not configured",
                    ),
                    ErrorCode::NetworkError,
                    &format!(
                        "Service discovery not configured for {}",
                        message_orchestrator_service
                    ),
                ));
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
                &format!("Timeout waiting for service discovery to get channel for {} (svid={}) (3s)", message_orchestrator_service, svid::IM)
            ))?
            .map_err(|e| map_infra_error(e, ErrorCode::NetworkError, &format!("Failed to get channel from service discovery for {} (svid={})", message_orchestrator_service, svid::IM)))?;

            return Ok(MessageSendServiceClient::new(channel));
        }

        // 其他业务系统：判断 endpoint 是服务名还是 URL
        let channel = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
            // 直接 URL
            tonic::transport::Endpoint::from_shared(endpoint.to_string())
                .map_err(|e| {
                    map_infra_error(e, ErrorCode::NetworkError, "Invalid endpoint format")
                })?
                .connect()
                .await
                .map_err(|e| {
                    map_infra_error(
                        e,
                        ErrorCode::NetworkError,
                        "Failed to connect to business service",
                    )
                })?
        } else if endpoint.contains(':') && !endpoint.contains('.') {
            // host:port 格式
            let endpoint_url = format!("http://{}", endpoint);
            tonic::transport::Endpoint::from_shared(endpoint_url)
                .map_err(|e| {
                    map_infra_error(e, ErrorCode::NetworkError, "Invalid endpoint format")
                })?
                .connect()
                .await
                .map_err(|e| {
                    map_infra_error(
                        e,
                        ErrorCode::NetworkError,
                        "Failed to connect to business service",
                    )
                })?
        } else {
            // 服务名（通过服务发现，支持 SVID 过滤）
            use flare_im_core::config::app_config;
            use flare_im_core::discovery::create_discover_from_registry_config_with_filters;

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
                    .map_err(|e| map_infra_error(e, ErrorCode::NetworkError, &format!("Failed to create service discover for {} with SVID filter (svid={})", endpoint, svid)))?
                } else {
                    // 没有 SVID，使用普通的服务发现
                    let discover_result = flare_im_core::discovery::create_discover(endpoint)
                        .await
                        .map_err(|e| {
                            map_infra_error(
                                e,
                                ErrorCode::NetworkError,
                                &format!("Failed to create service discover for {}", endpoint),
                            )
                        })?;

                    discover_result.ok_or_else(|| {
                        map_infra_error(
                            std::io::Error::new(
                                std::io::ErrorKind::NotFound,
                                "Service discovery not configured",
                            ),
                            ErrorCode::NetworkError,
                            &format!("Service discovery not configured for {}", endpoint),
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
                    &format!("Service discovery not configured for {}", endpoint),
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
                &format!("Timeout waiting for service discovery to get channel for {} (svid={}) (3s)", endpoint, svid)
            ))?
            .map_err(|e| map_infra_error(e, ErrorCode::NetworkError, &format!("Failed to get channel from service discovery for {} (svid={})", endpoint, svid)))?
        };

        Ok(MessageSendServiceClient::new(channel))
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
    async fn get_business_client(
        &self,
        endpoint: &str,
        svid: &str,
    ) -> Result<MessageSendServiceClient<Channel>> {
        // 对于 svid.im，使用固定的 endpoint（MESSAGE_ORCHESTRATOR 服务名）
        let actual_endpoint = if svid == svid::IM {
            use flare_im_core::service_names::{MESSAGE_ORCHESTRATOR, get_service_name};
            get_service_name(MESSAGE_ORCHESTRATOR)
        } else {
            endpoint.to_string()
        };

        // 从缓存获取或创建客户端
        self.get_or_create_cached_client(svid, &actual_endpoint)
            .await
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
        // 提取或使用默认 SVID
        let resolved_svid = if svid.is_empty() { svid::IM } else { svid };
        debug!(svid = %resolved_svid, "Forwarding message to business system");

        // 对于 svid.im，直接使用服务发现，不需要 RouteRepository
        let endpoint = if resolved_svid == svid::IM {
            use flare_im_core::service_names::{MESSAGE_ORCHESTRATOR, get_service_name};
            get_service_name(MESSAGE_ORCHESTRATOR)
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
                        &format!("Business service not found for SVID {}", resolved_svid),
                    ));
                }
                Err(e) => {
                    return Err(map_infra_error(
                        e,
                        ErrorCode::DatabaseError,
                        &format!("Failed to resolve route for SVID {}", resolved_svid),
                    ));
                }
            }
        };

        // 获取或创建业务系统客户端（带缓存，使用 SVID 过滤）
        let mut client = self.get_business_client(&endpoint, &resolved_svid).await?;

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

        let forwarding_ctx = match ctx.tenant_id().filter(|t| !t.is_empty()) {
            Some(_) => ctx.clone(),
            None => ctx.with_tenant_id(self.default_tenant_id.clone()),
        };

        let mut grpc_request = tonic::Request::new(request);
        flare_server_core::grpc::utils::encode_context_to_metadata(
            grpc_request.metadata_mut(),
            &forwarding_ctx,
        );

        let response = match client.send_message(grpc_request).await {
            Ok(resp) => resp,
            Err(e) => {
                use tracing::error;
                error!(
                    message_id = %message_id,
                    message_type = message_type,
                    conversation_id = %conversation_id_for_log,
                    svid = %resolved_svid,
                    endpoint = %endpoint,
                    error = %e,
                    error_code = ?e.code(),
                    error_message = %e.message(),
                    "❌ Failed to send message to business service"
                );
                return Err(map_infra_error(
                    std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        e.message().to_string(),
                    ),
                    ErrorCode::NetworkError,
                    &format!(
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
            "✅ Message forwarded to business service successfully: SVID={}, Endpoint={}",
            resolved_svid, endpoint
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
            use flare_im_core::service_names::{MESSAGE_ORCHESTRATOR, get_service_name};
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
                        &format!("Business service not found for SVID {}", resolved_svid),
                    ));
                }
                Err(e) => {
                    return Err(map_infra_error(
                        e,
                        ErrorCode::DatabaseError,
                        &format!("Failed to resolve route for SVID {}", resolved_svid),
                    ));
                }
            }
        };

        let mut client = self.get_business_client(&endpoint, resolved_svid).await?;

        let forwarding_ctx = match ctx.tenant_id().filter(|t| !t.is_empty()) {
            Some(_) => ctx.clone(),
            None => ctx.with_tenant_id(self.default_tenant_id.clone()),
        };

        let exec_req = ExecuteEventRequest {
            svid: resolved_svid.to_string(),
            event: Some(event.clone()),
        };
        let mut grpc_request = tonic::Request::new(exec_req);
        flare_server_core::grpc::utils::encode_context_to_metadata(
            grpc_request.metadata_mut(),
            &forwarding_ctx,
        );

        let response = client.execute_event(grpc_request).await.map_err(|e| {
            map_infra_error(
                std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    e.message().to_string(),
                ),
                ErrorCode::NetworkError,
                &format!(
                    "ExecuteEvent failed (svid={}): {} ({:?})",
                    resolved_svid,
                    e.message(),
                    e.code()
                ),
            )
        })?;
        // ExecuteEvent returns google.protobuf.Empty, no response data to encode
        let _ = response.into_inner();
        info!(
            "✅ Event forwarded to business service: SVID={}, Endpoint={}",
            resolved_svid, endpoint
        );
        Ok((endpoint, Vec::new()))
    }
}
