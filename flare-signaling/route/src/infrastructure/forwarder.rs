//! 消息转发服务
//!
//! 负责将消息转发到对应的业务系统

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context as AnyhowContext, Result};
use flare_proto::common::TenantContext;
use flare_proto::message::message_service_client::MessageServiceClient;
use flare_proto::message::{SendMessageRequest, SendMessageResponse};
use prost::Message as ProstMessage;
use tokio::sync::RwLock;
use tonic::transport::Channel;

use flare_server_core::context::Context as ServerContext;
use tracing::{debug, info};

use crate::domain::repository::RouteRepository;
use flare_server_core::discovery::ServiceClient;

/// SVID 常量定义
pub mod svid {
    /// IM 业务系统 SVID（默认）
    pub const IM: &str = "svid.im";
}

/// 缓存的客户端条目
struct CachedClient {
    /// 客户端
    client: MessageServiceClient<Channel>,
    /// 最后使用时间
    last_used: Instant,
}

impl CachedClient {
    fn new(client: MessageServiceClient<Channel>) -> Self {
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
            max_cache_size: 100, // 最多缓存 100 个客户端
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
        
        clients.retain(|_key, cached_client| {
            !cached_client.should_cleanup(self.max_idle_duration)
        });
        
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
    ) -> Result<MessageServiceClient<Channel>> {
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
    ) -> Result<MessageServiceClient<Channel>> {

        // 特殊处理：如果 SVID 是 svid.im，直接使用 MESSAGE_ORCHESTRATOR 服务名
        if svid == svid::IM {
            use flare_im_core::service_names::{MESSAGE_ORCHESTRATOR, get_service_name};
            use flare_im_core::config::app_config;
            use flare_im_core::discovery::create_discover_from_registry_config_with_filters;
            
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
                    anyhow::anyhow!(
                        "Failed to create service discover for {} with SVID filter (svid={}): {}",
                        message_orchestrator_service,
                        svid::IM,
                        e
                    )
                })?
            } else {
                return Err(anyhow::anyhow!(
                    "Service discovery not configured for {}",
                    message_orchestrator_service
                ));
            };

            let mut service_client = ServiceClient::new(discover);
            // 添加超时保护，避免服务发现阻塞过长时间
            let channel = tokio::time::timeout(
                std::time::Duration::from_secs(3), // 3秒超时
                service_client.get_channel(),
            )
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "Timeout waiting for service discovery to get channel for {} (svid={}) (3s)",
                    message_orchestrator_service,
                    svid::IM
                )
            })?
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to get channel from service discovery for {} (svid={}): {}",
                    message_orchestrator_service,
                    svid::IM,
                    e
                )
            })?;

            return Ok(MessageServiceClient::new(channel));
        }

        // 其他业务系统：判断 endpoint 是服务名还是 URL
        let channel = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
            // 直接 URL
            tonic::transport::Endpoint::from_shared(endpoint.to_string())
                .with_context(|| "Invalid endpoint format")?
                .connect()
                .await
                .with_context(|| "Failed to connect to business service")?
        } else if endpoint.contains(':') && !endpoint.contains('.') {
            // host:port 格式
            let endpoint_url = format!("http://{}", endpoint);
            tonic::transport::Endpoint::from_shared(endpoint_url)
                .with_context(|| "Invalid endpoint format")?
                .connect()
                .await
                .with_context(|| "Failed to connect to business service")?
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
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "Failed to create service discover for {} with SVID filter (svid={}): {}",
                            endpoint,
                            svid,
                            e
                        )
                    })?
                } else {
                    // 没有 SVID，使用普通的服务发现
                    let discover_result = flare_im_core::discovery::create_discover(endpoint)
                        .await
                        .map_err(|e| {
                            anyhow::anyhow!("Failed to create service discover for {}: {}", endpoint, e)
                        })?;
                    
                    discover_result.ok_or_else(|| {
                        anyhow::anyhow!("Service discovery not configured for {}", endpoint)
                    })?
                }
            } else {
                return Err(anyhow::anyhow!(
                    "Service discovery not configured for {}",
                    endpoint
                ));
            };

            let mut service_client = ServiceClient::new(discover);
            // 添加超时保护，避免服务发现阻塞过长时间
            tokio::time::timeout(
                std::time::Duration::from_secs(3), // 3秒超时
                service_client.get_channel(),
            )
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "Timeout waiting for service discovery to get channel for {} (svid={}) (3s)",
                    endpoint,
                    svid
                )
            })?
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to get channel from service discovery for {} (svid={}): {}",
                    endpoint,
                    svid,
                    e
                )
            })?
        };

        Ok(MessageServiceClient::new(channel))
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
    async fn get_business_client(&self, endpoint: &str, svid: &str) -> Result<MessageServiceClient<Channel>> {
        // 对于 svid.im，使用固定的 endpoint（MESSAGE_ORCHESTRATOR 服务名）
        let actual_endpoint = if svid == svid::IM {
            use flare_im_core::service_names::{MESSAGE_ORCHESTRATOR, get_service_name};
            get_service_name(MESSAGE_ORCHESTRATOR)
        } else {
            endpoint.to_string()
        };

        // 从缓存获取或创建客户端
        self.get_or_create_cached_client(svid, &actual_endpoint).await
    }
    
    /// 转发消息到业务系统
    ///
    /// 返回 (端点, 响应数据) 元组
    pub async fn forward_message(
        &self,
        ctx: &ServerContext,
        svid: &str,
        payload: Vec<u8>,
        route_repository: Option<Arc<dyn RouteRepository + Send + Sync>>,
    ) -> Result<(String, Vec<u8>)> {
        // 提取或使用默认 SVID
        let resolved_svid = if svid.is_empty() {
            svid::IM
        } else {
            svid
        };
        debug!(svid = %resolved_svid, "Forwarding message to business system");

        // 对于 svid.im，直接使用服务发现，不需要 RouteRepository
        let endpoint = if resolved_svid == svid::IM {
            use flare_im_core::service_names::{MESSAGE_ORCHESTRATOR, get_service_name};
            get_service_name(MESSAGE_ORCHESTRATOR)
        } else if let Some(repo) = route_repository {
            // 其他 SVID：从路由仓储解析端点
            use crate::domain::model::Svid;
            let svid_obj = Svid::new(resolved_svid.to_string())
                .map_err(|e| anyhow::anyhow!("Invalid SVID: {}", e))?;
            
            match repo.find_by_svid(svid_obj.as_str()).await {
                Ok(Some(route)) => route.endpoint().as_str().to_string(),
                Ok(None) => {
                    return Err(anyhow::anyhow!(
                        "Business service not found for SVID {}",
                        resolved_svid
                    ));
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "Failed to resolve route for SVID {}: {}",
                        resolved_svid,
                        e
                    ));
                }
            }
        } else {
            return Err(anyhow::anyhow!(
                "Route repository not available, cannot resolve endpoint for SVID {}",
                resolved_svid
            ));
        };

        // 获取或创建业务系统客户端（带缓存，使用 SVID 过滤）
        let mut client = self.get_business_client(&endpoint, &resolved_svid).await?;

        // 解析 payload 为 Message 对象
        use flare_proto::common::Message;
        if payload.is_empty() {
            return Err(anyhow::anyhow!("Empty payload for message forwarding"));
        }

        let message = Message::decode(&payload[..])
            .map_err(|e| anyhow::anyhow!("Failed to decode payload as Message: {}", e))?;

        // 保存消息信息用于错误日志
        let message_id = message.server_id.clone();
        let message_type = message.message_type;
        let conversation_id_for_log = message.conversation_id.clone();

        // 提取 conversation_id（优先从 message，否则从 context）
        let conversation_id = if !message.conversation_id.is_empty() {
            message.conversation_id.clone()
        } else {
            ctx.request()
                .and_then(|req| req.attributes.get("conversation_id").cloned())
                .unwrap_or_default()
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

        // 从 Context 中提取 TenantContext（用于 protobuf 兼容性）
        let tenant_context: TenantContext = ctx.tenant().cloned().map(|tc| tc.into()).or_else(|| {
            ctx.tenant_id().map(|tenant_id| TenantContext {
                tenant_id: tenant_id.to_string(),
                business_type: String::new(),
                environment: String::new(),
                organization_id: String::new(),
                labels: std::collections::HashMap::new(),
                attributes: std::collections::HashMap::new(),
            })
        }).unwrap_or_else(|| {
            TenantContext {
                tenant_id: self.default_tenant_id.clone(),
                business_type: String::new(),
                environment: String::new(),
                organization_id: String::new(),
                labels: std::collections::HashMap::new(),
                attributes: std::collections::HashMap::new(),
            }
        });

        // 从 Context 中提取 RequestContext（用于 protobuf 兼容性）
        use flare_proto::common::RequestContext;
        let request_context: RequestContext = ctx.request().cloned().map(|rc| rc.into()).unwrap_or_else(|| {
            RequestContext {
                request_id: ctx.request_id().to_string(),
                trace: None,
                actor: None,
                device: None,
                channel: String::new(),
                user_agent: String::new(),
                attributes: std::collections::HashMap::new(),
            }
        });

        // 构造转发请求
        let request = SendMessageRequest {
            conversation_id,
            message: Some(message),
            sync: false,
        };

        // 发送请求到业务系统
        let response = match client
            .send_message(tonic::Request::new(request))
            .await
        {
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
                return Err(anyhow::anyhow!(
                    "Failed to send message to business service {} (svid={}): {} (code: {:?})",
                    endpoint,
                    resolved_svid,
                    e.message(),
                    e.code()
                ));
            }
        };

        let response_inner = response.into_inner();

        // 序列化响应
        let mut response_bytes = Vec::new();
        SendMessageResponse::encode(&response_inner, &mut response_bytes)
            .with_context(|| "Failed to encode SendMessageResponse")?;

        info!(
            "✅ Message forwarded to business service successfully: SVID={}, Endpoint={}",
            resolved_svid, endpoint
        );
        Ok((endpoint, response_bytes))
    }

}
