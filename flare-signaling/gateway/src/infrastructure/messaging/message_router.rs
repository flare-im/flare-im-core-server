//! 消息路由模块
//!
//! 负责将客户端消息通过 Route 服务路由到对应的业务系统
//! 架构：Gateway -> Route Service -> Business Service (如 message-orchestrator)
//!
//! Route 服务根据 SVID 决定将消息路由到哪个业务系统：
//! - svid.im -> message-orchestrator
//! - svid.customer -> customer-service
//! - svid.ai.bot -> ai-bot-service
//! 等等

use std::sync::Arc;

use anyhow::{Context as AnyhowContext, Result};
use flare_proto::common::{RequestContext, TenantContext, TraceContext};
use flare_proto::message::SendMessageResponse;
use flare_proto::signaling::router::router_service_client::RouterServiceClient;
use flare_proto::signaling::router::{
    LoadBalanceStrategy, RetryStrategy, RouteMessageRequest, RouteOptions,
};
use prost::Message as ProstMessage;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::{error, info, instrument, warn, Span};

use flare_server_core::discovery::ServiceClient;
use crate::infrastructure::connection_context::{
    build_tenant_context_from_metadata, build_request_context_from_metadata,
    METADATA_KEY_TENANT_ID,
};

/// 消息路由服务
pub struct MessageRouter {
    /// Route 服务名称（用于服务发现）
    route_service_name: String,
    /// 默认租户ID
    default_tenant_id: String,
    /// RouterService gRPC 客户端
    router_client: Arc<Mutex<Option<RouterServiceClient<Channel>>>>,
    /// 服务发现客户端
    service_client: Arc<Mutex<Option<ServiceClient>>>,
    /// 默认 SVID（业务系统标识符）
    default_svid: String,
    /// 连接管理器（用于获取连接信息，包括 metadata）
    connection_manager: Option<Arc<tokio::sync::Mutex<Option<Arc<dyn flare_core::server::connection::ConnectionManagerTrait>>>>>,
}

impl MessageRouter {
    /// 创建新的消息路由服务（使用 Route 服务名称，内部创建服务发现）
    pub fn new(route_service_name: String, default_tenant_id: String, default_svid: String) -> Self {
        Self {
            router_client: Arc::new(Mutex::new(None)),
            route_service_name,
            default_tenant_id,
            service_client: Arc::new(Mutex::new(None)),
            default_svid,
            connection_manager: None,
        }
    }

    /// 使用 ServiceClient 创建新的消息路由服务（推荐，通过 wire 注入）
    pub fn with_service_client(
        service_client: ServiceClient,
        default_tenant_id: String,
        default_svid: String,
    ) -> Self {
        Self {
            router_client: Arc::new(Mutex::new(None)),
            route_service_name: String::new(), // 不需要 service_name
            default_tenant_id,
            service_client: Arc::new(Mutex::new(Some(service_client))),
            default_svid,
            connection_manager: None,
        }
    }
    
    /// 设置连接管理器（用于获取连接 metadata）
    pub fn with_connection_manager(
        mut self,
        connection_manager: Arc<tokio::sync::Mutex<Option<Arc<dyn flare_core::server::connection::ConnectionManagerTrait>>>>,
    ) -> Self {
        self.connection_manager = Some(connection_manager);
        self
    }

    /// 初始化 Route 服务客户端连接
    pub async fn initialize(&self) -> Result<()> {
        use flare_server_core::discovery::ServiceClient;

        info!(
            route_service = %self.route_service_name,
            "Initializing Route Service client..."
        );

        // 使用服务发现获取 Channel
        let mut service_client_guard = self.service_client.lock().await;
        if service_client_guard.is_none() {
            // 如果没有注入 ServiceClient，则创建服务发现器
            let discover = flare_im_core::discovery::create_discover(&self.route_service_name)
                .await
                .map_err(|e| {
                    error!(
                        route_service = %self.route_service_name,
                        error = %e,
                        "Failed to create service discover for Route service"
                    );
                    anyhow::anyhow!("Failed to create service discover: {}", e)
                })?;

            let discover =
                discover.ok_or_else(|| anyhow::anyhow!("Service discovery not configured"))?;

            *service_client_guard = Some(ServiceClient::new(discover));
        }

        let service_client = service_client_guard
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Service client not initialized"))?;
        let channel = service_client.get_channel().await.map_err(|e| {
            error!(
                route_service = %self.route_service_name,
                error = %e,
                "Failed to get channel from service discovery"
            );
            anyhow::anyhow!("Failed to get channel: {}", e)
        })?;

        info!(
            route_service = %self.route_service_name,
            "Successfully got channel from service discovery"
        );

        let client = RouterServiceClient::new(channel);
        *self.router_client.lock().await = Some(client);

        info!(
            route_service = %self.route_service_name,
            "✅ Route Service client initialized successfully"
        );
        Ok(())
    }

    /// 路由消息到业务系统（通过 Route 服务）
    ///
    /// Route 服务会根据 SVID 将消息路由到对应的业务系统：
    /// - svid.im -> message-orchestrator
    /// - svid.customer -> customer-service
    /// - 等等
    #[instrument(skip(self), fields(user_id = %user_id, conversation_id = %conversation_id, svid = %self.default_svid))]
    pub async fn route_message(
        &self,
        user_id: &str,
        conversation_id: &str,
        payload: Vec<u8>,
        connection_id: Option<&str>,
    ) -> Result<SendMessageResponse> {
        self.route_message_with_options(user_id, conversation_id, payload, connection_id, None).await
    }

    /// 路由消息到业务系统（带选项配置）
    ///
    /// 支持自定义路由选项，如超时、重试策略等
    #[instrument(skip(self), fields(user_id = %user_id, conversation_id = %conversation_id, svid = %self.default_svid))]
    pub async fn route_message_with_options(
        &self,
        user_id: &str,
        conversation_id: &str,
        payload: Vec<u8>,
        connection_id: Option<&str>,
        options: Option<RouteOptions>,
    ) -> Result<SendMessageResponse> {
        let start_time = std::time::Instant::now();

        // 确保客户端已初始化
        let mut client_guard = self.ensure_client().await?;

        let client = client_guard.as_mut().ok_or_else(|| {
            anyhow::anyhow!("Route Service client not available after initialization")
        })?;

        // 从 connection_id 获取 metadata（如果可用）
        let connection_metadata = if let Some(conn_id) = connection_id {
            self.get_connection_metadata(conn_id).await
        } else {
            None
        };
        
        // 使用统一的Context构建函数（确保tenant_id总是存在）
        use crate::infrastructure::connection_context::build_context_from_connection;
        let ctx = build_context_from_connection(
            connection_metadata.as_ref(),
            Some(user_id),
            &self.default_tenant_id,
        );
        
        // 从Context转换为protobuf类型
        let mut request_context = RequestContext::default();
        request_context.request_id = ctx.request_id().to_string();
        if !ctx.trace_id().is_empty() {
            request_context.trace = Some(TraceContext {
                trace_id: ctx.trace_id().to_string(),
                span_id: String::new(),
                parent_span_id: String::new(),
                sampled: String::new(),
                tags: std::collections::HashMap::new(),
            });
        }
        
        // 构建租户上下文（从Context中提取，确保总是存在）
        let tenant_context = if let Some(tenant) = ctx.tenant() {
            flare_proto::common::TenantContext::from(tenant.clone())
        } else {
            // 如果Context中没有TenantContext，从tenant_id构建
            let tenant_id = ctx.tenant_id().unwrap_or(&self.default_tenant_id);
            flare_proto::common::TenantContext {
                tenant_id: tenant_id.to_string(),
                business_type: "im".to_string(),
                environment: String::new(),
                organization_id: String::new(),
                labels: std::collections::HashMap::new(),
                attributes: std::collections::HashMap::new(),
            }
        };

        // 构建路由选项（使用默认值或提供的选项）
        let route_options = options.unwrap_or_else(|| RouteOptions {
            timeout_seconds: 5, // 默认5秒超时
            enable_tracing: true, // 默认启用追踪
            retry_strategy: RetryStrategy::None as i32, // 默认不重试
            load_balance_strategy: LoadBalanceStrategy::RoundRobin as i32, // 默认轮询
            priority: 0, // 默认优先级
        });

        // 构建路由请求
        // 使用 prost::Message::default() 来创建默认实例，然后设置字段
        let mut route_request = RouteMessageRequest::default();
        route_request.svid = self.default_svid.clone();
        route_request.payload = payload;
        
        // 设置路由选项（proto 重新生成后确保字段存在）
        // 如果字段不存在，这行代码会编译失败，需要重新生成 proto 代码
        route_request.options = Some(route_options);

        // 根据选项中的超时配置设置超时
        let timeout_duration = std::time::Duration::from_secs(route_options.timeout_seconds as u64);

        info!(
            user_id = %user_id,
            conversation_id = %conversation_id,
            svid = %self.default_svid,
            timeout_secs = timeout_duration.as_secs(),
            payload_len = route_request.payload.len(),
            "Routing message to Route Service"
        );

        // 发送请求到 Route 服务（添加超时保护，避免阻塞）
        let response = match tokio::time::timeout(
            timeout_duration,
            client.route_message(tonic::Request::new(route_request)),
        )
        .await
        {
            Ok(Ok(resp)) => resp.into_inner(),
            Ok(Err(e)) => {
                error!(
                    error = %e,
                    user_id = %user_id,
                    conversation_id = %conversation_id,
                    svid = %self.default_svid,
                    duration_ms = start_time.elapsed().as_millis(),
                    "Failed to route message via Route Service"
                );
                // 如果连接失败，清除客户端以便下次重试
                {
                    let mut client_guard = self.router_client.lock().await;
                    *client_guard = None;
                }
                return Err(anyhow::anyhow!(
                    "Failed to route message via Route Service: {}",
                    e
                ));
            }
            Err(_) => {
                error!(
                    user_id = %user_id,
                    conversation_id = %conversation_id,
                    svid = %self.default_svid,
                    timeout_secs = timeout_duration.as_secs(),
                    duration_ms = start_time.elapsed().as_millis(),
                    "Timeout routing message via Route Service"
                );
                return Err(anyhow::anyhow!(
                    "Timeout routing message via Route Service (timeout: {}s)",
                    timeout_duration.as_secs()
                ));
            }
        };

        // 检查响应状态
        // 注意：ERROR_CODE_OK = 1（不是 0），所以成功时 status.code = 1
        use flare_proto::common::ErrorCode as ProtoErrorCode;
        if let Some(status) = &response.status {
            if status.code != ProtoErrorCode::Ok as i32 {
                error!(
                    user_id = %user_id,
                    conversation_id = %conversation_id,
                    error_code = status.code,
                    error_message = %status.message,
                    duration_ms = start_time.elapsed().as_millis(),
                    "Route Service returned error"
                );
                return Err(anyhow::anyhow!(
                    "Route Service error: {} (code: {})",
                    status.message,
                    status.code
                ));
            }
        }

        // 解析响应数据
        let send_response = SendMessageResponse::decode(&response.response_data[..])
            .with_context(|| "Failed to decode RouteMessageResponse.response_data as SendMessageResponse")?;

        // 记录路由元数据（如果可用）
        if let Some(metadata) = &response.metadata {
            info!(
                user_id = %user_id,
                conversation_id = %conversation_id,
                message_id = %send_response.server_msg_id,
                svid = %self.default_svid,
                routed_endpoint = %response.routed_endpoint,
                route_duration_ms = metadata.route_duration_ms,
                business_duration_ms = metadata.business_duration_ms,
                decision_duration_ms = metadata.decision_duration_ms,
                from_cache = metadata.from_cache,
                total_duration_ms = start_time.elapsed().as_millis(),
                "Message routed successfully via Route Service"
            );
        } else {
            info!(
                user_id = %user_id,
                conversation_id = %conversation_id,
                message_id = %send_response.server_msg_id,
                svid = %self.default_svid,
                routed_endpoint = %response.routed_endpoint,
                total_duration_ms = start_time.elapsed().as_millis(),
                "Message routed successfully via Route Service"
            );
        }

        Ok(send_response)
    }

    /// 检查客户端是否已连接
    pub async fn is_connected(&self) -> bool {
        self.router_client.lock().await.is_some()
    }

    /// 确保客户端已初始化（内部辅助函数）
    ///
    /// 如果客户端未初始化，尝试初始化（最多重试3次）
    /// 返回客户端引用，如果初始化失败则返回错误
    async fn ensure_client(
        &self,
    ) -> Result<tokio::sync::MutexGuard<'_, Option<RouterServiceClient<Channel>>>> {
        let client_guard = self.router_client.lock().await;

        // 如果客户端已初始化，直接返回
        if client_guard.is_some() {
            return Ok(client_guard);
        }

        // 客户端未初始化，需要初始化
        warn!(
            route_service = %self.route_service_name,
            "Route Service client not initialized, attempting to initialize..."
        );
        drop(client_guard);

        // 重试初始化（最多3次）
        let mut retries = 3;
        let mut last_error = None;
        while retries > 0 {
            match self.initialize().await {
                Ok(_) => {
                    info!(
                        route_service = %self.route_service_name,
                        "✅ Route Service client initialized successfully after retry"
                    );
                    break;
                }
                Err(e) => {
                    let error_msg = format!("{}", e);
                    last_error = Some(anyhow::anyhow!("{}", error_msg));
                    retries -= 1;
                    if retries > 0 {
                        warn!(
                            route_service = %self.route_service_name,
                            error = %error_msg,
                            retries_left = retries,
                            "Failed to initialize Route Service client, retrying..."
                        );
                        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                    } else {
                        error!(
                            route_service = %self.route_service_name,
                            error = %error_msg,
                            "❌ Failed to initialize Route Service client after all retries"
                        );
                    }
                }
            }
        }

        if let Some(ref err) = last_error {
            error!(
                route_service = %self.route_service_name,
                error = %err,
                "Cannot route message: Route Service client initialization failed"
            );
            return Err(anyhow::anyhow!(
                "Failed to initialize Route Service client after retries: {}",
                err
            ));
        }

        // 重新获取锁
        Ok(self.router_client.lock().await)
    }

    /// 构建 RequestContext（内部辅助函数，不包含追踪信息）
    fn build_request_context(&self, actor_id: &str, conversation_id: &str) -> RequestContext {
        self.build_request_context_with_trace(actor_id, conversation_id)
    }

    /// 构建 RequestContext（包含追踪信息传播）
    fn build_request_context_with_trace(&self, actor_id: &str, conversation_id: &str) -> RequestContext {
        let mut attributes = std::collections::HashMap::new();
        attributes.insert("conversation_id".to_string(), conversation_id.to_string());
        attributes.insert("source".to_string(), "access_gateway".to_string());

        // 从当前 Span 提取追踪信息（如果存在）
        let trace_context = Span::current()
            .id()
            .and_then(|span_id| {
                // 尝试从 tracing span 提取追踪信息
                // 这里使用 span_id 作为 trace_id，实际生产环境应该使用分布式追踪系统（如 Jaeger）
                Some(TraceContext {
                    trace_id: uuid::Uuid::new_v4().to_string(), // 生成新的 trace_id
                    span_id: format!("{}", span_id.into_u64()), // 使用当前 span_id
                    parent_span_id: String::new(), // 父 span_id（如果有的话）
                    sampled: "yes".to_string(),
                    tags: std::collections::HashMap::new(),
                })
            });

        RequestContext {
            request_id: uuid::Uuid::new_v4().to_string(),
            trace: trace_context,
            actor: Some(flare_proto::common::ActorContext {
                actor_id: actor_id.to_string(),
                r#type: flare_proto::common::ActorType::User as i32, // ACTOR_TYPE_USER = 1
                roles: vec![],
                attributes: std::collections::HashMap::new(),
            }),
            device: None, // 设备信息应该从连接管理器中获取，这里暂时为空
            channel: "websocket".to_string(), // Gateway 使用的通道类型
            user_agent: String::new(),
            attributes,
        }
    }

    /// 构建 TenantContext（从连接 metadata 中提取，如果没有则使用默认值）
    fn build_tenant_context(&self, connection_metadata: Option<&std::collections::HashMap<String, String>>) -> TenantContext {
        let tenant_ctx = if let Some(metadata) = connection_metadata {
            build_tenant_context_from_metadata(metadata, &self.default_tenant_id)
        } else {
            flare_server_core::context::TenantContext::new(&self.default_tenant_id)
                .with_business_type("chatroom")
        };
        // 转换为 proto 类型
        #[cfg(feature = "proto")]
        {
            flare_proto::common::TenantContext::from(tenant_ctx)
        }
        #[cfg(not(feature = "proto"))]
        {
            // 如果没有 proto feature，构建默认的 proto 类型
            TenantContext {
                tenant_id: self.default_tenant_id.clone(),
                business_type: "chatroom".to_string(),
                environment: String::new(),
                organization_id: String::new(),
                labels: std::collections::HashMap::new(),
                attributes: std::collections::HashMap::new(),
            }
        }
    }
    
    /// 构建 RequestContext（从连接 metadata 中提取用户ID等信息）
    fn build_request_context_with_trace_from_metadata(
        &self,
        connection_metadata: Option<&std::collections::HashMap<String, String>>,
        user_id: Option<&str>,
    ) -> RequestContext {
        if let Some(metadata) = connection_metadata {
            build_request_context_from_metadata(metadata, user_id).into()
        } else {
            let actor_id = user_id.unwrap_or("unknown");
            let conversation_id = "unknown";
            self.build_request_context_with_trace(actor_id, conversation_id).into()
        }
    }

    /// 从连接管理器中获取连接的 metadata（内部辅助函数）
    pub async fn get_connection_metadata(
        &self,
        connection_id: &str,
    ) -> Option<std::collections::HashMap<String, String>> {
        if let Some(ref manager_mutex) = self.connection_manager {
            let manager_guard: tokio::sync::MutexGuard<'_, Option<Arc<dyn flare_core::server::connection::ConnectionManagerTrait>>> = manager_mutex.lock().await;
            if let Some(ref manager) = *manager_guard {
                if let Some((_, conn_info)) = manager.get_connection(connection_id).await {
                    return Some(conn_info.metadata.clone());
                }
            }
        }
        None
    }
}
