//! Route 服务客户端
//!
//! 负责与 Route 服务通信，转发消息到业务系统

use std::sync::Arc;

use anyhow::{Context as AnyhowContext, Result};
use flare_proto::common::TenantContext;
use flare_proto::signaling::router::router_service_client::RouterServiceClient;
use flare_proto::signaling::router::{SelectPushTargetsRequest, SelectPushTargetsResponse};
use prost::Message as ProstMessage;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::{error, info};

use flare_server_core::context::Context;
use flare_server_core::discovery::ServiceClient;

/// Route 服务客户端
pub struct RouteServiceClient {
    client: Arc<Mutex<Option<RouterServiceClient<Channel>>>>,
    service_name: String,
    default_tenant_id: String,
    service_client: Arc<Mutex<Option<ServiceClient>>>,
}

impl RouteServiceClient {
    /// 创建新的 Route 服务客户端（使用服务名称，内部创建服务发现）
    pub fn new(service_name: String, default_tenant_id: String) -> Self {
        Self {
            client: Arc::new(Mutex::new(None)),
            service_name,
            default_tenant_id,
            service_client: Arc::new(Mutex::new(None)),
        }
    }

    /// 使用 ServiceClient 创建新的 Route 服务客户端（推荐，通过 wire 注入）
    pub fn with_service_client(service_client: ServiceClient, default_tenant_id: String) -> Self {
        Self {
            client: Arc::new(Mutex::new(None)),
            service_name: String::new(),
            default_tenant_id,
            service_client: Arc::new(Mutex::new(Some(service_client))),
        }
    }

    /// 初始化客户端连接
    pub async fn initialize(&self) -> Result<()> {
        let mut client_guard = self.client.lock().await;
        if client_guard.is_some() {
            return Ok(());
        }

        info!(
            service_name = %self.service_name,
            "Initializing Route Service client..."
        );

        // 使用服务发现获取 Channel
        let mut service_client_guard = self.service_client.lock().await;
        if service_client_guard.is_none() {
            // 如果没有注入 ServiceClient，则创建服务发现器
            let discover_result = flare_im_core::discovery::create_discover(&self.service_name)
                .await
                .map_err(|e| {
                    error!(
                        service_name = %self.service_name,
                        error = %e,
                        "Failed to create service discover for route service"
                    );
                    anyhow::anyhow!("Failed to create service discover: {}", e)
                })?;

            let discover = discover_result.ok_or_else(|| {
                anyhow::anyhow!("Service discovery not configured for route service")
            })?;

            *service_client_guard = Some(ServiceClient::new(discover));
        }

        let service_client = service_client_guard.as_mut().unwrap();
        let channel = service_client.get_channel().await.map_err(|e| {
            error!(
                service_name = %self.service_name,
                error = %e,
                "Failed to get channel from service discovery"
            );
            anyhow::anyhow!("Failed to get channel: {}", e)
        })?;

        info!(
            service_name = %self.service_name,
            "Successfully got channel from service discovery"
        );

        let client = RouterServiceClient::new(channel);
        *client_guard = Some(client);

        info!(
            service_name = %self.service_name,
            "✅ Route Service client initialized successfully"
        );
        Ok(())
    }

    /// 选择推送目标设备
    #[tracing::instrument(skip(self, ctx))]
    pub async fn select_push_targets(
        &self,
        ctx: &Context,
        user_id: &str,
        strategy: flare_proto::signaling::router::PushStrategy,
    ) -> Result<SelectPushTargetsResponse> {
        self.initialize()
            .await
            .with_context(|| "Failed to initialize Route Service client")?;

        let mut client_guard = self.client.lock().await;
        let client = client_guard
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Route Service client not available"))?;

        let request = SelectPushTargetsRequest {
            user_id: user_id.to_string(),
            strategy: strategy as i32,
        };

        let response = client
            .select_push_targets(tonic::Request::new(request))
            .await
            .with_context(|| "Failed to call SelectPushTargets RPC")?;

        Ok(response.into_inner())
    }
}
