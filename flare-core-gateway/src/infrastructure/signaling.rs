use flare_proto::signaling::online::online_service_client::OnlineServiceClient;
use flare_proto::signaling::online::{
    GetOnlineStatusRequest, GetOnlineStatusResponse, LoginRequest, LoginResponse, LogoutRequest,
    LogoutResponse,
};
use flare_server_core::discovery::ServiceClient;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Channel;

pub trait SignalingClient: Send + Sync {
    async fn login(&self, request: LoginRequest) -> Result<LoginResponse>;
    async fn logout(&self, request: LogoutRequest) -> Result<LogoutResponse>;
    async fn get_online_status(
        &self,
        request: GetOnlineStatusRequest,
    ) -> Result<GetOnlineStatusResponse>;
}

pub struct GrpcSignalingClient {
    service_name: String,
    service_client: Mutex<Option<ServiceClient>>,
    client: Mutex<Option<OnlineServiceClient<Channel>>>,
}

impl GrpcSignalingClient {
    /// 创建新的客户端（使用服务名称，内部创建服务发现）
    pub fn new(service_name: String) -> Arc<Self> {
        Arc::new(Self {
            service_name,
            service_client: Mutex::new(None),
            client: Mutex::new(None),
        })
    }

    /// 使用 ServiceClient 创建新的客户端（推荐，通过 wire 注入）
    pub fn with_service_client(service_client: ServiceClient) -> Arc<Self> {
        Arc::new(Self {
            service_name: String::new(), // 不需要 service_name
            service_client: Mutex::new(Some(service_client)),
            client: Mutex::new(None),
        })
    }

    async fn ensure_client(&self) -> Result<OnlineServiceClient<Channel>> {
        let mut guard = self.client.lock().await;
        if let Some(client) = guard.as_ref() {
            return Ok(client.clone());
        }

        // 使用服务发现获取 Channel
        let mut service_client_guard = self.service_client.lock().await;
        if service_client_guard.is_none() {
            // 如果没有注入 ServiceClient，则创建服务发现器
            let discover = flare_im_core::discovery::create_discover(&self.service_name)
                .await
                .map_err(|e| {
                    ErrorBuilder::new(
                        ErrorCode::ServiceUnavailable,
                        "signaling service unavailable",
                    )
                    .details(format!(
                        "Failed to create service discover for {}: {}",
                        self.service_name, e
                    ))
                    .build_error()
                })?;

            if let Some(discover) = discover {
                *service_client_guard = Some(ServiceClient::new(discover));
            } else {
                return Err(ErrorBuilder::new(
                    ErrorCode::ServiceUnavailable,
                    "signaling service unavailable",
                )
                .details("Service discovery not configured")
                .build_error());
            }
        }

        let service_client = service_client_guard.as_mut().unwrap();
        let channel = service_client.get_channel().await.map_err(|e| {
            ErrorBuilder::new(ErrorCode::ServiceUnavailable, "signaling unavailable")
                .details(format!("Failed to get channel: {}", e))
                .build_error()
        })?;

        tracing::debug!("Got channel for signaling service from service discovery");

        let client = OnlineServiceClient::new(channel);
        *guard = Some(client.clone());
        Ok(client)
    }
}

impl SignalingClient for GrpcSignalingClient {
    async fn login(&self, request: LoginRequest) -> Result<LoginResponse> {
        let mut client = self.ensure_client().await?;
        client
            .login(request)
            .await
            .map(|resp| resp.into_inner())
            .map_err(|status| {
                ErrorBuilder::new(ErrorCode::ServiceUnavailable, "signaling login failed")
                    .details(status.to_string())
                    .build_error()
            })
    }

    async fn logout(&self, request: LogoutRequest) -> Result<LogoutResponse> {
        let mut client = self.ensure_client().await?;

        client
            .logout(request)
            .await
            .map(|resp| resp.into_inner())
            .map_err(|status| {
                ErrorBuilder::new(ErrorCode::ServiceUnavailable, "signaling logout failed")
                    .details(status.to_string())
                    .build_error()
            })
    }

    async fn get_online_status(
        &self,
        request: GetOnlineStatusRequest,
    ) -> Result<GetOnlineStatusResponse> {
        let mut client = self.ensure_client().await?;

        client
            .get_online_status(request)
            .await
            .map(|resp| resp.into_inner())
            .map_err(|status| {
                ErrorBuilder::new(
                    ErrorCode::ServiceUnavailable,
                    "signaling get_online_status failed",
                )
                .details(status.to_string())
                .build_error()
            })
    }
}
