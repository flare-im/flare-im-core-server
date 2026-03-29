use std::sync::Arc;

use flare_proto::push::push_service_client::PushServiceClient;
use flare_proto::push::{
    PushMessageRequest, PushMessageResponse, PushNotificationRequest, PushNotificationResponse,
};
use flare_server_core::discovery::ServiceClient;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use tokio::sync::Mutex;
use tonic::transport::Channel;

pub trait PushClient: Send + Sync {
    async fn push_message(&self, request: PushMessageRequest) -> Result<PushMessageResponse>;
    async fn push_notification(
        &self,
        request: PushNotificationRequest,
    ) -> Result<PushNotificationResponse>;
}

pub struct GrpcPushClient {
    service_name: String,
    service_client: Mutex<Option<ServiceClient>>,
    client: Mutex<Option<PushServiceClient<Channel>>>,
}

impl GrpcPushClient {
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

    async fn ensure_client(&self) -> Result<PushServiceClient<Channel>> {
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
                    ErrorBuilder::new(ErrorCode::ServiceUnavailable, "push service unavailable")
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
                    "push service unavailable",
                )
                .details("Service discovery not configured")
                .build_error());
            }
        }

        let service_client = service_client_guard.as_mut().unwrap();
        let channel = service_client.get_channel().await.map_err(|e| {
            ErrorBuilder::new(ErrorCode::ServiceUnavailable, "push service unavailable")
                .details(format!("Failed to get channel: {}", e))
                .build_error()
        })?;

        tracing::debug!("Got channel for push service from service discovery");

        let client = PushServiceClient::new(channel);
        *guard = Some(client.clone());
        Ok(client)
    }
}

impl PushClient for GrpcPushClient {
    async fn push_message(&self, request: PushMessageRequest) -> Result<PushMessageResponse> {
        let mut client = self.ensure_client().await?;
        client
            .push_message(request)
            .await
            .map(|resp| resp.into_inner())
            .map_err(|status| {
                ErrorBuilder::new(ErrorCode::ServiceUnavailable, "push message failed")
                    .details(status.to_string())
                    .build_error()
            })
    }

    async fn push_notification(
        &self,
        request: PushNotificationRequest,
    ) -> Result<PushNotificationResponse> {
        let mut client = self.ensure_client().await?;
        client
            .push_notification(request)
            .await
            .map(|resp| resp.into_inner())
            .map_err(|status| {
                ErrorBuilder::new(ErrorCode::ServiceUnavailable, "push notification failed")
                    .details(status.to_string())
                    .build_error()
            })
    }
}
