//! Signaling Online客户端（用于批量查询在线状态）

use std::collections::HashMap;
use std::sync::Arc;

use flare_proto::signaling::online::online_service_client::OnlineServiceClient;
use flare_proto::signaling::online::{GetOnlineStatusRequest, GetOnlineStatusResponse};
use flare_server_core::context::Context;
use flare_server_core::discovery::ServiceClient;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::domain::repository::OnlineStatus;

/// Signaling Online客户端
pub struct SignalingOnlineClient {
    service_name: String,
    service_client: Mutex<Option<ServiceClient>>,
    client: Mutex<Option<OnlineServiceClient<Channel>>>,
}

impl SignalingOnlineClient {
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

        let service_client = service_client_guard.as_mut().ok_or_else(|| {
            ErrorBuilder::new(
                ErrorCode::ServiceUnavailable,
                "signaling service unavailable",
            )
            .details("Service client not initialized")
            .build_error()
        })?;
        // 添加超时保护，避免服务发现阻塞过长时间
        let channel = tokio::time::timeout(
            std::time::Duration::from_secs(3), // 3秒超时
            service_client.get_channel(),
        )
        .await
        .map_err(|_| {
            ErrorBuilder::new(ErrorCode::ServiceUnavailable, "signaling unavailable")
                .details("Timeout waiting for service discovery to get channel (3s)")
                .build_error()
        })?
        .map_err(|e| {
            ErrorBuilder::new(ErrorCode::ServiceUnavailable, "signaling unavailable")
                .details(format!("Failed to get channel: {}", e))
                .build_error()
        })?;

        tracing::debug!("Got channel for signaling service from service discovery");

        let client = OnlineServiceClient::new(channel);
        *guard = Some(client.clone());
        Ok(client)
    }

    /// 批量查询用户在线状态
    pub async fn batch_get_online_status(
        &self,
        ctx: &Context,
        user_ids: &[String],
    ) -> Result<HashMap<String, OnlineStatus>> {
        let mut client = self.ensure_client().await?;

        let request = GetOnlineStatusRequest {
            user_ids: user_ids.to_vec(),
        };

        let response: GetOnlineStatusResponse = client
            .get_online_status(tonic::Request::new(request))
            .await
            .map_err(|status| {
                ErrorBuilder::new(ErrorCode::ServiceUnavailable, "signaling query failed")
                    .details(status.to_string())
                    .build_error()
            })?
            .into_inner();

        // 转换为OnlineStatus映射
        let mut result = HashMap::new();
        for (user_id, status) in response.statuses {
            // 直接使用 gateway_id（必需字段，不再兼容旧版本）
            let gateway_id = if !status.gateway_id.is_empty() {
                Some(status.gateway_id.clone())
            } else {
                None
            };

            let server_id = if !status.server_id.is_empty() {
                Some(status.server_id.clone())
            } else {
                None
            };

            result.insert(
                user_id.clone(),
                OnlineStatus {
                    user_id,
                    online: status.online,
                    gateway_id,
                    server_id,
                },
            );
        }

        Ok(result)
    }
}
