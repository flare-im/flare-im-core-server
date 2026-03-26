//! Signaling Route gRPC 客户端池（与 [`super::connection_port::ConnectionRepository`] 相同的服务发现模式）

use flare_im_core::service_names::{get_service_name, SIGNALING_ROUTE};
use flare_proto::signaling::router::router_upstream_service_client::RouterUpstreamServiceClient;
use flare_server_core::discovery::ServiceClient;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use tokio::sync::Mutex;
use tonic::transport::Channel;

/// 懒加载、可共享的 `RouterUpstreamServiceClient`（多端口实现共用）
pub struct SignalingRouteGrpcPool {
    service_name: String,
    service_client: Mutex<Option<ServiceClient>>,
    grpc: Mutex<Option<RouterUpstreamServiceClient<Channel>>>,
}

impl SignalingRouteGrpcPool {
    pub fn new() -> Self {
        Self {
            service_name: get_service_name(SIGNALING_ROUTE),
            service_client: Mutex::new(None),
            grpc: Mutex::new(None),
        }
    }

    pub async fn ensure_client(&self) -> Result<RouterUpstreamServiceClient<Channel>> {
        let mut guard = self.grpc.lock().await;
        if let Some(c) = guard.as_ref() {
            return Ok(c.clone());
        }

        let mut sc_guard = self.service_client.lock().await;
        if sc_guard.is_none() {
            let discover = flare_im_core::discovery::create_discover(&self.service_name)
                .await
                .map_err(|e| {
                    ErrorBuilder::new(
                        ErrorCode::ServiceUnavailable,
                        "signaling route service unavailable",
                    )
                    .details(format!(
                        "Failed to create service discover for {}: {}",
                        self.service_name, e
                    ))
                    .build_error()
                })?;

            if let Some(discover) = discover {
                *sc_guard = Some(ServiceClient::new(discover));
            } else {
                return Err(
                    ErrorBuilder::new(
                        ErrorCode::ServiceUnavailable,
                        "signaling route service unavailable",
                    )
                    .details("Service discovery not configured for route")
                    .build_error(),
                );
            }
        }

        let service_client = sc_guard.as_mut().ok_or_else(|| {
            ErrorBuilder::new(ErrorCode::InternalError, "route service client not initialized")
                .build_error()
        })?;
        let channel = service_client.get_channel().await.map_err(|e| {
            ErrorBuilder::new(ErrorCode::ServiceUnavailable, "signaling route unavailable")
                .details(format!("Failed to get channel: {}", e))
                .build_error()
        })?;

        let client = RouterUpstreamServiceClient::new(channel);
        *guard = Some(client.clone());
        Ok(client)
    }
}

impl Default for SignalingRouteGrpcPool {
    fn default() -> Self {
        Self::new()
    }
}
