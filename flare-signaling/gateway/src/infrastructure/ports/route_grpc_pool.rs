//! Signaling Route gRPC 客户端池（与 [`super::connection_port::ConnectionRepository`] 相同的服务发现模式）

use flare_grpc_proto::signaling::router::router_upstream_service_client::RouterUpstreamServiceClient;
use flare_im_core::discovery::connect_grpc_channel_resilient;
use flare_im_core::service_names::{SIGNALING_ROUTE, get_service_name};
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use tokio::sync::Mutex;
use tonic::transport::Channel;

const DEFAULT_SIGNALING_ROUTE_URI: &str = "http://127.0.0.1:50062";

/// 懒加载、可共享的 `RouterIngressServiceClient`（多端口实现共用）
pub struct SignalingRouteGrpcPool {
    service_name: String,
    grpc: Mutex<Option<RouterUpstreamServiceClient<Channel>>>,
}

impl SignalingRouteGrpcPool {
    pub fn new() -> Self {
        Self {
            service_name: get_service_name(SIGNALING_ROUTE),
            grpc: Mutex::new(None),
        }
    }

    pub async fn ensure_client(&self) -> Result<RouterUpstreamServiceClient<Channel>> {
        let mut guard = self.grpc.lock().await;
        if let Some(c) = guard.as_ref() {
            return Ok(c.clone());
        }

        let channel =
            connect_grpc_channel_resilient(&self.service_name, DEFAULT_SIGNALING_ROUTE_URI)
                .await
                .map_err(|e| {
                    ErrorBuilder::new(ErrorCode::ServiceUnavailable, "signaling route unavailable")
                        .details(e)
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
