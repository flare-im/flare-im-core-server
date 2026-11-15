//! Push Proxy gRPC 客户端池（与 [`super::route_grpc_pool::SignalingRouteGrpcPool`] 相同的服务发现模式）

use flare_grpc_proto::push::push_service_client::PushServiceClient;
use flare_im_contracts::service_names::{PUSH_PROXY, get_service_name};
use flare_im_service_kit::discovery::connect_grpc_channel_resilient;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use tokio::sync::Mutex;
use tonic::transport::Channel;

const DEFAULT_PUSH_PROXY_URI: &str = "http://127.0.0.1:50091";

/// 懒加载、可共享的 `PushServiceClient`
pub struct PushProxyGrpcPool {
    service_name: String,
    grpc: Mutex<Option<PushServiceClient<Channel>>>,
}

impl PushProxyGrpcPool {
    pub fn new() -> Self {
        Self {
            service_name: get_service_name(PUSH_PROXY),
            grpc: Mutex::new(None),
        }
    }

    pub async fn ensure_client(&self) -> Result<PushServiceClient<Channel>> {
        let mut guard = self.grpc.lock().await;
        if let Some(c) = guard.as_ref() {
            return Ok(c.clone());
        }

        let channel = connect_grpc_channel_resilient(&self.service_name, DEFAULT_PUSH_PROXY_URI)
            .await
            .map_err(|e| {
                ErrorBuilder::new(ErrorCode::ServiceUnavailable, "push proxy unavailable")
                    .details(e)
                    .build_error()
            })?;

        let client = PushServiceClient::new(channel);
        *guard = Some(client.clone());
        Ok(client)
    }
}

impl Default for PushProxyGrpcPool {
    fn default() -> Self {
        Self::new()
    }
}
