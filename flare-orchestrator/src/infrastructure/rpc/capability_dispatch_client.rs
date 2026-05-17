//! `flare.capability.v1.CapabilityService` 客户端（编排器侧，仅 Dispatch）。
//!
//! 不在启动阶段连接 capability；每次 `dispatch` 经注册中心发现或静态回退，失败返回明确错误。

use std::sync::Arc;
use std::time::Duration;

use flare_grpc_proto::capability::capability_service_client::CapabilityServiceClient;
use flare_grpc_proto::capability::{
    CapabilityDispatchResult, DispatchCapabilityRequest, DispatchCapabilityResponse,
};
use flare_im_core::config::FlareAppConfig;
use flare_im_core::service_names::CAPABILITY;
use flare_server_core::ServiceClient;
use flare_server_core::client::set_context_metadata;
use flare_server_core::context::Context;
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError};
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};
use tracing::instrument;

use crate::error::Result;

const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(10);
const STATIC_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// 能力服务 gRPC 客户端（当前仅封装 `Dispatch`，供 RTC 通话与 `EVENT_CALL_SIGNAL` 联动）。
pub struct CapabilityDispatchClient {
    discover: Option<Arc<Mutex<ServiceClient>>>,
    static_fallback: String,
}

impl CapabilityDispatchClient {
    /// 启动时不建连；`dispatch` 时再解析 `flare-capability` 通道。
    pub async fn from_app_config(
        app_config: &FlareAppConfig,
        static_fallback: impl Into<String>,
    ) -> std::result::Result<Self, FlareError> {
        let static_fallback = static_fallback.into();
        let discover =
            flare_im_core::discovery::create_discover_from_config(app_config, CAPABILITY)
                .await
                .map_err(|e| {
                    FlareError::system(format!(
                        "create capability service discover ({CAPABILITY}): {e}"
                    ))
                })?
                .map(|discover| Arc::new(Mutex::new(ServiceClient::new(discover))));
        Ok(Self {
            discover,
            static_fallback,
        })
    }

    async fn resolve_channel(&self) -> std::result::Result<Channel, FlareError> {
        if let Some(client) = &self.discover {
            let mut guard = client.lock().await;
            match tokio::time::timeout(DISCOVERY_TIMEOUT, guard.get_channel()).await {
                Ok(Ok(channel)) => return Ok(channel),
                Ok(Err(e)) => {
                    tracing::debug!(
                        error = %e,
                        fallback = %self.static_fallback,
                        "capability discovery failed; trying static fallback"
                    );
                }
                Err(_) => {
                    tracing::debug!(
                        fallback = %self.static_fallback,
                        "capability discovery timed out; trying static fallback"
                    );
                }
            }
        }

        let endpoint = Endpoint::from_shared(self.static_fallback.clone()).map_err(|e| {
            FlareError::system(format!(
                "invalid capability static gRPC URI {}: {e}",
                self.static_fallback
            ))
        })?;
        tokio::time::timeout(STATIC_CONNECT_TIMEOUT, endpoint.connect())
            .await
            .map_err(|_| {
                FlareError::system(format!(
                    "timeout connecting to capability static endpoint {}",
                    self.static_fallback
                ))
            })?
            .map_err(|e| {
                FlareError::system(format!(
                    "failed to connect capability static endpoint {}: {e}",
                    self.static_fallback
                ))
            })
    }

    /// 调用 `Dispatch`；`success == false` 或空 `result` 时返回业务错误。
    #[instrument(skip(self, ctx, req), fields(capability_id = %req.capability_id))]
    pub async fn dispatch(
        &self,
        ctx: &Context,
        req: DispatchCapabilityRequest,
    ) -> Result<CapabilityDispatchResult> {
        let channel = self.resolve_channel().await?;
        let mut client = CapabilityServiceClient::new(channel);

        let mut grpc = tonic::Request::new(req);
        set_context_metadata(&mut grpc, ctx);

        let resp: DispatchCapabilityResponse = client
            .dispatch(grpc)
            .await
            .map_err(|e| FlareError::system(format!("capability Dispatch gRPC failed: {e}")))?
            .into_inner();

        let result = resp
            .result
            .ok_or_else(|| FlareError::system("capability Dispatch: empty result"))?;

        if !result.success {
            return Err(ErrorBuilder::new(
                ErrorCode::OperationFailed,
                "capability Dispatch rejected",
            )
            .details(result.error_message.clone())
            .build_error());
        }

        Ok(result)
    }
}
