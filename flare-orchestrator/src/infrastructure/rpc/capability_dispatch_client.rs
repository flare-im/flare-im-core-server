//! `flare.capability.v1.CapabilityService` 客户端（编排器侧，仅 Dispatch）。

use flare_grpc_proto::capability::capability_service_client::CapabilityServiceClient;
use flare_grpc_proto::capability::{
    CapabilityDispatchResult, DispatchCapabilityRequest, DispatchCapabilityResponse,
};
use flare_server_core::client::set_context_metadata;
use flare_server_core::context::Context;
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError};
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::instrument;

use crate::error::Result;

/// 能力服务 gRPC 客户端（当前仅封装 `Dispatch`，供 RTC 通话与 `EVENT_CALL_SIGNAL` 联动）。
#[derive(Debug)]
pub struct CapabilityDispatchClient {
    inner: Mutex<CapabilityServiceClient<Channel>>,
}

impl CapabilityDispatchClient {
    pub fn new(channel: Channel) -> Self {
        Self {
            inner: Mutex::new(CapabilityServiceClient::new(channel)),
        }
    }

    /// 调用 `Dispatch`；`success == false` 或空 `result` 时返回业务错误。
    #[instrument(skip(self, ctx, req), fields(capability_id = %req.capability_id))]
    pub async fn dispatch(
        &self,
        ctx: &Context,
        req: DispatchCapabilityRequest,
    ) -> Result<CapabilityDispatchResult> {
        let mut grpc = tonic::Request::new(req);
        set_context_metadata(&mut grpc, ctx);

        let mut client = self.inner.lock().await;
        let resp: DispatchCapabilityResponse = client
            .dispatch(grpc)
            .await
            .map_err(|e| {
                FlareError::system(format!("capability Dispatch gRPC failed: {e}"))
            })?
            .into_inner();

        let result = resp.result.ok_or_else(|| {
            FlareError::system("capability Dispatch: empty result")
        })?;

        if !result.success {
            return Err(
                ErrorBuilder::new(
                    ErrorCode::OperationFailed,
                    "capability Dispatch rejected",
                )
                .details(result.error_message.clone())
                .build_error(),
            );
        }

        Ok(result)
    }
}

