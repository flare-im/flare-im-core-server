//! [`IDataCommandPort`]：经 Router `RouteData` 投递 **内层** [`CustomData`]（`DataPacket.user_custom` 解包后的业务载荷）

use std::sync::Arc;

use async_trait::async_trait;
use flare_im_core::Ctx;
use flare_proto::common::CustomData;
use flare_grpc_proto::signaling::router::{RouteDataRequest, RouteOptions};
use flare_server_core::client::request_with_context;
use flare_server_core::error::{ErrorBuilder, ErrorCode as ServerErrorCode, Result};

use crate::constants::DEFAULT_ROUTE_SVID;
use crate::domain::ports::IDataCommandPort;

use super::route_grpc_pool::SignalingRouteGrpcPool;

pub struct RouterDataCommandPort {
    pool: Arc<SignalingRouteGrpcPool>,
}

impl RouterDataCommandPort {
    pub fn new(pool: Arc<SignalingRouteGrpcPool>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl IDataCommandPort for RouterDataCommandPort {
    async fn send_data(&self, tx: &Ctx, data: CustomData) -> Result<Option<Vec<u8>>> {
        let mut client = self.pool.ensure_client().await?;
        let req = RouteDataRequest {
            svid: DEFAULT_ROUTE_SVID.to_string(),
            data: Some(data),
            options: Some(RouteOptions {
                timeout_seconds: 30,
                enable_tracing: true,
                retry_strategy: 1,
                load_balance_strategy: 1,
                priority: 0,
            }),
        };

        let request = request_with_context(req, tx);
        let resp = client.route_data(request).await.map_err(|status| {
            ErrorBuilder::new(ServerErrorCode::ServiceUnavailable, "RouteData RPC failed")
                .details(status.to_string())
                .build_error()
        })?;

        let inner = resp.into_inner();
        if inner.response_data.is_empty() {
            Ok(None)
        } else {
            Ok(Some(inner.response_data))
        }
    }
}
