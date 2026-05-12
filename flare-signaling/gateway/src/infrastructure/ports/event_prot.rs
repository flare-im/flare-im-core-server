//! [`IEventCommandPort`]：经 Signaling **Router** `RouteEvent`（与 `domain/ports/event_prot.rs` 对应）
//!
//! 成功时 `RouteEventResponse.response_data` 可为空（业务仅通过 gRPC OK 表达）；不再解码 `OperationResponse`。

use std::sync::Arc;

use async_trait::async_trait;
use flare_grpc_proto::signaling::router::{RouteEventRequest, RouteOptions};
use flare_im_core::Ctx;
use flare_proto::common::Event;
use flare_server_core::client::request_with_context;
use flare_server_core::error::{ErrorBuilder, ErrorCode as ServerErrorCode, Result};

use crate::constants::DEFAULT_ROUTE_SVID;
use crate::domain::ports::IEventCommandPort;

use super::route_grpc_pool::SignalingRouteGrpcPool;

pub struct RouterEventCommandPort {
    pool: Arc<SignalingRouteGrpcPool>,
}

impl RouterEventCommandPort {
    pub fn new(pool: Arc<SignalingRouteGrpcPool>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl IEventCommandPort for RouterEventCommandPort {
    async fn send_event(&self, tx: &Ctx, event: Event) -> Result<()> {
        let mut client = self.pool.ensure_client().await?;
        let req = RouteEventRequest {
            svid: DEFAULT_ROUTE_SVID.to_string(),
            event: Some(event),
            options: Some(RouteOptions {
                timeout_seconds: 15,
                enable_tracing: true,
                retry_strategy: 1,
                load_balance_strategy: 1,
                priority: 0,
            }),
        };

        let request = request_with_context(req, tx);
        client.route_event(request).await.map_err(|status| {
            ErrorBuilder::new(ServerErrorCode::ServiceUnavailable, "RouteEvent RPC failed")
                .details(status.to_string())
                .build_error()
        })?;
        Ok(())
    }
}
