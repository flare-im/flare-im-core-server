//! [`IEventCommandPort`]：经 Signaling **Router** `RouteEvent`（与 `domain/ports/event_prot.rs` 对应）

use std::sync::Arc;

use async_trait::async_trait;
use flare_im_core::Ctx;
use flare_proto::common::{ErrorCode, Event, OperationResponse, RpcStatus};
use flare_proto::signaling::router::{RouteEventRequest, RouteOptions};
use flare_server_core::client::request_with_context;
use flare_server_core::error::{ErrorBuilder, ErrorCode as ServerErrorCode, Result};
use prost::Message as ProstMessage;

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
    async fn send_event(&self, tx: &Ctx, event: Event) -> Result<OperationResponse> {
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
        let resp = client.route_event(request).await.map_err(|status| {
            ErrorBuilder::new(ServerErrorCode::ServiceUnavailable, "RouteEvent RPC failed")
                .details(status.to_string())
                .build_error()
        })?;

        let inner = resp.into_inner();

        if let Some(st) = inner.status.as_ref()
            && st.code != ErrorCode::Ok as i32
        {
            return Ok(OperationResponse {
                request_id: None,
                status: inner.status,
            });
        }

        if inner.response_data.is_empty() {
            return Ok(OperationResponse {
                request_id: None,
                status: Some(RpcStatus {
                    code: ErrorCode::Internal as i32,
                    message: "empty RouteEventResponse.response_data".to_string(),
                    details: Vec::new(),
                    context: None,
                    localization_key: String::new(),
                    localization_params: Default::default(),
                }),
            });
        }

        OperationResponse::decode(inner.response_data.as_slice()).map_err(|e| {
            ErrorBuilder::new(ServerErrorCode::InternalError, "decode OperationResponse")
                .details(e.to_string())
                .build_error()
        })
    }
}
