//! `ExecuteEvent` gRPC 适配器。
//!
//! orchestrator 只保留事件入口；发送类 RPC 由 `flare-message-ingest` 持有。

use std::sync::Arc;

use crate::application::handlers::EventHandler;
use flare_grpc_proto::message::ExecuteEventRequest;
use flare_grpc_proto::message::message_event_service_server::MessageEventService;
use flare_server_core::error::grpc::IntoGrpc;
use flare_server_core::utils::require_ctx_from_request;
use tonic::{Request, Response, Status};
use tracing::{debug, instrument};

#[derive(Clone)]
pub struct MessageEventExecuteGrpcHandler {
    event_handler: Arc<EventHandler>,
}

impl MessageEventExecuteGrpcHandler {
    pub fn new(event_handler: Arc<EventHandler>) -> Self {
        Self { event_handler }
    }
}

#[tonic::async_trait]
impl MessageEventService for MessageEventExecuteGrpcHandler {
    #[instrument(skip(self, request))]
    async fn execute_event(
        &self,
        request: Request<ExecuteEventRequest>,
    ) -> Result<Response<()>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        if !req.svid.is_empty() {
            debug!(svid = %req.svid, "ExecuteEvent");
        }
        let event = req
            .event
            .ok_or_else(|| Status::invalid_argument("event required"))?;

        self.event_handler
            .handle_event(&ctx, event)
            .await
            .into_grpc()?;

        Ok(Response::new(()))
    }
}
