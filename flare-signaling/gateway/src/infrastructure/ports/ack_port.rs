//! [`IAckReportPort`]：经 Router `RouteAck` 投递 [`flare_proto::common::Ack`]（与 `domain/ports/ack_port.rs` 对应）

use std::sync::Arc;

use async_trait::async_trait;
use flare_im_core::Ctx;
use flare_proto::common::ack::Payload as AckPayload;
use flare_proto::common::{Ack, AckBatch, AckType, ConversationAck, PushAck};
use flare_proto::signaling::router::{RouteAckRequest, RouteOptions};
use flare_server_core::client::request_with_context;
use flare_server_core::error::{ErrorBuilder, ErrorCode as ServerErrorCode, Result};
use crate::constants::DEFAULT_ROUTE_SVID;
use crate::domain::ports::IAckReportPort;

use super::route_grpc_pool::SignalingRouteGrpcPool;

pub struct RouterAckReportPort {
    pool: Arc<SignalingRouteGrpcPool>,
}

impl RouterAckReportPort {
    pub fn new(pool: Arc<SignalingRouteGrpcPool>) -> Self {
        Self { pool }
    }

    async fn route_ack(&self, tx: &Ctx, ack: Ack) -> Result<()> {
        let mut client = self.pool.ensure_client().await?;
        let req = RouteAckRequest {
            svid: DEFAULT_ROUTE_SVID.to_string(),
            ack: Some(ack),
            options: Some(RouteOptions {
                timeout_seconds: 10,
                enable_tracing: true,
                retry_strategy: 1,
                load_balance_strategy: 1,
                priority: 0,
            }),
        };

        let request = request_with_context(req, tx);
        client.route_ack(request).await.map_err(|status| {
            ErrorBuilder::new(ServerErrorCode::ServiceUnavailable, "RouteAck RPC failed")
                .details(status.to_string())
                .build_error()
        })?;
        Ok(())
    }
}

#[async_trait]
impl IAckReportPort for RouterAckReportPort {
    async fn report_push_ack(&self, tx: &Ctx, ack: PushAck) -> Result<()> {
        let wrapped = Ack {
            r#type: AckType::Push as i32,
            ack_id: ack.ack_id.clone(),
            at: ack.ack_at,
            payload: Some(AckPayload::Push(ack)),
        };
        self.route_ack(tx, wrapped).await
    }

    async fn report_conversation_ack(&self, tx: &Ctx, ack: ConversationAck) -> Result<()> {
        let wrapped = Ack {
            r#type: AckType::Converstion as i32,
            ack_id: None,
            at: None,
            payload: Some(AckPayload::Conversation(ack)),
        };
        self.route_ack(tx, wrapped).await
    }

    async fn report_ack_batch(&self, tx: &Ctx, batch: AckBatch) -> Result<()> {
        let wrapped = Ack {
            r#type: AckType::Batch as i32,
            ack_id: None,
            at: None,
            payload: Some(AckPayload::Batch(batch)),
        };
        self.route_ack(tx, wrapped).await
    }
}
