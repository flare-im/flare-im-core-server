//! 客户端 ACK 上行路由：Push / Conversation / Batch → Push Proxy（Kafka ack topic）。

use std::sync::Arc;
use std::time::Instant;

use flare_proto::common::Ack;
use flare_proto::common::ack::Payload as AckPayload;
use flare_grpc_proto::signaling::router::RouteOptions;
use flare_server_core::context::Context;
use flare_im_core::error::{ErrorCode, Result, map_infra_error};
use flare_server_core::flare_err;
use tracing::instrument;

use crate::application::dto::{MessageRouteResult, build_route_metadata};
use crate::infrastructure::AckToPushProxyForwarder;

pub struct AckRoutingHandler {
    ack_to_push_proxy: Arc<AckToPushProxyForwarder>,
}

impl AckRoutingHandler {
    pub fn new(ack_to_push_proxy: Arc<AckToPushProxyForwarder>) -> Self {
        Self { ack_to_push_proxy }
    }

    #[instrument(skip(self, ctx, ack), fields(request_id = %ctx.request_id(), trace_id = %ctx.trace_id(), svid = %svid))]
    pub async fn route_ack(
        &self,
        ctx: &Context,
        svid: &str,
        ack: Ack,
        route_options: RouteOptions,
    ) -> Result<MessageRouteResult> {
        let start_time = Instant::now();
        let decision_duration = start_time.elapsed();

        let forward = matches!(
            ack.payload.as_ref(),
            Some(AckPayload::Push(_) | AckPayload::Conversation(_) | AckPayload::Batch(_))
        );

        if !forward {
            tracing::warn!(
                request_id = %ctx.request_id(),
                ack_type = ack.r#type,
                "RouteAck: not a client uplink variant (push/conversation/batch)"
            );
            return Err(flare_err!(
                ErrorCode::InvalidParameter,
                "use RouteAck only for Push/Conversation/Batch; Send/Event are invalid uplink"
            ));
        }

        self.ack_to_push_proxy
            .forward_client_ack(ctx, ack)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::InternalError, "forward client ack"))?;

        let total_duration = start_time.elapsed();
        tracing::info!(
            request_id = %ctx.request_id(),
            svid = %svid,
            total_duration_ms = total_duration.as_millis(),
            "client Ack routed to push proxy"
        );
        
        Ok(MessageRouteResult {
            response_data: vec![],
            routed_endpoint: "push-proxy".to_string(),
            metadata: build_route_metadata(
                total_duration.as_millis() as i64,
                total_duration.as_millis() as i64,
                decision_duration.as_millis() as i64,
                svid,
                route_options.load_balance_strategy,
            ),
        })
    }
}
