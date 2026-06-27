//! 客户端 ACK 上行路由：Push / Conversation / Read / Batch。

use std::sync::Arc;
use std::time::Instant;

use flare_grpc_proto::signaling::router::RouteOptions;
use flare_proto::common::Ack;
use flare_proto::common::ack::Payload as AckPayload;
use flare_server_core::context::Context;
use flare_server_core::error::{ErrorCode, Result, map_infra_error};
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

        let forward = is_client_uplink_ack(ack.payload.as_ref());

        if !forward {
            tracing::warn!(
                request_id = %ctx.request_id(),
                ack_payload = ack_payload_name(ack.payload.as_ref()),
                "RouteAck: not a client uplink ack variant"
            );
            return Err(flare_err!(
                ErrorCode::InvalidParameter,
                "use RouteAck only for Push/Conversation/Read/Batch; Send/Event are invalid uplink"
            ));
        }

        self.ack_to_push_proxy
            .forward_client_ack(ctx, ack)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::InternalError, "forward client ack"))?;

        let total_duration = start_time.elapsed();
        tracing::trace!(
            request_id = %ctx.request_id(),
            svid = %svid,
            total_duration_ms = total_duration.as_millis(),
            "client Ack routed"
        );

        Ok(MessageRouteResult {
            response_data: vec![],
            routed_endpoint: "flare-conversation".to_string(),
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

fn ack_payload_name(payload: Option<&AckPayload>) -> &'static str {
    match payload {
        Some(AckPayload::Send(_)) => "send",
        Some(AckPayload::Event(_)) => "event",
        Some(AckPayload::Push(_)) => "push",
        Some(AckPayload::Conversation(_)) => "conversation",
        Some(AckPayload::Read(_)) => "read",
        Some(AckPayload::Batch(_)) => "batch",
        None => "none",
    }
}

fn is_client_uplink_ack(payload: Option<&AckPayload>) -> bool {
    matches!(
        payload,
        Some(
            AckPayload::Push(_)
                | AckPayload::Conversation(_)
                | AckPayload::Read(_)
                | AckPayload::Batch(_),
        )
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_proto::common::{AckBatch, ConversationAck, EventAck, PushAck, ReadAck, SendAck};

    #[test]
    fn read_ack_is_a_client_uplink_ack() {
        assert!(is_client_uplink_ack(Some(&AckPayload::Read(
            ReadAck::default()
        ))));
    }

    #[test]
    fn only_client_uplink_ack_payloads_are_forwarded() {
        assert!(is_client_uplink_ack(Some(&AckPayload::Push(
            PushAck::default()
        ))));
        assert!(is_client_uplink_ack(Some(&AckPayload::Conversation(
            ConversationAck::default(),
        ))));
        assert!(is_client_uplink_ack(Some(&AckPayload::Read(
            ReadAck::default()
        ))));
        assert!(is_client_uplink_ack(Some(&AckPayload::Batch(
            AckBatch::default()
        ))));

        assert!(!is_client_uplink_ack(Some(&AckPayload::Send(
            SendAck::default()
        ))));
        assert!(!is_client_uplink_ack(Some(&AckPayload::Event(
            EventAck::default()
        ))));
        assert!(!is_client_uplink_ack(None));
    }
}
