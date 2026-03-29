use std::sync::Arc;

use flare_proto::common::Event;
use flare_proto::push::PushMessageRequest;
use flare_server_core::context::Ctx;
use flare_server_core::event_bus::{EventEnvelope, EventHandler};
use flare_server_core::{FlareError, Result};
use prost::Message as _;
use tracing::{debug, error};

use crate::application::PushRouterHandler;
use crate::infrastructure::mq::publisher::PushServerMqPublisher;

const MAIN_KIND_MESSAGE: u8 = 1;
const MAIN_KIND_EVENT: u8 = 2;

fn decode_main_payload(payload: &[u8]) -> Result<(u8, &[u8], &[u8])> {
    if payload.len() < 9 {
        return Err(FlareError::general_error("invalid main payload: too short"));
    }
    let kind = payload[0];
    let first_len = u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]) as usize;
    let first_start = 5usize;
    let first_end = first_start.saturating_add(first_len);
    if payload.len() < first_end + 4 {
        return Err(FlareError::general_error(
            "invalid main payload: first length overflow",
        ));
    }
    let second_len = u32::from_be_bytes([
        payload[first_end],
        payload[first_end + 1],
        payload[first_end + 2],
        payload[first_end + 3],
    ]) as usize;
    let second_start = first_end + 4;
    let second_end = second_start.saturating_add(second_len);
    if payload.len() < second_end {
        return Err(FlareError::general_error(
            "invalid main payload: second length overflow",
        ));
    }
    Ok((
        kind,
        &payload[first_start..first_end],
        &payload[second_start..second_end],
    ))
}

pub struct PushMainRequestHandler {
    route_handler: Arc<PushRouterHandler>,
    publisher: Arc<PushServerMqPublisher>,
}

impl PushMainRequestHandler {
    pub fn new(
        route_handler: Arc<PushRouterHandler>,
        publisher: Arc<PushServerMqPublisher>,
    ) -> Self {
        Self {
            route_handler,
            publisher,
        }
    }
}

#[async_trait::async_trait]
impl EventHandler for PushMainRequestHandler {
    async fn handle(&self, ctx: &Ctx, envelope: EventEnvelope) -> Result<()> {
        debug!(trace_id = %ctx.trace_id(), "received main queue event");
        let (kind, first, second) = match decode_main_payload(envelope.payload.as_slice()) {
            Ok(v) => v,
            Err(e) => {
                error!(error = %e, "invalid main payload, sending to dlq");
                self.publisher
                    .publish_dlq(ctx, Some(envelope.partition_key.as_str()), envelope.payload)
                    .await
                    .map_err(|err| FlareError::general_error(err.to_string()))?;
                return Ok(());
            }
        };

        let push_req = match kind {
            MAIN_KIND_MESSAGE => PushMessageRequest::decode(second),
            MAIN_KIND_EVENT => {
                if second.is_empty() {
                    let _ = Event::decode(first);
                    return Ok(());
                }
                PushMessageRequest::decode(second)
            }
            _ => return Ok(()),
        };

        match push_req {
            Ok(req) => {
                if let Err(e) = self.route_handler.handle_message(ctx, req).await {
                    error!(error = %e, "route main push request failed, sending to dlq");
                    self.publisher
                        .publish_dlq(ctx, Some(envelope.partition_key.as_str()), envelope.payload)
                        .await
                        .map_err(|err| FlareError::general_error(err.to_string()))?;
                }
            }
            Err(e) => {
                error!(error = %e, "decode main push request failed, sending to dlq");
                self.publisher
                    .publish_dlq(ctx, Some(envelope.partition_key.as_str()), envelope.payload)
                    .await
                    .map_err(|err| FlareError::general_error(err.to_string()))?;
            }
        }

        Ok(())
    }

    fn name(&self) -> &str {
        "push_main_request_handler"
    }
}
