use std::sync::Arc;

use async_trait::async_trait;
use flare_proto::common::{Event, Message};
use flare_server_core::context::Ctx;
use flare_server_core::event_bus::{EventEnvelope, EventHandler};
use flare_server_core::{FlareError, Result};
use prost::Message as _;
use tracing::{debug, warn};

use crate::infrastructure::messaging::mq_publisher::MqMessagePublisher;

const MAIN_KIND_MESSAGE: u8 = 1;
const MAIN_KIND_EVENT: u8 = 2;

fn decode_main_payload(payload: &[u8]) -> Result<(u8, &[u8], &[u8])> {
    if payload.len() < 9 {
        return Err(FlareError::system("invalid main payload: too short"));
    }
    let kind = payload[0];
    let first_len = u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]) as usize;
    let first_start = 5usize;
    let first_end = first_start.saturating_add(first_len);
    if payload.len() < first_end + 4 {
        return Err(FlareError::system(
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
        return Err(FlareError::system(
            "invalid main payload: second length overflow",
        ));
    }
    Ok((
        kind,
        &payload[first_start..first_end],
        &payload[second_start..second_end],
    ))
}

pub struct MainQueueEventHandler {
    publisher: Arc<MqMessagePublisher>,
}

impl MainQueueEventHandler {
    pub fn new(publisher: Arc<MqMessagePublisher>) -> Self {
        Self { publisher }
    }
}

#[async_trait]
impl EventHandler for MainQueueEventHandler {
    async fn handle(&self, ctx: &Ctx, envelope: EventEnvelope) -> Result<()> {
        debug!(trace_id = %ctx.trace_id(), "received main queue event");
        let (kind, first, _second) = match decode_main_payload(&envelope.payload) {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "invalid main payload, skip");
                return Ok(());
            }
        };

        match kind {
            MAIN_KIND_MESSAGE => {
                // 解析 Message 并发布到存储 topic
                match Message::decode(first) {
                    Ok(message) => {
                        if let Err(e) = self.publisher.publish_storage_message(ctx, &message).await
                        {
                            warn!(error = %e, "failed to publish message to storage");
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "failed to decode message from main payload");
                    }
                }
            }
            MAIN_KIND_EVENT => {
                // 解析 Event 并发布到事件 topic
                match Event::decode(first) {
                    Ok(event) => {
                        if let Err(e) = self.publisher.publish_domain_event(ctx, &event).await {
                            warn!(error = %e, "failed to publish event to events topic");
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "failed to decode event from main payload");
                    }
                }
            }
            _ => {
                warn!(kind = kind, "unknown main payload kind, skip");
            }
        }

        Ok(())
    }

    fn name(&self) -> &str {
        "orchestrator-main-queue-handler"
    }
}
