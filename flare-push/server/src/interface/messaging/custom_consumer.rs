use std::sync::Arc;

use flare_im_core::event::types::types;
use flare_proto::push::PushCustomRequest;
use flare_server_core::context::Ctx;
use flare_server_core::event_bus::{EventEnvelope, EventHandler};
use flare_server_core::{FlareError, Result};
use prost::Message as _;
use tracing::error;

use crate::application::PushRouterHandler;
use crate::infrastructure::mq::publisher::PushServerMqPublisher;

pub struct PushCustomRequestHandler {
    route_handler: Arc<PushRouterHandler>,
    publisher: Arc<PushServerMqPublisher>,
}

impl PushCustomRequestHandler {
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
impl EventHandler for PushCustomRequestHandler {
    async fn handle(&self, ctx: &Ctx, envelope: EventEnvelope) -> Result<()> {
        if envelope.event_type != types::CUSTOM {
            error!(event_type = %envelope.event_type, "unexpected event_type, sending to dlq");
            self.publisher
                .publish_dlq(ctx, Some(envelope.partition_key.as_str()), envelope.payload)
                .await
                .map_err(|e| FlareError::general_error(e.to_string()))?;
            return Ok(());
        }

        match PushCustomRequest::decode(envelope.payload.as_slice()) {
            Ok(req) => {
                if let Err(e) = self.route_handler.handle_custom(ctx, req).await {
                    error!(error = %e, "route push custom failed, sending to dlq");
                    self.publisher
                        .publish_dlq(ctx, Some(envelope.partition_key.as_str()), envelope.payload)
                        .await
                        .map_err(|e| FlareError::general_error(e.to_string()))?;
                }
            }
            Err(e) => {
                error!(error = %e, "invalid push custom payload in envelope, sending to dlq");
                self.publisher
                    .publish_dlq(ctx, Some(envelope.partition_key.as_str()), envelope.payload)
                    .await
                    .map_err(|e| FlareError::general_error(e.to_string()))?;
            }
        }

        Ok(())
    }

    fn name(&self) -> &str {
        "push_custom_request_handler"
    }
}
