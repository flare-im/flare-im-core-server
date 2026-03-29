use std::sync::Arc;

use flare_im_core::event::types::types;
use flare_proto::push::PushNotificationRequest;
use flare_server_core::context::Ctx;
use flare_server_core::event_bus::{EventEnvelope, EventHandler};
use flare_server_core::{FlareError, Result};
use prost::Message as _;
use tracing::error;

use crate::application::PushRouterHandler;
use crate::infrastructure::mq::publisher::PushServerMqPublisher;

pub struct PushNotificationRequestHandler {
    route_handler: Arc<PushRouterHandler>,
    publisher: Arc<PushServerMqPublisher>,
}

impl PushNotificationRequestHandler {
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
impl EventHandler for PushNotificationRequestHandler {
    async fn handle(&self, ctx: &Ctx, envelope: EventEnvelope) -> Result<()> {
        if envelope.event_type != types::NOTIFICATION {
            error!(event_type = %envelope.event_type, "unexpected event_type, sending to dlq");
            self.publisher
                .publish_dlq(ctx, Some(envelope.partition_key.as_str()), envelope.payload)
                .await
                .map_err(|e| FlareError::general_error(e.to_string()))?;
            return Ok(());
        }

        match PushNotificationRequest::decode(envelope.payload.as_slice()) {
            Ok(req) => {
                if let Err(e) = self.route_handler.handle_notification(ctx, req).await {
                    error!(error = %e, "route push notification failed, sending to dlq");
                    self.publisher
                        .publish_dlq(ctx, Some(envelope.partition_key.as_str()), envelope.payload)
                        .await
                        .map_err(|e| FlareError::general_error(e.to_string()))?;
                }
            }
            Err(e) => {
                error!(error = %e, "invalid push notification payload in envelope, sending to dlq");
                self.publisher
                    .publish_dlq(ctx, Some(envelope.partition_key.as_str()), envelope.payload)
                    .await
                    .map_err(|e| FlareError::general_error(e.to_string()))?;
            }
        }

        Ok(())
    }

    fn name(&self) -> &str {
        "push_notification_request_handler"
    }
}
