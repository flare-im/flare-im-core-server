use std::sync::Arc;

use flare_im_core::event::types::types;
use flare_proto::access_gateway;
use flare_proto::common::{PushTaskEnvelope, PushTaskPayloadKind};
use flare_proto::signaling::router::PushStrategy;
use flare_server_core::context::Ctx;
use flare_server_core::event_bus::EventEnvelope;
use flare_server_core::event_bus::EventHandler;
use flare_server_core::{FlareError, Result};
use prost::Message as _;
use tracing::error;

use crate::application::GatewayPushExecutor;
use crate::infrastructure::mq::dlq_publisher::DlqPublisher;

pub struct OnlinePushTaskHandler {
    gateway_push: Arc<GatewayPushExecutor>,
    dlq: Arc<DlqPublisher>,
}

impl OnlinePushTaskHandler {
    pub fn new(gateway_push: Arc<GatewayPushExecutor>, dlq: Arc<DlqPublisher>) -> Self {
        Self { gateway_push, dlq }
    }
}

#[async_trait::async_trait]
impl EventHandler for OnlinePushTaskHandler {
    async fn handle(&self, ctx: &Ctx, event: EventEnvelope) -> Result<()> {
        if event.event_type != types::SYSTEM {
            error!(event_type = %event.event_type, "unexpected event_type, sending to dlq");
            self.dlq
                .publish(ctx, Some(event.partition_key.as_str()), event.payload)
                .await
                .map_err(|e| FlareError::general_error(e.to_string()))?;
            return Ok(());
        }
        match PushTaskEnvelope::decode(event.payload.as_slice()) {
            Ok(env) => {
                let kind = PushTaskPayloadKind::try_from(env.payload_kind)
                    .unwrap_or(PushTaskPayloadKind::Unspecified);
                let strategy = PushStrategy::AllDevices;
                let user_id = env.user_id.clone();

                let route_result = match kind {
                    PushTaskPayloadKind::Message => match access_gateway::PushMessageRequest::decode(
                        env.push_payload.as_slice(),
                    ) {
                        Ok(push) => {
                            self.gateway_push
                                .push_message(ctx, &user_id, strategy, push)
                                .await
                        }
                        Err(e) => Err(anyhow::anyhow!("decode PushMessageRequest: {}", e)),
                    },
                    PushTaskPayloadKind::Event => match access_gateway::PushEventRequest::decode(
                        env.push_payload.as_slice(),
                    ) {
                        Ok(push) => {
                            self.gateway_push
                                .push_event(ctx, &user_id, strategy, push)
                                .await
                        }
                        Err(e) => Err(anyhow::anyhow!("decode PushEventRequest: {}", e)),
                    },
                    PushTaskPayloadKind::Notification => {
                        match access_gateway::PushNotificationRequest::decode(env.push_payload.as_slice()) {
                            Ok(push) => {
                                self.gateway_push
                                    .push_notification(ctx, &user_id, strategy, push)
                                    .await
                            }
                            Err(e) => Err(anyhow::anyhow!("decode PushNotificationRequest: {}", e)),
                        }
                    }
                    PushTaskPayloadKind::Ack => match access_gateway::PushAckRequest::decode(
                        env.push_payload.as_slice(),
                    ) {
                        Ok(push) => {
                            self.gateway_push
                                .push_ack(ctx, &user_id, strategy, push)
                                .await
                        }
                        Err(e) => Err(anyhow::anyhow!("decode PushAckRequest: {}", e)),
                    },
                    PushTaskPayloadKind::Custom => match access_gateway::PushCustomRequest::decode(
                        env.push_payload.as_slice(),
                    ) {
                        Ok(push) => {
                            self.gateway_push
                                .push_custom(ctx, &user_id, strategy, push)
                                .await
                        }
                        Err(e) => Err(anyhow::anyhow!("decode PushCustomRequest: {}", e)),
                    },
                    PushTaskPayloadKind::Unspecified => Err(anyhow::anyhow!(
                        "PushTaskEnvelope.payload_kind unspecified; reject task"
                    )),
                };

                if let Err(e) = route_result {
                    error!(error = %e, "downstream push failed, sending to dlq");
                    self.dlq
                        .publish(ctx, Some(event.partition_key.as_str()), event.payload)
                        .await
                        .map_err(|err| FlareError::general_error(err.to_string()))?;
                }
            }
            Err(e) => {
                error!(error = %e, "invalid push task envelope payload, sending to dlq");
                self.dlq
                    .publish(ctx, Some(event.partition_key.as_str()), event.payload)
                    .await
                    .map_err(|err| FlareError::general_error(err.to_string()))?;
            }
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "push_online_handler"
    }
}
