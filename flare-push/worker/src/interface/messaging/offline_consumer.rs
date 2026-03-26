use std::sync::Arc;

use flare_im_core::event::types::types;
use flare_proto::common::PushTaskEnvelope;
use flare_server_core::context::Ctx;
use flare_server_core::event_bus::EventEnvelope;
use flare_server_core::event_bus::EventHandler;
use flare_server_core::{FlareError, Result};
use prost::Message as _;
use tracing::{error, info};

use crate::infrastructure::mq::dlq_publisher::DlqPublisher;

pub struct OfflinePushTaskHandler {
    dlq: Arc<DlqPublisher>,
}

impl OfflinePushTaskHandler {
    pub fn new(dlq: Arc<DlqPublisher>) -> Self {
        Self { dlq }
    }
}

#[async_trait::async_trait]
impl EventHandler for OfflinePushTaskHandler {
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
                        info!(
                            trace_id = %ctx.trace_id(),
                            user_id = %env.user_id,
                            tenant_id = %env.tenant_id,
                            message_id = %env.message_id,
                            conversation_id = %env.conversation_id,
                            "[离线推送占位实现] offline task received"
                        );
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
        "push_offline_handler"
    }
}
