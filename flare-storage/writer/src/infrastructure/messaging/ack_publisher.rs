use std::sync::Arc;

use anyhow::{Result, anyhow};
use flare_im_core::Ctx;
use flare_server_core::mq::producer::Producer;
use serde_json::to_vec;
use tracing::instrument;

use crate::config::StorageWriterConfig;
use crate::domain::events::AckEvent;
use crate::domain::repository::AckPublisher;

pub struct MqAckPublisher {
    producer: Arc<dyn Producer>,
    subject: String,
}

impl MqAckPublisher {
    pub fn new(
        producer: Arc<dyn Producer>,
        _config: Arc<StorageWriterConfig>,
        subject: String,
    ) -> Self {
        Self { producer, subject }
    }
}

impl AckPublisher for MqAckPublisher {
    #[instrument(skip(self, event), fields(message_id = %event.message_id, conversation_id = %event.conversation_id))]
    async fn publish(&self, ctx: &Ctx, event: AckEvent<'_>) -> Result<()> {
        let payload = to_vec(&event)?;

        self.producer
            .send(
                ctx,
                &self.subject,
                Some(event.conversation_id),
                payload,
                Some(std::collections::HashMap::from([(
                    "x-message-id".to_string(),
                    event.message_id.to_string(),
                )])),
            )
            .await
            .map_err(|err| anyhow!("failed to publish ACK: {err}"))?;

        Ok(())
    }
}
