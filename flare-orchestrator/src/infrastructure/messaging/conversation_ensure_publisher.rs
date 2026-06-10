use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use flare_im_core::constants::topics::TOPIC_CONVERSATION_ENSURE;
use flare_im_core::event::EVENT_TYPE_OPERATION_CONVERSATION_ENSURE;
use flare_server_core::context::{Context, Ctx};
use flare_server_core::eventbus::EventEnvelope;
use flare_server_core::mq::producer::Producer;
use serde::Serialize;
use uuid::Uuid;

use crate::domain::service::ConversationEventPublisher;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};

#[derive(Serialize)]
struct ConversationEnsurePayload {
    tenant_id: String,
    conversation_type: i32,
    business_type: String,
    participants: Vec<String>,
    channel_id: String,
}

pub struct MqConversationEnsurePublisher {
    producer: Arc<dyn Producer>,
}

impl MqConversationEnsurePublisher {
    pub fn new(producer: Arc<dyn Producer>) -> Arc<Self> {
        Arc::new(Self { producer })
    }
}

impl ConversationEventPublisher for MqConversationEnsurePublisher {
    fn publish_conversation_ensure<'a>(
        &'a self,
        conversation_id: &'a str,
        tenant_id: &'a str,
        conversation_type: i32,
        business_type: &'a str,
        participants: Vec<String>,
        stored_channel_id: String,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let payload = ConversationEnsurePayload {
                tenant_id: tenant_id.to_string(),
                conversation_type,
                business_type: business_type.to_string(),
                participants,
                channel_id: stored_channel_id,
            };

            let payload_bytes = serde_json::to_vec(&payload).map_err(|e| {
                ErrorBuilder::new(
                    ErrorCode::SerializationError,
                    "serialize conversation ensure payload failed",
                )
                .details(e.to_string())
                .build_error()
            })?;

            let envelope = EventEnvelope::new(
                EVENT_TYPE_OPERATION_CONVERSATION_ENSURE,
                conversation_id,
                0,
                payload_bytes,
            );
            let event_bytes = envelope.to_json_bytes().map_err(|e| {
                ErrorBuilder::new(
                    ErrorCode::SerializationError,
                    "serialize conversation ensure envelope failed",
                )
                .details(e.to_string())
                .build_error()
            })?;

            let ctx: Ctx = Context::with_request_id(Uuid::new_v4().to_string())
                .with_tenant_id(tenant_id)
                .into();

            self.producer
                .send(
                    &ctx,
                    TOPIC_CONVERSATION_ENSURE,
                    Some(conversation_id),
                    event_bytes,
                    None,
                )
                .await
                .map_err(|e| e.into_flare_error())?;

            Ok(())
        })
    }
}
