use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use flare_im_contracts::constants::topics::TOPIC_CONVERSATION_ENSURE;
use flare_im_contracts::event::mq_envelope_for_main_queue_event_with_headers;
use flare_proto::common::{CustomEvent, Event, EventType, event};
use flare_server_core::context::{Context, Ctx};
use flare_server_core::mq::producer::Producer;
use prost::Message as _;
use serde::Serialize;
use uuid::Uuid;

use crate::domain::service::ConversationEventPublisher;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};

const CONVERSATION_ENSURE_NAMESPACE: &str = "flare.core";
const CONVERSATION_ENSURE_EVENT_NAME: &str = "conversation.ensure";
const CONVERSATION_ENSURE_EVENT_VERSION: &str = "1";

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

            let event_id = Uuid::new_v4().to_string();
            let produced_at = chrono::Utc::now().timestamp_millis();
            let custom_attributes = HashMap::from([
                ("tenant_id".to_string(), tenant_id.to_string()),
                (
                    "event_name".to_string(),
                    CONVERSATION_ENSURE_EVENT_NAME.to_string(),
                ),
            ]);
            let event = Event {
                conversation_id: conversation_id.to_string(),
                conversation_seq: 0,
                r#type: EventType::EventCustom as i32,
                created_at: produced_at,
                event_id: event_id.clone(),
                request_id: Some(event_id.clone()),
                payload: Some(event::Payload::Custom(CustomEvent {
                    namespace: CONVERSATION_ENSURE_NAMESPACE.to_string(),
                    name: CONVERSATION_ENSURE_EVENT_NAME.to_string(),
                    version: CONVERSATION_ENSURE_EVENT_VERSION.to_string(),
                    payload: payload_bytes,
                    attributes: custom_attributes,
                })),
            };
            let headers = HashMap::from([
                ("tenant_id".to_string(), tenant_id.to_string()),
                (
                    "event_type".to_string(),
                    CONVERSATION_ENSURE_EVENT_NAME.to_string(),
                ),
            ]);
            let mut envelope =
                mq_envelope_for_main_queue_event_with_headers(&event, Vec::new(), headers);
            envelope.persistence_only = true;
            let event_bytes = envelope.encode_to_vec();

            let ctx: Ctx = Context::with_request_id(event_id)
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
