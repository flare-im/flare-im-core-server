//! IM topic envelope 事件常量与转换工具。
//!
//! Topic 名请使用 `crate::constants::topics`。

use std::collections::HashMap;

use super::EventEnvelope;
use crate::Ctx;
use crate::TopicEventBus;
use flare_proto::common::EventType;
use flare_server_core::error::{FlareError, Result as ServerResult};
use prost::Message as _;

// --- event_type 字符串（TopicEventEnvelope.event_type） ------------------------

/// Orchestrator 异步会话创建（JetStream `TOPIC_CONVERSATION_ENSURE`，载荷为 JSON，见 `MqMessagePublisher::publish_conversation_ensure`）
pub const EVENT_TYPE_OPERATION_CONVERSATION_ENSURE: &str = "operation.conversation_ensure";
pub const EVENT_TYPE_CONVERSATION_ENSURE: &str = "conversation.ensure";
pub const EVENT_TYPE_MESSAGE_CREATED: &str = "message.created";
pub const EVENT_TYPE_OPERATION_RECALLED: &str = "operation.recalled";
pub const EVENT_TYPE_OPERATION_EDITED: &str = "operation.edited";
pub const EVENT_TYPE_OPERATION_DELETED: &str = "operation.deleted";
pub const EVENT_TYPE_OPERATION_READ_RECEIPT: &str = "operation.read_receipt";
pub const EVENT_TYPE_OPERATION_REACTION: &str = "operation.reaction";
pub const EVENT_TYPE_OPERATION_PIN: &str = "operation.pin";
pub const EVENT_TYPE_OPERATION_UNPIN: &str = "operation.unpin";
pub const EVENT_TYPE_OPERATION_MARK: &str = "operation.mark";
pub const EVENT_TYPE_OPERATION_UNMARK: &str = "operation.unmark";

pub const CONVERSATION_UPDATE_TYPE_UNREAD: &str = "unread";
pub const CONVERSATION_UPDATE_TYPE_SUMMARY: &str = "summary";
pub const CONVERSATION_UPDATE_TYPE_REMOVE: &str = "remove";

pub type EventBusPublishError = FlareError;

pub trait ImTopicEventPublisher: Send + Sync {
    async fn publish_topic_event(
        &self,
        ctx: &Ctx,
        topic: &str,
        envelope: &flare_proto::common::TopicEventEnvelope,
    ) -> Result<(), EventBusPublishError>;
}

pub async fn publish_proto_as_server_event_envelope<B>(
    bus: &B,
    ctx: &Ctx,
    topic: &str,
    envelope: &flare_proto::common::TopicEventEnvelope,
) -> ServerResult<()>
where
    B: TopicEventBus + ?Sized,
{
    let ev = to_event_envelope(envelope);
    bus.publish(ctx, topic, &ev).await
}

pub fn encode_topic_event_envelope(
    envelope: &flare_proto::common::TopicEventEnvelope,
) -> Result<Vec<u8>, EventBusPublishError> {
    let mut buf = Vec::with_capacity(envelope.encoded_len());
    envelope
        .encode(&mut buf)
        .map_err(|e| FlareError::serialization_error(e.to_string()))?;
    Ok(buf)
}

const EVENT_ENVELOPE_SOURCE_IM_CORE: &str = "flare-im-core";

fn timestamp_ms_from_proto(created_at_ms: i64) -> Option<u64> {
    if created_at_ms <= 0 {
        None
    } else {
        Some(created_at_ms as u64)
    }
}

pub fn to_event_envelope(envelope: &flare_proto::common::TopicEventEnvelope) -> EventEnvelope {
    let payload = envelope.encode_to_vec();
    let mut core = EventEnvelope::new(
        envelope.event_type.as_str(),
        envelope.conversation_id.as_str(),
        envelope.seq,
        payload,
    );
    if let Some(ev) = envelope.event.as_ref() {
        if !ev.event_id.is_empty() {
            core.event_id = ev.event_id.clone();
        }
        core.timestamp_ms = timestamp_ms_from_proto(ev.created_at);
    }
    if !envelope.request_id.is_empty() {
        core = core.with_source(format!(
            "{};request_id={}",
            EVENT_ENVELOPE_SOURCE_IM_CORE, envelope.request_id
        ));
    } else {
        core = core.with_source(EVENT_ENVELOPE_SOURCE_IM_CORE);
    }
    core
}

pub fn event_type_str_from_proto_event(event: &flare_proto::common::Event) -> Option<&'static str> {
    match EventType::try_from(event.r#type).ok()? {
        EventType::EventMessageRecall => Some(EVENT_TYPE_OPERATION_RECALLED),
        EventType::EventMessageEdit => Some(EVENT_TYPE_OPERATION_EDITED),
        EventType::EventMessageDelete => Some(EVENT_TYPE_OPERATION_DELETED),
        EventType::EventReadReceipt => Some(EVENT_TYPE_OPERATION_READ_RECEIPT),
        EventType::EventReaction => Some(EVENT_TYPE_OPERATION_REACTION),
        EventType::EventPin => Some(EVENT_TYPE_OPERATION_PIN),
        EventType::EventUnpin => Some(EVENT_TYPE_OPERATION_UNPIN),
        EventType::EventMark => Some(EVENT_TYPE_OPERATION_MARK),
        EventType::EventUnmark => Some(EVENT_TYPE_OPERATION_UNMARK),
        _ => None,
    }
}

pub fn message_envelope_from_message(
    msg: &flare_proto::common::Message,
    event_type: impl Into<String>,
    created_at_ms: i64,
    tenant_id: impl AsRef<str>,
) -> flare_proto::common::MessageEnvelope {
    use crate::abstractions::storage_payload::{EXTRA_KEY_SYNC, EXTRA_KEY_TAGS};
    let sync = msg.attributes.get(EXTRA_KEY_SYNC).map(|s| s.as_str()) == Some("true");
    let tags = msg
        .attributes
        .get(EXTRA_KEY_TAGS)
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default();
    let mut metadata = HashMap::new();
    for (k, v) in &msg.attributes {
        if k.as_str() != EXTRA_KEY_SYNC && k.as_str() != EXTRA_KEY_TAGS {
            metadata.insert(k.clone(), v.clone());
        }
    }
    flare_proto::common::MessageEnvelope {
        conversation_id: msg.conversation_id.clone(),
        message: Some(msg.clone()),
        sync,
        attributes: tags,
        headers: metadata,
        event_type: event_type.into(),
        created_at: created_at_ms,
        tenant_id: tenant_id.as_ref().to_string(),
    }
}

pub fn topic_event_envelope_from_event(
    conversation_id: impl Into<String>,
    event: Option<flare_proto::common::Event>,
    tenant_id: impl Into<String>,
    event_type: impl Into<String>,
    seq: u64,
    request_id: impl Into<String>,
) -> flare_proto::common::TopicEventEnvelope {
    flare_proto::common::TopicEventEnvelope {
        conversation_id: conversation_id.into(),
        event,
        tenant_id: tenant_id.into(),
        event_type: event_type.into(),
        seq,
        request_id: request_id.into(),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn conversation_update_envelope(
    user_id: impl Into<String>,
    conversation_id: impl Into<String>,
    tenant_id: impl Into<String>,
    update_type: impl Into<String>,
    max_seq: u64,
    last_read_seq: u64,
    summary_snapshot: Vec<u8>,
    metadata: HashMap<String, String>,
    updated_at_ms: i64,
) -> flare_proto::common::ConversationUpdateEnvelope {
    flare_proto::common::ConversationUpdateEnvelope {
        user_id: user_id.into(),
        conversation_id: conversation_id.into(),
        tenant_id: tenant_id.into(),
        update_type: update_type.into(),
        max_seq,
        last_read_seq,
        summary_snapshot,
        attributes: metadata,
        updated_at: updated_at_ms,
    }
}

pub fn message_to_topic_event_envelope(
    msg: &flare_proto::common::Message,
    tenant_id: impl AsRef<str>,
    seq: u64,
) -> flare_proto::common::TopicEventEnvelope {
    use flare_proto::common::{Event, EventType};
    let payload = Some(flare_proto::common::event::Payload::Message(msg.clone()));
    let event = Event {
        conversation_id: msg.conversation_id.clone(),
        conversation_seq: seq,
        r#type: EventType::EventMessage as i32,
        created_at: msg.created_at,
        event_id: String::new(),
        request_id: None,
        payload,
    };
    let request_id = msg
        .attributes
        .get("x-request-id")
        .map(|s| s.as_str())
        .unwrap_or("");
    topic_event_envelope_from_event(
        &msg.conversation_id,
        Some(event),
        tenant_id.as_ref().to_string(),
        EVENT_TYPE_MESSAGE_CREATED,
        seq,
        request_id,
    )
}
