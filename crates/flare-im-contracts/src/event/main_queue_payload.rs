//! `TOPIC_MESSAGE_MAIN` 内层载荷：[`MqEnvelope`]（protobuf），外层为 [`flare_im_contracts::EventEnvelope`] JSON 的 `payload` 字节。

use std::collections::HashMap;

use flare_core_base::error::FlareError;
use flare_proto::common::{Event, Message, MqEnvelope, MqPayloadKind, mq_envelope};
use prost::Message as _;

pub type MqEnvelopeDecodeError = FlareError;

/// 解析 `EventEnvelope.payload` 为 [`MqEnvelope`]。
pub fn decode_mq_envelope(
    payload: &[u8],
) -> std::result::Result<MqEnvelope, MqEnvelopeDecodeError> {
    if payload.is_empty() {
        return Err(FlareError::deserialization_error(
            "mq envelope payload empty",
        ));
    }
    MqEnvelope::decode(payload)
        .map_err(|err| FlareError::deserialization_error(format!("decode MqEnvelope: {err}")))
}

/// 构造主队列 [`MqEnvelope`]（`payload_kind` = Message）。
pub fn mq_envelope_for_main_queue_message(
    message: &Message,
    recipient_user_ids: Vec<String>,
    large_conversation: bool,
) -> MqEnvelope {
    mq_envelope_for_main_queue_message_with_headers(
        message,
        recipient_user_ids,
        HashMap::new(),
        large_conversation,
    )
}

/// 构造主队列 [`MqEnvelope`]（`payload_kind` = Message），并携带跨队列元数据。
pub fn mq_envelope_for_main_queue_message_with_headers(
    message: &Message,
    recipient_user_ids: Vec<String>,
    headers: HashMap<String, String>,
    large_conversation: bool,
) -> MqEnvelope {
    MqEnvelope {
        envelope_id: uuid::Uuid::new_v4().to_string(),
        recipient_user_ids,
        conversation_id: message.conversation_id.clone(),
        seq: message.conversation_seq,
        produced_at: chrono::Utc::now().timestamp_millis(),
        payload_kind: MqPayloadKind::Message as i32,
        headers,
        push_only: false,
        persistence_only: false,
        large_conversation,
        payload: Some(mq_envelope::Payload::Message(message.clone())),
    }
}

/// 构造主队列 [`MqEnvelope`]（`payload_kind` = Event）。
pub fn mq_envelope_for_main_queue_event(
    event: &Event,
    recipient_user_ids: Vec<String>,
) -> MqEnvelope {
    mq_envelope_for_main_queue_event_with_headers(event, recipient_user_ids, HashMap::new())
}

/// 构造主队列 [`MqEnvelope`]（`payload_kind` = Event），并携带跨队列元数据。
pub fn mq_envelope_for_main_queue_event_with_headers(
    event: &Event,
    recipient_user_ids: Vec<String>,
    headers: HashMap<String, String>,
) -> MqEnvelope {
    MqEnvelope {
        envelope_id: uuid::Uuid::new_v4().to_string(),
        recipient_user_ids,
        conversation_id: event.conversation_id.clone(),
        seq: event.conversation_seq,
        produced_at: chrono::Utc::now().timestamp_millis(),
        payload_kind: MqPayloadKind::Event as i32,
        headers,
        push_only: false,
        persistence_only: false,
        large_conversation: false,
        payload: Some(mq_envelope::Payload::Event(event.clone())),
    }
}
