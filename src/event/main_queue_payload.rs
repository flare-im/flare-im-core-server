//! `TOPIC_MESSAGE_MAIN` 内层载荷：[`MqEnvelope`]（protobuf），外层为 [`flare_im_core::EventEnvelope`] JSON 的 `payload` 字节。

use flare_proto::common::{Event, Message, MqEnvelope, MqPayloadKind, mq_envelope};
use prost::Message as _;

#[derive(Debug, thiserror::Error)]
pub enum MqEnvelopeDecodeError {
    #[error("mq envelope payload empty")]
    Empty,
    #[error("decode MqEnvelope: {0}")]
    Decode(#[from] prost::DecodeError),
}

/// 解析 `EventEnvelope.payload` 为 [`MqEnvelope`]。
pub fn decode_mq_envelope(payload: &[u8]) -> Result<MqEnvelope, MqEnvelopeDecodeError> {
    if payload.is_empty() {
        return Err(MqEnvelopeDecodeError::Empty);
    }
    Ok(MqEnvelope::decode(payload)?)
}

/// 构造主队列 [`MqEnvelope`]（`payload_kind` = Message）。
pub fn mq_envelope_for_main_queue_message(
    message: &Message,
    recipient_user_ids: Vec<String>,
) -> MqEnvelope {
    MqEnvelope {
        envelope_id: uuid::Uuid::new_v4().to_string(),
        recipient_user_ids,
        conversation_id: message.conversation_id.clone(),
        seq: message.seq,
        produced_at_ms: chrono::Utc::now().timestamp_millis(),
        payload_kind: MqPayloadKind::Message as i32,
        headers: std::collections::HashMap::new(),
        push_only: false,
        persistence_only: false,
        payload: Some(mq_envelope::Payload::Message(message.clone())),
    }
}

/// 构造主队列 [`MqEnvelope`]（`payload_kind` = Event）。
pub fn mq_envelope_for_main_queue_event(
    event: &Event,
    recipient_user_ids: Vec<String>,
) -> MqEnvelope {
    MqEnvelope {
        envelope_id: uuid::Uuid::new_v4().to_string(),
        recipient_user_ids,
        conversation_id: event.conversation_id.clone(),
        seq: event.seq,
        produced_at_ms: chrono::Utc::now().timestamp_millis(),
        payload_kind: MqPayloadKind::Event as i32,
        headers: std::collections::HashMap::new(),
        push_only: false,
        persistence_only: false,
        payload: Some(mq_envelope::Payload::Event(event.clone())),
    }
}
