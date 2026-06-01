//! 将 Storage Writer 写入 `events` 表的 `payload` 列还原为 `flare.common.v1.Event`。
//! 写入侧仅序列化 `Event.payload` oneof 的内层消息（见 `flare-storage/writer` `event_stream.rs`）。

use anyhow::Context;
use chrono::{DateTime, Utc};
use flare_proto::common::event::Payload as EventPayload;
use flare_proto::common::{
    CallSignalEvent, ConversationDeleteEvent, ConversationUpdateEvent, CustomEvent, Event,
    EventType as ProtoEventType, MarkEvent, Message, MessageBurnScheduledEvent, MessageBurnedEvent,
    MessageDeleteEvent, MessageEditEvent, MessageHardDeletedEvent, MessageRecallEvent, PinEvent,
    PresenceEvent, ReactionEvent, ReadReceiptEvent, TypingEvent, UnmarkEvent, UnpinEvent,
};
use prost::Message as ProstMessage;

use crate::convert::datetime_to_timestamp;

/// 由 `events` 表一行组装完整 `Event`（`event_id` = `{conversation_id}:{seq}`）。
pub fn proto_event_from_events_row(
    conversation_id: &str,
    seq: i64,
    event_type: i32,
    created_at: DateTime<Utc>,
    _operator_id: String,
    request_id: Option<String>,
    event_seq: Option<i64>,
    payload: &[u8],
) -> anyhow::Result<Event> {
    let created_at_ts = datetime_to_timestamp(Some(created_at));
    let event_id = format!("{conversation_id}:{seq}");
    let payload_oneof = decode_payload_oneof(event_type, payload)?;

    Ok(Event {
        conversation_id: conversation_id.to_string(),
        seq: seq as u64,
        r#type: event_type,
        created_at: created_at_ts,
        event_id,
        event_seq: event_seq.map(|v| v as u64),
        request_id,
        payload: payload_oneof,
    })
}

fn decode_payload_oneof(event_type: i32, payload: &[u8]) -> anyhow::Result<Option<EventPayload>> {
    if payload.is_empty() {
        return Ok(None);
    }
    let Ok(ty) = ProtoEventType::try_from(event_type) else {
        return Ok(None);
    };
    let p = match ty {
        ProtoEventType::EventMessage => {
            EventPayload::Message(Message::decode(payload).context("decode Message")?)
        }
        ProtoEventType::EventMessageRecall => {
            EventPayload::Recall(MessageRecallEvent::decode(payload).context("decode Recall")?)
        }
        ProtoEventType::EventMessageEdit => {
            EventPayload::Edit(MessageEditEvent::decode(payload).context("decode Edit")?)
        }
        ProtoEventType::EventMessageDelete => {
            EventPayload::Delete(MessageDeleteEvent::decode(payload).context("decode Delete")?)
        }
        ProtoEventType::EventReadReceipt => {
            EventPayload::Read(ReadReceiptEvent::decode(payload).context("decode Read")?)
        }
        ProtoEventType::EventTyping => {
            EventPayload::Typing(TypingEvent::decode(payload).context("decode Typing")?)
        }
        ProtoEventType::EventConversationUpdate => EventPayload::Conversation(
            ConversationUpdateEvent::decode(payload).context("decode ConversationUpdate")?,
        ),
        ProtoEventType::EventConversationDelete => EventPayload::ConversationDelete(
            ConversationDeleteEvent::decode(payload).context("decode ConversationDelete")?,
        ),
        ProtoEventType::EventPresence => {
            EventPayload::Presence(PresenceEvent::decode(payload).context("decode Presence")?)
        }
        ProtoEventType::EventCallSignal => {
            EventPayload::CallSignal(CallSignalEvent::decode(payload).context("decode CallSignal")?)
        }
        ProtoEventType::EventReaction => {
            EventPayload::Reaction(ReactionEvent::decode(payload).context("decode Reaction")?)
        }
        ProtoEventType::EventPin => {
            EventPayload::Pin(PinEvent::decode(payload).context("decode Pin")?)
        }
        ProtoEventType::EventUnpin => {
            EventPayload::Unpin(UnpinEvent::decode(payload).context("decode Unpin")?)
        }
        ProtoEventType::EventMark => {
            EventPayload::Mark(MarkEvent::decode(payload).context("decode Mark")?)
        }
        ProtoEventType::EventUnmark => {
            EventPayload::Unmark(UnmarkEvent::decode(payload).context("decode Unmark")?)
        }
        ProtoEventType::EventMessageBurnScheduled => EventPayload::BurnScheduled(
            MessageBurnScheduledEvent::decode(payload).context("decode BurnScheduled")?,
        ),
        ProtoEventType::EventMessageBurned => {
            EventPayload::Burned(MessageBurnedEvent::decode(payload).context("decode Burned")?)
        }
        ProtoEventType::EventMessageHardDeleted => EventPayload::HardDeleted(
            MessageHardDeletedEvent::decode(payload).context("decode HardDeleted")?,
        ),
        ProtoEventType::EventCustom => {
            EventPayload::Custom(CustomEvent::decode(payload).context("decode Custom")?)
        }
        ProtoEventType::Unspecified => return Ok(None),
    };
    Ok(Some(p))
}
