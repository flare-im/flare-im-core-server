//! 将 Storage Writer 写入 `events` 表的 `payload` 列还原为 `flare.common.v1.Event`。
//! 写入侧仅序列化 `Event.payload` oneof 的内层消息（见 `flare-storage/writer` `event_stream.rs`）。

use chrono::{DateTime, Utc};
use flare_proto::common::event::Payload as EventPayload;
use flare_proto::common::{
    ConversationDeleteEvent, ConversationUpdateEvent, CustomEvent, Event,
    EventType as ProtoEventType, MarkEvent, Message, MessageDeleteEvent, MessageEditEvent,
    MessageRecallEvent, MessageRetentionExpiredEvent, MessageRetentionPurgedEvent,
    MessageRetentionScheduledEvent, PinEvent, PluginStateEvent, ReactionEvent, ReadReceiptEvent,
    UnmarkEvent, UnpinEvent,
};
use flare_server_core::error::{ErrorCode, FlareError, map_infra_error};
use prost::Message as ProstMessage;

/// 由 `events` 表一行组装完整 `Event`（`event_id` = `{conversation_id}:{seq}`）。
#[allow(clippy::too_many_arguments)]
pub fn proto_event_from_events_row(
    conversation_id: &str,
    seq: i64,
    event_type: i32,
    created_at: DateTime<Utc>,
    _operator_id: String,
    request_id: Option<String>,
    _event_seq: Option<i64>,
    payload: &[u8],
) -> flare_server_core::error::Result<Event> {
    let event_id = format!("{conversation_id}:{seq}");
    let payload_oneof = decode_payload_oneof(event_type, payload)?;

    Ok(Event {
        conversation_id: conversation_id.to_string(),
        conversation_seq: seq as u64,
        r#type: event_type,
        created_at: created_at.timestamp_millis(),
        event_id,
        request_id,
        payload: payload_oneof,
    })
}

/// 事件行的 payload 解不开（损坏、或写入侧换了 oneof 的内层类型）：分类为反序列化错误。
/// prost 的报文只进 `details`，原因只写解的是哪一种事件。
fn decode_failure(kind: &'static str) -> impl Fn(prost::DecodeError) -> FlareError {
    move |err| {
        map_infra_error(
            err,
            ErrorCode::DeserializationError,
            format!("decode {kind} event payload"),
        )
    }
}

fn decode_payload_oneof(
    event_type: i32,
    payload: &[u8],
) -> flare_server_core::error::Result<Option<EventPayload>> {
    if payload.is_empty() {
        return Ok(None);
    }
    let Ok(ty) = ProtoEventType::try_from(event_type) else {
        return Ok(None);
    };
    let p = match ty {
        ProtoEventType::EventMessage => {
            EventPayload::Message(Message::decode(payload).map_err(decode_failure("Message"))?)
        }
        ProtoEventType::EventMessageRecall => EventPayload::Recall(
            MessageRecallEvent::decode(payload).map_err(decode_failure("Recall"))?,
        ),
        ProtoEventType::EventMessageEdit => {
            EventPayload::Edit(MessageEditEvent::decode(payload).map_err(decode_failure("Edit"))?)
        }
        ProtoEventType::EventMessageDelete => EventPayload::Delete(
            MessageDeleteEvent::decode(payload).map_err(decode_failure("Delete"))?,
        ),
        ProtoEventType::EventReadReceipt => {
            EventPayload::Read(ReadReceiptEvent::decode(payload).map_err(decode_failure("Read"))?)
        }
        ProtoEventType::EventConversationUpdate => EventPayload::Conversation(
            ConversationUpdateEvent::decode(payload)
                .map_err(decode_failure("ConversationUpdate"))?,
        ),
        ProtoEventType::EventConversationDelete => EventPayload::ConversationDelete(
            ConversationDeleteEvent::decode(payload)
                .map_err(decode_failure("ConversationDelete"))?,
        ),
        ProtoEventType::EventReaction => EventPayload::Reaction(
            ReactionEvent::decode(payload).map_err(decode_failure("Reaction"))?,
        ),
        ProtoEventType::EventPin => {
            EventPayload::Pin(PinEvent::decode(payload).map_err(decode_failure("Pin"))?)
        }
        ProtoEventType::EventUnpin => {
            EventPayload::Unpin(UnpinEvent::decode(payload).map_err(decode_failure("Unpin"))?)
        }
        ProtoEventType::EventMark => {
            EventPayload::Mark(MarkEvent::decode(payload).map_err(decode_failure("Mark"))?)
        }
        ProtoEventType::EventUnmark => {
            EventPayload::Unmark(UnmarkEvent::decode(payload).map_err(decode_failure("Unmark"))?)
        }
        ProtoEventType::EventMessageRetentionScheduled => EventPayload::RetentionScheduled(
            MessageRetentionScheduledEvent::decode(payload)
                .map_err(decode_failure("RetentionScheduled"))?,
        ),
        ProtoEventType::EventMessageRetentionExpired => EventPayload::RetentionExpired(
            MessageRetentionExpiredEvent::decode(payload)
                .map_err(decode_failure("RetentionExpired"))?,
        ),
        ProtoEventType::EventMessageRetentionPurged => EventPayload::RetentionPurged(
            MessageRetentionPurgedEvent::decode(payload)
                .map_err(decode_failure("RetentionPurged"))?,
        ),
        ProtoEventType::EventCustom => {
            EventPayload::Custom(CustomEvent::decode(payload).map_err(decode_failure("Custom"))?)
        }
        ProtoEventType::EventMessagePluginState => EventPayload::PluginState(
            PluginStateEvent::decode(payload).map_err(decode_failure("PluginState"))?,
        ),
        ProtoEventType::Unspecified => return Ok(None),
    };
    Ok(Some(p))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// payload 解不开的事件行是分好类的反序列化错误（调用方据此记日志并回空壳事件）；修复前
    /// 是 `.context()` 包成的系统错误，没有错误码，文本里带着 prost 的报文。
    #[test]
    fn an_undecodable_payload_is_a_classified_deserialization_error() {
        let err = proto_event_from_events_row(
            "c1",
            7,
            ProtoEventType::EventMessage as i32,
            Utc::now(),
            String::new(),
            None,
            None,
            &[0xff, 0xff, 0xff, 0xff],
        )
        .unwrap_err();
        assert_eq!(err.code(), Some(ErrorCode::DeserializationError), "{err}");
        assert_eq!(err.reason(), "decode Message event payload");
    }

    fn decode_row(event_type: ProtoEventType, payload: &[u8]) -> Option<EventPayload> {
        proto_event_from_events_row(
            "c1",
            7,
            event_type as i32,
            Utc::now(),
            String::new(),
            None,
            None,
            payload,
        )
        .expect("row decodes")
        .payload
    }

    /// Writer 的行（`event_type` 为 proto 类型，`payload` 为内层消息的 prost 编码）按类型解回
    /// 原内容。与 Writer `event_stream.rs` 的 `operation_rows_decode_as_their_declared_proto_type`
    /// 写死同一组「类型 ↔ 内层消息」，两边合起来是一次往返。
    #[test]
    fn writer_rows_decode_into_the_matching_payload() {
        let reaction = ReactionEvent {
            server_msg_id: "m1".into(),
            user_id: "u1".into(),
            emoji: "thumbsup".into(),
            action: 1,
        };
        assert_eq!(
            decode_row(ProtoEventType::EventReaction, &reaction.encode_to_vec()),
            Some(EventPayload::Reaction(reaction))
        );
        let pin = PinEvent {
            server_msg_id: "m1".into(),
            pinned_by: "u1".into(),
            reason: Some("keep".into()),
            expire_at: None,
            scope: 1,
        };
        assert_eq!(
            decode_row(ProtoEventType::EventPin, &pin.encode_to_vec()),
            Some(EventPayload::Pin(pin))
        );
        let unpin = UnpinEvent {
            server_msg_id: "m1".into(),
            unpinned_by: "u1".into(),
            scope: 1,
        };
        assert_eq!(
            decode_row(ProtoEventType::EventUnpin, &unpin.encode_to_vec()),
            Some(EventPayload::Unpin(unpin))
        );
        let mark = MarkEvent {
            server_msg_id: "m1".into(),
            user_id: "u1".into(),
            mark_type: 2,
            color: "#FF0000".into(),
        };
        assert_eq!(
            decode_row(ProtoEventType::EventMark, &mark.encode_to_vec()),
            Some(EventPayload::Mark(mark))
        );
        let unmark = UnmarkEvent {
            server_msg_id: "m1".into(),
            user_id: "u1".into(),
            mark_type: 2,
        };
        assert_eq!(
            decode_row(ProtoEventType::EventUnmark, &unmark.encode_to_vec()),
            Some(EventPayload::Unmark(unmark))
        );
    }

    /// 修复前的 Writer 把反应写成 `event_type = 11`（`EVENT_MARK`）：`emoji`（字段 3，字符串）与
    /// `MarkEvent.mark_type`（字段 3，varint）线型不同，解码失败，正是 W10-S1 F1 每轮看到的
    /// `decode Mark event payload`。置顶写成 12（`EVENT_UNMARK`）且不带 `reason` 时线型恰好兼容，
    /// 被静默解成取消标记——这类存量行不能靠 Reader 自行纠正。
    #[test]
    fn legacy_rows_with_domain_discriminants_are_misread() {
        let reaction = ReactionEvent {
            server_msg_id: "m1".into(),
            user_id: "u1".into(),
            emoji: "thumbsup".into(),
            action: 1,
        };
        let err = proto_event_from_events_row(
            "c1",
            7,
            11,
            Utc::now(),
            String::new(),
            None,
            None,
            &reaction.encode_to_vec(),
        )
        .unwrap_err();
        assert_eq!(err.code(), Some(ErrorCode::DeserializationError), "{err}");
        assert_eq!(err.reason(), "decode Mark event payload");

        let pin = PinEvent {
            server_msg_id: "m1".into(),
            pinned_by: "u1".into(),
            reason: None,
            expire_at: None,
            scope: 1,
        };
        assert!(matches!(
            decode_row(ProtoEventType::EventUnmark, &pin.encode_to_vec()),
            Some(EventPayload::Unmark(_))
        ));
    }

    #[test]
    fn an_empty_or_unknown_payload_is_not_an_error() {
        let shell = proto_event_from_events_row(
            "c1",
            7,
            ProtoEventType::EventMessage as i32,
            Utc::now(),
            String::new(),
            None,
            None,
            &[],
        )
        .unwrap();
        assert_eq!(shell.event_id, "c1:7");
        assert!(shell.payload.is_none());
    }
}
