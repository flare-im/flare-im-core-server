//! Storage Writer 与 Storage Reader 之间的事件编码契约（W12-R）。
//!
//! Writer 写 `events` 行（`event_type` + `Event.payload` 内层消息的编码）与
//! `message_operation_history.operation_type`，Reader 读回。两侧各有一份映射，曾经各自错位：
//! Writer 以领域判别值落类型号、Reader 按 proto 解（W10-S1 F1），Writer 写 `EVENT_*` 串、Reader
//! 只认旧名而全部读成 Custom（W11-R）。这里用两侧的真实代码做往返，覆盖 Writer 领域 `EventType`
//! 的全部变体与 proto `EventType` 的全部取值：proto 新增取值、或任一侧改了映射而另一侧没跟上，
//! 这里都会失败。

use flare_proto::common::{self, EventType as ProtoEventType};
use flare_storage_reader::convert as reader_convert;
use flare_storage_reader::infrastructure::persistence::event_stream_row::proto_event_from_events_row;
use flare_storage_reader::infrastructure::persistence::optimized_postgres_store::event_type_from_operation_type;
use flare_storage_writer::convert as writer_convert;
use flare_storage_writer::domain::model::{
    BurnScheduledPayload, BurnedPayload, CustomPayload, DeletePayload, EditPayload, Event,
    EventPayload, EventType, HardDeletedPayload, MarkPayload, Message, PinPayload,
    PluginStatePayload, ReactionPayload, ReadPayload, RecallPayload, UnmarkPayload, UnpinPayload,
};
use flare_storage_writer::infrastructure::persistence::repository::event_stream::event_to_insert_row;
use flare_storage_writer::infrastructure::persistence::repository::operation_store::operation_type_name;

/// Writer 领域 `EventType` 的全部变体。新增变体时 `writer_variant_list_is_exhaustive` 的 match 不再
/// 编译，提醒把它加进来。
const WRITER_EVENT_TYPES: [EventType; 21] = [
    EventType::Unspecified,
    EventType::Message,
    EventType::MessageRecall,
    EventType::MessageEdit,
    EventType::MessageDelete,
    EventType::ReadReceipt,
    EventType::Typing,
    EventType::ConversationUpdate,
    EventType::ConversationDelete,
    EventType::Presence,
    EventType::CallSignal,
    EventType::Reaction,
    EventType::Pin,
    EventType::Unpin,
    EventType::Mark,
    EventType::Unmark,
    EventType::MessageBurnScheduled,
    EventType::MessageBurned,
    EventType::MessageHardDeleted,
    EventType::PluginState,
    EventType::Custom,
];

#[test]
fn writer_variant_list_is_exhaustive() {
    for event_type in WRITER_EVENT_TYPES {
        // 穷举 match：Writer 领域枚举新增变体时这里编译失败。
        match event_type {
            EventType::Unspecified
            | EventType::Message
            | EventType::MessageRecall
            | EventType::MessageEdit
            | EventType::MessageDelete
            | EventType::ReadReceipt
            | EventType::Typing
            | EventType::ConversationUpdate
            | EventType::ConversationDelete
            | EventType::Presence
            | EventType::CallSignal
            | EventType::Reaction
            | EventType::Pin
            | EventType::Unpin
            | EventType::Mark
            | EventType::Unmark
            | EventType::MessageBurnScheduled
            | EventType::MessageBurned
            | EventType::MessageHardDeleted
            | EventType::PluginState
            | EventType::Custom => {}
        }
    }
}

/// proto `EventType` 的全部取值（按数值枚举，不靠手写列表）。
fn proto_event_types() -> Vec<ProtoEventType> {
    (0..=256)
        .filter_map(|value| ProtoEventType::try_from(value).ok())
        .collect()
}

/// Reader 读回后应得到的领域类型名：两侧领域枚举的变体同名；proto 没有的实时类
/// （Typing/Presence/CallSignal）落库为未指定，读回也是未指定。
fn expected_reader_type_name(event_type: EventType) -> String {
    match event_type {
        EventType::Typing | EventType::Presence | EventType::CallSignal => {
            "Unspecified".to_string()
        }
        other => format!("{other:?}"),
    }
}

fn text_content(text: &str) -> common::MessageContent {
    common::MessageContent {
        content: Some(common::message_content::Content::Text(
            common::TextContent {
                text: text.to_string(),
                mentions: Vec::new(),
            },
        )),
    }
}

/// 每种类型一个有内容的载荷；Writer 没有对应载荷变体的类型为 `None`。
fn sample_payload(event_type: EventType) -> Option<EventPayload> {
    let server_msg_id = "srv-1".to_string();
    Some(match event_type {
        EventType::Message => EventPayload::Message(Box::new(Message {
            server_id: server_msg_id,
            conversation_id: "conv-a".to_string(),
            client_msg_id: "cli-1".to_string(),
            sender_id: "u1".to_string(),
            conversation_seq: 42,
            conversation_type: 2,
            message_type: 1,
            content: Some(text_content("hello")),
            extra: [
                ("tenant_id", "tenant-a"),
                ("business_type", "chat"),
                ("k", "v"),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
            ..Default::default()
        })),
        EventType::MessageRecall => EventPayload::Recall(RecallPayload {
            server_msg_id,
            reason: "typo".to_string(),
            time_limit_seconds: Some(120),
            allow_admin_recall: Some(true),
        }),
        EventType::MessageEdit => EventPayload::Edit(EditPayload {
            server_msg_id,
            new_content: prost::Message::encode_to_vec(&text_content("edited")),
            reason: "fix".to_string(),
            show_edited_mark: true,
        }),
        EventType::MessageDelete => EventPayload::Delete(DeletePayload {
            server_msg_id,
            delete_type: Some(1),
            scope: Some(2),
            target_user_id: Some("u2".to_string()),
            reason: Some("spam".to_string()),
            notify_others: Some(true),
        }),
        EventType::ReadReceipt => EventPayload::Read(ReadPayload {
            conversation_id: "conv-a".to_string(),
            read_seq: 41,
            user_id: "u2".to_string(),
            message_ids: vec![server_msg_id],
            read_at: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            burn_after_read: None,
        }),
        EventType::Reaction => EventPayload::Reaction(ReactionPayload {
            server_msg_id,
            user_id: "u2".to_string(),
            emoji: "thumbsup".to_string(),
            action: 1,
        }),
        EventType::Pin => EventPayload::Pin(PinPayload {
            server_msg_id,
            pinned_by: "u2".to_string(),
            reason: Some("keep".to_string()),
            expire_at: Some(prost_types::Timestamp {
                seconds: 1_700_000_100,
                nanos: 0,
            }),
            scope: 1,
        }),
        EventType::Unpin => EventPayload::Unpin(UnpinPayload {
            server_msg_id,
            unpinned_by: "u2".to_string(),
            scope: 1,
        }),
        EventType::Mark => EventPayload::Mark(MarkPayload {
            server_msg_id,
            user_id: "u2".to_string(),
            mark_type: 2,
            color: "#FF0000".to_string(),
        }),
        EventType::Unmark => EventPayload::Unmark(UnmarkPayload {
            server_msg_id,
            user_id: "u2".to_string(),
            mark_type: 2,
        }),
        EventType::MessageBurnScheduled => EventPayload::BurnScheduled(BurnScheduledPayload {
            tenant_id: "tenant-a".to_string(),
            conversation_id: "conv-a".to_string(),
            message_id: server_msg_id.clone(),
            server_id: server_msg_id,
            seq: Some(42),
            reader_id: Some("u2".to_string()),
            burn_at: 1_700_000_200_000,
            event_time: 1_700_000_190_000,
        }),
        EventType::MessageBurned => EventPayload::Burned(BurnedPayload {
            tenant_id: "tenant-a".to_string(),
            conversation_id: "conv-a".to_string(),
            message_id: server_msg_id.clone(),
            server_id: server_msg_id,
            seq: Some(42),
            reader_id: Some("u2".to_string()),
            burn_at: 1_700_000_200_000,
            burned_at: 1_700_000_201_000,
            event_time: 1_700_000_201_000,
        }),
        EventType::MessageHardDeleted => EventPayload::HardDeleted(HardDeletedPayload {
            tenant_id: "tenant-a".to_string(),
            conversation_id: "conv-a".to_string(),
            message_id: server_msg_id.clone(),
            server_id: server_msg_id,
            seq: Some(42),
            reader_id: Some("u2".to_string()),
            burn_at: Some(1_700_000_200_000),
            burned_at: Some(1_700_000_201_000),
            event_time: 1_700_000_300_000,
        }),
        EventType::PluginState => EventPayload::PluginState(PluginStatePayload {
            server_msg_id,
            plugin_id: "example.poll".to_string(),
            entity: "poll-1".to_string(),
            version: 3,
            values_json: r#"{"counts":[2,1],"closed":false}"#.to_string(),
        }),
        EventType::Custom => EventPayload::Custom(CustomPayload {
            namespace: "biz".to_string(),
            name: "poke".to_string(),
            version: "1".to_string(),
            payload: b"opaque".to_vec(),
            metadata: Default::default(),
        }),
        EventType::Unspecified
        | EventType::Typing
        | EventType::ConversationUpdate
        | EventType::ConversationDelete
        | EventType::Presence
        | EventType::CallSignal => return None,
    })
}

fn writer_event(event_type: EventType) -> Event {
    Event {
        tenant_id: "tenant-a".to_string(),
        conversation_id: "conv-a".to_string(),
        seq: 42,
        r#type: event_type,
        created_at: Some(prost_types::Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
        }),
        operator_id: "u2".to_string(),
        event_seq: Some(7),
        request_id: Some("req-1".to_string()),
        payload: sample_payload(event_type),
    }
}

/// `events` 行：Writer `event_to_insert_row` 编码，Reader `proto_event_from_events_row` 解码，
/// 得到的类型号与内层载荷等于 Writer 要表达的 proto 事件；Reader 领域类型与 Writer 一致。
#[test]
fn events_rows_round_trip_from_writer_to_reader_for_every_event_type() {
    let mut proto_types_written = Vec::new();
    for event_type in WRITER_EVENT_TYPES {
        let event = writer_event(event_type);
        let intended = writer_convert::event_to_proto(&event);
        let row = event_to_insert_row(&event)
            .expect("encode")
            .expect("row for a conversation event");
        assert_eq!(row.event_type, intended.r#type, "{event_type:?}");

        let read = proto_event_from_events_row(
            &row.conversation_id,
            row.seq,
            row.event_type,
            row.created_at,
            row.operator_id.clone(),
            row.request_id.clone(),
            row.event_seq,
            &row.payload,
        )
        .unwrap_or_else(|err| panic!("{event_type:?}: reader cannot decode the row: {err}"));
        assert_eq!(read.r#type, intended.r#type, "{event_type:?}");
        assert_eq!(read.payload, intended.payload, "{event_type:?}");
        assert_eq!(read.conversation_seq, 42, "{event_type:?}");
        assert_eq!(read.request_id.as_deref(), Some("req-1"), "{event_type:?}");
        assert_eq!(read.created_at, 1_700_000_000_000, "{event_type:?}");

        let reader_type = reader_convert::event_from_proto(&read).r#type;
        assert_eq!(
            format!("{reader_type:?}"),
            expected_reader_type_name(event_type),
            "{event_type:?}"
        );
        assert_eq!(
            reader_convert::event_type_to_proto_i32(reader_type),
            row.event_type,
            "{event_type:?}"
        );
        proto_types_written.push(row.event_type);
    }

    // Writer 能写出 proto 的每一个取值（否则 Reader 那一侧的分支没有来源）。
    for proto in proto_event_types() {
        assert!(
            proto_types_written.contains(&(proto as i32)),
            "no writer event type persists {}",
            proto.as_str_name()
        );
    }
}

/// 判断 Reader 解回的载荷是不是期望的变体。
type IsVariant = fn(&common::event::Payload) -> bool;

/// 有内层载荷的类型：Reader 解回的是对应的载荷变体，不是别的类型（W10-S1 F1 的反应被读成标记）。
#[test]
fn every_persisted_payload_is_read_back_as_its_own_variant() {
    use common::event::Payload as P;
    let cases: [(EventType, IsVariant); 13] = [
        (EventType::Message, |p| matches!(p, P::Message(_))),
        (EventType::MessageRecall, |p| matches!(p, P::Recall(_))),
        (EventType::MessageEdit, |p| matches!(p, P::Edit(_))),
        (EventType::MessageDelete, |p| matches!(p, P::Delete(_))),
        (EventType::ReadReceipt, |p| matches!(p, P::Read(_))),
        (EventType::Reaction, |p| matches!(p, P::Reaction(_))),
        (EventType::Pin, |p| matches!(p, P::Pin(_))),
        (EventType::Unpin, |p| matches!(p, P::Unpin(_))),
        (EventType::Mark, |p| matches!(p, P::Mark(_))),
        (EventType::Unmark, |p| matches!(p, P::Unmark(_))),
        (EventType::MessageBurnScheduled, |p| {
            matches!(p, P::RetentionScheduled(_))
        }),
        (EventType::MessageBurned, |p| {
            matches!(p, P::RetentionExpired(_))
        }),
        (EventType::MessageHardDeleted, |p| {
            matches!(p, P::RetentionPurged(_))
        }),
    ];
    for (event_type, is_expected_variant) in cases {
        let row = event_to_insert_row(&writer_event(event_type))
            .expect("encode")
            .expect("row");
        assert!(!row.payload.is_empty(), "{event_type:?}");
        let read = proto_event_from_events_row(
            &row.conversation_id,
            row.seq,
            row.event_type,
            row.created_at,
            row.operator_id.clone(),
            row.request_id.clone(),
            row.event_seq,
            &row.payload,
        )
        .expect("decode");
        let payload = read.payload.expect("payload present");
        assert!(is_expected_variant(&payload), "{event_type:?}: {payload:?}");
    }

    // 自定义事件的载荷不进事件流（Writer 只编码上面这些变体），Reader 读回无载荷。
    let row = event_to_insert_row(&writer_event(EventType::Custom))
        .expect("encode")
        .expect("row");
    assert!(row.payload.is_empty());
}

/// `message_operation_history.operation_type`：Writer `operation_type_name` 写串，Reader
/// `event_type_from_operation_type` 解回，覆盖全部 proto 取值与 Writer 全部领域类型。
#[test]
fn operation_history_type_names_round_trip_for_every_event_type() {
    for proto in proto_event_types() {
        let name = operation_type_name(proto as i32);
        let reader_type = event_type_from_operation_type(name);
        assert_eq!(
            reader_convert::event_type_to_proto_i32(reader_type),
            proto as i32,
            "{name}"
        );
    }

    for event_type in WRITER_EVENT_TYPES {
        let proto = writer_convert::event_to_proto(&writer_event(event_type)).r#type;
        let reader_type = event_type_from_operation_type(operation_type_name(proto));
        assert_eq!(
            format!("{reader_type:?}"),
            expected_reader_type_name(event_type),
            "{event_type:?}"
        );
    }
}

/// Writer 消费 MQ 上的 proto 事件时用的反向映射（`convert::event_from_proto`）与落库方向互逆。
#[test]
fn writer_proto_mapping_is_its_own_inverse_for_every_proto_type() {
    for proto in proto_event_types() {
        let event = writer_convert::event_from_proto(&common::Event {
            conversation_id: "conv-a".to_string(),
            conversation_seq: 1,
            r#type: proto as i32,
            ..Default::default()
        });
        assert_eq!(
            writer_convert::event_to_proto(&event).r#type,
            proto as i32,
            "{}",
            proto.as_str_name()
        );
    }
}
