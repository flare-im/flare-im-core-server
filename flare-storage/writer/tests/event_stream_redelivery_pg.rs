//! 事件流重投的真库用例（W12-R）：同一 `(tenant_id, conversation_id, seq)` 已有一行时，
//! `PostgresEventStreamStore` 对修复前类型号的重投、换了 map 顺序的重投按重复接受并保留已有行，
//! 对同一位置上的另一个事件仍报冲突。单条与批量两条写入路径都覆盖。
//!
//! 需要 `FLARE_STORAGE_TEST_DATABASE_URL`（任意 PostgreSQL；没有 `events` 表时按
//! `deploy/init.sql` 的列建一张）。用例标了 `#[ignore]`：默认的 `cargo test` 只记为 ignored，
//! `-- --ignored` 显式运行时变量必须已设置，否则直接失败。每次运行用新的会话 ID，结束时删掉自己的行。

use std::sync::Arc;

use flare_im_contracts::Ctx;
use flare_im_contracts::utils::Context;
use flare_proto::common;
use flare_storage_writer::domain::model::{
    Event, EventPayload, EventType, Message, PinPayload, ReactionPayload,
};
use flare_storage_writer::domain::repository::EventStreamRepository;
use flare_storage_writer::infrastructure::persistence::repository::event_stream::{
    EventStreamInsertRow, PostgresEventStreamStore, event_to_insert_row,
};
use sqlx::{PgPool, Row};

const ENV_DATABASE_URL: &str = "FLARE_STORAGE_TEST_DATABASE_URL";
const TENANT: &str = "tenant-w12r";

const EVENTS_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS events (
    tenant_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    seq BIGINT NOT NULL,
    event_type INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    operator_id TEXT,
    request_id TEXT,
    event_seq BIGINT,
    payload BYTEA,
    PRIMARY KEY (tenant_id, conversation_id, seq)
)
"#;

fn event(conversation_id: &str, seq: u64, r#type: EventType, payload: EventPayload) -> Event {
    Event {
        tenant_id: TENANT.to_string(),
        conversation_id: conversation_id.to_string(),
        seq,
        r#type,
        created_at: None,
        operator_id: "u1".to_string(),
        event_seq: None,
        request_id: None,
        payload: Some(payload),
    }
}

fn reaction(conversation_id: &str, seq: u64, emoji: &str) -> Event {
    event(
        conversation_id,
        seq,
        EventType::Reaction,
        EventPayload::Reaction(ReactionPayload {
            server_msg_id: "srv-1".to_string(),
            user_id: "u1".to_string(),
            emoji: emoji.to_string(),
            action: 1,
        }),
    )
}

fn pin(conversation_id: &str, seq: u64) -> Event {
    event(
        conversation_id,
        seq,
        EventType::Pin,
        EventPayload::Pin(PinPayload {
            server_msg_id: "srv-1".to_string(),
            pinned_by: "u1".to_string(),
            reason: None,
            expire_at: None,
            scope: 1,
        }),
    )
}

fn message(conversation_id: &str, seq: u64) -> Event {
    event(
        conversation_id,
        seq,
        EventType::Message,
        EventPayload::Message(Box::new(Message {
            server_id: format!("srv-{seq}"),
            conversation_id: conversation_id.to_string(),
            client_msg_id: format!("cli-{seq}"),
            sender_id: "u1".to_string(),
            conversation_seq: seq,
            message_type: 1,
            content: Some(common::MessageContent {
                content: Some(common::message_content::Content::Text(
                    common::TextContent {
                        text: "hello".to_string(),
                        mentions: Vec::new(),
                    },
                )),
            }),
            extra: [("tenant_id", TENANT), ("business_type", "chat"), ("k", "v")]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            ..Default::default()
        })),
    )
}

fn row_for(event: &Event) -> EventStreamInsertRow {
    event_to_insert_row(event)
        .expect("encode")
        .expect("row for a conversation event")
}

/// 同一条消息换一种 map 条目顺序的编码（与 prost 按 HashMap 迭代序编码时两次投递的字节差异同类）。
fn reordered_message_payload(row: &EventStreamInsertRow) -> Vec<u8> {
    let message = <common::Message as prost::Message>::decode(row.payload.as_slice())
        .expect("message payload");
    let mut entries: Vec<_> = message.attributes.clone().into_iter().collect();
    entries.sort();
    let mut base = message.clone();
    base.attributes.clear();
    let encode = |entries: &[(String, String)]| {
        let mut bytes = prost::Message::encode_to_vec(&base);
        for (key, value) in entries {
            bytes.extend(prost::Message::encode_to_vec(&common::Message {
                attributes: [(key.clone(), value.clone())].into_iter().collect(),
                ..Default::default()
            }));
        }
        bytes
    };
    let ascending = encode(&entries);
    if ascending != row.payload {
        return ascending;
    }
    entries.reverse();
    let descending = encode(&entries);
    assert_ne!(descending, row.payload);
    descending
}

async fn insert_raw(
    pool: &PgPool,
    conversation_id: &str,
    seq: i64,
    event_type: i32,
    payload: &[u8],
) {
    sqlx::query(
        "INSERT INTO events (tenant_id, conversation_id, seq, event_type, payload) VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(TENANT)
    .bind(conversation_id)
    .bind(seq)
    .bind(event_type)
    .bind(payload)
    .execute(pool)
    .await
    .expect("insert existing row");
}

async fn stored(pool: &PgPool, conversation_id: &str, seq: i64) -> Option<(i32, Vec<u8>)> {
    sqlx::query(
        "SELECT event_type, payload FROM events WHERE tenant_id = $1 AND conversation_id = $2 AND seq = $3",
    )
    .bind(TENANT)
    .bind(conversation_id)
    .bind(seq)
    .fetch_optional(pool)
    .await
    .expect("select row")
    .map(|row| {
        (
            row.get::<i32, _>("event_type"),
            row.get::<Option<Vec<u8>>, _>("payload").unwrap_or_default(),
        )
    })
}

#[tokio::test]
#[ignore = "requires FLARE_STORAGE_TEST_DATABASE_URL pointing at a PostgreSQL database"]
async fn redeliveries_are_duplicates_and_other_events_at_the_same_seq_conflict() {
    let url = std::env::var(ENV_DATABASE_URL)
        .unwrap_or_else(|_| panic!("set {ENV_DATABASE_URL} to a PostgreSQL database"));
    let pool = PgPool::connect(&url).await.expect("connect");
    sqlx::query(EVENTS_DDL)
        .execute(&pool)
        .await
        .expect("events table");
    let store = PostgresEventStreamStore::new(pool.clone());
    let ctx: Ctx = Arc::new(Context::with_request_id("w12r").with_tenant_id(TENANT));
    let conv = format!("conv-w12r-{}", uuid::Uuid::new_v4());

    // 1. 修复前的 Writer 以领域判别值 11 落的反应行；升级后同一事件重投（类型号 8）按重复接受，
    //    已有行原样保留。
    let redelivered = reaction(&conv, 1, "thumbsup");
    let row = row_for(&redelivered);
    assert_eq!((row.event_type, row.legacy_event_type), (8, 11));
    insert_raw(&pool, &conv, 1, 11, &row.payload).await;
    store
        .append_event_to_stream(&ctx, &redelivered)
        .await
        .expect("pre-fix row of the same reaction is a duplicate");
    assert_eq!(
        stored(&pool, &conv, 1).await,
        Some((11, row.payload.clone()))
    );

    // 2. 同一位置上的另一个反应仍是冲突。
    store
        .append_event_to_stream(&ctx, &reaction(&conv, 1, "heart"))
        .await
        .expect_err("another reaction at the same seq conflicts");

    // 3. 同一条消息、map 条目顺序不同的已有行：按重复接受，保留已有字节。
    let message_event = message(&conv, 2);
    let message_row = row_for(&message_event);
    let reordered = reordered_message_payload(&message_row);
    insert_raw(&pool, &conv, 2, 1, &reordered).await;
    store
        .append_event_to_stream(&ctx, &message_event)
        .await
        .expect("re-encoded message is a duplicate");
    assert_eq!(stored(&pool, &conv, 2).await, Some((1, reordered)));

    // 4. 批量路径：修复前类型号的置顶（12）重投 + 一条新消息，整批成功、新消息落库。
    let pin_event = pin(&conv, 3);
    let pin_row = row_for(&pin_event);
    assert_eq!((pin_row.event_type, pin_row.legacy_event_type), (9, 12));
    insert_raw(&pool, &conv, 3, 12, &pin_row.payload).await;
    store
        .append_events_to_stream(&ctx, &[pin_event, message(&conv, 4)])
        .await
        .expect("batch with a pre-fix redelivery and a new message");
    assert_eq!(stored(&pool, &conv, 3).await, Some((12, pin_row.payload)));
    assert_eq!(
        stored(&pool, &conv, 4)
            .await
            .map(|(event_type, _)| event_type),
        Some(1)
    );

    // 5. 批量里夹着同一位置上的另一个事件：整批报冲突。
    store
        .append_events_to_stream(&ctx, &[message(&conv, 5), reaction(&conv, 1, "heart")])
        .await
        .expect_err("batch with a conflicting event fails");

    sqlx::query("DELETE FROM events WHERE tenant_id = $1 AND conversation_id = $2")
        .bind(TENANT)
        .bind(&conv)
        .execute(&pool)
        .await
        .expect("clean up");
}
