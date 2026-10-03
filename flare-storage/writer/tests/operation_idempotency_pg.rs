//! 操作事件判重与操作历史幂等的真库用例（W13-R1）。
//!
//! - 应用前判重：同一 `(tenant_id, conversation_id, seq)` 上已有同一个事件（重投）时跳过；已有的是另一个
//!   事件时不应用、报不可重试的冲突（`HttpConflict`）。修复前只按键查、不比内容，另一个事件被静默吞掉。
//! - 操作历史：各 handler 先记操作历史、再写事件流；写事件流失败后的重投再记一次时，按（租户，会话，seq，
//!   消息）只留一行（迁移 0009 的唯一索引）。修复前每次重投多记一行。
//!
//! 需要 `FLARE_STORAGE_TEST_DATABASE_URL`，指向已按 `migrations/`（`flare-migrate run`）或 `deploy/init.sql`
//! 建好的库。用例标了 `#[ignore]`：默认的 `cargo test` 只记为 ignored，`-- --ignored` 显式运行时变量必须
//! 已设置，否则直接失败。每次运行用新的会话与消息 ID，结束时删掉自己的行。

use std::sync::Arc;

use flare_im_contracts::Ctx;
use flare_im_contracts::utils::Context;
use flare_im_service_kit::metrics::StorageWriterMetrics;
use flare_server_core::error::ErrorCode;
use flare_storage_writer::application::commands::ProcessEventCommand;
use flare_storage_writer::application::handlers::MessageOperationCommandHandler;
use flare_storage_writer::application::handlers::operation_handler::is_operation_conflict;
use flare_storage_writer::domain::model::{
    Event, EventPayload, EventType, PinPayload, ReactionPayload, ReadPayload,
};
use flare_storage_writer::domain::repository::ArchiveStoreRepository;
use flare_storage_writer::domain::service::EventApplicationService;
use flare_storage_writer::infrastructure::persistence::repository::event_stream::PostgresEventStreamStore;
use flare_storage_writer::infrastructure::persistence::repository::postgres_store::PostgresMessageStore;
use sqlx::PgPool;

const ENV_DATABASE_URL: &str = "FLARE_STORAGE_TEST_DATABASE_URL";
const TENANT: &str = "tenant-w13r1";

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

fn reaction(conversation_id: &str, seq: u64, message_id: &str, emoji: &str) -> Event {
    event(
        conversation_id,
        seq,
        EventType::Reaction,
        EventPayload::Reaction(ReactionPayload {
            server_msg_id: message_id.to_string(),
            user_id: "u1".to_string(),
            emoji: emoji.to_string(),
            action: 1,
        }),
    )
}

fn pin(conversation_id: &str, seq: u64, message_id: &str) -> Event {
    event(
        conversation_id,
        seq,
        EventType::Pin,
        EventPayload::Pin(PinPayload {
            server_msg_id: message_id.to_string(),
            pinned_by: "u1".to_string(),
            reason: None,
            expire_at: None,
            scope: 2,
        }),
    )
}

fn read(conversation_id: &str, seq: u64, message_ids: &[&str]) -> Event {
    event(
        conversation_id,
        seq,
        EventType::ReadReceipt,
        EventPayload::Read(ReadPayload {
            conversation_id: conversation_id.to_string(),
            read_seq: seq,
            user_id: "u2".to_string(),
            message_ids: message_ids.iter().map(ToString::to_string).collect(),
            read_at: None,
            burn_after_read: None,
        }),
    )
}

async fn count(pool: &PgPool, sql: &str, binds: &[&str]) -> i64 {
    let mut query = sqlx::query_scalar::<_, i64>(sql);
    for bind in binds {
        query = query.bind(*bind);
    }
    query.fetch_one(pool).await.expect("count")
}

/// 该消息的操作历史行数（不论是否带身份列，修复前写入的行也算）。
async fn history_rows(pool: &PgPool, message_id: &str) -> i64 {
    count(
        pool,
        "SELECT count(*) FROM message_operation_history WHERE tenant_id = $1 AND message_id = $2",
        &[TENANT, message_id],
    )
    .await
}

/// 该消息带（会话，seq）身份的操作历史行数。
async fn identified_rows(pool: &PgPool, conversation_id: &str, seq: i64, message_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM message_operation_history \
         WHERE tenant_id = $1 AND conversation_id = $2 AND seq = $3 AND message_id = $4",
    )
    .bind(TENANT)
    .bind(conversation_id)
    .bind(seq)
    .bind(message_id)
    .fetch_one(pool)
    .await
    .expect("count identified history rows")
}

async fn stored_event_type(pool: &PgPool, conversation_id: &str, seq: i64) -> Option<i32> {
    sqlx::query_scalar(
        "SELECT event_type FROM events WHERE tenant_id = $1 AND conversation_id = $2 AND seq = $3",
    )
    .bind(TENANT)
    .bind(conversation_id)
    .bind(seq)
    .fetch_optional(pool)
    .await
    .expect("select event")
}

#[tokio::test]
#[ignore = "requires FLARE_STORAGE_TEST_DATABASE_URL pointing at a migrated Core database"]
async fn operation_events_are_deduplicated_by_content_and_history_rows_by_identity() {
    let url = std::env::var(ENV_DATABASE_URL)
        .unwrap_or_else(|_| panic!("set {ENV_DATABASE_URL} to a migrated Core database"));
    let pool = PgPool::connect(&url).await.expect("connect");
    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM pg_indexes WHERE schemaname = 'public' \
             AND indexname = 'idx_message_operation_history_event_identity'",
            &[],
        )
        .await,
        1,
        "the database lacks migration 0009; run flare-migrate first"
    );

    let archive = Arc::new(PostgresMessageStore::from_pool(pool.clone()));
    let service = Arc::new(EventApplicationService::new(
        Some(archive.clone()),
        Some(Arc::new(PostgresEventStreamStore::new(pool.clone()))),
    ));
    let handler =
        MessageOperationCommandHandler::new(service.clone(), Arc::new(StorageWriterMetrics::new()));
    let ctx: Ctx = Arc::new(Context::with_request_id("w13r1").with_tenant_id(TENANT));
    let run = uuid::Uuid::new_v4();
    let conv = format!("conv-w13r1-{run}");
    let [m1, m2, m3, m4] = [1, 2, 3, 4].map(|n| format!("srv-w13r1-{run}-{n}"));

    // 1. 新事件：应用，记一行操作历史，落事件流。
    let first = reaction(&conv, 1, &m1, "thumbsup");
    service
        .process_event(&ctx, &first)
        .await
        .expect("a new reaction is applied");
    assert_eq!(stored_event_type(&pool, &conv, 1).await, Some(8));
    assert_eq!(history_rows(&pool, &m1).await, 1);

    // 2. 重投同一事件：判为重复，跳过。
    service
        .process_event(&ctx, &first)
        .await
        .expect("a redelivery is acknowledged");
    assert_eq!(history_rows(&pool, &m1).await, 1);

    // 3. 同一 seq 上的另一个事件（置顶）：不应用，报不可重试的冲突；已有行与操作历史不变，没有置顶。
    let err = service
        .process_event(&ctx, &pin(&conv, 1, &m1))
        .await
        .expect_err("another event at the same seq must not be acknowledged");
    assert_eq!(err.code(), Some(ErrorCode::HttpConflict), "{err}");
    assert!(!err.is_retryable(), "{err}");
    // 经应用层处理器时冲突分类不被改写成数据库错误：消费者据此直接进死信，不做无用的重投。
    let err = handler
        .handle(
            &ctx,
            ProcessEventCommand {
                event: pin(&conv, 1, &m1),
            },
        )
        .await
        .expect_err("the handler reports the conflict");
    assert!(is_operation_conflict(&err), "{err}");
    assert_eq!(stored_event_type(&pool, &conv, 1).await, Some(8));
    assert_eq!(history_rows(&pool, &m1).await, 1);
    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM pinned_messages WHERE tenant_id = $1 AND message_id = $2",
            &[TENANT, &m1],
        )
        .await,
        0
    );

    // 4. 上一次投递记了操作历史、写事件流失败（事件流里没有这一行），重投时操作历史不再多记。
    let retried = reaction(&conv, 2, &m2, "heart");
    archive
        .append_event(&ctx, TENANT, &m2, &retried)
        .await
        .expect("history row of the failed attempt");
    service
        .process_event(&ctx, &retried)
        .await
        .expect("the retried reaction is applied");
    assert_eq!(stored_event_type(&pool, &conv, 2).await, Some(8));
    assert_eq!(history_rows(&pool, &m2).await, 1);

    // 5. 已读回执一个事件对应两条消息：各一行，重投后仍各一行。
    let receipt = read(&conv, 3, &[&m3, &m4]);
    for message_id in [&m3, &m4] {
        archive
            .append_event(&ctx, TENANT, message_id, &receipt)
            .await
            .expect("history rows of the failed attempt");
    }
    service
        .process_event(&ctx, &receipt)
        .await
        .expect("the retried read receipt is applied");
    for message_id in [&m3, &m4] {
        assert_eq!(history_rows(&pool, message_id).await, 1, "{message_id}");
    }

    // 6. 新写入的行都带（会话，seq）身份，唯一索引据此判重。
    for (seq, message_id) in [(1, &m1), (2, &m2), (3, &m3), (3, &m4)] {
        assert_eq!(
            identified_rows(&pool, &conv, seq, message_id).await,
            1,
            "{message_id}"
        );
    }

    for sql in [
        "DELETE FROM events WHERE tenant_id = $1 AND conversation_id = $2",
        "DELETE FROM message_operation_history WHERE tenant_id = $1 AND conversation_id = $2",
    ] {
        sqlx::query(sql)
            .bind(TENANT)
            .bind(&conv)
            .execute(&pool)
            .await
            .expect("clean up");
    }
    for sql in [
        "DELETE FROM message_reactions WHERE tenant_id = $1 AND message_id = ANY($2)",
        "DELETE FROM message_read_records WHERE tenant_id = $1 AND message_id = ANY($2)",
        "DELETE FROM message_operation_history WHERE tenant_id = $1 AND message_id = ANY($2)",
    ] {
        sqlx::query(sql)
            .bind(TENANT)
            .bind(vec![m1.clone(), m2.clone(), m3.clone(), m4.clone()])
            .execute(&pool)
            .await
            .expect("clean up");
    }
}
