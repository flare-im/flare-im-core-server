//! 发送幂等账本的真库用例：`PostgresMessageStore` 在写消息的同一事务里认领
//! `(tenant_id, sender_id, client_msg_id)`，以及按保留期分批删除。
//!
//! - 单条路径：同一会话里带同一 client_msg_id 的第二条消息判为重复，不写消息行、不写写入台账；首条的重投
//!   不算重复；同一发送者在另一会话复用 client_msg_id 照常写入；
//! - 批量路径（多于 10 条，走 VALUES 多行插入）：与账本里已有消息重复的、同批内两条互相重复的后一条都跳过，
//!   同批其余消息照常写入；
//! - 保留期清理只删入账早于截止时间的行，每次至多 `limit` 行。
//!
//! 需要 `FLARE_FUNCTIONAL_PG_ADMIN_URL`（可建库的连接串）。用例在其中新建临时库，只建这几张表（消息表不转成
//! Timescale 超表，账本本来就不是超表），结束时删库。标了 `#[ignore]`，默认的 `cargo test` 不跑。

use std::str::FromStr;
use std::sync::Arc;

use flare_im_contracts::Ctx;
use flare_im_contracts::message::Message;
use flare_im_contracts::utils::Context;
use flare_storage_writer::domain::repository::{
    ArchiveOutcome, ArchiveStoreRepository, IdempotencyLedgerRetention, LedgerDuplicate,
};
use flare_storage_writer::infrastructure::persistence::repository::postgres_store::PostgresMessageStore;
use sqlx::PgPool;
use sqlx::postgres::PgConnectOptions;

const ENV_ADMIN_URL: &str = "FLARE_FUNCTIONAL_PG_ADMIN_URL";
const TENANT: &str = "tenant-ledger";

#[tokio::test]
#[ignore = "requires explicit owned-test FLARE_FUNCTIONAL_PG_ADMIN_URL"]
async fn the_writer_stores_one_message_per_client_message_and_expires_ledger_rows() {
    let url = std::env::var(ENV_ADMIN_URL)
        .unwrap_or_else(|_| panic!("set {ENV_ADMIN_URL} to a role that may create databases"));
    let admin = PgPool::connect(&url).await.expect("connect admin");
    let name = format!("flare_writer_ledger_test_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE DATABASE {name}"))
        .execute(&admin)
        .await
        .expect("create database");
    let pool = PgPool::connect_with(
        PgConnectOptions::from_str(&url)
            .expect("admin url")
            .database(&name),
    )
    .await
    .expect("connect test database");
    let run = pool.clone();
    let result = tokio::spawn(async move { check(run).await }).await;
    pool.close().await;
    sqlx::query(&format!("DROP DATABASE {name} WITH (FORCE)"))
        .execute(&admin)
        .await
        .expect("drop database");
    admin.close().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error.into_panic());
    }
}

async fn create_schema(pool: &PgPool) {
    let baseline = include_str!("../../../migrations/0001_baseline.sql");
    let start = baseline.find("CREATE TABLE messages (").unwrap();
    let end = baseline
        .find("SELECT create_hypertable('messages'")
        .unwrap();
    sqlx::raw_sql(&baseline[start..end])
        .execute(pool)
        .await
        .expect("messages table");
    let start = baseline
        .find("CREATE TABLE message_write_ledger (")
        .unwrap();
    let end = start + baseline[start..].find("\n);").unwrap() + 3;
    sqlx::raw_sql(&baseline[start..end])
        .execute(pool)
        .await
        .expect("message write ledger");
    sqlx::raw_sql(include_str!(
        "../../../migrations/0020_20261104_message_idempotency_ledger.sql"
    ))
    .execute(pool)
    .await
    .expect("migration 0020 applies");
}

fn ctx() -> Ctx {
    Arc::new(Context::with_request_id("req-ledger").with_tenant_id(TENANT))
}

fn message(server_id: &str, conversation_id: &str, client_msg_id: &str, seq: u64) -> Message {
    Message {
        server_id: server_id.to_string(),
        conversation_id: conversation_id.to_string(),
        client_msg_id: client_msg_id.to_string(),
        sender_id: "alice".to_string(),
        conversation_seq: seq,
        extra: [("tenant_id".to_string(), TENANT.to_string())]
            .into_iter()
            .collect(),
        ..Message::default()
    }
}

fn duplicate(server_id: &str, original_server_id: &str) -> LedgerDuplicate {
    LedgerDuplicate {
        server_id: server_id.to_string(),
        original_server_id: original_server_id.to_string(),
    }
}

async fn stored(pool: &PgPool, table: &str, server_id: &str) -> bool {
    sqlx::query_scalar::<_, i64>(&format!(
        "SELECT count(*) FROM {table} WHERE tenant_id = $1 AND server_id = $2"
    ))
    .bind(TENANT)
    .bind(server_id)
    .fetch_one(pool)
    .await
    .expect("count")
        > 0
}

async fn ledger_owner(pool: &PgPool, client_msg_id: &str) -> Option<String> {
    sqlx::query_scalar(
        "SELECT server_id FROM message_idempotency_ledger \
         WHERE tenant_id = $1 AND sender_id = 'alice' AND client_msg_id = $2",
    )
    .bind(TENANT)
    .bind(client_msg_id)
    .fetch_optional(pool)
    .await
    .expect("ledger owner")
}

async fn check(pool: PgPool) {
    create_schema(&pool).await;
    let partitions: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_inherits \
         WHERE inhparent = 'message_idempotency_ledger'::regclass",
    )
    .fetch_one(&pool)
    .await
    .expect("partitions");
    assert_eq!(partitions, 32);

    let store = PostgresMessageStore::from_pool(pool.clone());
    let ctx = ctx();

    // 单条：首条入账并落库。
    let first = store
        .archive_messages(&ctx, &[message("srv-1", "conv-a", "cli-1", 1)])
        .await
        .expect("first send");
    assert_eq!(first, ArchiveOutcome::default());
    assert!(stored(&pool, "messages", "srv-1").await);
    assert_eq!(ledger_owner(&pool, "cli-1").await.as_deref(), Some("srv-1"));

    // 同一会话、同一 client_msg_id 的第二条：判为重复，消息行与写入台账都不写。
    let resend = store
        .archive_messages(&ctx, &[message("srv-2", "conv-a", "cli-1", 2)])
        .await
        .expect("a duplicate does not fail");
    assert_eq!(resend.duplicates, [duplicate("srv-2", "srv-1")]);
    assert!(!stored(&pool, "messages", "srv-2").await);
    assert!(!stored(&pool, "message_write_ledger", "srv-2").await);

    // 首条的重投：不是重复。
    let redelivered = store
        .archive_messages(&ctx, &[message("srv-1", "conv-a", "cli-1", 1)])
        .await
        .expect("redelivery");
    assert_eq!(redelivered, ArchiveOutcome::default());

    // 另一会话复用同一 client_msg_id：照常写入，账本仍指向首条。
    let reused = store
        .archive_messages(&ctx, &[message("srv-3", "conv-b", "cli-1", 1)])
        .await
        .expect("reuse in another conversation");
    assert!(reused.duplicates.is_empty());
    assert_eq!(reused.reused_client_msg_ids, 1);
    assert!(stored(&pool, "messages", "srv-3").await);
    assert_eq!(ledger_owner(&pool, "cli-1").await.as_deref(), Some("srv-1"));

    // 没有 client_msg_id 的消息不入账，照常写入。
    let anonymous = store
        .archive_messages(&ctx, &[message("srv-4", "conv-a", "", 3)])
        .await
        .expect("no client message id");
    assert_eq!(anonymous, ArchiveOutcome::default());
    assert!(stored(&pool, "messages", "srv-4").await);

    // 批量（13 条，走 VALUES 多行插入）：b10 与账本里的 srv-1 重复；b12 与同批的 b11 重复。
    let mut batch: Vec<Message> = (0..10)
        .map(|i| message(&format!("srv-b{i}"), "conv-a", &format!("cli-b{i}"), 10 + i))
        .collect();
    batch.push(message("srv-b10", "conv-a", "cli-1", 20));
    batch.push(message("srv-b11", "conv-a", "cli-pair", 21));
    batch.push(message("srv-b12", "conv-a", "cli-pair", 22));
    let outcome = store
        .archive_messages(&ctx, &batch)
        .await
        .expect("the batch succeeds");
    let mut duplicates = outcome.duplicates.clone();
    duplicates.sort_by(|a, b| a.server_id.cmp(&b.server_id));
    assert_eq!(
        duplicates,
        [
            duplicate("srv-b10", "srv-1"),
            duplicate("srv-b12", "srv-b11")
        ]
    );
    for i in 0..10 {
        assert!(
            stored(&pool, "messages", &format!("srv-b{i}")).await,
            "b{i}"
        );
    }
    assert!(stored(&pool, "messages", "srv-b11").await);
    assert!(!stored(&pool, "messages", "srv-b10").await);
    assert!(!stored(&pool, "messages", "srv-b12").await);
    assert!(!stored(&pool, "message_write_ledger", "srv-b12").await);
    assert_eq!(
        ledger_owner(&pool, "cli-pair").await.as_deref(),
        Some("srv-b11")
    );

    // 保留期：入账 8 天前的行过期，删除至多 limit 行，新行保留。
    sqlx::query(
        "INSERT INTO message_idempotency_ledger \
         (tenant_id, sender_id, client_msg_id, conversation_id, server_id, conversation_seq, durability, created_at) \
         SELECT $1, 'old-sender', 'cli-old-' || n, 'conv-old', 'srv-old-' || n, n, 4, now() - interval '8 days' \
         FROM generate_series(1, 3) AS n",
    )
    .bind(TENANT)
    .execute(&pool)
    .await
    .expect("expired rows");
    let total_before: i64 = sqlx::query_scalar("SELECT count(*) FROM message_idempotency_ledger")
        .fetch_one(&pool)
        .await
        .unwrap();
    let cutoff = chrono::Utc::now() - chrono::Duration::days(7);
    assert_eq!(store.delete_created_before(cutoff, 2).await.unwrap(), 2);
    assert_eq!(store.delete_created_before(cutoff, 2).await.unwrap(), 1);
    assert_eq!(store.delete_created_before(cutoff, 2).await.unwrap(), 0);
    let total_after: i64 = sqlx::query_scalar("SELECT count(*) FROM message_idempotency_ledger")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(total_before - total_after, 3);
    assert_eq!(ledger_owner(&pool, "cli-1").await.as_deref(), Some("srv-1"));
}
