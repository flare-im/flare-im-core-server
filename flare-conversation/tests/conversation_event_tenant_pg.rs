//! 会话事件消费者的租户隔离（W9-S3 F1）：消费者曾把租户写死为 `"0"`，非 0 租户的会话 seq、
//! 成员未读与已读游标从不推进；单聊会话 ID 不含租户，同一 user_id 对出现在两个租户时，非 0
//! 租户的事件还会改写租户 0 的同 ID 会话。
//!
//! 这里让两个租户各有同一 user_id 对、同一会话 ID，经 [`ConversationEventHandler`]（两个 MQ
//! 消费者共用的处理）喂事件，核对各自的会话行与成员行只被各自的事件更新；没有租户或租户
//! 不一致的事件被拒绝，任何租户都不写。另核对存量回填脚本
//! `scripts/backfill_conversation_last_message_seq.sql` 按租户回填、只升不降、可重复执行。
//!
//! 需要 `FLARE_CONVERSATION_TEST_DATABASE_URL`（任意可建 schema 的 PostgreSQL）。用例标了
//! `#[ignore]`：默认的 `cargo test` 只记为 ignored，`-- --ignored` 显式运行时变量必须已设置，
//! 否则直接失败。表结构取自 `migrations/0001_baseline.sql`，建在独立 schema 里，跑完整个删掉。

use std::collections::HashMap;
use std::sync::Arc;

use flare_conversation::config::ConversationConfig;
use flare_conversation::domain::model::{
    ConflictResolutionPolicy, ConversationDomainConfig, ConversationPolicy,
};
use flare_conversation::domain::service::DefaultConversationDomainService;
use flare_conversation::infrastructure::event_consumer::ConversationEventHandler;
use flare_conversation::infrastructure::persistence::PostgresConversationRepository;
use flare_conversation::infrastructure::persistence::redis_presence::RedisPresenceRepository;
use flare_proto::common::{
    CustomEvent, Event, EventType, Message, MqEnvelope, MqPayloadKind, ReadReceiptEvent, event,
    mq_envelope,
};
use flare_server_core::error::ErrorCode;
use prost::Message as _;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

const ENV_DATABASE_URL: &str = "FLARE_CONVERSATION_TEST_DATABASE_URL";
const TENANT_ZERO: &str = "0";
const TENANT_B: &str = "tenant-w10a-b";
/// 两个租户里同一个会话 ID（单聊 ID 只由两名用户 id 推出，不含租户）。
const CID: &str = "1AW10A0000000000AB";
/// 会话确保用的单聊 ID（`validate_conversation_id` 要求 `1A` + 16 位 Crockford Base32）。
const ENSURE_B: &str = "1AW10A00000000000B";
const ENSURE_NONE: &str = "1AW10A00000000000C";
const ENSURE_CONFLICT: &str = "1AW10A00000000000D";
const ALICE: &str = "alice";
const BOB: &str = "bob";

fn test_config() -> Arc<ConversationConfig> {
    Arc::new(ConversationConfig {
        mq_backend: "nats".into(),
        redis_url: "redis://127.0.0.1:9/".into(),
        postgres_url: None,
        postgres_max_connections: 2,
        postgres_min_connections: 0,
        postgres_acquire_timeout_seconds: 5,
        postgres_idle_timeout_seconds: 60,
        postgres_max_lifetime_seconds: 600,
        conversation_state_prefix: "test:state".into(),
        conversation_unread_prefix: "test:unread".into(),
        user_cursor_prefix: "test:cursor".into(),
        presence_prefix: "test:presence".into(),
        large_conversation_precise_unread_threshold: 500,
        storage_reader_service: None,
        recent_message_limit: 20,
        default_policy: ConversationPolicy {
            conflict_resolution: ConflictResolutionPolicy::Coexist,
            max_devices: 5,
            allow_anonymous: false,
            allow_history_sync: true,
            metadata: Default::default(),
        },
        jetstream_url: None,
        jetstream_operation_subject: None,
        jetstream_events_subject: None,
        jetstream_group: None,
        jetstream_stream_specs: vec![],
        kafka_brokers: vec![],
        kafka_client_id: "test".into(),
        jetstream_ensure_subject: None,
        jetstream_ensure_group: None,
    })
}

fn migration(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../migrations")
        .join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

fn backfill_script() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../scripts/backfill_conversation_last_message_seq.sql");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// 从基线迁移里原样取出一张表的建表语句（到第一个独占一行的 `);`）。
fn create_table(baseline: &str, table: &str) -> String {
    let start = baseline
        .find(&format!("CREATE TABLE {table} ("))
        .unwrap_or_else(|| panic!("{table} not in baseline"));
    let end = baseline[start..]
        .find("\n);")
        .unwrap_or_else(|| panic!("{table} block not closed"));
    format!("{};", &baseline[start..start + end + 2])
}

async fn isolated_schema(url: &str) -> (PgPool, PgPool, String) {
    let admin = PgPool::connect(url).await.expect("connect test database");
    let schema = format!("conv_event_tenant_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await
        .unwrap();
    let search_path = schema.clone();
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .after_connect(move |conn, _| {
            let schema = search_path.clone();
            Box::pin(async move {
                sqlx::query(&format!("SET search_path TO {schema}"))
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect(url)
        .await
        .unwrap();
    let baseline = migration("0001_baseline.sql");
    let ddl = [
        create_table(&baseline, "messages"),
        create_table(&baseline, "conversations"),
        create_table(&baseline, "conversation_participants"),
    ]
    .join("\n");
    sqlx::raw_sql(&ddl).execute(&pool).await.unwrap();
    (admin, pool, schema)
}

async fn drop_schema(admin: PgPool, pool: PgPool, schema: &str) {
    pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

async fn seed_conversation(
    pool: &PgPool,
    tenant: &str,
    cid: &str,
    last_message_seq: Option<i64>,
    members: &[&str],
) {
    sqlx::query(
        "INSERT INTO conversations (tenant_id, conversation_id, conversation_type, last_message_seq) \
         VALUES ($1, $2, 1, $3)",
    )
    .bind(tenant)
    .bind(cid)
    .bind(last_message_seq)
    .execute(pool)
    .await
    .unwrap();
    for user in members {
        sqlx::query(
            "INSERT INTO conversation_participants (tenant_id, conversation_id, user_id) \
             VALUES ($1, $2, $3)",
        )
        .bind(tenant)
        .bind(cid)
        .bind(user)
        .execute(pool)
        .await
        .unwrap();
    }
}

async fn seed_messages(pool: &PgPool, tenant: &str, cid: &str, sender: &str, seqs: &[i64]) {
    for seq in seqs {
        sqlx::query(
            "INSERT INTO messages (tenant_id, server_id, conversation_id, sender_id, seq, status) \
             VALUES ($1, $2, $3, $4, $5, 2)",
        )
        .bind(tenant)
        .bind(format!("{tenant}-{cid}-{seq}"))
        .bind(cid)
        .bind(sender)
        .bind(seq)
        .execute(pool)
        .await
        .unwrap();
    }
}

/// 一个会话行与其成员行的可比快照。
#[derive(Debug, Clone, PartialEq, Eq)]
struct ConversationState {
    last_message_seq: Option<i64>,
    /// (user_id, last_read_seq, unread_count)，按 user_id 排序。
    members: Vec<(String, i64, i32)>,
}

async fn state(pool: &PgPool, tenant: &str, cid: &str) -> Option<ConversationState> {
    let row = sqlx::query(
        "SELECT last_message_seq FROM conversations WHERE tenant_id = $1 AND conversation_id = $2",
    )
    .bind(tenant)
    .bind(cid)
    .fetch_optional(pool)
    .await
    .unwrap()?;
    let members = sqlx::query(
        "SELECT user_id, COALESCE(last_read_seq, 0) AS last_read_seq, \
                COALESCE(unread_count, 0) AS unread_count \
         FROM conversation_participants WHERE tenant_id = $1 AND conversation_id = $2 \
         ORDER BY user_id",
    )
    .bind(tenant)
    .bind(cid)
    .fetch_all(pool)
    .await
    .unwrap()
    .into_iter()
    .map(|row| {
        (
            row.get::<String, _>("user_id"),
            row.get::<i64, _>("last_read_seq"),
            row.get::<i32, _>("unread_count"),
        )
    })
    .collect();
    Some(ConversationState {
        last_message_seq: row.get("last_message_seq"),
        members,
    })
}

fn members(rows: &[(&str, i64, i32)]) -> Vec<(String, i64, i32)> {
    rows.iter()
        .map(|(user, read, unread)| (user.to_string(), *read, *unread))
        .collect()
}

fn tenant_headers(tenant: Option<&str>) -> HashMap<String, String> {
    tenant
        .map(|tenant| HashMap::from([("x-tenant-id".to_string(), tenant.to_string())]))
        .unwrap_or_default()
}

/// `TOPIC_MESSAGE_CREATED` 上的消息本体：信封直接装 Message，租户在信封头（与
/// `MqPushRepository` 的写法一致）。
fn message_created(envelope_tenant: Option<&str>, seq: u64, sender: &str) -> Vec<u8> {
    MqEnvelope {
        envelope_id: format!("env-{seq}"),
        conversation_id: CID.to_string(),
        seq,
        payload_kind: MqPayloadKind::Message as i32,
        headers: tenant_headers(envelope_tenant),
        payload: Some(mq_envelope::Payload::Message(Message {
            conversation_id: CID.to_string(),
            conversation_seq: seq,
            sender_id: sender.to_string(),
            status: 2,
            ..Default::default()
        })),
        ..Default::default()
    }
    .encode_to_vec()
}

/// `TOPIC_MESSAGE_EVENTS` 上的已读事件。
fn read_receipt(envelope_tenant: Option<&str>, user: &str, read_seq: u64) -> Vec<u8> {
    MqEnvelope {
        envelope_id: format!("read-{user}-{read_seq}"),
        conversation_id: CID.to_string(),
        payload_kind: MqPayloadKind::Event as i32,
        headers: tenant_headers(envelope_tenant),
        payload: Some(mq_envelope::Payload::Event(Event {
            conversation_id: CID.to_string(),
            r#type: EventType::EventReadReceipt as i32,
            event_id: format!("read-{user}-{read_seq}"),
            payload: Some(event::Payload::Read(ReadReceiptEvent {
                conversation_id: CID.to_string(),
                read_seq,
                user_id: user.to_string(),
                message_ids: Vec::new(),
                read_at: None,
            })),
            ..Default::default()
        })),
        ..Default::default()
    }
    .encode_to_vec()
}

/// `TOPIC_CONVERSATION_ENSURE` 上的会话确保事件（与 message-ingest 的发布方同形：载荷与信封头
/// 都用 `tenant_id`）。
fn conversation_ensure(tenant: Option<&str>, cid: &str) -> Vec<u8> {
    let payload = serde_json::json!({
        "tenant_id": tenant.unwrap_or_default(),
        "conversation_type": 1,
        "business_type": "",
        "participants": [ALICE, BOB],
        "channel_id": "",
    });
    let headers = tenant
        .map(|tenant| HashMap::from([("tenant_id".to_string(), tenant.to_string())]))
        .unwrap_or_default();
    MqEnvelope {
        envelope_id: format!("ensure-{cid}"),
        conversation_id: cid.to_string(),
        payload_kind: MqPayloadKind::Event as i32,
        headers,
        persistence_only: true,
        payload: Some(mq_envelope::Payload::Event(Event {
            conversation_id: cid.to_string(),
            r#type: EventType::EventCustom as i32,
            event_id: format!("ensure-{cid}"),
            payload: Some(event::Payload::Custom(CustomEvent {
                namespace: "flare.core".to_string(),
                name: "conversation.ensure".to_string(),
                version: "1".to_string(),
                payload: serde_json::to_vec(&payload).unwrap(),
                attributes: HashMap::new(),
            })),
            ..Default::default()
        })),
        ..Default::default()
    }
    .encode_to_vec()
}

fn handler(pool: &PgPool) -> ConversationEventHandler {
    let presence = Arc::new(RedisPresenceRepository::new(
        Arc::new(redis::Client::open("redis://127.0.0.1:9/").unwrap()),
        test_config(),
    ));
    let domain = DefaultConversationDomainService::new(
        Arc::new(PostgresConversationRepository::new(
            Arc::new(pool.clone()),
            test_config(),
        )),
        presence,
        None,
        ConversationDomainConfig::default(),
    );
    ConversationEventHandler::new(Arc::new(domain))
}

fn assert_rejected(result: flare_server_core::error::Result<()>, what: &str) {
    let error = result.expect_err(what);
    assert_eq!(
        error.code(),
        Some(ErrorCode::MessageFormatError),
        "{what}: {error}"
    );
}

#[tokio::test]
#[ignore = "requires FLARE_CONVERSATION_TEST_DATABASE_URL pointing at a disposable PostgreSQL"]
async fn each_tenant_conversation_follows_only_its_own_events() {
    let url = std::env::var(ENV_DATABASE_URL)
        .unwrap_or_else(|_| panic!("set {ENV_DATABASE_URL} to a disposable PostgreSQL instance"));
    let (admin, pool, schema) = isolated_schema(&url).await;

    // 两个租户：同一 user_id 对、同一会话 ID，会话行 last_message_seq 为 NULL（栈上的真实形态）。
    seed_conversation(&pool, TENANT_ZERO, CID, None, &[ALICE, BOB]).await;
    seed_conversation(&pool, TENANT_B, CID, None, &[ALICE, BOB]).await;
    seed_messages(&pool, TENANT_ZERO, CID, ALICE, &[1, 2]).await;
    seed_messages(&pool, TENANT_B, CID, ALICE, &[1, 2, 3, 4, 5]).await;
    let untouched = ConversationState {
        last_message_seq: None,
        members: members(&[(ALICE, 0, 0), (BOB, 0, 0)]),
    };
    let handler = handler(&pool);

    // 1. 租户 B 的消息事件（信封头与传输层消息头都声明 B）：只推进 B。
    handler
        .handle_message_event(&message_created(Some(TENANT_B), 5, ALICE), Some(TENANT_B))
        .await
        .expect("tenant B message event");
    let b_after_message = ConversationState {
        last_message_seq: Some(5),
        members: members(&[(ALICE, 0, 0), (BOB, 0, 1)]),
    };
    assert_eq!(
        state(&pool, TENANT_B, CID).await,
        Some(b_after_message.clone())
    );
    assert_eq!(
        state(&pool, TENANT_ZERO, CID).await,
        Some(untouched.clone()),
        "tenant B's message event must not touch tenant 0's conversation with the same id"
    );

    // 2. 租户 B 的已读回执（只有信封头声明 B）：只推进 B 的 bob。
    handler
        .handle_message_event(&read_receipt(Some(TENANT_B), BOB, 5), None)
        .await
        .expect("tenant B read receipt");
    let b_after_read = ConversationState {
        last_message_seq: Some(5),
        members: members(&[(ALICE, 0, 0), (BOB, 5, 0)]),
    };
    assert_eq!(
        state(&pool, TENANT_B, CID).await,
        Some(b_after_read.clone())
    );
    assert_eq!(
        state(&pool, TENANT_ZERO, CID).await,
        Some(untouched.clone()),
        "tenant B's read receipt must not move tenant 0's read cursor"
    );

    // 3. 租户 0 的消息事件（只有传输层消息头声明 0）：只推进租户 0，B 不动。
    handler
        .handle_message_event(&message_created(None, 2, ALICE), Some(TENANT_ZERO))
        .await
        .expect("tenant 0 message event");
    let zero_after_message = ConversationState {
        last_message_seq: Some(2),
        members: members(&[(ALICE, 0, 0), (BOB, 0, 1)]),
    };
    assert_eq!(
        state(&pool, TENANT_ZERO, CID).await,
        Some(zero_after_message.clone())
    );
    assert_eq!(
        state(&pool, TENANT_B, CID).await,
        Some(b_after_read.clone())
    );

    // 4. 没有任何租户声明：拒绝，两个租户都不写（以前落到租户 0）。
    assert_rejected(
        handler
            .handle_message_event(&message_created(None, 9, ALICE), None)
            .await,
        "tenantless message event",
    );
    assert_rejected(
        handler
            .handle_message_event(&read_receipt(None, BOB, 2), Some("  "))
            .await,
        "tenantless read receipt",
    );
    // 5. 信封头与传输层消息头不一致：拒绝，不挑其中一个。
    assert_rejected(
        handler
            .handle_message_event(&read_receipt(Some(TENANT_B), BOB, 2), Some(TENANT_ZERO))
            .await,
        "conflicting read receipt",
    );
    assert_eq!(
        state(&pool, TENANT_ZERO, CID).await,
        Some(zero_after_message.clone())
    );
    assert_eq!(
        state(&pool, TENANT_B, CID).await,
        Some(b_after_read.clone())
    );

    // 6. 会话确保：载荷声明 B 的只建在 B；什么都没声明的拒绝，不建在租户 0。
    handler
        .handle_conversation_ensure(&conversation_ensure(Some(TENANT_B), ENSURE_B), None)
        .await
        .expect("tenant B ensure");
    assert!(state(&pool, TENANT_B, ENSURE_B).await.is_some());
    assert_eq!(state(&pool, TENANT_ZERO, ENSURE_B).await, None);
    assert_rejected(
        handler
            .handle_conversation_ensure(&conversation_ensure(None, ENSURE_NONE), None)
            .await,
        "tenantless ensure",
    );
    assert_rejected(
        handler
            .handle_conversation_ensure(
                &conversation_ensure(Some(TENANT_B), ENSURE_CONFLICT),
                Some(TENANT_ZERO),
            )
            .await,
        "conflicting ensure",
    );
    let created: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM conversations WHERE conversation_id = ANY($1)")
            .bind(vec![ENSURE_NONE, ENSURE_CONFLICT])
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        created, 0,
        "rejected ensure events must not create anything"
    );

    drop_schema(admin, pool, &schema).await;
}

#[tokio::test]
#[ignore = "requires FLARE_CONVERSATION_TEST_DATABASE_URL pointing at a disposable PostgreSQL"]
async fn backfill_raises_last_message_seq_per_tenant_and_is_idempotent() {
    let url = std::env::var(ENV_DATABASE_URL)
        .unwrap_or_else(|_| panic!("set {ENV_DATABASE_URL} to a disposable PostgreSQL instance"));
    let (admin, pool, schema) = isolated_schema(&url).await;

    // 同 ID 会话在两个租户各有不同的消息尾；B 另有一个列已大于剩余消息的会话（清除过历史）、
    // 一个没有消息的会话。
    seed_conversation(&pool, TENANT_ZERO, CID, None, &[ALICE, BOB]).await;
    seed_conversation(&pool, TENANT_B, CID, None, &[ALICE, BOB]).await;
    seed_conversation(&pool, TENANT_B, "PURGED-B", Some(9), &[ALICE, BOB]).await;
    seed_conversation(&pool, TENANT_B, "EMPTY-B", None, &[ALICE, BOB]).await;
    seed_messages(&pool, TENANT_ZERO, CID, ALICE, &[1, 2, 3]).await;
    seed_messages(&pool, TENANT_B, CID, ALICE, &[1, 2, 3, 4, 5, 6, 7]).await;
    seed_messages(&pool, TENANT_B, "PURGED-B", ALICE, &[3, 4]).await;

    let script = backfill_script();
    let first = sqlx::raw_sql(&script).execute(&pool).await.unwrap();
    assert_eq!(
        first.rows_affected(),
        2,
        "two NULL conversations backfilled"
    );

    let seq = |tenant: &'static str, cid: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, Option<i64>>(
                "SELECT last_message_seq FROM conversations WHERE tenant_id = $1 AND conversation_id = $2",
            )
            .bind(tenant)
            .bind(cid)
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };
    assert_eq!(seq(TENANT_ZERO, CID).await, Some(3));
    assert_eq!(seq(TENANT_B, CID).await, Some(7));
    assert_eq!(seq(TENANT_B, "PURGED-B").await, Some(9), "never lowered");
    assert_eq!(
        seq(TENANT_B, "EMPTY-B").await,
        None,
        "no messages, no value"
    );
    // 成员行不在回填范围内。
    assert_eq!(
        state(&pool, TENANT_B, CID).await.unwrap().members,
        members(&[(ALICE, 0, 0), (BOB, 0, 0)])
    );

    let second = sqlx::raw_sql(&script).execute(&pool).await.unwrap();
    assert_eq!(second.rows_affected(), 0, "rerun changes nothing");
    assert_eq!(seq(TENANT_B, CID).await, Some(7));

    drop_schema(admin, pool, &schema).await;
}
