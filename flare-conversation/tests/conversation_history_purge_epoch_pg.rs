//! `PurgeConversationHistory` 纪元围栏契约：带纪元属性的消息只按纪元判新旧，与任何时钟无关；
//! 没有纪元的消息回落到时间边界；事件按被删消息与剩余最早消息的 seq 判定，不看时间。
//!
//! 线格式与参数校验不依赖数据库，照常运行。真库用例需要 `FLARE_CONVERSATION_TEST_DATABASE_URL`
//! （任意可建 schema 的 PostgreSQL；用例在独立 schema 里建出清理会碰到的表与列，跑完整个删掉），
//! 标了 `#[ignore]`：默认的 `cargo test` 只记为 ignored，`-- --ignored` 显式运行时变量必须已设置，
//! 否则直接失败，不会把没跑的用例记为通过。

use std::sync::Arc;
use std::sync::Mutex;

use chrono::{DateTime, TimeZone, Utc};
use flare_conversation::config::ConversationConfig;
use flare_conversation::domain::model::{
    ConflictResolutionPolicy, ConversationDomainConfig, ConversationHistoryPurge,
    ConversationPolicy,
};
use flare_conversation::domain::service::DefaultConversationDomainService;
use flare_conversation::infrastructure::persistence::PostgresConversationRepository;
use flare_conversation::infrastructure::persistence::message_cache::MessageCacheInvalidator;
use flare_conversation::infrastructure::persistence::redis_presence::RedisPresenceRepository;
use flare_grpc_proto::conversation::PurgeConversationHistoryRequest;
use flare_server_core::context::Context;
use flare_server_core::error::ErrorCode;
use prost::Message as _;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const ENV_DATABASE_URL: &str = "FLARE_CONVERSATION_TEST_DATABASE_URL";
const EPOCH: &str = "flare.social.friendship_epoch";
/// 时间回落边界（服务端毫秒）。
const BOUNDARY: i64 = 1_790_000_000_000;

/// 新字段只能追加：老请求的字节解出来纪元为 0（老语义），新字段的编号固定为 3、4。
#[test]
fn epoch_fence_wire_format_is_stable() {
    let request = PurgeConversationHistoryRequest {
        conversation_id: "c".into(),
        before_ms: 5,
        epoch_attribute: "k".into(),
        before_epoch: 2,
    };
    // field 1 = "c", field 2 = 5, field 3 (string) = "k", field 4 (varint) = 2
    assert_eq!(
        request.encode_to_vec(),
        vec![0x0a, 0x01, b'c', 0x10, 0x05, 0x1a, 0x01, b'k', 0x20, 0x02]
    );
    let legacy =
        PurgeConversationHistoryRequest::decode(&[0x0a, 0x01, b'c', 0x10, 0x05][..]).unwrap();
    assert_eq!(legacy.before_epoch, 0);
    assert!(legacy.epoch_attribute.is_empty());
}

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

fn domain_over(repo: PostgresConversationRepository) -> DefaultConversationDomainService {
    let presence = Arc::new(RedisPresenceRepository::new(
        Arc::new(redis::Client::open("redis://127.0.0.1:9/").unwrap()),
        test_config(),
    ));
    DefaultConversationDomainService::new(
        Arc::new(repo),
        presence,
        None,
        ConversationDomainConfig::default(),
    )
}

/// 参数校验在碰库之前完成：池指向一个不监听的端口，任何一次真实访问都会报连接错误而不是参数错误。
#[tokio::test]
async fn epoch_fence_arguments_are_validated_before_touching_storage() {
    let pool = PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_millis(200))
        .connect_lazy("postgres://nobody@127.0.0.1:9/none")
        .unwrap();
    let domain = domain_over(PostgresConversationRepository::new(
        Arc::new(pool),
        test_config(),
    ));
    let ctx = Context::with_request_id("purge-epoch-args").with_tenant_id("tenant-a");
    let too_long = "k".repeat(129);
    let cases: [(i64, &str, i64, &str); 7] = [
        (BOUNDARY, EPOCH, -1, "negative epoch"),
        (BOUNDARY, "", 2, "fence without attribute"),
        (BOUNDARY, "   ", 2, "blank attribute"),
        (BOUNDARY, too_long.as_str(), 2, "oversized attribute"),
        (BOUNDARY, EPOCH, 0, "attribute without fence"),
        (-1, EPOCH, 2, "negative time fallback"),
        (0, "", 0, "neither fence nor time boundary"),
    ];
    for (before_ms, attribute, before_epoch, case) in cases {
        let error = domain
            .purge_history(&ctx, "single-ab", before_ms, attribute, before_epoch)
            .await
            .expect_err(case);
        assert_eq!(error.code(), Some(ErrorCode::InvalidParameter), "{case}");
    }
}

fn at(ms: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(ms).single().unwrap()
}

/// 一条消息：`epoch=None` 不带纪元属性；同 seq 上写一条 EVENT_MESSAGE（created_at 与消息相同），
/// 并给它加置顶与标记。
async fn seed_message(
    pool: &PgPool,
    tenant: &str,
    conversation: &str,
    id: &str,
    seq: i64,
    ms: i64,
    epoch: Option<&str>,
) {
    // 与 storage-writer 一致：attributes 以字符串值落在 extra。
    let mut extra = serde_json::Map::new();
    extra.insert("tenant_id".into(), tenant.into());
    if let Some(value) = epoch {
        extra.insert(EPOCH.into(), value.into());
    }
    sqlx::query(
        "INSERT INTO messages (tenant_id, server_id, conversation_id, sender_id, seq, created_at, extra) \
         VALUES ($1, $2, $3, 'alice', $4, $5, $6::TEXT::JSONB)",
    )
    .bind(tenant)
    .bind(id)
    .bind(conversation)
    .bind(seq)
    .bind(at(ms))
    .bind(serde_json::Value::Object(extra).to_string())
    .execute(pool)
    .await
    .unwrap();
    seed_event(pool, tenant, conversation, seq, 1, ms).await;
    for table in ["pinned_messages", "marked_messages"] {
        sqlx::query(&format!(
            "INSERT INTO {table} (tenant_id, message_id, conversation_id) VALUES ($1, $2, $3)"
        ))
        .bind(tenant)
        .bind(id)
        .bind(conversation)
        .execute(pool)
        .await
        .unwrap();
    }
}

async fn seed_event(
    pool: &PgPool,
    tenant: &str,
    conversation: &str,
    seq: i64,
    event_type: i32,
    ms: i64,
) {
    sqlx::query(
        "INSERT INTO events (tenant_id, conversation_id, seq, event_type, created_at) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(tenant)
    .bind(conversation)
    .bind(seq)
    .bind(event_type)
    .bind(at(ms))
    .execute(pool)
    .await
    .unwrap();
}

async fn seed_participant(
    pool: &PgPool,
    tenant: &str,
    conversation: &str,
    user: &str,
    unread: i32,
) {
    sqlx::query(
        "INSERT INTO conversation_participants \
         (tenant_id, conversation_id, user_id, last_read_seq, unread_count, is_deleted, updated_at) \
         VALUES ($1, $2, $3, 0, $4, FALSE, TIMESTAMPTZ '2026-01-01 00:00:00+00')",
    )
    .bind(tenant)
    .bind(conversation)
    .bind(user)
    .bind(unread)
    .execute(pool)
    .await
    .unwrap();
}

async fn ids(
    pool: &PgPool,
    table: &str,
    column: &str,
    tenant: &str,
    conversation: &str,
) -> Vec<String> {
    sqlx::query_scalar(&format!(
        "SELECT {column} FROM {table} WHERE tenant_id = $1 AND conversation_id = $2 ORDER BY 1"
    ))
    .bind(tenant)
    .bind(conversation)
    .fetch_all(pool)
    .await
    .unwrap()
}

async fn event_seqs(pool: &PgPool, tenant: &str, conversation: &str) -> Vec<i64> {
    sqlx::query_scalar(
        "SELECT seq FROM events WHERE tenant_id = $1 AND conversation_id = $2 ORDER BY seq",
    )
    .bind(tenant)
    .bind(conversation)
    .fetch_all(pool)
    .await
    .unwrap()
}

async fn unread(pool: &PgPool, tenant: &str, conversation: &str) -> Vec<(String, i32)> {
    sqlx::query_as(
        "SELECT user_id, unread_count FROM conversation_participants \
         WHERE tenant_id = $1 AND conversation_id = $2 ORDER BY user_id",
    )
    .bind(tenant)
    .bind(conversation)
    .fetch_all(pool)
    .await
    .unwrap()
}

/// 独立 schema：清理会碰到的列与主键，与 migrations/0001_baseline.sql 一致（不依赖 TimescaleDB）。
async fn isolated_schema(url: &str) -> (PgPool, PgPool, String) {
    let admin = PgPool::connect(url).await.expect("connect test database");
    let schema = format!("conv_purge_epoch_{}", uuid::Uuid::new_v4().simple());
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
    sqlx::raw_sql(
        r#"
        CREATE TABLE conversation_participants (
            tenant_id TEXT NOT NULL, conversation_id TEXT NOT NULL, user_id TEXT NOT NULL,
            last_read_seq BIGINT DEFAULT 0, unread_count INT DEFAULT 0,
            is_deleted BOOLEAN DEFAULT FALSE, visible_from_seq BIGINT NOT NULL DEFAULT 0,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (tenant_id, conversation_id, user_id)
        );
        CREATE TABLE messages (
            tenant_id TEXT NOT NULL, server_id TEXT NOT NULL, conversation_id TEXT NOT NULL,
            sender_id TEXT NOT NULL, seq BIGINT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            status INT NOT NULL DEFAULT 1,
            extra JSONB DEFAULT '{}'::jsonb,
            PRIMARY KEY (created_at, server_id)
        );
        CREATE INDEX idx_messages_tenant_conv_seq ON messages(tenant_id, conversation_id, seq);
        CREATE TABLE events (
            tenant_id TEXT NOT NULL, conversation_id TEXT NOT NULL, seq BIGINT NOT NULL,
            event_type INT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (tenant_id, conversation_id, seq)
        );
        CREATE TABLE pinned_messages (
            id BIGSERIAL PRIMARY KEY, tenant_id TEXT NOT NULL, message_id TEXT NOT NULL,
            conversation_id TEXT NOT NULL
        );
        CREATE TABLE marked_messages (
            id BIGSERIAL PRIMARY KEY, tenant_id TEXT NOT NULL, message_id TEXT NOT NULL,
            conversation_id TEXT NOT NULL
        );
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();
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

#[derive(Default)]
struct RecordingInvalidator {
    calls: Mutex<Vec<Vec<String>>>,
}

#[async_trait::async_trait]
impl MessageCacheInvalidator for RecordingInvalidator {
    async fn invalidate_messages(
        &self,
        _tenant_id: &str,
        _conversation_id: &str,
        message_ids: &[String],
    ) -> flare_server_core::error::Result<()> {
        let mut sorted = message_ids.to_vec();
        sorted.sort();
        self.calls.lock().unwrap().push(sorted);
        Ok(())
    }
}

/// 重新加好友到纪元 2：两台主机的时钟谁快谁慢都不影响带纪元的消息。
///
/// - 旧纪元的最后一条被一台快 5 s 的时钟盖在边界之后：照删；
/// - 新纪元的一条被一台慢了远超安全余量的时钟盖在边界之前：照留；
/// - 没有纪元、纪元不合法（非数字、超出 BIGINT）的消息按时间边界回落；
/// - 事件不看时间：被删消息的 EVENT_MESSAGE 行随消息删，seq 小于剩余最早消息的非消息事件删，
///   之后的留下，哪怕它的时间戳很早。
#[tokio::test]
#[ignore = "requires FLARE_CONVERSATION_TEST_DATABASE_URL pointing at a disposable PostgreSQL"]
async fn epoch_fence_purges_by_epoch_regardless_of_either_clock() {
    let url = std::env::var(ENV_DATABASE_URL)
        .unwrap_or_else(|_| panic!("set {ENV_DATABASE_URL} to a disposable PostgreSQL instance"));
    let (admin, pool, schema) = isolated_schema(&url).await;
    let (tenant, other_tenant) = ("tenant-a", "tenant-b");
    let (conversation, other_conversation) = ("single-ab", "single-ac");

    // 主键是 (created_at, server_id)：别的会话/租户的同名消息加前缀区分。
    for (t, c, prefix) in [
        (tenant, conversation, ""),
        (tenant, other_conversation, "other-"),
        (other_tenant, conversation, "foreign-"),
    ] {
        let id = |name: &str| format!("{prefix}{name}");
        seed_message(&pool, t, c, &id("e1-old"), 1, BOUNDARY - 60_000, Some("1")).await;
        seed_message(
            &pool,
            t,
            c,
            &id("e1-fast-clock"),
            2,
            BOUNDARY + 5_000,
            Some("1"),
        )
        .await;
        seed_event(&pool, t, c, 3, 5, BOUNDARY + 9_000).await; // 已读，旧历史
        seed_message(&pool, t, c, &id("untagged-old"), 4, BOUNDARY - 30_000, None).await;
        seed_message(
            &pool,
            t,
            c,
            &id("forged"),
            5,
            BOUNDARY - 20_000,
            Some("abc"),
        )
        .await;
        seed_event(&pool, t, c, 6, 3, BOUNDARY + 20_000).await; // 编辑，旧历史
        seed_message(
            &pool,
            t,
            c,
            &id("e2-slow-clock"),
            7,
            BOUNDARY - 10_000,
            Some("2"),
        )
        .await;
        seed_message(&pool, t, c, &id("untagged-new"), 8, BOUNDARY + 1_000, None).await;
        seed_event(&pool, t, c, 9, 8, BOUNDARY - 50_000).await; // 反应，新消息之后
        seed_message(&pool, t, c, &id("e3"), 10, BOUNDARY - 100_000, Some("3")).await;
        seed_message(
            &pool,
            t,
            c,
            &id("overflow"),
            11,
            BOUNDARY - 5_000,
            Some("99999999999999999999"),
        )
        .await;
        seed_event(&pool, t, c, 12, 5, BOUNDARY - 3_000).await; // 已读，新消息之后
    }
    seed_participant(&pool, tenant, conversation, "alice", 0).await;
    seed_participant(&pool, tenant, conversation, "bob", 8).await;

    let cache = Arc::new(RecordingInvalidator::default());
    let domain = domain_over(
        PostgresConversationRepository::new(Arc::new(pool.clone()), test_config())
            .with_message_cache_invalidator(cache.clone()),
    );
    let ctx = Context::with_request_id("purge-epoch-contract").with_tenant_id(tenant);

    let first = domain
        .purge_history(&ctx, conversation, BOUNDARY, EPOCH, 2)
        .await
        .unwrap();
    assert_eq!(
        first,
        ConversationHistoryPurge {
            purged_messages: 5,
            max_purged_seq: 11,
        }
    );
    assert_eq!(
        ids(&pool, "messages", "server_id", tenant, conversation).await,
        vec!["e2-slow-clock", "e3", "untagged-new"],
    );
    assert_eq!(
        event_seqs(&pool, tenant, conversation).await,
        vec![7, 8, 9, 10, 12],
        "message events of purged seqs and non-message events before the first survivor go"
    );
    for table in ["pinned_messages", "marked_messages"] {
        assert_eq!(
            ids(&pool, table, "message_id", tenant, conversation).await,
            vec!["e2-slow-clock", "e3", "untagged-new"],
            "{table}"
        );
    }
    assert_eq!(
        unread(&pool, tenant, conversation).await,
        vec![("alice".to_string(), 0), ("bob".to_string(), 3)],
        "unread recomputed from the surviving messages"
    );
    assert_eq!(
        cache.calls.lock().unwrap().clone(),
        vec![
            vec![
                "e1-fast-clock".to_string(),
                "e1-old".to_string(),
                "forged".to_string(),
                "overflow".to_string(),
                "untagged-old".to_string(),
            ];
            2
        ],
        "the purged IDs are invalidated before and after commit"
    );

    // 别的会话、别的租户一条不动。
    for (t, c) in [(tenant, other_conversation), (other_tenant, conversation)] {
        assert_eq!(
            ids(&pool, "messages", "server_id", t, c).await.len(),
            8,
            "{t}/{c}"
        );
        assert_eq!(event_seqs(&pool, t, c).await.len(), 12, "{t}/{c}");
    }

    // 重放：什么都不删。
    let replay = domain
        .purge_history(&ctx, conversation, BOUNDARY, EPOCH, 2)
        .await
        .unwrap();
    assert_eq!(replay, ConversationHistoryPurge::default());
    assert_eq!(
        event_seqs(&pool, tenant, conversation).await,
        vec![7, 8, 9, 10, 12],
    );

    drop_schema(admin, pool, &schema).await;
}

/// 没给时间边界（`before_ms = 0`）：没有纪元的消息一条不删，只按纪元清除；会话里不剩消息时，
/// 全部非消息事件都属于旧历史，不论时间戳多晚。
#[tokio::test]
#[ignore = "requires FLARE_CONVERSATION_TEST_DATABASE_URL pointing at a disposable PostgreSQL"]
async fn epoch_fence_without_time_boundary_never_touches_untagged_messages() {
    let url = std::env::var(ENV_DATABASE_URL)
        .unwrap_or_else(|_| panic!("set {ENV_DATABASE_URL} to a disposable PostgreSQL instance"));
    let (admin, pool, schema) = isolated_schema(&url).await;
    let tenant = "tenant-a";
    let (mixed, all_old) = ("single-ab", "single-ac");

    seed_message(
        &pool,
        tenant,
        mixed,
        "untagged-ancient",
        1,
        BOUNDARY - 3_600_000,
        None,
    )
    .await;
    seed_message(
        &pool,
        tenant,
        mixed,
        "e1",
        2,
        BOUNDARY + 3_600_000,
        Some("1"),
    )
    .await;
    seed_event(&pool, tenant, mixed, 3, 5, BOUNDARY - 3_600_000).await;

    seed_message(
        &pool,
        tenant,
        all_old,
        "e1-a",
        1,
        BOUNDARY - 60_000,
        Some("1"),
    )
    .await;
    seed_event(&pool, tenant, all_old, 2, 5, BOUNDARY - 50_000).await;
    seed_message(
        &pool,
        tenant,
        all_old,
        "e1-b",
        3,
        BOUNDARY - 40_000,
        Some("0"),
    )
    .await;
    seed_event(&pool, tenant, all_old, 4, 3, BOUNDARY + 3_600_000).await;
    seed_event(&pool, tenant, all_old, 5, 8, BOUNDARY + 7_200_000).await;

    let domain = domain_over(PostgresConversationRepository::new(
        Arc::new(pool.clone()),
        test_config(),
    ));
    let ctx = Context::with_request_id("purge-epoch-no-time").with_tenant_id(tenant);

    let mixed_purge = domain
        .purge_history(&ctx, mixed, 0, EPOCH, 2)
        .await
        .unwrap();
    assert_eq!(
        mixed_purge,
        ConversationHistoryPurge {
            purged_messages: 1,
            max_purged_seq: 2,
        }
    );
    assert_eq!(
        ids(&pool, "messages", "server_id", tenant, mixed).await,
        vec!["untagged-ancient"]
    );
    // seq 3 的已读在剩余最早消息（seq 1）之后，留下；seq 2 是被删消息的 EVENT_MESSAGE。
    assert_eq!(event_seqs(&pool, tenant, mixed).await, vec![1, 3]);

    let all_old_purge = domain
        .purge_history(&ctx, all_old, 0, EPOCH, 2)
        .await
        .unwrap();
    assert_eq!(
        all_old_purge,
        ConversationHistoryPurge {
            purged_messages: 2,
            max_purged_seq: 3,
        }
    );
    assert!(
        ids(&pool, "messages", "server_id", tenant, all_old)
            .await
            .is_empty()
    );
    assert!(
        event_seqs(&pool, tenant, all_old).await.is_empty(),
        "with no surviving message every event belongs to the old history"
    );

    drop_schema(admin, pool, &schema).await;
}

/// 纪元计数器上线后、回填之前就重新加回的老好友对，两端纪元都是 1，上一段关系的历史没有
/// 纪元属性。人工修复时以 `before_epoch = 1`、`before_ms = 本段关系的接受时刻 − 余量` 清除：
/// 只删边界之前没有纪元的旧历史（以及纪元为 0 的值），本段关系里带纪元 1 的消息即使被慢时钟
/// 盖在边界之前也保留，边界之后没有纪元的消息（纪元打标上线之前发的）也保留。
/// 清除写的 info 日志在 `conversation_history_purge_log_pg.rs` 里单独验证。
#[tokio::test]
#[ignore = "requires FLARE_CONVERSATION_TEST_DATABASE_URL pointing at a disposable PostgreSQL"]
async fn epoch_one_repair_purge_keeps_the_current_relationship() {
    let url = std::env::var(ENV_DATABASE_URL)
        .unwrap_or_else(|_| panic!("set {ENV_DATABASE_URL} to a disposable PostgreSQL instance"));
    let (admin, pool, schema) = isolated_schema(&url).await;
    let tenant = "tenant-a";
    let conversation = "single-readded";

    // 上一段关系（计数器之前）：没有纪元，盖在边界之前很久。
    seed_message(
        &pool,
        tenant,
        conversation,
        "old-untagged",
        1,
        BOUNDARY - 86_400_000,
        None,
    )
    .await;
    seed_event(&pool, tenant, conversation, 2, 5, BOUNDARY - 80_000_000).await; // 旧历史上的已读
    // 不经 Hook 的消息带着客户端自带的 0：小于 1，按纪元删。
    seed_message(
        &pool,
        tenant,
        conversation,
        "client-zero",
        3,
        BOUNDARY - 70_000_000,
        Some("0"),
    )
    .await;
    // 本段关系（纪元 1）：一条被慢时钟盖在边界之前，照留。
    seed_message(
        &pool,
        tenant,
        conversation,
        "e1-slow-clock",
        4,
        BOUNDARY - 10_000,
        Some("1"),
    )
    .await;
    // 本段关系里纪元打标上线之前发的消息：没有纪元，但在边界之后，留下。
    seed_message(
        &pool,
        tenant,
        conversation,
        "new-untagged",
        5,
        BOUNDARY + 60_000,
        None,
    )
    .await;
    seed_message(
        &pool,
        tenant,
        conversation,
        "e1",
        6,
        BOUNDARY + 120_000,
        Some("1"),
    )
    .await;
    seed_event(&pool, tenant, conversation, 7, 8, BOUNDARY + 130_000).await; // 反应，在留下的消息之后

    let domain = domain_over(PostgresConversationRepository::new(
        Arc::new(pool.clone()),
        test_config(),
    ));
    let ctx = Context::with_request_id("purge-epoch-one-repair").with_tenant_id(tenant);

    let repaired = domain
        .purge_history(&ctx, conversation, BOUNDARY, EPOCH, 1)
        .await
        .unwrap();
    assert_eq!(
        repaired,
        ConversationHistoryPurge {
            purged_messages: 2,
            max_purged_seq: 3,
        }
    );
    assert_eq!(
        ids(&pool, "messages", "server_id", tenant, conversation).await,
        vec!["e1", "e1-slow-clock", "new-untagged"],
    );
    assert_eq!(
        event_seqs(&pool, tenant, conversation).await,
        vec![4, 5, 6, 7],
        "events of the purged seqs and the read receipt before the first survivor go"
    );

    drop_schema(admin, pool, &schema).await;
}
