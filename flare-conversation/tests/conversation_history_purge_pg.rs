//! `PurgeConversationHistory` 契约：按服务端时间边界硬删会话历史，边界及之后的一条都不碰。
//!
//! 真库部分只在设置了 `FLARE_CONVERSATION_TEST_DATABASE_URL` 时运行（任意可建 schema 的
//! PostgreSQL；用例在独立 schema 里建出清理会碰到的几张表，跑完整个删掉），否则打印 skipped
//! 并直接通过。Redis 热缓存失效另需 `FLARE_CONVERSATION_TEST_REDIS_URL`（用完删掉自己写的键）。
//! 线格式部分不依赖数据库。

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use chrono::{DateTime, TimeZone, Utc};
use flare_conversation::config::ConversationConfig;
use flare_conversation::domain::model::{
    ConflictResolutionPolicy, ConversationDomainConfig, ConversationHistoryPurge,
    ConversationPolicy,
};
use flare_conversation::domain::service::DefaultConversationDomainService;
use flare_conversation::infrastructure::persistence::PostgresConversationRepository;
use flare_conversation::infrastructure::persistence::message_cache::{
    MessageCacheInvalidator, RedisMessageCacheInvalidator,
};
use flare_conversation::infrastructure::persistence::redis_presence::RedisPresenceRepository;
use flare_grpc_proto::conversation::{
    PurgeConversationHistoryRequest, PurgeConversationHistoryResponse,
};
use flare_server_core::context::Context;
use flare_server_core::error::ErrorCode;
use prost::Message as _;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const ENV_DATABASE_URL: &str = "FLARE_CONVERSATION_TEST_DATABASE_URL";
const ENV_REDIS_URL: &str = "FLARE_CONVERSATION_TEST_REDIS_URL";

/// 字段号是契约的一部分：老客户端/新服务端之间只能加字段，不能改号。
#[test]
fn purge_wire_format_is_stable() {
    let request = PurgeConversationHistoryRequest {
        conversation_id: "c".into(),
        before_ms: 5,
    };
    // field 1 (string) = "c", field 2 (varint) = 5
    assert_eq!(request.encode_to_vec(), vec![0x0a, 0x01, b'c', 0x10, 0x05]);
    let response = PurgeConversationHistoryResponse {
        purged_messages: 3,
        max_purged_seq: 7,
    };
    assert_eq!(response.encode_to_vec(), vec![0x08, 0x03, 0x10, 0x07]);
    // 旧服务端从不回填的空响应 = 什么都没删。
    assert_eq!(
        PurgeConversationHistoryResponse::decode(&[][..]).unwrap(),
        PurgeConversationHistoryResponse::default()
    );
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

fn at(ms: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(ms).single().unwrap()
}

async fn seed_message(
    pool: &PgPool,
    tenant: &str,
    conversation: &str,
    id: &str,
    seq: i64,
    ms: i64,
) {
    seed_message_from(pool, tenant, conversation, id, seq, ms, "alice", 1).await;
}

/// 带发送者与状态的消息（状态 6/7/8 = 撤回/硬删/软删，不计未读）。
#[allow(clippy::too_many_arguments)]
async fn seed_message_from(
    pool: &PgPool,
    tenant: &str,
    conversation: &str,
    id: &str,
    seq: i64,
    ms: i64,
    sender: &str,
    status: i32,
) {
    sqlx::query(
        "INSERT INTO messages (tenant_id, server_id, conversation_id, sender_id, seq, created_at, status) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(tenant)
    .bind(id)
    .bind(conversation)
    .bind(sender)
    .bind(seq)
    .bind(at(ms))
    .bind(status)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO events (tenant_id, conversation_id, seq, event_type, created_at) \
         VALUES ($1, $2, $3, 1, $4)",
    )
    .bind(tenant)
    .bind(conversation)
    .bind(seq)
    .bind(at(ms))
    .execute(pool)
    .await
    .unwrap();
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

async fn ids(
    pool: &PgPool,
    table: &str,
    column: &str,
    tenant: &str,
    conversation: &str,
) -> Vec<String> {
    sqlx::query_scalar(&format!(
        "SELECT {column}::TEXT FROM {table} WHERE tenant_id = $1 AND conversation_id = $2 ORDER BY 1"
    ))
    .bind(tenant)
    .bind(conversation)
    .fetch_all(pool)
    .await
    .unwrap()
}

/// 独立 schema：清理与未读重算会碰到的列与主键，与 migrations/0001_baseline.sql 一致
/// （不依赖 TimescaleDB）。返回（管理连接, 绑定该 schema 的池, schema 名）。
async fn isolated_schema(url: &str) -> (PgPool, PgPool, String) {
    let admin = PgPool::connect(url).await.expect("connect test database");
    let schema = format!("conv_purge_{}", uuid::Uuid::new_v4().simple());
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
        CREATE TABLE conversations (
            tenant_id TEXT NOT NULL, conversation_id TEXT NOT NULL,
            attributes JSONB DEFAULT '{}'::jsonb,
            PRIMARY KEY (tenant_id, conversation_id)
        );
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
            PRIMARY KEY (created_at, server_id)
        );
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

#[tokio::test]
async fn purge_deletes_only_history_before_the_boundary() {
    let Ok(url) = std::env::var(ENV_DATABASE_URL) else {
        eprintln!("{ENV_DATABASE_URL} not set; skipped");
        return;
    };
    let (admin, pool, schema) = isolated_schema(&url).await;

    const BOUNDARY: i64 = 1_790_000_000_000;
    let (tenant, other_tenant) = ("tenant-a", "tenant-b");
    let (conversation, other_conversation) = ("single-ab", "single-ac");
    sqlx::query(
        "INSERT INTO conversations (tenant_id, conversation_id, attributes) \
         VALUES ($1, $2, '{\"flare.social.friendship_epoch\": \"1\"}')",
    )
    .bind(tenant)
    .bind(conversation)
    .execute(&pool)
    .await
    .unwrap();
    seed_message(&pool, tenant, conversation, "old-1", 1, BOUNDARY - 60_000).await;
    seed_message(&pool, tenant, conversation, "old-2", 2, BOUNDARY - 1).await;
    // 边界本身与之后：接受之后、清理之前已经写入的消息。
    seed_message(&pool, tenant, conversation, "new-3", 3, BOUNDARY).await;
    seed_message(&pool, tenant, conversation, "new-4", 4, BOUNDARY + 50).await;
    // 同租户别的会话、别的租户同一会话 ID：都不能被波及。
    seed_message(
        &pool,
        tenant,
        other_conversation,
        "other-1",
        1,
        BOUNDARY - 60_000,
    )
    .await;
    seed_message(
        &pool,
        other_tenant,
        conversation,
        "foreign-1",
        1,
        BOUNDARY - 60_000,
    )
    .await;

    let domain = domain_over(PostgresConversationRepository::new(
        Arc::new(pool.clone()),
        test_config(),
    ));
    let ctx = Context::with_request_id("purge-contract").with_tenant_id(tenant);

    for missing in [0, -1] {
        let error = domain
            .purge_history_before(&ctx, conversation, missing)
            .await
            .expect_err("a non-positive boundary must be rejected, not treated as 1970");
        assert_eq!(error.code(), Some(ErrorCode::InvalidParameter));
    }
    assert_eq!(
        ids(&pool, "messages", "server_id", tenant, conversation)
            .await
            .len(),
        4,
        "rejected calls must not delete anything"
    );

    let first = domain
        .purge_history_before(&ctx, conversation, BOUNDARY)
        .await
        .unwrap();
    assert_eq!(
        first,
        ConversationHistoryPurge {
            purged_messages: 2,
            max_purged_seq: 2,
        }
    );
    assert_eq!(
        ids(&pool, "messages", "server_id", tenant, conversation).await,
        vec!["new-3", "new-4"]
    );
    assert_eq!(
        ids(&pool, "events", "seq", tenant, conversation).await,
        vec!["3", "4"]
    );
    for table in ["pinned_messages", "marked_messages"] {
        assert_eq!(
            ids(&pool, table, "message_id", tenant, conversation).await,
            vec!["new-3", "new-4"],
            "{table}"
        );
    }
    let kept_conversation: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM conversations WHERE tenant_id = $1 AND conversation_id = $2",
    )
    .bind(tenant)
    .bind(conversation)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(kept_conversation, 1, "purge must keep the conversation row");
    assert_eq!(
        ids(&pool, "messages", "server_id", tenant, other_conversation).await,
        vec!["other-1"]
    );
    assert_eq!(
        ids(&pool, "messages", "server_id", other_tenant, conversation).await,
        vec!["foreign-1"]
    );
    assert_eq!(
        ids(&pool, "events", "seq", other_tenant, conversation).await,
        vec!["1"]
    );

    // 重放：同一边界再清一次什么都不删。
    let replay = domain
        .purge_history_before(&ctx, conversation, BOUNDARY)
        .await
        .unwrap();
    assert_eq!(replay, ConversationHistoryPurge::default());
    assert_eq!(
        ids(&pool, "messages", "server_id", tenant, conversation).await,
        vec!["new-3", "new-4"]
    );

    drop_schema(admin, pool, &schema).await;
}

/// 记录失效调用的替身；`fail` 置位时模拟 Redis 不可用。
#[derive(Default)]
struct RecordingInvalidator {
    calls: Mutex<Vec<(String, String, Vec<String>)>>,
    fail: AtomicBool,
}

#[async_trait::async_trait]
impl MessageCacheInvalidator for RecordingInvalidator {
    async fn invalidate_messages(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        message_ids: &[String],
    ) -> flare_server_core::error::Result<()> {
        let mut sorted = message_ids.to_vec();
        sorted.sort();
        self.calls
            .lock()
            .unwrap()
            .push((tenant_id.into(), conversation_id.into(), sorted));
        if self.fail.load(Ordering::SeqCst) {
            return Err(flare_server_core::error::ErrorBuilder::new(
                ErrorCode::NetworkError,
                "injected cache outage",
            )
            .build_error());
        }
        Ok(())
    }
}

async fn seed_participant(
    pool: &PgPool,
    tenant: &str,
    conversation: &str,
    user: &str,
    last_read_seq: i64,
    unread: i32,
    deleted: bool,
) {
    sqlx::query(
        "INSERT INTO conversation_participants \
         (tenant_id, conversation_id, user_id, last_read_seq, unread_count, is_deleted, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $6, TIMESTAMPTZ '2026-01-01 00:00:00+00')",
    )
    .bind(tenant)
    .bind(conversation)
    .bind(user)
    .bind(last_read_seq)
    .bind(unread)
    .bind(deleted)
    .execute(pool)
    .await
    .unwrap();
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

/// 清除之后：成员未读按剩余消息重算（只会变少），被删消息的读侧热缓存在提交前失效；
/// 缓存失效失败则整次清除回滚，重试时拿到同一组消息 ID。
#[tokio::test]
async fn purge_recomputes_unread_and_invalidates_reader_cache() {
    let Ok(url) = std::env::var(ENV_DATABASE_URL) else {
        eprintln!("{ENV_DATABASE_URL} not set; skipped");
        return;
    };
    let (admin, pool, schema) = isolated_schema(&url).await;
    const BOUNDARY: i64 = 1_790_000_000_000;
    let tenant = "tenant-a";
    let (conversation, other_conversation) = ("single-ab", "single-ac");
    // 旧关系：alice 两条、bob 一条；新关系：alice 三条，其中一条已撤回（状态 6）。
    for (id, seq, offset_ms, sender, status) in [
        ("old-1", 1, -9_000, "alice", 1),
        ("old-2", 2, -8_000, "alice", 1),
        ("old-3", 3, -7_000, "bob", 1),
        ("new-4", 4, 10, "alice", 1),
        ("new-5", 5, 20, "alice", 6),
        ("new-6", 6, 30, "alice", 1),
    ] {
        let ms = BOUNDARY + offset_ms;
        seed_message_from(&pool, tenant, conversation, id, seq, ms, sender, status).await;
    }
    // bob 读到 seq 1，物化未读 3（old-2、new-4、new-6）；alice 未读 1（bob 的 old-3）。
    seed_participant(&pool, tenant, conversation, "alice", 0, 1, false).await;
    seed_participant(&pool, tenant, conversation, "bob", 1, 3, false).await;
    // 大群口径不物化未读（0）：重算只会让未读变少，不能凭空加出来。
    seed_participant(&pool, tenant, conversation, "carol", 0, 0, false).await;
    // 已退出的成员不动。
    seed_participant(&pool, tenant, conversation, "dave", 0, 5, true).await;
    seed_participant(&pool, tenant, other_conversation, "bob", 0, 7, false).await;

    let cache = Arc::new(RecordingInvalidator::default());
    let domain = domain_over(
        PostgresConversationRepository::new(Arc::new(pool.clone()), test_config())
            .with_message_cache_invalidator(cache.clone()),
    );
    let ctx = Context::with_request_id("purge-unread").with_tenant_id(tenant);
    let purged_ids = vec![
        "old-1".to_string(),
        "old-2".to_string(),
        "old-3".to_string(),
    ];
    let expected_call = (
        tenant.to_string(),
        conversation.to_string(),
        purged_ids.clone(),
    );
    let before = vec![
        ("alice".to_string(), 1),
        ("bob".to_string(), 3),
        ("carol".to_string(), 0),
        ("dave".to_string(), 5),
    ];

    // 缓存不可用：清除整体回滚，消息、未读都不动。
    cache.fail.store(true, Ordering::SeqCst);
    domain
        .purge_history_before(&ctx, conversation, BOUNDARY)
        .await
        .expect_err("a cache outage must abort the purge so the retry still knows the IDs");
    assert_eq!(
        ids(&pool, "messages", "server_id", tenant, conversation)
            .await
            .len(),
        6,
        "rolled back"
    );
    assert_eq!(unread(&pool, tenant, conversation).await, before);
    assert_eq!(*cache.calls.lock().unwrap(), vec![expected_call.clone()]);

    // 重试：删掉同一组消息，提交前、提交后各失效一次，未读按剩余消息重算。
    cache.fail.store(false, Ordering::SeqCst);
    cache.calls.lock().unwrap().clear();
    let purged = domain
        .purge_history_before(&ctx, conversation, BOUNDARY)
        .await
        .unwrap();
    assert_eq!(
        purged,
        ConversationHistoryPurge {
            purged_messages: 3,
            max_purged_seq: 3,
        }
    );
    assert_eq!(
        *cache.calls.lock().unwrap(),
        vec![expected_call.clone(), expected_call]
    );
    assert_eq!(
        unread(&pool, tenant, conversation).await,
        vec![
            ("alice".to_string(), 0),
            ("bob".to_string(), 2),
            ("carol".to_string(), 0),
            ("dave".to_string(), 5),
        ],
        "unread must count only surviving, visible, non-recalled messages from others"
    );
    let refreshed: Vec<String> = sqlx::query_scalar(
        "SELECT user_id FROM conversation_participants WHERE tenant_id = $1 \
         AND conversation_id = $2 AND updated_at > TIMESTAMPTZ '2026-01-01 00:00:00+00' ORDER BY 1",
    )
    .bind(tenant)
    .bind(conversation)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        refreshed,
        vec!["alice", "bob", "carol"],
        "incremental conversation sync must pick up the new unread counts"
    );
    assert_eq!(
        unread(&pool, tenant, other_conversation).await,
        vec![("bob".to_string(), 7)]
    );

    // 重放：没有可删的消息，不重算、不失效。
    cache.calls.lock().unwrap().clear();
    let replay = domain
        .purge_history_before(&ctx, conversation, BOUNDARY)
        .await
        .unwrap();
    assert_eq!(replay, ConversationHistoryPurge::default());
    assert!(cache.calls.lock().unwrap().is_empty());
    assert_eq!(
        unread(&pool, tenant, conversation).await[1],
        ("bob".to_string(), 2)
    );

    drop_schema(admin, pool, &schema).await;
}

/// 真 Redis：失效只摘掉被删消息的尾部索引与单条缓存，同会话其余消息、别的租户原样保留。
#[tokio::test]
async fn redis_invalidator_removes_only_purged_messages_from_reader_cache() {
    let Ok(url) = std::env::var(ENV_REDIS_URL) else {
        eprintln!("{ENV_REDIS_URL} not set; skipped");
        return;
    };
    let client = Arc::new(redis::Client::open(url).unwrap());
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let run = uuid::Uuid::new_v4().simple().to_string();
    let (tenant, other_tenant) = (format!("t-{run}"), format!("u-{run}"));
    let conversation = "single-ab";
    let seed = |tenant: String| {
        let mut pipe = redis::pipe();
        for (seq, id) in ["m1", "m2", "m3", "m4"].iter().enumerate() {
            pipe.cmd("ZADD")
                .arg(flare_im_contracts::message_tail_key(&tenant, conversation))
                .arg(seq as i64 + 1)
                .arg(*id)
                .ignore();
            pipe.cmd("SET")
                .arg(flare_im_contracts::message_cache_key(
                    &tenant,
                    conversation,
                    id,
                ))
                .arg("encoded")
                .ignore();
        }
        pipe
    };
    seed(tenant.clone())
        .query_async::<()>(&mut conn)
        .await
        .unwrap();
    seed(other_tenant.clone())
        .query_async::<()>(&mut conn)
        .await
        .unwrap();

    let invalidator = RedisMessageCacheInvalidator::new(client.clone());
    let purged = vec!["m1".to_string(), "m2".to_string()];
    for _ in 0..2 {
        // 第二次模拟提交后的再次失效：幂等。
        invalidator
            .invalidate_messages(&tenant, conversation, &purged)
            .await
            .unwrap();
    }

    let tail = |tenant: &str| flare_im_contracts::message_tail_key(tenant, conversation);
    let remaining: Vec<String> = redis::cmd("ZRANGE")
        .arg(tail(&tenant))
        .arg(0)
        .arg(-1)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(remaining, vec!["m3", "m4"]);
    for (id, expected) in [("m1", 0), ("m2", 0), ("m3", 1), ("m4", 1)] {
        let exists: i64 = redis::cmd("EXISTS")
            .arg(flare_im_contracts::message_cache_key(
                &tenant,
                conversation,
                id,
            ))
            .query_async(&mut conn)
            .await
            .unwrap();
        assert_eq!(exists, expected, "{id}");
    }
    let untouched: i64 = redis::cmd("ZCARD")
        .arg(tail(&other_tenant))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(untouched, 4, "another tenant's cache must not be touched");

    let mut cleanup: Vec<String> = vec![tail(&tenant), tail(&other_tenant)];
    for owner in [&tenant, &other_tenant] {
        for id in ["m1", "m2", "m3", "m4"] {
            cleanup.push(flare_im_contracts::message_cache_key(
                owner,
                conversation,
                id,
            ));
        }
    }
    redis::cmd("DEL")
        .arg(&cleanup)
        .query_async::<()>(&mut conn)
        .await
        .unwrap();
}
