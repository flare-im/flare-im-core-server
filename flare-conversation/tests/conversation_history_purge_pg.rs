//! `PurgeConversationHistory` 契约：按服务端时间边界硬删会话历史，边界及之后的一条都不碰。
//!
//! 真库部分只在设置了 `FLARE_CONVERSATION_TEST_DATABASE_URL` 时运行（任意可建 schema 的
//! PostgreSQL；用例在独立 schema 里建出清理会碰到的几张表，跑完整个删掉），否则打印 skipped
//! 并直接通过。线格式部分不依赖数据库。

use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use flare_conversation::config::ConversationConfig;
use flare_conversation::domain::model::{
    ConflictResolutionPolicy, ConversationDomainConfig, ConversationHistoryPurge,
    ConversationPolicy,
};
use flare_conversation::domain::service::DefaultConversationDomainService;
use flare_conversation::infrastructure::persistence::PostgresConversationRepository;
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
    sqlx::query(
        "INSERT INTO messages (tenant_id, server_id, conversation_id, sender_id, seq, created_at) \
         VALUES ($1, $2, $3, 'alice', $4, $5)",
    )
    .bind(tenant)
    .bind(id)
    .bind(conversation)
    .bind(seq)
    .bind(at(ms))
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

#[tokio::test]
async fn purge_deletes_only_history_before_the_boundary() {
    let Ok(url) = std::env::var(ENV_DATABASE_URL) else {
        eprintln!("{ENV_DATABASE_URL} not set; skipped");
        return;
    };
    let admin = PgPool::connect(&url).await.expect("connect test database");
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
        .connect(&url)
        .await
        .unwrap();
    // 清理会碰到的列与主键，与 migrations/0001_baseline.sql 一致（不依赖 TimescaleDB）。
    sqlx::raw_sql(
        r#"
        CREATE TABLE conversations (
            tenant_id TEXT NOT NULL, conversation_id TEXT NOT NULL,
            attributes JSONB DEFAULT '{}'::jsonb,
            PRIMARY KEY (tenant_id, conversation_id)
        );
        CREATE TABLE messages (
            tenant_id TEXT NOT NULL, server_id TEXT NOT NULL, conversation_id TEXT NOT NULL,
            sender_id TEXT NOT NULL, seq BIGINT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
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

    let repo = Arc::new(PostgresConversationRepository::new(
        Arc::new(pool.clone()),
        test_config(),
    ));
    let presence = Arc::new(RedisPresenceRepository::new(
        Arc::new(redis::Client::open("redis://127.0.0.1:9/").unwrap()),
        test_config(),
    ));
    let domain = DefaultConversationDomainService::new(
        repo,
        presence,
        None,
        ConversationDomainConfig::default(),
    );
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

    pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
