//! `PurgeConversationHistory` 的汇总日志：部署默认只输出 info，按纪元与按时间回落各删了几条
//! 必须在 info 级可见，栈上核对清除结果靠这一行。
//!
//! 单独成一个测试二进制、只有一个用例：tracing 的 callsite 兴趣缓存在多线程并发注册时，可能
//! 按「没有订阅者」缓存成永不启用，与别的用例同进程并行时这里的线程局部订阅者会收不到事件。
//!
//! 真库用例需要 `FLARE_CONVERSATION_TEST_DATABASE_URL`（任意可建 schema 的 PostgreSQL；用例在
//! 独立 schema 里建出清理会碰到的表，跑完整个删掉），标了 `#[ignore]`：`-- --ignored` 显式运行时
//! 变量必须已设置，否则直接失败。

use std::sync::{Arc, Mutex};

use chrono::{TimeZone, Utc};
use flare_conversation::config::ConversationConfig;
use flare_conversation::domain::model::{
    ConflictResolutionPolicy, ConversationDomainConfig, ConversationHistoryPurge,
    ConversationPolicy,
};
use flare_conversation::domain::service::DefaultConversationDomainService;
use flare_conversation::infrastructure::persistence::PostgresConversationRepository;
use flare_conversation::infrastructure::persistence::redis_presence::RedisPresenceRepository;
use flare_server_core::context::Context;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const ENV_DATABASE_URL: &str = "FLARE_CONVERSATION_TEST_DATABASE_URL";
const EPOCH: &str = "flare.social.friendship_epoch";
const BOUNDARY: i64 = 1_790_000_000_000;

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

/// 进程内收集日志输出。
#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
    type Writer = CapturedLogs;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// 独立 schema：清理会碰到的表与列，与 `conversation_history_purge_epoch_pg.rs` 一致。
async fn isolated_schema(url: &str) -> (PgPool, PgPool, String) {
    let admin = PgPool::connect(url).await.expect("connect test database");
    let schema = format!("conv_purge_log_{}", uuid::Uuid::new_v4().simple());
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
            last_read_seq BIGINT DEFAULT 0, last_sync_seq BIGINT DEFAULT 0, unread_count INT DEFAULT 0,
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

async fn seed_message(pool: &PgPool, id: &str, seq: i64, ms: i64, epoch: Option<&str>) {
    let mut extra = serde_json::Map::new();
    if let Some(value) = epoch {
        extra.insert(EPOCH.into(), value.into());
    }
    sqlx::query(
        "INSERT INTO messages (tenant_id, server_id, conversation_id, sender_id, seq, created_at, extra) \
         VALUES ('tenant-a', $1, 'single-ab', 'alice', $2, $3, $4::TEXT::JSONB)",
    )
    .bind(id)
    .bind(seq)
    .bind(Utc.timestamp_millis_opt(ms).single().unwrap())
    .bind(serde_json::Value::Object(extra).to_string())
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires FLARE_CONVERSATION_TEST_DATABASE_URL pointing at a disposable PostgreSQL"]
async fn purge_summary_reports_epoch_and_time_fallback_counts_at_info() {
    let url = std::env::var(ENV_DATABASE_URL)
        .unwrap_or_else(|_| panic!("set {ENV_DATABASE_URL} to a disposable PostgreSQL instance"));
    let (admin, pool, schema) = isolated_schema(&url).await;
    // 没有纪元、在边界之前：按时间回落删；纪元 1、被快时钟盖在边界之后：按纪元删；纪元 2：留下。
    seed_message(&pool, "untagged-old", 1, BOUNDARY - 60_000, None).await;
    seed_message(&pool, "e1-fast-clock", 2, BOUNDARY + 5_000, Some("1")).await;
    seed_message(&pool, "e2", 3, BOUNDARY - 10_000, Some("2")).await;

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
    let ctx = Context::with_request_id("purge-log").with_tenant_id("tenant-a");

    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .with_writer(logs.clone())
        .finish();
    let purged = {
        let _guard = tracing::subscriber::set_default(subscriber);
        domain
            .purge_history(&ctx, "single-ab", BOUNDARY, EPOCH, 2)
            .await
            .unwrap()
    };
    assert_eq!(
        purged,
        ConversationHistoryPurge {
            purged_messages: 2,
            max_purged_seq: 2,
        }
    );

    let output = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    let line = output
        .lines()
        .find(|line| line.contains("Conversation history purged before boundary"))
        .unwrap_or_else(|| panic!("purge summary must be logged at info level, got: {output}"));
    for field in [
        " INFO ",
        "tenant_id=tenant-a",
        "conversation_id=single-ab",
        "purged_messages=2",
        "max_purged_seq=2",
        "before_epoch=2",
        "time_boundary_ms=1790000000000",
        "fenced_messages=1",
        "time_fallback_messages=1",
    ] {
        assert!(line.contains(field), "missing {field} in: {line}");
    }

    pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
