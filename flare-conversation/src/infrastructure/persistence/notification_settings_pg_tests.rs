//! 用户级通知设置与会话通知订阅者的真库校验：版本化写入、批量读的默认值、`notify_all` 经会话
//! 设置写入后进入订阅者列表、订阅者分页与 5000 上限之前的令牌。
//!
//! 需要 `FLARE_CONVERSATION_TEST_DATABASE_URL`（任意可建 schema 的 PostgreSQL）。用例标了
//! `#[ignore]`：默认的 `cargo test` 只记为 ignored，`-- --ignored` 显式运行时变量必须已设置。
//! 表结构取自 `migrations/0001_baseline.sql` 与后续迁移（含 0030），建在独立 schema 里，跑完整个删掉。

use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use flare_server_core::context::{ActorContext, ActorType, Context};
use flare_server_core::error::ErrorCode;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use crate::config::ConversationConfig;
use crate::domain::model::{
    ConflictResolutionPolicy, ConversationDomainConfig, ConversationPolicy, MultiDeviceAlertPolicy,
    NotificationPreview, QuietHours, SubscriberCursor, UpdateConversationUserSettingsPatch,
    UserNotificationSettings, UserNotificationSettingsPatch,
};
use crate::domain::repository::{ConversationRepository, NotificationSettingsRepository};
use crate::domain::service::DefaultConversationDomainService;
use crate::infrastructure::persistence::PostgresConversationRepository;
use crate::infrastructure::persistence::redis_presence::RedisPresenceRepository;

const ENV_DATABASE_URL: &str = "FLARE_CONVERSATION_TEST_DATABASE_URL";

fn test_config() -> Arc<ConversationConfig> {
    Arc::new(ConversationConfig {
        mq_backend: "nats".into(),
        redis_url: "redis://127.0.0.1:9/".into(),
        postgres_url: None,
        postgres_max_connections: 1,
        postgres_min_connections: 0,
        postgres_acquire_timeout_seconds: 1,
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
    let schema = format!("conv_notify_{}", uuid::Uuid::new_v4().simple());
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
        migration("0003_20260810_add_participant_visible_from_seq.sql"),
        migration("0004_20260812_add_participant_mention_only.sql"),
        migration("0021_20261103_conversation_history_clear.sql"),
        migration("0006_20260901_participants_joined_at_index.sql"),
        migration("0030_20261111_user_notification_settings.sql"),
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

fn at(seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(1_750_000_000 + seconds, 0)
        .single()
        .unwrap()
}

async fn conversation(pool: &PgPool, tenant: &str, id: &str) {
    sqlx::query(
        "INSERT INTO conversations (tenant_id, conversation_id, conversation_type, display_name, \
         lifecycle_state, created_at, updated_at) VALUES ($1, $2, 2, $2, 'active', $3, $3)",
    )
    .bind(tenant)
    .bind(id)
    .bind(at(0))
    .execute(pool)
    .await
    .unwrap();
}

/// 一个成员行：`notify_all`、免打扰、仅 @我、已退出、隐藏了会话。
#[derive(Clone, Copy, Default)]
struct Member {
    notify_all: bool,
    muted: bool,
    mention_only: bool,
    quit: bool,
    hidden: bool,
}

async fn member(pool: &PgPool, tenant: &str, conversation_id: &str, user: &str, row: Member) {
    sqlx::query(
        "INSERT INTO conversation_participants (tenant_id, conversation_id, user_id, roles, \
         joined_at, muted, mention_only, notify_all, quit_at, is_deleted) \
         VALUES ($1, $2, $3, ARRAY['member'], $4, $5, $6, $7, $8, $9)",
    )
    .bind(tenant)
    .bind(conversation_id)
    .bind(user)
    .bind(at(1))
    .bind(row.muted)
    .bind(row.mention_only)
    .bind(row.notify_all)
    .bind(row.quit.then(|| at(9)))
    .bind(row.hidden)
    .execute(pool)
    .await
    .unwrap();
}

fn user(tenant: &str, user: &str) -> Context {
    Context::with_request_id("req-notify")
        .with_tenant_id(tenant)
        .with_user_id(user)
}

fn service(tenant: &str) -> Context {
    Context::with_request_id("req-notify")
        .with_tenant_id(tenant)
        .with_actor(ActorContext::new("flare-push-server").with_type(ActorType::Service))
}

fn domain(repo: Arc<PostgresConversationRepository>) -> DefaultConversationDomainService {
    let presence = Arc::new(RedisPresenceRepository::new(
        Arc::new(redis::Client::open("redis://127.0.0.1:9/").unwrap()),
        test_config(),
    ));
    DefaultConversationDomainService::new(repo, presence, None, ConversationDomainConfig::default())
}

fn quiet(start: u32, end: u32) -> QuietHours {
    QuietHours {
        start_minute: start,
        end_minute: end,
        weekdays: 0b001_1111,
        allow_mentions: true,
    }
}

fn patch(
    quiet_hours: Option<QuietHours>,
    clear: bool,
    preview: Option<i32>,
    multi_device: Option<i32>,
    base: u64,
) -> UserNotificationSettingsPatch {
    UserNotificationSettingsPatch::from_request(quiet_hours, clear, preview, multi_device, base)
        .unwrap()
}

#[tokio::test]
#[ignore = "requires FLARE_CONVERSATION_TEST_DATABASE_URL pointing at a disposable PostgreSQL"]
async fn user_notification_settings_are_versioned_partial_writes() {
    let url = std::env::var(ENV_DATABASE_URL)
        .unwrap_or_else(|_| panic!("set {ENV_DATABASE_URL} to a disposable PostgreSQL instance"));
    let (admin, pool, schema) = isolated_schema(&url).await;
    let repo = Arc::new(PostgresConversationRepository::new(
        Arc::new(pool.clone()),
        test_config(),
    ));
    let domain = domain(repo.clone());
    let alice = user("tenant-a", "alice");

    // 从没设置过：仓储没有行，领域给默认值。
    assert_eq!(
        repo.get_user_notification_settings(&alice, "alice")
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        domain
            .get_user_notification_settings(&alice, "alice")
            .await
            .unwrap(),
        UserNotificationSettings::default()
    );

    // 没存过的用户带非 0 的基准版本：冲突，不插入。
    let error = repo
        .update_user_notification_settings(&alice, "alice", &patch(None, false, Some(3), None, 1))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some(ErrorCode::HttpConflict));
    assert_eq!(
        repo.get_user_notification_settings(&alice, "alice")
            .await
            .unwrap(),
        None
    );

    // 第一次写入：以默认值为底，版本 1。
    let first = domain
        .update_user_notification_settings(
            &alice,
            "alice",
            &patch(Some(quiet(22 * 60, 7 * 60)), false, None, None, 0),
        )
        .await
        .unwrap();
    assert_eq!(
        first,
        UserNotificationSettings {
            quiet_hours: Some(quiet(22 * 60, 7 * 60)),
            preview: NotificationPreview::Unspecified,
            multi_device: MultiDeviceAlertPolicy::Unspecified,
            settings_version: 1,
        }
    );

    // 只改预览：勿扰时段保留，版本加一。
    let second = repo
        .update_user_notification_settings(&alice, "alice", &patch(None, false, Some(2), None, 1))
        .await
        .unwrap();
    assert_eq!(second.quiet_hours, Some(quiet(22 * 60, 7 * 60)));
    assert_eq!(second.preview, NotificationPreview::SenderOnly);
    assert_eq!(second.settings_version, 2);

    // 过期的基准版本：冲突，什么都不改。
    let error = repo
        .update_user_notification_settings(&alice, "alice", &patch(None, true, None, Some(2), 1))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some(ErrorCode::HttpConflict));
    assert_eq!(
        repo.get_user_notification_settings(&alice, "alice")
            .await
            .unwrap(),
        Some(second.clone())
    );

    // 版本相符：取消勿扰、改多端策略。
    let third = repo
        .update_user_notification_settings(&alice, "alice", &patch(None, true, None, Some(2), 2))
        .await
        .unwrap();
    assert_eq!(
        third,
        UserNotificationSettings {
            quiet_hours: None,
            preview: NotificationPreview::SenderOnly,
            multi_device: MultiDeviceAlertPolicy::Immediate,
            settings_version: 3,
        }
    );

    // 同一用户在别的租户是另一份设置。
    assert_eq!(
        repo.get_user_notification_settings(&user("tenant-b", "alice"), "alice")
            .await
            .unwrap(),
        None
    );

    drop_schema(admin, pool, &schema).await;
}

#[tokio::test]
#[ignore = "requires FLARE_CONVERSATION_TEST_DATABASE_URL pointing at a disposable PostgreSQL"]
async fn a_batch_read_answers_every_user_in_order_with_defaults() {
    let url = std::env::var(ENV_DATABASE_URL)
        .unwrap_or_else(|_| panic!("set {ENV_DATABASE_URL} to a disposable PostgreSQL instance"));
    let (admin, pool, schema) = isolated_schema(&url).await;
    let repo = Arc::new(PostgresConversationRepository::new(
        Arc::new(pool.clone()),
        test_config(),
    ));
    let domain = domain(repo.clone());
    repo.update_user_notification_settings(
        &user("tenant-a", "bob"),
        "bob",
        &patch(None, false, Some(3), None, 0),
    )
    .await
    .unwrap();
    // 另一个租户的同名用户不能混进来。
    repo.update_user_notification_settings(
        &user("tenant-b", "carol"),
        "carol",
        &patch(None, false, None, Some(3), 0),
    )
    .await
    .unwrap();

    let users = vec!["carol".to_string(), "bob".to_string(), "ghost".to_string()];
    let stored = repo
        .batch_get_user_notification_settings(&service("tenant-a"), &users)
        .await
        .unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].0, "bob");

    let answered = domain
        .batch_get_user_notification_settings(&service("tenant-a"), &users)
        .await
        .unwrap();
    assert_eq!(
        answered
            .iter()
            .map(|(id, _)| id.as_str())
            .collect::<Vec<_>>(),
        ["carol", "bob", "ghost"]
    );
    assert_eq!(answered[0].1, UserNotificationSettings::default());
    assert_eq!(answered[1].1.preview, NotificationPreview::Hidden);
    assert_eq!(answered[1].1.settings_version, 1);
    assert_eq!(answered[2].1, UserNotificationSettings::default());

    drop_schema(admin, pool, &schema).await;
}

#[tokio::test]
#[ignore = "requires FLARE_CONVERSATION_TEST_DATABASE_URL pointing at a disposable PostgreSQL"]
async fn notify_all_round_trips_through_conversation_settings_into_the_subscriber_list() {
    let url = std::env::var(ENV_DATABASE_URL)
        .unwrap_or_else(|_| panic!("set {ENV_DATABASE_URL} to a disposable PostgreSQL instance"));
    let (admin, pool, schema) = isolated_schema(&url).await;
    conversation(&pool, "tenant-a", "2G1").await;
    conversation(&pool, "tenant-b", "2G1").await;
    member(&pool, "tenant-a", "2G1", "alice", Member::default()).await;
    let on = Member {
        notify_all: true,
        ..Member::default()
    };
    member(&pool, "tenant-a", "2G1", "dave", on).await;
    member(&pool, "tenant-a", "2G1", "carol", on).await;
    // 开了 notify_all 但免打扰、仅 @我、已退出、隐藏了会话的都不列。
    let muted = Member { muted: true, ..on };
    let mention_only = Member {
        mention_only: true,
        ..on
    };
    let quit = Member { quit: true, ..on };
    let hidden = Member { hidden: true, ..on };
    for (name, row) in [
        ("erin", muted),
        ("frank", mention_only),
        ("grace", quit),
        ("heidi", hidden),
    ] {
        member(&pool, "tenant-a", "2G1", name, row).await;
    }
    // 别的租户里同一个会话 ID 的订阅者不能混进来。
    member(&pool, "tenant-b", "2G1", "bob", on).await;
    let repo = Arc::new(PostgresConversationRepository::new(
        Arc::new(pool.clone()),
        test_config(),
    ));
    let domain = domain(repo.clone());
    let push = service("tenant-a");

    let first = domain
        .list_notification_subscribers(&push, "2G1", &SubscriberCursor::default(), 1000)
        .await
        .unwrap();
    assert_eq!(first.user_ids, ["carol", "dave"]);
    assert_eq!(first.next_page_token, None);
    assert!(!first.truncated);

    // alice 在会话设置里打开 notify_all：返回值带出来，并进入订阅者列表。
    let settings = repo
        .update_user_settings(
            &user("tenant-a", "alice"),
            &UpdateConversationUserSettingsPatch {
                conversation_id: "2G1".into(),
                is_mention_only: None,
                is_pinned: None,
                is_muted: None,
                is_archived: None,
                draft: None,
                base_settings_version: 0,
                clear_history_through_seq: None,
                notify_all: Some(true),
            },
        )
        .await
        .unwrap();
    assert!(settings.notify_all);
    assert_eq!(settings.settings_version, 1);

    // 一页一个：令牌接着上一页的最后一个往后取。
    let page = domain
        .list_notification_subscribers(&push, "2G1", &SubscriberCursor::default(), 1)
        .await
        .unwrap();
    assert_eq!(page.user_ids, ["alice"]);
    let token = page.next_page_token.expect("more subscribers");
    let page = domain
        .list_notification_subscribers(&push, "2G1", &SubscriberCursor::parse(&token).unwrap(), 1)
        .await
        .unwrap();
    assert_eq!(page.user_ids, ["carol"]);
    let token = page.next_page_token.expect("more subscribers");
    let page = domain
        .list_notification_subscribers(&push, "2G1", &SubscriberCursor::parse(&token).unwrap(), 1)
        .await
        .unwrap();
    assert_eq!(page.user_ids, ["dave"]);
    assert_eq!(page.next_page_token, None);

    // 关掉之后不再是订阅者；其余个人设置没被这次写入改动。
    let settings = repo
        .update_user_settings(
            &user("tenant-a", "alice"),
            &UpdateConversationUserSettingsPatch {
                conversation_id: "2G1".into(),
                is_mention_only: None,
                is_pinned: None,
                is_muted: None,
                is_archived: None,
                draft: None,
                base_settings_version: 1,
                clear_history_through_seq: None,
                notify_all: Some(false),
            },
        )
        .await
        .unwrap();
    assert!(!settings.notify_all);
    assert!(!settings.is_muted);
    assert_eq!(settings.settings_version, 2);
    assert_eq!(
        repo.list_notification_subscribers(&push, "2G1", None, 10)
            .await
            .unwrap(),
        ["carol", "dave"]
    );

    drop_schema(admin, pool, &schema).await;
}
