//! 按 id 查成员的真库校验：仓储 `find_conversation_members` 与读接口 `ext["member_ids"]`
//! （上行事件的操作者核对、插件资源桥的成员核对走这条路）。
//!
//! 需要 `FLARE_CONVERSATION_TEST_DATABASE_URL`（任意可建 schema 的 PostgreSQL）。用例标了
//! `#[ignore]`：默认的 `cargo test` 只记为 ignored，`-- --ignored` 显式运行时变量必须已设置。
//! 表结构取自 `migrations/0001_baseline.sql` 与后续迁移，建在独立 schema 里，跑完整个删掉。

use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use flare_grpc_proto::conversation::ListConversationParticipantsRequest;
use flare_grpc_proto::conversation::conversation_read_service_server::ConversationReadService;
use flare_server_core::context::{ActorContext, ActorType, Context};
use flare_server_core::error::ErrorCode;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use crate::application::handlers::{ConversationCommandHandler, ConversationQueryHandler};
use crate::config::ConversationConfig;
use crate::domain::model::{
    ConflictResolutionPolicy, ConversationDomainConfig, ConversationPolicy,
};
use crate::domain::repository::ConversationRepository;
use crate::domain::service::DefaultConversationDomainService;
use crate::infrastructure::persistence::PostgresConversationRepository;
use crate::infrastructure::persistence::redis_presence::RedisPresenceRepository;
use crate::interface::grpc::ConversationGrpcHandler;

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
    let schema = format!("conv_lookup_{}", uuid::Uuid::new_v4().simple());
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
        create_table(&baseline, "conversations"),
        create_table(&baseline, "conversation_participants"),
        migration("0003_20260810_add_participant_visible_from_seq.sql"),
        migration("0004_20260812_add_participant_mention_only.sql"),
        migration("0006_20260901_participants_joined_at_index.sql"),
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

async fn member(pool: &PgPool, tenant: &str, user: &str, joined: i64, removed: bool) {
    sqlx::query(
        "INSERT INTO conversation_participants (tenant_id, conversation_id, user_id, roles, \
         joined_at, nickname, is_deleted, quit_at) VALUES ($1, 'g1', $2, ARRAY['member'], $3, $4, \
         $5, $6)",
    )
    .bind(tenant)
    .bind(user)
    .bind(at(joined))
    .bind(format!("nick-{user}"))
    .bind(removed)
    .bind(removed.then(|| at(99)))
    .execute(pool)
    .await
    .unwrap();
}

fn user(tenant: &str, user: &str) -> Context {
    Context::with_request_id("req-lookup")
        .with_tenant_id(tenant)
        .with_user_id(user)
}

fn ids(ids: &[&str]) -> Vec<String> {
    ids.iter().map(|id| id.to_string()).collect()
}

fn handler(repo: Arc<PostgresConversationRepository>) -> ConversationGrpcHandler {
    let presence = Arc::new(RedisPresenceRepository::new(
        Arc::new(redis::Client::open("redis://127.0.0.1:9/").unwrap()),
        test_config(),
    ));
    let domain = Arc::new(DefaultConversationDomainService::new(
        repo.clone(),
        presence,
        None,
        ConversationDomainConfig::default(),
    ));
    ConversationGrpcHandler::new(
        Arc::new(ConversationCommandHandler::new(domain.clone())),
        Arc::new(ConversationQueryHandler::new(repo, None, domain)),
    )
}

async fn lookup(
    handler: &ConversationGrpcHandler,
    ctx: &Context,
    user_ids: &str,
) -> Result<flare_grpc_proto::conversation::ListConversationParticipantsResponse, tonic::Status> {
    let mut request = tonic::Request::new(ListConversationParticipantsRequest {
        conversation_id: "g1".into(),
        // A lookup ignores paging: neither the cursor nor the limit narrows it.
        cursor: "1".into(),
        limit: 1,
        include_removed: true,
        ext: [("member_ids".to_string(), user_ids.to_string())].into(),
    });
    flare_server_core::client::set_context_metadata(&mut request, ctx);
    handler
        .list_conversation_participants(request)
        .await
        .map(tonic::Response::into_inner)
}

#[tokio::test]
#[ignore = "requires FLARE_CONVERSATION_TEST_DATABASE_URL pointing at a disposable PostgreSQL"]
async fn lookup_returns_the_active_members_among_the_ids() {
    let url = std::env::var(ENV_DATABASE_URL)
        .unwrap_or_else(|_| panic!("set {ENV_DATABASE_URL} to a disposable PostgreSQL instance"));
    let (admin, pool, schema) = isolated_schema(&url).await;
    conversation(&pool, "tenant-a", "g1").await;
    conversation(&pool, "tenant-b", "g1").await;
    member(&pool, "tenant-a", "carol", 2, false).await;
    member(&pool, "tenant-a", "alice", 0, false).await;
    member(&pool, "tenant-a", "bob", 1, false).await;
    member(&pool, "tenant-a", "dave", 3, true).await;
    // The same conversation id in another tenant must not leak in.
    member(&pool, "tenant-b", "erin", 0, false).await;
    member(&pool, "tenant-b", "alice", 0, false).await;
    let repo = Arc::new(PostgresConversationRepository::new(
        Arc::new(pool.clone()),
        test_config(),
    ));

    let alice = user("tenant-a", "alice");
    let found = repo
        .find_conversation_members(
            &alice,
            "g1",
            &ids(&["carol", "ghost", "dave", "alice", "erin", "bob"]),
        )
        .await
        .unwrap();
    assert_eq!(
        found.iter().map(|p| p.user_id.as_str()).collect::<Vec<_>>(),
        ["alice", "bob", "carol"],
        "members only, in joined order; who left, absent and other-tenant ids are left out"
    );
    assert_eq!(found[0].roles, ["member"]);
    assert_eq!(
        found[0].attributes.get("nickname").map(String::as_str),
        Some("nick-alice")
    );
    assert!(
        repo.find_conversation_members(&alice, "g1", &[])
            .await
            .unwrap()
            .is_empty()
    );

    // A removed caller reads nothing; a trusted internal caller needs no membership.
    let error = repo
        .find_conversation_members(&user("tenant-a", "dave"), "g1", &ids(&["alice"]))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some(ErrorCode::MessageNotFound));
    let service = Context::with_request_id("req-lookup")
        .with_tenant_id("tenant-a")
        .with_actor(ActorContext::new("svc-capability").with_type(ActorType::Service));
    assert_eq!(
        repo.find_conversation_members(&service, "g1", &ids(&["bob", "dave"]))
            .await
            .unwrap()
            .len(),
        1
    );

    // The paged list shares the row mapping and the caller check.
    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let page = repo
            .list_conversation_participants(
                &alice,
                "g1",
                cursor.as_deref(),
                1,
                false,
                true,
                Default::default(),
            )
            .await
            .unwrap();
        seen.extend(page.participants.into_iter().map(|p| p.user_id));
        if !page.has_more {
            break;
        }
        cursor = page.next_cursor;
    }
    assert_eq!(seen, ["alice", "bob", "carol"]);
    assert!(
        repo.list_conversation_participants(
            &user("tenant-a", "dave"),
            "g1",
            None,
            10,
            false,
            true,
            Default::default(),
        )
        .await
        .is_err()
    );

    // The read interface answers `ext["member_ids"]` with one unpaged lookup.
    let handler = handler(repo);
    let response = lookup(&handler, &alice, "carol,ghost,alice,dave")
        .await
        .unwrap();
    assert_eq!(response.conversation_id, "g1");
    assert_eq!(
        response
            .participants
            .iter()
            .map(|p| p.user_id.as_str())
            .collect::<Vec<_>>(),
        ["alice", "carol"]
    );
    assert!(!response.has_more);
    assert!(response.next_cursor.is_empty());
    for invalid in ["alice,alice", "", "alice,,bob"] {
        assert_eq!(
            lookup(&handler, &alice, invalid).await.unwrap_err().code(),
            tonic::Code::InvalidArgument,
            "{invalid:?}"
        );
    }
    let too_many = (0..501)
        .map(|i| format!("u{i}"))
        .collect::<Vec<_>>()
        .join(",");
    assert_eq!(
        lookup(&handler, &alice, &too_many)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    drop(handler);
    drop_schema(admin, pool, &schema).await;
}
