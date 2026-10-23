//! 已读相关读写的真库校验：
//! - 群聊已读摘要与已读名单：读接口 `MessageReadReceiptService` 经仓储读成员的已读水位（由
//!   `mark_as_read` 推进，即已读回执消费者写的那一列）与消息表；
//! - Orchestrator 核对上行事件操作者的成员查找（`ext["member_ids"]`）：隐藏了会话的仍是成员；
//! - 撤回、删除与未读：状态按 proto `MessageStatus` 判断，撤回后未读不再含这条消息。
//!
//! 需要 `FLARE_CONVERSATION_TEST_DATABASE_URL`（任意可建 schema 的 PostgreSQL）。用例标了
//! `#[ignore]`：默认的 `cargo test` 只记为 ignored，`-- --ignored` 显式运行时变量必须已设置。
//! 表结构取自 `migrations/0001_baseline.sql` 与后续迁移，建在独立 schema 里，跑完整个删掉。

use std::sync::Arc;
use std::time::Instant;

use flare_core::common::conversation::{
    generate_group_conversation_id, generate_single_chat_conversation_id,
};
use flare_grpc_proto::conversation::message_read_receipt_service_server::MessageReadReceiptService;
use flare_grpc_proto::conversation::{
    GetMessageReadSummariesRequest, GetMessageReadSummariesResponse, ListMessageReadersRequest,
    ListMessageReadersResponse, MessageReadSummary, MessageReaderState,
};
use flare_server_core::context::{ActorContext, ActorType, Context};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use crate::application::handlers::{ConversationCommandHandler, ConversationQueryHandler};
use crate::config::ConversationConfig;
use crate::domain::model::ConversationDomainConfig;
use crate::domain::repository::ConversationRepository;
use crate::domain::service::DefaultConversationDomainService;
use crate::infrastructure::persistence::PostgresConversationRepository;
use crate::infrastructure::persistence::redis_presence::RedisPresenceRepository;
use crate::interface::grpc::ConversationGrpcHandler;

const ENV_DATABASE_URL: &str = "FLARE_CONVERSATION_TEST_DATABASE_URL";
const TENANT: &str = "tenant-a";

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
        default_policy: crate::domain::model::ConversationPolicy {
            conflict_resolution: crate::domain::model::ConflictResolutionPolicy::Coexist,
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

/// 基线里给 `table` 建索引的语句（读路径依赖的索引与线上一致）。
fn indexes(baseline: &str, table: &str) -> String {
    baseline
        .lines()
        .filter(|line| line.starts_with("CREATE") && line.contains(&format!(" ON {table}(")))
        .collect::<Vec<_>>()
        .join("\n")
}

async fn isolated_schema(url: &str) -> (PgPool, PgPool, String) {
    let admin = PgPool::connect(url).await.expect("connect test database");
    let schema = format!("conv_receipts_{}", uuid::Uuid::new_v4().simple());
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
        indexes(&baseline, "messages"),
        create_table(&baseline, "conversations"),
        create_table(&baseline, "conversation_participants"),
        indexes(&baseline, "conversation_participants"),
        create_table(&baseline, "user_sync_cursor"),
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

async fn conversation(pool: &PgPool, id: &str, kind: i32) {
    sqlx::query(
        "INSERT INTO conversations (tenant_id, conversation_id, conversation_type, display_name, \
         lifecycle_state) VALUES ($1, $2, $3, $2, 'active')",
    )
    .bind(TENANT)
    .bind(id)
    .bind(kind)
    .execute(pool)
    .await
    .unwrap();
}

/// A member: `hidden` hid the conversation, `quit` left it.
async fn member(
    pool: &PgPool,
    cid: &str,
    user: &str,
    visible_from_seq: i64,
    hidden: bool,
    quit: bool,
) {
    sqlx::query(
        "INSERT INTO conversation_participants (tenant_id, conversation_id, user_id, \
         visible_from_seq, is_deleted, quit_at) VALUES ($1, $2, $3, $4, $5, \
         CASE WHEN $6 THEN CURRENT_TIMESTAMP END)",
    )
    .bind(TENANT)
    .bind(cid)
    .bind(user)
    .bind(visible_from_seq)
    .bind(hidden)
    .bind(quit)
    .execute(pool)
    .await
    .unwrap();
}

/// `count` members `{prefix}{i}` with no read position, in one statement.
async fn many_members(pool: &PgPool, cid: &str, prefix: &str, count: i64) {
    sqlx::query(
        "INSERT INTO conversation_participants (tenant_id, conversation_id, user_id) \
         SELECT $1, $2, $3 || lpad(i::text, 6, '0') FROM generate_series(1, $4) AS i",
    )
    .bind(TENANT)
    .bind(cid)
    .bind(prefix)
    .bind(count)
    .execute(pool)
    .await
    .unwrap();
}

async fn message(pool: &PgPool, cid: &str, id: &str, seq: i64, sender: &str, status: i32) {
    sqlx::query(
        "INSERT INTO messages (tenant_id, server_id, conversation_id, sender_id, seq, status) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(TENANT)
    .bind(id)
    .bind(cid)
    .bind(sender)
    .bind(seq)
    .bind(status)
    .execute(pool)
    .await
    .unwrap();
}

fn user(user: &str) -> Context {
    Context::with_request_id("req-receipts")
        .with_tenant_id(TENANT)
        .with_user_id(user)
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

fn ids(ids: &[&str]) -> Vec<String> {
    ids.iter().map(|id| id.to_string()).collect()
}

async fn summaries(
    handler: &ConversationGrpcHandler,
    caller: &str,
    cid: &str,
    server_msg_ids: Vec<String>,
) -> Result<GetMessageReadSummariesResponse, tonic::Status> {
    let mut request = tonic::Request::new(GetMessageReadSummariesRequest {
        conversation_id: cid.into(),
        server_msg_ids,
    });
    flare_server_core::client::set_context_metadata(&mut request, &user(caller));
    handler
        .get_message_read_summaries(request)
        .await
        .map(tonic::Response::into_inner)
}

async fn readers(
    handler: &ConversationGrpcHandler,
    caller: &str,
    cid: &str,
    server_msg_id: &str,
    state: MessageReaderState,
    cursor: &str,
    limit: i32,
) -> Result<ListMessageReadersResponse, tonic::Status> {
    let mut request = tonic::Request::new(ListMessageReadersRequest {
        conversation_id: cid.into(),
        server_msg_id: server_msg_id.into(),
        state: state as i32,
        cursor: cursor.into(),
        limit,
    });
    flare_server_core::client::set_context_metadata(&mut request, &user(caller));
    handler
        .list_message_readers(request)
        .await
        .map(tonic::Response::into_inner)
}

fn summary(id: &str, read: u32, unread: u32) -> MessageReadSummary {
    MessageReadSummary {
        server_msg_id: id.into(),
        read_count: read,
        unread_count: unread,
        total: read + unread,
    }
}

const SENT: i32 = flare_proto::common::MessageStatus::Sent as i32;
const RECALLED: i32 = flare_proto::common::MessageStatus::Recalled as i32;
const DELETED: i32 = flare_proto::common::MessageStatus::Deleted as i32;

/// The member lookup request (`member_ids`) the Orchestrator's membership check sends, as `ctx`.
async fn lookup(
    handler: &ConversationGrpcHandler,
    ctx: &Context,
    cid: &str,
    key: &str,
    user_ids: &str,
) -> Result<Vec<String>, tonic::Status> {
    use flare_grpc_proto::conversation::conversation_read_service_server::ConversationReadService;
    let mut request = tonic::Request::new(
        flare_grpc_proto::conversation::ListConversationParticipantsRequest {
            conversation_id: cid.into(),
            ext: [(key.to_string(), user_ids.to_string())].into(),
            ..Default::default()
        },
    );
    flare_server_core::client::set_context_metadata(&mut request, ctx);
    handler
        .list_conversation_participants(request)
        .await
        .map(|response| {
            response
                .into_inner()
                .participants
                .into_iter()
                .map(|participant| participant.user_id)
                .collect()
        })
}

fn orchestrator() -> Context {
    Context::with_request_id("req-membership")
        .with_tenant_id(TENANT)
        .with_actor(ActorContext::new("flare-orchestrator").with_type(ActorType::Service))
}

/// The member lookup (the Orchestrator's operator check, the plugin resource bridge's members
/// check) keeps a member who hid the conversation (hiding only takes it off their list, so their
/// read receipts and reactions still go through) and drops one who left.
#[tokio::test]
#[ignore = "requires FLARE_CONVERSATION_TEST_DATABASE_URL pointing at a disposable PostgreSQL"]
async fn the_member_lookup_keeps_who_hid_the_conversation_and_drops_who_left() {
    let url = std::env::var(ENV_DATABASE_URL)
        .unwrap_or_else(|_| panic!("set {ENV_DATABASE_URL} to a disposable PostgreSQL instance"));
    let (admin, pool, schema) = isolated_schema(&url).await;
    let group = generate_group_conversation_id("membership-g1");
    conversation(&pool, &group, 2).await;
    for user in ["alice", "bob", "erin"] {
        member(&pool, &group, user, 0, false, false).await;
    }
    member(&pool, &group, "frank", 0, false, true).await;
    let repo = Arc::new(PostgresConversationRepository::new(
        Arc::new(pool.clone()),
        test_config(),
    ));
    // The real hide path: what "delete conversation" does for one user.
    repo.delete_conversation(&user("erin"), &group, false)
        .await
        .unwrap();
    let handler = handler(repo);

    let asked = "erin,frank,bob,zed";
    assert_eq!(
        lookup(&handler, &orchestrator(), &group, "member_ids", asked)
            .await
            .unwrap(),
        ["bob", "erin"]
    );
    // A user caller must be a member by the same rule: Erin may look, Frank may not.
    assert_eq!(
        lookup(&handler, &user("erin"), &group, "member_ids", "alice")
            .await
            .unwrap(),
        ["alice"]
    );
    let error = lookup(&handler, &user("frank"), &group, "member_ids", "alice")
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::NotFound);
    for bad in ["", "bob,bob", "bob,,erin"] {
        let error = lookup(&handler, &orchestrator(), &group, "member_ids", bad)
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument, "{bad:?}");
    }

    drop(handler);
    drop_schema(admin, pool, &schema).await;
}

/// Unread per member, as stored and as the conversation list recounts it.
async fn unread_of(
    pool: &PgPool,
    repo: &PostgresConversationRepository,
    cid: &str,
    reader: &str,
) -> (i32, i32) {
    let stored: i32 = sqlx::query_scalar(
        "SELECT COALESCE(unread_count, 0) FROM conversation_participants \
         WHERE tenant_id = $1 AND conversation_id = $2 AND user_id = $3",
    )
    .bind(TENANT)
    .bind(cid)
    .bind(reader)
    .fetch_one(pool)
    .await
    .unwrap();
    let listed = repo
        .load_bootstrap(&user(reader), &Default::default(), 0)
        .await
        .unwrap()
        .summaries
        .into_iter()
        .find(|summary| summary.conversation_id == cid)
        .map(|summary| summary.unread_count)
        .expect("the conversation is listed");
    (stored, listed)
}

/// A recalled or deleted message stops counting as unread, by proto `MessageStatus` (recalled 5,
/// deleted 6): the retraction recomputes the members who had not read it (whichever of Writer and
/// this consumer goes first), a repeat changes nothing, a partial read takes only live messages
/// off, the list's recount agrees, and a message that arrives already recalled adds nothing.
#[tokio::test]
#[ignore = "requires FLARE_CONVERSATION_TEST_DATABASE_URL pointing at a disposable PostgreSQL"]
async fn recalled_and_deleted_messages_leave_the_unread_count() {
    let url = std::env::var(ENV_DATABASE_URL)
        .unwrap_or_else(|_| panic!("set {ENV_DATABASE_URL} to a disposable PostgreSQL instance"));
    let (admin, pool, schema) = isolated_schema(&url).await;
    let group = generate_group_conversation_id("unread-g1");
    conversation(&pool, &group, 2).await;
    for user in ["alice", "bob", "carol"] {
        member(&pool, &group, user, 0, false, false).await;
    }
    let repo = Arc::new(PostgresConversationRepository::new(
        Arc::new(pool.clone()),
        test_config(),
    ));
    let system = Context::with_request_id("req-unread").with_tenant_id(TENANT);
    for seq in 1..=4 {
        let id = format!("m{seq}");
        message(&pool, &group, &id, seq, "alice", SENT).await;
        repo.apply_message_event(&system, &group, "alice", seq, SENT)
            .await
            .unwrap();
    }
    assert_eq!(unread_of(&pool, &repo, &group, "bob").await, (4, 4));
    assert_eq!(unread_of(&pool, &repo, &group, "alice").await, (0, 0));

    // m2 recalled: Writer has already stored RECALLED.
    sqlx::query("UPDATE messages SET status = $1 WHERE tenant_id = $2 AND server_id = 'm2'")
        .bind(RECALLED)
        .bind(TENANT)
        .execute(&pool)
        .await
        .unwrap();
    repo.retract_message_unread(&system, &group, "m2")
        .await
        .unwrap();
    assert_eq!(unread_of(&pool, &repo, &group, "bob").await, (3, 3));
    // A redelivered recall changes no row (not even its update time).
    let stamp = || async {
        sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(
            "SELECT updated_at FROM conversation_participants \
             WHERE tenant_id = $1 AND conversation_id = $2 AND user_id = 'carol'",
        )
        .bind(TENANT)
        .bind(&group)
        .fetch_one(&pool)
        .await
        .unwrap()
    };
    let before = stamp().await;
    repo.retract_message_unread(&system, &group, "m2")
        .await
        .unwrap();
    assert_eq!(unread_of(&pool, &repo, &group, "carol").await, (3, 3));
    assert_eq!(stamp().await, before);

    // m3 deleted for everyone, handled before Writer stores DELETED.
    repo.retract_message_unread(&system, &group, "m3")
        .await
        .unwrap();
    assert_eq!(unread_of(&pool, &repo, &group, "bob").await.0, 2);
    sqlx::query("UPDATE messages SET status = $1 WHERE tenant_id = $2 AND server_id = 'm3'")
        .bind(DELETED)
        .bind(TENANT)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(unread_of(&pool, &repo, &group, "bob").await, (2, 2));

    // Bob reads up to m3: of m1, m2 (recalled), m3 (deleted), only m1 was unread.
    repo.mark_as_read(&user("bob"), &group, 3).await.unwrap();
    assert_eq!(unread_of(&pool, &repo, &group, "bob").await, (1, 1));
    // Carol's list recount and stored count agree (m1 and m4).
    assert_eq!(unread_of(&pool, &repo, &group, "carol").await, (2, 2));

    // A message that is already recalled when it arrives adds nothing.
    message(&pool, &group, "m5", 5, "alice", RECALLED).await;
    repo.apply_message_event(&system, &group, "alice", 5, RECALLED)
        .await
        .unwrap();
    assert_eq!(unread_of(&pool, &repo, &group, "carol").await, (2, 2));

    // Unknown messages and the sender's own unread are left alone.
    repo.retract_message_unread(&system, &group, "ghost")
        .await
        .unwrap();
    assert_eq!(unread_of(&pool, &repo, &group, "carol").await, (2, 2));

    drop_schema(admin, pool, &schema).await;
}

/// Above the precise unread threshold nothing is materialized, so a retraction touches nothing
/// (and reads at most threshold + 1 member rows to find that out).
#[tokio::test]
#[ignore = "requires FLARE_CONVERSATION_TEST_DATABASE_URL pointing at a disposable PostgreSQL"]
async fn a_retraction_in_a_large_group_touches_no_member() {
    let url = std::env::var(ENV_DATABASE_URL)
        .unwrap_or_else(|_| panic!("set {ENV_DATABASE_URL} to a disposable PostgreSQL instance"));
    let (admin, pool, schema) = isolated_schema(&url).await;
    let group = generate_group_conversation_id("unread-large");
    conversation(&pool, &group, 2).await;
    for user in ["alice", "bob", "carol"] {
        member(&pool, &group, user, 0, false, false).await;
    }
    message(&pool, &group, "m1", 1, "alice", RECALLED).await;
    sqlx::query(
        "UPDATE conversation_participants SET unread_count = 7 \
         WHERE tenant_id = $1 AND conversation_id = $2",
    )
    .bind(TENANT)
    .bind(&group)
    .execute(&pool)
    .await
    .unwrap();
    let mut config = (*test_config()).clone();
    config.large_conversation_precise_unread_threshold = 2;
    let repo = PostgresConversationRepository::new(Arc::new(pool.clone()), Arc::new(config));
    let system = Context::with_request_id("req-unread").with_tenant_id(TENANT);
    repo.retract_message_unread(&system, &group, "m1")
        .await
        .unwrap();
    let unread: Vec<i32> = sqlx::query_scalar(
        "SELECT unread_count FROM conversation_participants \
         WHERE tenant_id = $1 AND conversation_id = $2 ORDER BY user_id",
    )
    .bind(TENANT)
    .bind(&group)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(unread, [7, 7, 7]);

    drop_schema(admin, pool, &schema).await;
}

#[tokio::test]
#[ignore = "requires FLARE_CONVERSATION_TEST_DATABASE_URL pointing at a disposable PostgreSQL"]
async fn senders_see_who_read_their_group_messages_by_read_watermark() {
    let url = std::env::var(ENV_DATABASE_URL)
        .unwrap_or_else(|_| panic!("set {ENV_DATABASE_URL} to a disposable PostgreSQL instance"));
    let (admin, pool, schema) = isolated_schema(&url).await;
    let group = generate_group_conversation_id("receipts-g1");
    let other = generate_group_conversation_id("receipts-g2");
    conversation(&pool, &group, 2).await;
    conversation(&pool, &other, 2).await;
    member(&pool, &group, "alice", 0, false, false).await;
    member(&pool, &group, "bob", 0, false, false).await;
    member(&pool, &group, "carol", 0, false, false).await;
    // Joined with history hidden up to seq 8.
    member(&pool, &group, "dave", 8, false, false).await;
    // Erin hides the conversation below (still a member); Frank left it (no longer one).
    member(&pool, &group, "erin", 0, false, false).await;
    member(&pool, &group, "frank", 0, false, true).await;
    member(&pool, &other, "alice", 0, false, false).await;
    for (id, seq, sender, status) in [
        ("m5", 5, "alice", SENT),
        ("m8", 8, "alice", SENT),
        ("mb", 9, "bob", SENT),
        ("m10", 10, "alice", SENT),
        ("mr", 11, "alice", RECALLED),
        ("m12", 12, "alice", SENT),
    ] {
        message(&pool, &group, id, seq, sender, status).await;
    }
    message(&pool, &other, "mx", 1, "alice", SENT).await;
    let repo = Arc::new(PostgresConversationRepository::new(
        Arc::new(pool.clone()),
        test_config(),
    ));
    // Read positions through the path read receipts take.
    for (reader, seq) in [("bob", 10), ("carol", 5), ("dave", 12), ("erin", 12)] {
        repo.mark_as_read(&user(reader), &group, seq).await.unwrap();
    }
    // Hiding is the list's business: Erin stays a member, with the read position her receipts
    // gave her (12).
    repo.delete_conversation(&user("erin"), &group, false)
        .await
        .unwrap();
    let handler = handler(repo.clone());

    // Request order is kept; another member's, a recalled, another conversation's and an
    // unknown message are left out.
    let answer = summaries(
        &handler,
        "alice",
        &group,
        ids(&["m10", "mb", "m5", "ghost", "mx", "mr", "m8", "m12"]),
    )
    .await
    .unwrap();
    assert!(answer.available);
    assert_eq!(answer.conversation_id, group);
    assert_eq!(
        answer.summaries,
        [
            // bob 10, dave 12 (sees > 8), erin 12 (hid it, still a member); carol 5 has not.
            summary("m10", 3, 1),
            // bob, carol and erin; dave never sees seq 5.
            summary("m5", 3, 0),
            // dave's floor is 8: seq 8 is hidden from him.
            summary("m8", 2, 1),
            summary("m12", 2, 2),
        ]
    );
    // Bob sees only his own message: of alice (never read), carol (5), dave and erin (12), dave
    // and erin have.
    let answer = summaries(&handler, "bob", &group, ids(&["m10", "mb"]))
        .await
        .unwrap();
    assert_eq!(answer.summaries, [summary("mb", 2, 2)]);
    // Erin, who hid the conversation, still asks about her own messages (she has none here).
    let answer = summaries(&handler, "erin", &group, ids(&["m10"]))
        .await
        .unwrap();
    assert!(answer.available && answer.summaries.is_empty());

    // Readers, page by page, in user id order; both sides partition the members who see it.
    let mut read = Vec::new();
    let mut cursor = String::new();
    loop {
        let page = readers(
            &handler,
            "alice",
            &group,
            "m10",
            MessageReaderState::Read,
            &cursor,
            1,
        )
        .await
        .unwrap();
        assert!(page.available);
        assert_eq!(page.summary, Some(summary("m10", 3, 1)));
        read.extend(page.user_ids);
        if !page.has_more {
            break;
        }
        cursor = page.next_cursor;
    }
    assert_eq!(read, ["bob", "dave", "erin"]);
    let unread = readers(
        &handler,
        "alice",
        &group,
        "m10",
        MessageReaderState::Unread,
        "",
        0,
    )
    .await
    .unwrap();
    assert_eq!(unread.user_ids, ["carol"]);
    assert!(!unread.has_more);

    // Only the sender asks who read a message. Anyone else gets exactly the answer an id naming
    // nothing gets, so a member who joined later (Dave sees nothing up to seq 8) cannot tell
    // whether an id names a message from before he joined.
    let unknown = readers(
        &handler,
        "dave",
        &group,
        "ghost",
        MessageReaderState::Read,
        "",
        0,
    )
    .await
    .unwrap_err();
    assert_eq!(unknown.code(), tonic::Code::NotFound);
    for (caller, id) in [
        ("bob", "m10"),
        ("dave", "m5"),
        ("dave", "m8"),
        ("alice", "mb"),
    ] {
        let refused = readers(
            &handler,
            caller,
            &group,
            id,
            MessageReaderState::Read,
            "",
            0,
        )
        .await
        .unwrap_err();
        assert_eq!(
            (refused.code(), refused.message()),
            (unknown.code(), unknown.message()),
            "{caller} asking about {id}"
        );
    }
    // A recalled message, another conversation's and an unknown one are not found.
    for id in ["mr", "mx", "ghost"] {
        let missing = readers(
            &handler,
            "alice",
            &group,
            id,
            MessageReaderState::Read,
            "",
            0,
        )
        .await
        .unwrap_err();
        assert_eq!(missing.code(), tonic::Code::NotFound, "{id}");
    }

    // Callers who left, or never joined, are answered as for a missing conversation.
    for outsider in ["frank", "zed"] {
        let error = summaries(&handler, outsider, &group, ids(&["m10"]))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::NotFound, "{outsider}");
        let error = readers(
            &handler,
            outsider,
            &group,
            "m10",
            MessageReaderState::Read,
            "",
            0,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::NotFound, "{outsider}");
    }

    // Malformed requests.
    for bad in [
        Vec::new(),
        ids(&["m10", "m10"]),
        ids(&["m10", " "]),
        (0..51).map(|i| format!("m{i}")).collect(),
    ] {
        let error = summaries(&handler, "alice", &group, bad).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
    for (state, limit) in [
        (MessageReaderState::Unspecified, 0),
        (MessageReaderState::Read, 201),
        (MessageReaderState::Read, -1),
    ] {
        let error = readers(&handler, "alice", &group, "m10", state, "", limit)
            .await
            .unwrap_err();
        assert_eq!(
            error.code(),
            tonic::Code::InvalidArgument,
            "{state:?} {limit}"
        );
    }

    // A single chat has no group receipts (the peer's read position is the summary's
    // `peer_read_seq`).
    let single = generate_single_chat_conversation_id("alice", "bob");
    conversation(&pool, &single, 1).await;
    member(&pool, &single, "alice", 0, false, false).await;
    member(&pool, &single, "bob", 0, false, false).await;
    message(&pool, &single, "ms", 1, "alice", SENT).await;
    let answer = summaries(&handler, "alice", &single, ids(&["ms"]))
        .await
        .unwrap();
    assert!(!answer.available);
    assert!(answer.summaries.is_empty());
    let page = readers(
        &handler,
        "alice",
        &single,
        "ms",
        MessageReaderState::Read,
        "",
        0,
    )
    .await
    .unwrap();
    assert!(!page.available);
    assert!(page.summary.is_none() && page.user_ids.is_empty());

    drop(handler);
    drop_schema(admin, pool, &schema).await;
}

/// The member limit and the cost around it: a small group, exactly the limit, one past it, and a
/// 100,000-member group (which reads one row past the limit and stops). Prints the timings.
#[tokio::test]
#[ignore = "requires FLARE_CONVERSATION_TEST_DATABASE_URL pointing at a disposable PostgreSQL"]
async fn the_member_limit_bounds_every_read_and_the_cost_stays_flat() {
    let url = std::env::var(ENV_DATABASE_URL)
        .unwrap_or_else(|_| panic!("set {ENV_DATABASE_URL} to a disposable PostgreSQL instance"));
    let (admin, pool, schema) = isolated_schema(&url).await;
    let limit = crate::domain::model::read_receipt::READ_RECEIPT_MEMBER_LIMIT as i64;
    let batch = crate::domain::model::read_receipt::MAX_READ_SUMMARY_MESSAGES as i64;
    let repo = Arc::new(PostgresConversationRepository::new(
        Arc::new(pool.clone()),
        test_config(),
    ));
    let handler = handler(repo.clone());

    // (name, members besides the sender, available)
    let cases = [
        ("small", 9, true),
        ("at-limit", limit - 1, true),
        ("past-limit", limit, false),
        ("100k", 100_000, false),
    ];
    for (name, others, available) in cases {
        let cid = generate_group_conversation_id(&format!("receipts-{name}"));
        conversation(&pool, &cid, 2).await;
        member(&pool, &cid, "alice", 0, false, false).await;
        many_members(&pool, &cid, "m", others).await;
        let message_ids: Vec<String> = (1..=batch).map(|seq| format!("{name}-{seq}")).collect();
        for (seq, id) in (1..=batch).zip(&message_ids) {
            message(&pool, &cid, id, seq, "alice", SENT).await;
        }
        // Half of the members have read up to the middle of the page.
        sqlx::query(
            "UPDATE conversation_participants SET last_read_seq = $3 \
             WHERE tenant_id = $1 AND conversation_id = $2 AND user_id <> 'alice' \
               AND right(user_id, 1) IN ('0', '2', '4', '6', '8')",
        )
        .bind(TENANT)
        .bind(&cid)
        .bind(batch / 2)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("ANALYZE conversation_participants")
            .execute(&pool)
            .await
            .unwrap();

        // Warm, then time a few calls.
        summaries(&handler, "alice", &cid, message_ids.clone())
            .await
            .unwrap();
        let runs = 5;
        let started = Instant::now();
        let mut answer = None;
        for _ in 0..runs {
            answer = Some(
                summaries(&handler, "alice", &cid, message_ids.clone())
                    .await
                    .unwrap(),
            );
        }
        let per_call = started.elapsed() / runs;
        let answer = answer.unwrap();
        println!(
            "read summaries {name}: {} members, {batch} messages, {per_call:?} per call, available={}",
            others + 1,
            answer.available
        );
        assert_eq!(answer.available, available, "{name}");
        if available {
            assert_eq!(answer.summaries.len(), batch as usize);
            let even = (1..=others).filter(|i| i % 2 == 0).count() as u32;
            let first = &answer.summaries[0];
            assert_eq!(
                (first.read_count, first.total),
                (even, others as u32),
                "{name}"
            );
            let last = answer.summaries.last().unwrap();
            assert_eq!((last.read_count, last.unread_count), (0, others as u32));
        } else {
            assert!(answer.summaries.is_empty());
        }
        // The member read never takes more than one row past the limit.
        let rows = repo
            .list_member_read_positions(&user("alice"), &cid, limit as usize + 1)
            .await
            .unwrap();
        assert_eq!(rows.len() as i64, (others + 1).min(limit + 1), "{name}");
    }

    drop(handler);
    drop_schema(admin, pool, &schema).await;
}

/// One page of the participant list as `ctx`, with the member scope (`ext["members"]`) or the
/// in-seat default the delivery reads use.
async fn list_page(
    handler: &ConversationGrpcHandler,
    ctx: &Context,
    cid: &str,
    members: bool,
) -> Result<(Vec<String>, i32), tonic::Status> {
    use flare_grpc_proto::conversation::conversation_read_service_server::ConversationReadService;
    let mut ext: std::collections::HashMap<String, String> = Default::default();
    if members {
        ext.insert("members".into(), "1".into());
    }
    let mut request = tonic::Request::new(
        flare_grpc_proto::conversation::ListConversationParticipantsRequest {
            conversation_id: cid.into(),
            limit: 50,
            ext,
            ..Default::default()
        },
    );
    flare_server_core::client::set_context_metadata(&mut request, ctx);
    handler
        .list_conversation_participants(request)
        .await
        .map(|response| {
            let page = response.into_inner();
            (
                page.participants.into_iter().map(|p| p.user_id).collect(),
                page.member_count,
            )
        })
}

async fn detail(
    handler: &ConversationGrpcHandler,
    caller: &str,
    cid: &str,
) -> Result<flare_proto::common::ConversationDetail, tonic::Status> {
    use flare_grpc_proto::conversation::conversation_read_service_server::ConversationReadService;
    let mut request = tonic::Request::new(
        flare_grpc_proto::conversation::GetConversationDetailRequest {
            conversation_id: cid.into(),
        },
    );
    flare_server_core::client::set_context_metadata(&mut request, &user(caller));
    handler
        .get_conversation_detail(request)
        .await
        .map(|response| response.into_inner().detail.expect("detail"))
}

/// Hiding a group only takes it off one's own list: the member list (`members` scope), the
/// detail's member count and members, and the list summary's member count keep the member who
/// hid it, and that member may read them; who left is in none of them and may read none. The
/// in-seat default (message fanout, subscriptions, offline push) still leaves the hidden member
/// out.
#[tokio::test]
#[ignore = "requires FLARE_CONVERSATION_TEST_DATABASE_URL pointing at a disposable PostgreSQL"]
async fn member_lists_and_details_keep_who_hid_the_group_and_drop_who_left() {
    let url = std::env::var(ENV_DATABASE_URL)
        .unwrap_or_else(|_| panic!("set {ENV_DATABASE_URL} to a disposable PostgreSQL instance"));
    let (admin, pool, schema) = isolated_schema(&url).await;
    let group = generate_group_conversation_id("roster-g1");
    conversation(&pool, &group, 2).await;
    for user in ["alice", "bob", "erin"] {
        member(&pool, &group, user, 0, false, false).await;
    }
    member(&pool, &group, "frank", 0, false, true).await;
    let repo = Arc::new(PostgresConversationRepository::new(
        Arc::new(pool.clone()),
        test_config(),
    ));
    repo.delete_conversation(&user("erin"), &group, false)
        .await
        .unwrap();
    let handler = handler(repo.clone());

    // The member list: Erin is in it and may read it; Frank is in neither.
    for caller in ["alice", "erin"] {
        let (ids, count) = list_page(&handler, &user(caller), &group, true)
            .await
            .unwrap();
        assert_eq!(ids, ["alice", "bob", "erin"], "{caller}");
        assert_eq!(count, 3, "{caller}");
    }
    let error = list_page(&handler, &user("frank"), &group, true)
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::NotFound);
    // The in-seat default the delivery reads use is unchanged: a service's fanout read and one
    // made with Erin's own forwarded identity (her read receipt's recipients) both leave her out,
    // and her identity is still accepted for the read.
    for ctx in [orchestrator(), user("erin")] {
        assert_eq!(
            list_page(&handler, &ctx, &group, false).await.unwrap().0,
            ["alice", "bob"]
        );
    }
    let error = list_page(&handler, &user("frank"), &group, false)
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::NotFound);

    // The detail: member count and caller check by the member rule.
    for caller in ["alice", "erin"] {
        assert_eq!(
            detail(&handler, caller, &group).await.unwrap().member_count,
            3
        );
    }
    let error = detail(&handler, "frank", &group).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::NotFound);

    // The list summary's member count and preview, as Alice's conversation list shows them.
    let summary = repo
        .load_bootstrap(&user("alice"), &Default::default(), 0)
        .await
        .unwrap()
        .summaries
        .into_iter()
        .find(|summary| summary.conversation_id == group)
        .expect("listed");
    assert_eq!(
        summary.metadata.get("member_count").map(String::as_str),
        Some("3")
    );
    assert_eq!(
        summary
            .member_preview
            .iter()
            .map(|p| p.user_id.as_str())
            .collect::<Vec<_>>(),
        ["alice", "bob", "erin"]
    );

    drop(handler);
    drop_schema(admin, pool, &schema).await;
}

/// The stored read, list and unread state of one participant: (last_read_seq, last_sync_seq,
/// unread_count, is_deleted).
async fn row_state(pool: &PgPool, cid: &str, user: &str) -> (i64, i64, i32, bool) {
    sqlx::query_as(
        "SELECT COALESCE(last_read_seq, 0), COALESCE(last_sync_seq, 0), \
         COALESCE(unread_count, 0), COALESCE(is_deleted, false) \
         FROM conversation_participants \
         WHERE tenant_id = $1 AND conversation_id = $2 AND user_id = $3",
    )
    .bind(TENANT)
    .bind(cid)
    .bind(user)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// `alice` sends `id` at `seq` and the conversation consumer counts it.
async fn send(pool: &PgPool, repo: &PostgresConversationRepository, cid: &str, id: &str, seq: i64) {
    let system = Context::with_request_id("req-send").with_tenant_id(TENANT);
    message(pool, cid, id, seq, "alice", SENT).await;
    repo.apply_message_event(&system, cid, "alice", seq, SENT)
        .await
        .unwrap();
}

/// Hiding a conversation (what deleting a friend or blocking someone does to the side that does
/// it) is not reading it. The peer's view of their own messages does not move: a single chat's
/// `peer_read_seq` and a group's read summaries and readers stay what the hider's read receipts
/// made them. The hider's own unread is cleared, stays cleared when the chat comes back to their
/// list, and only what arrives afterwards counts.
#[tokio::test]
#[ignore = "requires FLARE_CONVERSATION_TEST_DATABASE_URL pointing at a disposable PostgreSQL"]
async fn hiding_a_conversation_is_not_reading_it() {
    let url = std::env::var(ENV_DATABASE_URL)
        .unwrap_or_else(|_| panic!("set {ENV_DATABASE_URL} to a disposable PostgreSQL instance"));
    let (admin, pool, schema) = isolated_schema(&url).await;
    let repo = Arc::new(PostgresConversationRepository::new(
        Arc::new(pool.clone()),
        test_config(),
    ));

    // A single chat: Alice sends five messages, Bob reads two of them.
    let single = generate_single_chat_conversation_id("alice", "bob");
    conversation(&pool, &single, 1).await;
    member(&pool, &single, "alice", 0, false, false).await;
    member(&pool, &single, "bob", 0, false, false).await;
    for seq in 1..=5 {
        send(&pool, &repo, &single, &format!("s{seq}"), seq).await;
    }
    let peer_read_seq = || async {
        repo.load_bootstrap(&user("alice"), &Default::default(), 0)
            .await
            .unwrap()
            .summaries
            .into_iter()
            .find(|summary| summary.conversation_id == single)
            .and_then(|summary| summary.metadata.get("peer_read_seq").cloned())
            .expect("listed with peer_read_seq")
    };
    repo.mark_as_read(&user("bob"), &single, 2).await.unwrap();
    assert_eq!(peer_read_seq().await, "2");
    assert_eq!(row_state(&pool, &single, "bob").await, (2, 0, 3, false));

    // Bob deletes Alice as a friend (or blocks her): his side of the chat is hidden.
    repo.delete_conversation(&user("bob"), &single, false)
        .await
        .unwrap();
    // Alice still sees s1 and s2 read and s3..s5 not: nothing tells her what Bob did.
    assert_eq!(peer_read_seq().await, "2");
    // Bob's read position is untouched; his unread is cleared up to the tail (5).
    assert_eq!(row_state(&pool, &single, "bob").await, (2, 5, 0, true));
    let bobs_list = repo
        .load_bootstrap(&user("bob"), &Default::default(), 0)
        .await
        .unwrap();
    assert!(
        bobs_list
            .summaries
            .iter()
            .all(|summary| summary.conversation_id != single),
        "hidden from his list"
    );

    // The chat comes back to Bob's list: what he cleared does not count as unread again, and his
    // own list shows it as read up to where he cleared it.
    let system = Context::with_request_id("req-readd").with_tenant_id(TENANT);
    repo.manage_participants(
        &system,
        &single,
        &[crate::domain::model::ConversationParticipant {
            user_id: "bob".into(),
            roles: vec![],
            muted: false,
            pinned: false,
            attributes: Default::default(),
            visible_from_seq: 0,
            mention_only: false,
        }],
        &[],
        &[],
    )
    .await
    .unwrap();
    assert_eq!(unread_of(&pool, &repo, &single, "bob").await, (0, 0));
    let own = repo
        .load_bootstrap(&user("bob"), &Default::default(), 0)
        .await
        .unwrap()
        .summaries
        .into_iter()
        .find(|summary| summary.conversation_id == single)
        .expect("back on his list");
    assert_eq!(own.last_read_seq, 5);
    // A cleared message delivered again does not count either.
    repo.apply_message_event(&system, &single, "alice", 4, SENT)
        .await
        .unwrap();
    assert_eq!(unread_of(&pool, &repo, &single, "bob").await, (0, 0));
    // New messages count, and only they: recalling one takes it off, reading up to another takes
    // off what was counted up to it, never the cleared ones.
    for seq in 6..=8 {
        send(&pool, &repo, &single, &format!("s{seq}"), seq).await;
    }
    assert_eq!(unread_of(&pool, &repo, &single, "bob").await, (3, 3));
    sqlx::query("UPDATE messages SET status = $1 WHERE tenant_id = $2 AND server_id = 's6'")
        .bind(RECALLED)
        .bind(TENANT)
        .execute(&pool)
        .await
        .unwrap();
    repo.retract_message_unread(&system, &single, "s6")
        .await
        .unwrap();
    assert_eq!(unread_of(&pool, &repo, &single, "bob").await, (2, 2));
    repo.mark_as_read(&user("bob"), &single, 7).await.unwrap();
    assert_eq!(unread_of(&pool, &repo, &single, "bob").await, (1, 1));
    assert_eq!(peer_read_seq().await, "7");

    // A group: Carol has read the first of Alice's three messages, then removes the group from
    // her list. Alice's read summaries and readers do not change.
    let group = generate_group_conversation_id("hide-g1");
    conversation(&pool, &group, 2).await;
    for who in ["alice", "bob", "carol"] {
        member(&pool, &group, who, 0, false, false).await;
    }
    for seq in 1..=3 {
        send(&pool, &repo, &group, &format!("g{seq}"), seq).await;
    }
    repo.mark_as_read(&user("carol"), &group, 1).await.unwrap();
    let handler = handler(repo.clone());
    let all = ids(&["g1", "g2", "g3"]);
    let before = summaries(&handler, "alice", &group, all.clone())
        .await
        .unwrap()
        .summaries;
    assert_eq!(
        before,
        [
            summary("g1", 1, 1),
            summary("g2", 0, 2),
            summary("g3", 0, 2)
        ]
    );
    repo.delete_conversation(&user("carol"), &group, false)
        .await
        .unwrap();
    assert_eq!(
        summaries(&handler, "alice", &group, all)
            .await
            .unwrap()
            .summaries,
        before
    );
    let unread = readers(
        &handler,
        "alice",
        &group,
        "g2",
        MessageReaderState::Unread,
        "",
        0,
    )
    .await
    .unwrap();
    assert_eq!(unread.user_ids, ["bob", "carol"]);
    assert_eq!(row_state(&pool, &group, "carol").await, (1, 3, 0, true));

    drop(handler);
    drop_schema(admin, pool, &schema).await;
}

/// Muting, pinning and "only mentions" are a member's own list settings. Member lists, member
/// lookups, the member management answer and the list summary's member preview carry them only on
/// the caller's own row; a service reading the member list gets none. The in-seat delivery read
/// (offline push decides by them) still has every member's.
#[tokio::test]
#[ignore = "requires FLARE_CONVERSATION_TEST_DATABASE_URL pointing at a disposable PostgreSQL"]
async fn member_rows_carry_personal_settings_only_for_their_owner() {
    use flare_grpc_proto::conversation::conversation_manage_service_server::ConversationManageService;
    use flare_grpc_proto::conversation::conversation_read_service_server::ConversationReadService;
    let url = std::env::var(ENV_DATABASE_URL)
        .unwrap_or_else(|_| panic!("set {ENV_DATABASE_URL} to a disposable PostgreSQL instance"));
    let (admin, pool, schema) = isolated_schema(&url).await;
    let group = generate_group_conversation_id("settings-g1");
    conversation(&pool, &group, 2).await;
    for who in ["alice", "bob", "carol"] {
        member(&pool, &group, who, 0, false, false).await;
    }
    let repo = Arc::new(PostgresConversationRepository::new(
        Arc::new(pool.clone()),
        test_config(),
    ));
    let set = |muted: bool, pinned: bool, mention_only: bool| {
        crate::domain::model::UpdateConversationUserSettingsPatch {
            conversation_id: group.clone(),
            is_mention_only: Some(mention_only),
            is_pinned: Some(pinned),
            is_muted: Some(muted),
            is_archived: None,
            draft: None,
            base_settings_version: 0,
        }
    };
    repo.update_user_settings(&user("bob"), &set(true, true, true))
        .await
        .unwrap();
    repo.update_user_settings(&user("alice"), &set(true, false, false))
        .await
        .unwrap();
    let handler = handler(repo.clone());
    type Row = (String, bool, bool, bool);
    let rows = |participants: Vec<flare_proto::common::ConversationParticipant>| -> Vec<Row> {
        let mut rows: Vec<Row> = participants
            .into_iter()
            .map(|p| (p.user_id, p.muted, p.pinned, p.mention_only))
            .collect();
        rows.sort();
        rows
    };
    let row = |user: &str, muted: bool, pinned: bool, mention_only: bool| -> Row {
        (user.to_string(), muted, pinned, mention_only)
    };
    let page = |ctx: Context, members: bool| {
        let handler = &handler;
        let group = group.clone();
        async move {
            let mut ext: std::collections::HashMap<String, String> = Default::default();
            if members {
                ext.insert("members".into(), "1".into());
            }
            let mut request = tonic::Request::new(
                flare_grpc_proto::conversation::ListConversationParticipantsRequest {
                    conversation_id: group,
                    limit: 50,
                    ext,
                    ..Default::default()
                },
            );
            flare_server_core::client::set_context_metadata(&mut request, &ctx);
            handler
                .list_conversation_participants(request)
                .await
                .unwrap()
                .into_inner()
                .participants
        }
    };

    // The member list, as each member sees it: only their own row has their settings.
    assert_eq!(
        rows(page(user("alice"), true).await),
        [
            row("alice", true, false, false),
            row("bob", false, false, false),
            row("carol", false, false, false)
        ]
    );
    assert_eq!(
        rows(page(user("bob"), true).await),
        [
            row("alice", false, false, false),
            row("bob", true, true, true),
            row("carol", false, false, false)
        ]
    );
    // A service reading the member list (resource bridge, open platform) gets nobody's.
    assert!(
        rows(page(orchestrator(), true).await)
            .iter()
            .all(|(_, muted, pinned, mention_only)| !muted && !pinned && !mention_only)
    );
    // The in-seat delivery read keeps them: offline push skips Bob by his settings.
    assert_eq!(
        rows(page(orchestrator(), false).await),
        [
            row("alice", true, false, false),
            row("bob", true, true, true),
            row("carol", false, false, false)
        ]
    );

    // The member lookup by id.
    let lookup = |ctx: Context| {
        let handler = &handler;
        let group = group.clone();
        async move {
            let mut request = tonic::Request::new(
                flare_grpc_proto::conversation::ListConversationParticipantsRequest {
                    conversation_id: group,
                    ext: [("member_ids".to_string(), "alice,bob".to_string())].into(),
                    ..Default::default()
                },
            );
            flare_server_core::client::set_context_metadata(&mut request, &ctx);
            handler
                .list_conversation_participants(request)
                .await
                .unwrap()
                .into_inner()
                .participants
        }
    };
    assert_eq!(
        rows(lookup(user("carol")).await),
        [
            row("alice", false, false, false),
            row("bob", false, false, false)
        ]
    );
    assert_eq!(
        rows(lookup(user("bob")).await),
        [
            row("alice", false, false, false),
            row("bob", true, true, true)
        ]
    );

    // The list summary's member preview, in Alice's conversation list.
    let mut request = tonic::Request::new(
        flare_grpc_proto::conversation::ConversationBootstrapRequest::default(),
    );
    flare_server_core::client::set_context_metadata(&mut request, &user("alice"));
    let preview = handler
        .conversation_bootstrap(request)
        .await
        .unwrap()
        .into_inner()
        .conversations
        .into_iter()
        .find(|summary| summary.conversation_id == group)
        .expect("listed")
        .member_preview;
    assert_eq!(
        rows(preview),
        [
            row("alice", true, false, false),
            row("bob", false, false, false),
            row("carol", false, false, false)
        ]
    );

    // The member management answer, to the member who added Dave.
    let mut request =
        tonic::Request::new(flare_grpc_proto::conversation::ManageParticipantsRequest {
            conversation_id: group.clone(),
            to_add: vec![flare_proto::common::ConversationParticipant {
                user_id: "dave".into(),
                ..Default::default()
            }],
            ..Default::default()
        });
    flare_server_core::client::set_context_metadata(&mut request, &user("carol"));
    let answer = handler
        .manage_participants(request)
        .await
        .unwrap()
        .into_inner()
        .participants;
    assert!(
        rows(answer)
            .iter()
            .all(|(_, muted, pinned, mention_only)| !muted && !pinned && !mention_only)
    );
    // The stored settings themselves are untouched.
    assert_eq!(
        rows(page(orchestrator(), false).await)
            .into_iter()
            .find(|(user, ..)| user == "bob"),
        Some(row("bob", true, true, true))
    );

    drop(handler);
    drop_schema(admin, pool, &schema).await;
}
