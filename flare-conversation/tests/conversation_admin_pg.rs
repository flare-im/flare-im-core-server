//! 管理员只读仓储的真库校验：按租户翻页、游标稳定、NULL 时间列、跨租户隔离、成员分页。
//!
//! 需要 `FLARE_CONVERSATION_TEST_DATABASE_URL`（任意可建 schema 的 PostgreSQL）。用例标了
//! `#[ignore]`：默认的 `cargo test` 只记为 ignored，`-- --ignored` 显式运行时变量必须已设置，
//! 否则直接失败，不会把没跑的用例记为通过。表结构直接取自 `migrations/0001_baseline.sql` 与后续迁移，
//! 建在独立 schema 里，跑完整个删掉；不依赖 TimescaleDB。

use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use flare_conversation::domain::repository::ConversationAdminRepository;
use flare_conversation::infrastructure::persistence::PostgresConversationAdminRepository;
use flare_server_core::error::ErrorCode;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const ENV_DATABASE_URL: &str = "FLARE_CONVERSATION_TEST_DATABASE_URL";

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

fn baseline_line(baseline: &str, needle: &str) -> String {
    baseline
        .lines()
        .find(|line| line.contains(needle))
        .unwrap_or_else(|| panic!("{needle} not in baseline"))
        .to_string()
}

async fn isolated_schema(url: &str) -> (PgPool, PgPool, String) {
    let admin = PgPool::connect(url).await.expect("connect test database");
    let schema = format!("conv_admin_{}", uuid::Uuid::new_v4().simple());
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
        baseline_line(&baseline, "idx_conversations_tenant_updated ON"),
        create_table(&baseline, "conversation_participants"),
        baseline_line(&baseline, "idx_conversation_participants_tenant_conv ON"),
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

async fn conversation(
    pool: &PgPool,
    tenant: &str,
    id: &str,
    updated_at: Option<DateTime<Utc>>,
    state: &str,
) {
    sqlx::query(
        "INSERT INTO conversations (tenant_id, conversation_id, conversation_type, display_name, \
         lifecycle_state, attributes, created_at, updated_at) \
         VALUES ($1, $2, 2, $3, $4, '{\"k\": 1}'::jsonb, $5, $6)",
    )
    .bind(tenant)
    .bind(id)
    .bind(format!("name-{id}"))
    .bind(state)
    .bind(at(0))
    .bind(updated_at)
    .execute(pool)
    .await
    .unwrap();
}

async fn member(
    pool: &PgPool,
    tenant: &str,
    conversation: &str,
    user: &str,
    joined_at: Option<DateTime<Utc>>,
    removed: bool,
) {
    sqlx::query(
        "INSERT INTO conversation_participants (tenant_id, conversation_id, user_id, joined_at, \
         nickname, is_deleted, quit_at) VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(tenant)
    .bind(conversation)
    .bind(user)
    .bind(joined_at)
    .bind(format!("nick-{user}"))
    .bind(removed)
    .bind(removed.then(|| at(99)))
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires FLARE_CONVERSATION_TEST_DATABASE_URL pointing at a disposable PostgreSQL"]
async fn admin_repository_pages_by_tenant_and_isolates_other_tenants() {
    let url = std::env::var(ENV_DATABASE_URL)
        .unwrap_or_else(|_| panic!("set {ENV_DATABASE_URL} to a disposable PostgreSQL instance"));
    let (admin, pool, schema) = isolated_schema(&url).await;
    let pool_arc = Arc::new(pool.clone());
    let repo = PostgresConversationAdminRepository::new(pool_arc);

    // tenant-a：c1..c5，其中 c2/c3 同一 updated_at（按 id 倒序决胜），c0 的 updated_at 为 NULL，
    // c9 已删除；tenant-b 有更新的 cb 与同名 c1，都不能出现在 tenant-a 的结果里。
    conversation(&pool, "tenant-a", "c0", None, "active").await;
    conversation(&pool, "tenant-a", "c00", None, "active").await;
    conversation(&pool, "tenant-a", "c1", Some(at(10)), "active").await;
    conversation(&pool, "tenant-a", "c2", Some(at(20)), "active").await;
    conversation(&pool, "tenant-a", "c3", Some(at(20)), "archived").await;
    conversation(&pool, "tenant-a", "c4", Some(at(30)), "active").await;
    conversation(&pool, "tenant-a", "c5", Some(at(40)), "active").await;
    conversation(&pool, "tenant-a", "c9", Some(at(50)), "deleted").await;
    conversation(&pool, "tenant-b", "cb", Some(at(60)), "active").await;
    conversation(&pool, "tenant-b", "c1", Some(at(70)), "active").await;

    // 页大小 1 与 2 各翻一遍：页边界分别落在 NULL 行与同刻并列行上。
    for (page_size, expected_pages) in [(2, 4), (1, 7)] {
        let mut seen = Vec::new();
        let mut cursor: Option<String> = None;
        let mut pages = 0;
        loop {
            let page = repo
                .admin_list_conversations("tenant-a", cursor.as_deref(), page_size, false)
                .await
                .unwrap();
            pages += 1;
            assert!(page.conversations.len() <= page_size as usize);
            seen.extend(
                page.conversations
                    .iter()
                    .map(|c| c.conversation.conversation_id.clone()),
            );
            assert!(
                page.conversations
                    .iter()
                    .all(|c| c.conversation.tenant_id == "tenant-a")
            );
            if !page.has_more {
                assert!(page.next_cursor.is_none());
                break;
            }
            cursor = page.next_cursor;
            assert!(cursor.is_some());
        }
        // NULLS FIRST，然后按 updated_at 倒序、同刻按 id 倒序；deleted 默认不出现。
        assert_eq!(
            seen,
            vec!["c00", "c0", "c5", "c4", "c3", "c2", "c1"],
            "page size {page_size}"
        );
        assert_eq!(pages, expected_pages, "page size {page_size}");
    }

    let with_deleted = repo
        .admin_list_conversations("tenant-a", None, 0, true)
        .await
        .unwrap();
    assert_eq!(with_deleted.conversations.len(), 8);
    assert!(!with_deleted.has_more);

    // 跨租户：同名 c1 读到的是本租户那一行；别的租户独有的会话读不到。
    let c1 = repo
        .admin_get_conversation("tenant-a", "c1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(c1.conversation.updated_at, at(10));
    assert_eq!(c1.conversation.display_name.as_deref(), Some("name-c1"));
    assert_eq!(
        c1.conversation.attributes.get("k").map(String::as_str),
        Some("1")
    );
    assert!(
        repo.admin_get_conversation("tenant-a", "cb")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        repo.admin_list_participants("tenant-a", "cb", None, 10, false)
            .await
            .unwrap()
            .is_none()
    );

    // 成员：6 个有效（其中 2 个 joined_at 为 NULL）+ 1 个已退出；tenant-b 同名会话的成员不串过来。
    for (user, second) in [
        ("u1", Some(1)),
        ("u2", Some(2)),
        ("u3", Some(2)),
        ("u4", Some(3)),
    ] {
        member(&pool, "tenant-a", "c1", user, second.map(at), false).await;
    }
    member(&pool, "tenant-a", "c1", "u0", None, false).await;
    member(&pool, "tenant-a", "c1", "u00", None, false).await;
    member(&pool, "tenant-a", "c1", "gone", Some(at(4)), true).await;
    member(&pool, "tenant-b", "c1", "outsider", Some(at(0)), false).await;

    let count = repo
        .admin_get_conversation("tenant-a", "c1")
        .await
        .unwrap()
        .unwrap()
        .member_count;
    assert_eq!(count, 6);
    let listed = repo
        .admin_list_conversations("tenant-a", None, 50, false)
        .await
        .unwrap();
    let listed_c1 = listed
        .conversations
        .iter()
        .find(|c| c.conversation.conversation_id == "c1")
        .unwrap();
    assert_eq!(listed_c1.member_count, 6);

    for page_size in [2, 1] {
        let mut users = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let page = repo
                .admin_list_participants("tenant-a", "c1", cursor.as_deref(), page_size, false)
                .await
                .unwrap()
                .unwrap();
            // 总数只在首页算，后续页从游标带回，口径不变。
            assert_eq!(page.member_count, 6);
            users.extend(page.participants.iter().map(|p| p.user_id.clone()));
            assert!(
                page.participants.iter().all(|p| {
                    p.attributes.get("nickname") == Some(&format!("nick-{}", p.user_id))
                })
            );
            if !page.has_more {
                break;
            }
            cursor = page.next_cursor;
        }
        // joined_at 升序、同刻按 user_id，NULL 排最后；已退出的成员默认不出现。
        assert_eq!(
            users,
            vec!["u1", "u2", "u3", "u4", "u0", "u00"],
            "page size {page_size}"
        );
    }

    let all = repo
        .admin_list_participants("tenant-a", "c1", None, 0, true)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(all.member_count, 7);
    assert_eq!(all.participants.len(), 7);

    // 损坏的游标报参数错误，不退回首页。
    let error = repo
        .admin_list_conversations("tenant-a", Some("c1:not-a-time:c1"), 2, false)
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some(ErrorCode::InvalidParameter));
    let error = repo
        .admin_list_participants("tenant-a", "c1", Some("k:1:2:3:u1"), 2, false)
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some(ErrorCode::InvalidParameter));

    drop_schema(admin, pool, &schema).await;
}
