//! 表态移除幂等的真库用例（W14-R (c)）。
//!
//! 修复前 `upsert_message_reaction` 的移除分支总是 `count.max(0) - 1`：同一移除事件在事件流写失败后
//! 重投、或从未表态的用户发来移除，都会减掉别人的计数，计数到 0 时把别人的表态整行删掉。现在只有该用户
//! 确实表态过才减；同一表情的变更以 `FOR UPDATE` 串行，并发的移除不会各自从同一个旧值算起。
//!
//! 需要 `FLARE_STORAGE_TEST_DATABASE_URL`，指向已按 `migrations/`（`flare-migrate run`）或 `deploy/init.sql`
//! 建好的库。用例标了 `#[ignore]`：默认的 `cargo test` 只记为 ignored，`-- --ignored` 显式运行时变量必须
//! 已设置，否则直接失败。每次运行用新的消息 ID，结束时（含断言失败时）删掉自己的行。

use std::sync::Arc;

use flare_im_contracts::Ctx;
use flare_im_contracts::utils::Context;
use flare_storage_writer::domain::repository::ArchiveStoreRepository;
use flare_storage_writer::infrastructure::persistence::repository::postgres_store::PostgresMessageStore;
use sqlx::{PgPool, Row};

const ENV_DATABASE_URL: &str = "FLARE_STORAGE_TEST_DATABASE_URL";
const TENANT: &str = "tenant-w14r";
const EMOJI: &str = "thumbsup";

/// 结束时删掉本次运行的行，断言失败（panic）时也删：在单独的线程与运行时里另开连接（测试的运行时
/// 可能正在展开）。
struct Cleanup {
    url: String,
    message_ids: Vec<String>,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        let url = self.url.clone();
        let message_ids = self.message_ids.clone();
        let cleanup = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("cleanup runtime")
                .block_on(async move {
                    let pool = PgPool::connect(&url).await?;
                    sqlx::query(
                        "DELETE FROM message_reactions WHERE tenant_id = $1 AND message_id = ANY($2)",
                    )
                    .bind(TENANT)
                    .bind(message_ids)
                    .execute(&pool)
                    .await
                })
        });
        if let Ok(Err(err)) = cleanup.join() {
            eprintln!("cleanup of {TENANT} reaction rows failed: {err}");
        }
    }
}

/// 该消息该表情的（用户列表，计数）；没有这一行时为 `None`。
async fn reaction_row(pool: &PgPool, message_id: &str) -> Option<(Vec<String>, i32)> {
    sqlx::query(
        "SELECT user_ids, count FROM message_reactions WHERE tenant_id = $1 AND message_id = $2 AND emoji = $3",
    )
    .bind(TENANT)
    .bind(message_id)
    .bind(EMOJI)
    .fetch_optional(pool)
    .await
    .expect("select reaction")
    .map(|row| (row.get("user_ids"), row.get("count")))
}

fn users(ids: &[&str]) -> Vec<String> {
    ids.iter().map(ToString::to_string).collect()
}

#[tokio::test]
#[ignore = "requires FLARE_STORAGE_TEST_DATABASE_URL pointing at a migrated Core database"]
async fn removing_a_reaction_is_idempotent_and_never_takes_away_another_users_reaction() {
    let url = std::env::var(ENV_DATABASE_URL)
        .unwrap_or_else(|_| panic!("set {ENV_DATABASE_URL} to a migrated Core database"));
    let pool = PgPool::connect(&url).await.expect("connect");
    let store = Arc::new(PostgresMessageStore::from_pool(pool.clone()));
    let ctx: Ctx = Arc::new(Context::with_request_id("w14r").with_tenant_id(TENANT));
    let run = uuid::Uuid::new_v4();
    let [m1, m2] = [1, 2].map(|n| format!("srv-w14r-{run}-{n}"));
    let _cleanup = Cleanup {
        url: url.clone(),
        message_ids: vec![m1.clone(), m2.clone()],
    };
    let react = |message_id: String, user_id: &'static str, add: bool| {
        let store = store.clone();
        let ctx = ctx.clone();
        async move {
            store
                .upsert_message_reaction(&ctx, TENANT, &message_id, EMOJI, user_id, add)
                .await
                .expect("reaction applied");
        }
    };

    // u1、u2 表态；重复的加不改。
    react(m1.clone(), "u1", true).await;
    react(m1.clone(), "u2", true).await;
    react(m1.clone(), "u2", true).await;
    assert_eq!(
        reaction_row(&pool, &m1).await,
        Some((users(&["u1", "u2"]), 2))
    );

    // u1 移除：减一。
    react(m1.clone(), "u1", false).await;
    assert_eq!(reaction_row(&pool, &m1).await, Some((users(&["u2"]), 1)));

    // 同一移除的重投（事件流写失败后再投一次）：不改。修复前计数到 0，u2 的表态整行被删。
    react(m1.clone(), "u1", false).await;
    assert_eq!(reaction_row(&pool, &m1).await, Some((users(&["u2"]), 1)));

    // 从未表态的用户移除：不改。
    react(m1.clone(), "u3", false).await;
    assert_eq!(reaction_row(&pool, &m1).await, Some((users(&["u2"]), 1)));

    // 最后一个表态的用户移除：计数到 0，删掉这一行（原有语义）。
    react(m1.clone(), "u2", false).await;
    assert_eq!(reaction_row(&pool, &m1).await, None);
    // 没有这一行时的移除：什么也不写。
    react(m1.clone(), "u2", false).await;
    assert_eq!(reaction_row(&pool, &m1).await, None);

    // 并发：先依次表态，再让每个用户的移除与它的重投同时到达；同一表情的变更串行，最后这一行被删，
    // 中途不会把还在的用户的表态删掉。
    let reactors: [&'static str; 6] = ["a1", "a2", "a3", "a4", "a5", "a6"];
    for user in reactors {
        react(m2.clone(), user, true).await;
    }
    assert_eq!(reaction_row(&pool, &m2).await, Some((users(&reactors), 6)));
    let mut removals = Vec::new();
    for user in reactors {
        for _ in 0..2 {
            removals.push(tokio::spawn(react(m2.clone(), user, false)));
        }
    }
    for removal in removals {
        removal.await.expect("removal task");
    }
    assert_eq!(reaction_row(&pool, &m2).await, None);
}
