//! `threads` / `thread_participants` 主键含 `tenant_id` 后的真库校验：同一 thread_id 在两个租户里互不可见。
//!
//! 只在设置了 `FLARE_TENANT_TEST_DATABASE_URL`（已用 `deploy/init.sql` 初始化的库）时运行，否则直接通过。

use std::sync::Arc;

use flare_conversation::domain::model::ThreadSortOrder;
use flare_conversation::domain::repository::ThreadRepository;
use flare_conversation::infrastructure::persistence::PostgresThreadRepository;
use flare_server_core::context::Context;
use sqlx::postgres::PgPoolOptions;

const ENV_DATABASE_URL: &str = "FLARE_TENANT_TEST_DATABASE_URL";

#[tokio::test]
async fn threads_are_isolated_per_tenant() {
    let Ok(url) = std::env::var(ENV_DATABASE_URL) else {
        eprintln!("{ENV_DATABASE_URL} not set; skipping");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect tenant test database");
    let repo = PostgresThreadRepository::new(Arc::new(pool.clone()));

    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let conversation = format!("conv-{suffix}");
    let root = format!("root-{suffix}");
    let tenant_a = format!("ta{suffix}");
    let tenant_b = format!("tb{suffix}");
    let ctx_a = Context::with_request_id("req-a").with_tenant_id(&tenant_a);
    let ctx_b = Context::with_request_id("req-b").with_tenant_id(&tenant_b);
    let ctx_default = Context::with_request_id("req-0");

    // 同一 thread_id 在两个租户各建一条。
    let id_a = repo
        .create_thread(&ctx_a, &conversation, &root, Some("a"), "creator-a")
        .await
        .unwrap();
    let id_b = repo
        .create_thread(&ctx_b, &conversation, &root, Some("b"), "creator-b")
        .await
        .unwrap();
    assert_eq!(id_a, root);
    assert_eq!(id_b, root);

    let a = repo
        .get_thread(&ctx_a, &root)
        .await
        .unwrap()
        .expect("tenant a sees its thread");
    let b = repo
        .get_thread(&ctx_b, &root)
        .await
        .unwrap()
        .expect("tenant b sees its thread");
    assert_eq!(a.title.as_deref(), Some("a"));
    assert_eq!(b.title.as_deref(), Some("b"));
    assert!(
        repo.get_thread(&ctx_default, &root)
            .await
            .unwrap()
            .is_none(),
        "default tenant \"0\" must not see other tenants' threads"
    );

    // 回复与参与者只落在自己的租户。
    repo.increment_reply_count(&ctx_a, &root, "reply-1", "user-x")
        .await
        .unwrap();
    let a = repo.get_thread(&ctx_a, &root).await.unwrap().unwrap();
    let b = repo.get_thread(&ctx_b, &root).await.unwrap().unwrap();
    assert_eq!(a.reply_count, 1);
    assert_eq!(a.participant_count, 2);
    assert_eq!(b.reply_count, 0);
    assert_eq!(b.participant_count, 1);
    assert_eq!(
        repo.get_participants(&ctx_b, &root).await.unwrap(),
        vec!["creator-b".to_string()]
    );

    let (listed_a, total_a) = repo
        .list_threads(
            &ctx_a,
            &conversation,
            10,
            0,
            true,
            ThreadSortOrder::UpdatedDesc,
        )
        .await
        .unwrap();
    assert_eq!(total_a, 1);
    assert_eq!(listed_a.len(), 1);

    // 更新 / 删除同样按租户定位。
    repo.update_thread(&ctx_a, &root, Some("a2"), Some(true), None, None)
        .await
        .unwrap();
    assert_eq!(
        repo.get_thread(&ctx_b, &root)
            .await
            .unwrap()
            .unwrap()
            .title
            .as_deref(),
        Some("b")
    );
    repo.delete_thread(&ctx_a, &root).await.unwrap();
    assert!(repo.get_thread(&ctx_a, &root).await.unwrap().is_none());
    assert!(repo.get_thread(&ctx_b, &root).await.unwrap().is_some());
    repo.delete_thread(&ctx_b, &root).await.unwrap();

    for tenant in [&tenant_a, &tenant_b] {
        sqlx::query("DELETE FROM thread_participants WHERE tenant_id = $1")
            .bind(tenant)
            .execute(&pool)
            .await
            .unwrap();
    }
}
