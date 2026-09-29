//! Opt-in real PostgreSQL regression. Only connection-local TEMP tables are used.
use super::*;
use flare_server_core::context::Context;

#[tokio::test]
#[ignore = "requires FLARE_MEDIA_TEST_DATABASE_URL pointing at disposable PostgreSQL"]
async fn postgres_media_owner_migration_and_tenant_cleanup() {
    let url = std::env::var("FLARE_MEDIA_TEST_DATABASE_URL")
        .expect("set FLARE_MEDIA_TEST_DATABASE_URL to a disposable PostgreSQL instance");
    // One connection keeps all repository operations in this session's pg_temp schema.
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    let source = include_str!("../../../../deploy/init.sql");
    let schema = source
        .split("CREATE TABLE media_assets (")
        .nth(1)
        .unwrap()
        .split("DROP TABLE IF EXISTS ack_archive_records")
        .next()
        .unwrap();
    let schema = format!("CREATE TABLE media_assets ({schema}")
        .replace(
            "CREATE TABLE media_assets",
            "CREATE TEMPORARY TABLE media_assets",
        )
        .replace(
            "CREATE TABLE media_references",
            "CREATE TEMPORARY TABLE media_references",
        );
    // Model an old deployment: the migration must add owner_id without trusting metadata.
    let schema = schema
        .lines()
        .filter(|line| {
            *line != "    owner_id TEXT,"
                && !line.starts_with("COMMENT ON COLUMN media_assets.owner_id")
        })
        .collect::<Vec<_>>()
        .join("\n");
    sqlx::raw_sql(&schema).execute(&pool).await.unwrap();
    let temporary: bool = sqlx::query_scalar(
        "SELECT relpersistence = 't' FROM pg_class WHERE oid = 'media_assets'::regclass",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        temporary,
        "never run this test against durable media tables"
    );
    sqlx::query("INSERT INTO media_assets (tenant_id,file_id,file_name,mime_type,file_size,url,metadata) VALUES ('a','legacy','legacy','text/plain',1,'', '{\"owner_id\":\"untrusted\"}'::jsonb)")
        .execute(&pool).await.unwrap();
    let patch = include_str!("../../../../deploy/patches/20260929_media_authenticated_owner.sql");
    sqlx::raw_sql(patch).execute(&pool).await.unwrap();
    sqlx::raw_sql(patch).execute(&pool).await.unwrap();
    let store = PostgresMetadataStore {
        pool: Arc::new(pool),
    };
    let a = Context::root().with_tenant_id("a").with_user_id("owner-a");
    let b = Context::root().with_tenant_id("b").with_user_id("owner-b");
    assert_eq!(
        store
            .load_metadata(&a, "legacy")
            .await
            .unwrap()
            .unwrap()
            .owner_id,
        None
    );
    for (ctx, id) in [(&a, "same-id"), (&a, "second"), (&b, "same-id")] {
        store
            .save_metadata(
                ctx,
                &MediaFileMetadata {
                    owner_id: ctx.user_id().map(str::to_string),
                    file_id: id.to_string(),
                    file_name: "fixture.txt".to_string(),
                    mime_type: "text/plain".to_string(),
                    file_size: 1,
                    status: MediaAssetStatus::Pending,
                    grace_expires_at: Some(Utc::now() - chrono::Duration::seconds(10)),
                    metadata: HashMap::from([
                        ("tenant_id".to_string(), "spoofed".to_string()),
                        ("media_lifecycle_scope".to_string(), "message".to_string()),
                    ]),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }
    let mut asset_a = store.load_metadata(&a, "same-id").await.unwrap().unwrap();
    assert_eq!(asset_a.owner_id.as_deref(), Some("owner-a"));
    assert_eq!(
        store
            .load_metadata(&b, "same-id")
            .await
            .unwrap()
            .unwrap()
            .owner_id
            .as_deref(),
        Some("owner-b")
    );
    asset_a.owner_id = Some("replacement-must-not-change-owner".to_string());
    store.save_metadata(&a, &asset_a).await.unwrap();
    assert_eq!(
        store
            .load_metadata(&a, "same-id")
            .await
            .unwrap()
            .unwrap()
            .owner_id
            .as_deref(),
        Some("owner-a")
    );
    assert!(
        store
            .load_metadata(&Context::root(), "same-id")
            .await
            .is_err()
    );
    let selected = store.list_orphaned_assets(&a, Utc::now(), 1).await.unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].owner_id.as_deref(), Some("owner-a"));
    assert_eq!(
        store
            .list_orphaned_assets(&a, Utc::now(), 100)
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        store
            .list_orphaned_assets(&b, Utc::now(), 100)
            .await
            .unwrap()
            .len(),
        1
    );
    store.delete_metadata(&a, "same-id").await.unwrap();
    assert!(store.load_metadata(&a, "same-id").await.unwrap().is_none());
    assert!(store.load_metadata(&b, "same-id").await.unwrap().is_some());
    store.pool.close().await; // PostgreSQL drops only this connection's temporary tables.
}
