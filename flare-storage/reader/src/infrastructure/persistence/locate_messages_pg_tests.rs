//! Locating a batch of messages on PostgreSQL (`MessageStorage::locate_messages`, behind
//! `StorageMessageLookupService.LocateMessages`): one lookup answers where each stored message is,
//! by server id only and within the caller's tenant. Needs `FLARE_STORAGE_TEST_DATABASE_URL`
//! naming a disposable `flare_c2_*` database (`deploy/init.sql` or migrations 0001..); it writes
//! and then deletes rows of two random tenants only.
use std::sync::Arc;

use sqlx::PgPool;

use super::optimized_postgres_store::OptimizedPostgresMessageStorageImpl;
use super::postgres_base::PostgresBaseStorage;
use crate::domain::repository::MessageStorage;

const ENV_DATABASE_URL: &str = "FLARE_STORAGE_TEST_DATABASE_URL";

async fn insert(pool: &PgPool, tenant: &str, conversation: &str, server_id: &str, seq: i64) {
    sqlx::query(
        "INSERT INTO messages (tenant_id, server_id, conversation_id, client_msg_id, sender_id, \
         seq, conversation_type, message_type, content) \
         VALUES ($1, $2, $3, $4, 'alice', $5, 2, 1, ''::bytea)",
    )
    .bind(tenant)
    .bind(server_id)
    .bind(conversation)
    .bind(format!("client-{server_id}"))
    .bind(seq)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "needs FLARE_STORAGE_TEST_DATABASE_URL (a disposable flare_c2_* database from deploy/init.sql or migrations)"]
async fn a_batch_is_located_in_one_read_by_server_id_within_the_tenant() {
    let url = std::env::var(ENV_DATABASE_URL).expect(ENV_DATABASE_URL);
    let database = url.rsplit('/').next().unwrap_or_default();
    assert!(
        database.starts_with("flare_c2_"),
        "refusing a database that is not a disposable flare_c2_* one"
    );
    let pool = PgPool::connect(&url).await.unwrap();
    let suffix = &uuid::Uuid::new_v4().simple().to_string()[..12];
    let tenant = format!("loc{suffix}");
    let other_tenant = format!("lox{suffix}");
    insert(&pool, &tenant, "2-here", "m-here", 1).await;
    insert(&pool, &tenant, "2-there", "m-there", 1).await;
    insert(&pool, &other_tenant, "2-here", "m-foreign", 1).await;

    let storage = OptimizedPostgresMessageStorageImpl::new(
        PostgresBaseStorage {
            pool: pool.clone(),
            cache: None,
        },
        None,
        None,
    );
    let ctx: flare_im_contracts::Ctx =
        Arc::new(flare_server_core::Context::with_request_id("locate").with_tenant_id(&tenant));
    let ids = |ids: &[&str]| ids.iter().map(|id| id.to_string()).collect::<Vec<_>>();
    let mut located = storage
        .locate_messages(
            &ctx,
            &ids(&["m-here", "m-there", "m-foreign", "client-m-here", "ghost"]),
        )
        .await
        .unwrap();
    located.sort();
    // Another tenant's message, a client id and an unknown id are absent.
    assert_eq!(
        located,
        [
            ("m-here".to_string(), "2-here".to_string()),
            ("m-there".to_string(), "2-there".to_string()),
        ]
    );
    assert!(storage.locate_messages(&ctx, &[]).await.unwrap().is_empty());

    for tenant in [&tenant, &other_tenant] {
        sqlx::query("DELETE FROM messages WHERE tenant_id = $1")
            .bind(tenant)
            .execute(&pool)
            .await
            .unwrap();
    }
    pool.close().await;
}
