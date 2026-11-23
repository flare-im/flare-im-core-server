//! Run against an owned PostgreSQL test instance; all rows live in a temporary schema.
use flare_server_core::context::Context;
use flare_storage_reader::{
    domain::repository::message_storage::MessageStorage,
    infrastructure::persistence::{
        optimized_postgres_store::OptimizedPostgresMessageStorageImpl,
        postgres_base::PostgresBaseStorage,
    },
};
use flare_storage_writer::infrastructure::persistence::repository::operation_store::OperationStore;
use sqlx::{PgPool, Row, postgres::PgConnectOptions};
use std::str::FromStr;
use std::sync::Arc;

#[tokio::test]
#[ignore = "requires FLARE_STORAGE_TEST_DATABASE_URL"]
async fn edits_only_touch_the_authorized_server_identity() {
    let url = std::env::var("FLARE_STORAGE_TEST_DATABASE_URL").expect("owned test database URL");
    let admin = PgPool::connect(&url).await.unwrap();
    let schema = format!("edit_identity_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await
        .unwrap();
    let options = PgConnectOptions::from_str(&url)
        .unwrap()
        .options([("search_path", schema.as_str())]);
    let pool = PgPool::connect_with(options).await.unwrap();
    let run = pool.clone();
    let result = tokio::spawn(async move { check(run).await }).await;
    pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error.into_panic());
    }
}

async fn check(pool: PgPool) {
    sqlx::raw_sql("CREATE TABLE messages(tenant_id TEXT NOT NULL, server_id TEXT NOT NULL, client_msg_id TEXT NOT NULL, content BYTEA NOT NULL, extra JSONB NOT NULL DEFAULT '{}', conversation_id TEXT NOT NULL DEFAULT 'c', PRIMARY KEY(tenant_id, server_id)); CREATE TABLE message_edit_history(tenant_id TEXT NOT NULL, message_id TEXT NOT NULL, edit_version INT NOT NULL, content BYTEA NOT NULL, editor_id TEXT NOT NULL, reason TEXT, show_edited_mark BOOLEAN, PRIMARY KEY(tenant_id, message_id, edit_version));").execute(&pool).await.unwrap();
    sqlx::raw_sql("CREATE TABLE message_operation_history(tenant_id TEXT, conversation_id TEXT, message_id TEXT, operation_type TEXT, seq BIGINT); INSERT INTO messages(tenant_id, server_id, client_msg_id, content) VALUES ('a', 'legacy', 'legacy-client', 'body'::bytea), ('b', 'legacy', 'legacy-client', 'body'::bytea); INSERT INTO message_operation_history VALUES ('a', 'c', 'legacy', 'EVENT_MESSAGE_EDIT', 20), ('a', 'other', 'legacy', 'EVENT_MESSAGE_EDIT', 90), ('b', 'other', 'legacy', 'EVENT_MESSAGE_EDIT', 99);").execute(&pool).await.unwrap();
    sqlx::raw_sql("ALTER TABLE messages ADD COLUMN sender_id TEXT NOT NULL DEFAULT 'alice', ADD COLUMN sender_name TEXT, ADD COLUMN sender_avatar TEXT, ADD COLUMN channel_id TEXT, ADD COLUMN thread_id TEXT, ADD COLUMN source INT NOT NULL DEFAULT 1, ADD COLUMN seq BIGINT NOT NULL DEFAULT 1, ADD COLUMN timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(), ADD COLUMN conversation_type INT NOT NULL DEFAULT 1, ADD COLUMN message_type INT NOT NULL DEFAULT 1, ADD COLUMN status INT NOT NULL DEFAULT 1, ADD COLUMN offline_push_info JSONB, ADD COLUMN extensions JSONB, ADD COLUMN created_at TIMESTAMPTZ DEFAULT NOW(), ADD COLUMN persisted_at TIMESTAMPTZ, ADD COLUMN delivered_at TIMESTAMPTZ;
CREATE TABLE message_reactions(tenant_id TEXT,message_id TEXT,emoji TEXT,user_ids TEXT[],count INT,last_updated TIMESTAMPTZ,PRIMARY KEY(tenant_id,message_id,emoji));
CREATE TABLE message_plugin_states(tenant_id TEXT,server_msg_id TEXT,version BIGINT,values_json JSONB,entity TEXT);
CREATE TABLE pinned_messages(tenant_id TEXT,conversation_id TEXT,message_id TEXT,scope INT,owner_user_id TEXT,expire_at TIMESTAMPTZ);").execute(&pool).await.unwrap();
    let reader = OptimizedPostgresMessageStorageImpl::new(
        PostgresBaseStorage {
            pool: pool.clone(),
            cache: None,
        },
        None,
        None,
    );
    let ctx = Arc::new(Context::with_request_id("edit-identity").with_tenant_id("a"));
    let store = OperationStore::new(pool.clone());
    assert_eq!(
        reader
            .get_message(&ctx, "legacy")
            .await
            .unwrap()
            .unwrap()
            .last_edit_seq,
        20
    );
    store
        .update_message_content("a", "legacy", b"stale", 15, "alice", None, None)
        .await
        .unwrap();
    let legacy: Vec<u8> = sqlx::query_scalar(
        "SELECT content FROM messages WHERE tenant_id='a' AND server_id='legacy'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(legacy, b"body");
    // A sender's string attribute cannot impersonate the server-owned nested JSON clock.
    sqlx::query(
        "UPDATE messages SET extra='{\"serverEdit\":\"{eventSeq:999}\"}' WHERE server_id='legacy'",
    )
    .execute(&pool)
    .await
    .unwrap();
    store
        .update_message_content("b", "legacy", b"accepted", 1, "alice", None, None)
        .await
        .unwrap();
    for reverse in [false, true] {
        let id = format!("target-{reverse}");
        let alias = format!("alias-{reverse}");
        let mut rows = vec![
            (id.clone(), format!("client-{reverse}")),
            (alias.clone(), id.clone()),
        ];
        if reverse {
            rows.reverse();
        }
        for tenant in ["a", "b"] {
            for (server, client) in &rows {
                sqlx::query("INSERT INTO messages(tenant_id, server_id, client_msg_id, content, extra) VALUES ($1, $2, $3, $4, '{}')").bind(tenant).bind(server).bind(client).bind(b"original".as_slice()).execute(&pool).await.unwrap();
            }
        }
        for (seq, content) in [(10, b"first".as_slice()), (20, b"second".as_slice())] {
            store
                .update_message_content("a", &id, content, seq, "alice", None, None)
                .await
                .unwrap();
            let body: Vec<u8> = sqlx::query_scalar(
                "SELECT content FROM messages WHERE tenant_id='a' AND server_id=$1",
            )
            .bind(&id)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(body, content);
            let snapshot = reader.get_message(&ctx, &id).await.unwrap().unwrap();
            assert_eq!(snapshot.server_id, id);
            assert_eq!(snapshot.last_edit_seq, seq);
        }
        store
            .update_message_content("a", &id, b"stale", 10, "alice", None, None)
            .await
            .unwrap();
        let watermark: i64 = sqlx::query_scalar(
            "SELECT (extra->'serverEdit'->>'eventSeq')::bigint FROM messages WHERE tenant_id='a' AND server_id=$1",
        )
        .bind(&id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(watermark, 20);
        let body: Vec<u8> =
            sqlx::query_scalar("SELECT content FROM messages WHERE tenant_id='a' AND server_id=$1")
                .bind(&id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(body, b"second");
        let untouched =
            sqlx::query("SELECT content FROM messages WHERE (tenant_id='b' OR server_id=$1) AND server_id IN ($1,$2)")
                .bind(&alias)
                .bind(&id)
                .fetch_all(&pool)
                .await
                .unwrap();
        assert!(
            untouched
                .iter()
                .all(|row| row.get::<Vec<u8>, _>("content") == b"original")
        );
        let wrong_history: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM message_edit_history WHERE (tenant_id <> 'a' OR message_id=$1) AND message_id IN ($1,$2)",
        )
        .bind(&alias)
        .bind(&id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(wrong_history, 0);
        // A rolling older Writer may leave the typed JSON behind its durable event history.
        sqlx::query("UPDATE messages SET content=$1 WHERE tenant_id='a' AND server_id=$2")
            .bind(b"third".as_slice())
            .bind(&id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO message_operation_history VALUES ('a','c',$1,'EVENT_MESSAGE_EDIT',30)",
        )
        .bind(&id)
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(
            reader
                .get_message(&ctx, &id)
                .await
                .unwrap()
                .unwrap()
                .last_edit_seq,
            30
        );
        store
            .update_message_content("a", &id, b"stale", 25, "alice", None, None)
            .await
            .unwrap();
        let body: Vec<u8> =
            sqlx::query_scalar("SELECT content FROM messages WHERE tenant_id='a' AND server_id=$1")
                .bind(&id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(body, b"third");
    }
    check_reaction_ordering_and_snapshots(&pool, &reader).await;
    pool.close().await;
}

async fn check_reaction_ordering_and_snapshots(
    pool: &PgPool,
    reader: &OptimizedPostgresMessageStorageImpl,
) {
    sqlx::query("INSERT INTO messages(tenant_id,server_id,client_msg_id,content) VALUES ('a','reaction-target','reaction-client','body'::bytea),('b','reaction-target','reaction-client','body'::bytea)").execute(pool).await.unwrap();
    let store = OperationStore::new(pool.clone());
    let ctx = Arc::new(Context::with_request_id("reaction-versions").with_tenant_id("a"));
    store
        .upsert_message_reaction("a", "reaction-target", "👍", "me", true, 10)
        .await
        .unwrap();
    store
        .upsert_message_reaction("a", "reaction-target", "👍", "me", false, 20)
        .await
        .unwrap();
    store
        .upsert_message_reaction("a", "reaction-target", "👍", "me", true, 10)
        .await
        .unwrap();
    let removed = reader
        .get_message(&ctx, "reaction-target")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(removed.extra["reactionsJson"], "[]");
    assert_eq!(removed.reaction_states.len(), 1);
    assert_eq!(removed.reaction_states[0].event_seq, 20);
    assert!(!removed.reaction_states[0].active);
    // A fresh store observes the persisted tombstone; another user's lower seq is independent.
    let store = OperationStore::new(pool.clone());
    store
        .upsert_message_reaction("a", "reaction-target", "👍", "me", false, 40)
        .await
        .unwrap();
    store
        .upsert_message_reaction("a", "reaction-target", "👍", "me", true, 35)
        .await
        .unwrap();
    store
        .upsert_message_reaction("a", "reaction-target", "👍", "other", true, 15)
        .await
        .unwrap();
    let snapshot = reader
        .get_message(&ctx, "reaction-target")
        .await
        .unwrap()
        .unwrap();
    let reactions: serde_json::Value =
        serde_json::from_str(&snapshot.extra["reactionsJson"]).unwrap();
    assert_eq!(reactions[0]["userIds"], serde_json::json!(["other"]));
    let (remove, stale) = tokio::join!(
        store.upsert_message_reaction("a", "reaction-target", "👍", "me", false, 70),
        store.upsert_message_reaction("a", "reaction-target", "👍", "me", true, 60)
    );
    remove.unwrap();
    stale.unwrap();
    let snapshot = reader
        .get_message(&ctx, "reaction-target")
        .await
        .unwrap()
        .unwrap();
    assert!(
        !snapshot
            .reaction_states
            .iter()
            .find(|state| state.user_id == "me")
            .unwrap()
            .active
    );
    store
        .upsert_message_reaction("b", "reaction-target", "👍", "me", true, 1)
        .await
        .unwrap();
    assert!(
        store
            .upsert_message_reaction("a", "missing", "👍", "me", true, 1)
            .await
            .is_err()
    );
    assert!(
        store
            .upsert_message_reaction("a", "reaction-target", "👍", "me", true, 0)
            .await
            .is_err()
    );
}
