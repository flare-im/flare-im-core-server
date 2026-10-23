//! The plugin card copy mark on PostgreSQL: bindings and the projection read from the Core
//! schema (`deploy/init.sql` or migrations 0001..), through the query handler that every gRPC read
//! uses. Needs `FLARE_STORAGE_TEST_DATABASE_URL` naming a disposable `flare_c2_*` database; it
//! writes and then deletes rows of one random tenant only.
use std::sync::Arc;

use flare_proto::common::MessageContent;
use prost::Message as _;
use sqlx::PgPool;

use super::optimized_postgres_store::OptimizedPostgresMessageStorageImpl;
use super::postgres_base::PostgresBaseStorage;
use crate::application::handlers::MessageStorageQueryHandler;
use crate::application::queries::{GetMessageQuery, QueryMessagesBySeqQuery};
use crate::domain::model::plugin_card::PLUGIN_CARD_UNBOUND_KEY;
use crate::domain::model::plugin_card::tests::{card, custom, poll_manifest, snapshot, text};

const ENV_DATABASE_URL: &str = "FLARE_STORAGE_TEST_DATABASE_URL";

async fn insert_message(
    pool: &PgPool,
    tenant: &str,
    conversation: &str,
    seq: i64,
    message: &flare_im_contracts::message::Message,
    extra: serde_json::Value,
) {
    let content = message
        .content
        .as_ref()
        .map(MessageContent::encode_to_vec)
        .unwrap_or_default();
    sqlx::query(
        "INSERT INTO messages (tenant_id, server_id, conversation_id, client_msg_id, sender_id, \
         seq, conversation_type, message_type, content, extra) \
         VALUES ($1, $2, $3, $2, 'alice', $4, 2, 1, $5, $6)",
    )
    .bind(tenant)
    .bind(&message.server_id)
    .bind(conversation)
    .bind(seq)
    .bind(content)
    .bind(extra)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "needs FLARE_STORAGE_TEST_DATABASE_URL (a disposable flare_c2_* database from deploy/init.sql or migrations)"]
async fn reads_mark_the_copies_of_a_bound_card_and_only_them() {
    let url = std::env::var(ENV_DATABASE_URL).expect(ENV_DATABASE_URL);
    let database = url.rsplit('/').next().unwrap_or_default();
    assert!(
        database.starts_with("flare_c2_"),
        "refusing a database that is not a disposable flare_c2_* one"
    );
    let pool = PgPool::connect(&url).await.unwrap();
    let tenant = format!("pcm{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
    let conversation = format!("2-{tenant}");
    let now = chrono::Utc::now().timestamp_millis();
    let projection = snapshot(&tenant, now, poll_manifest());
    sqlx::query(
        "INSERT INTO plugin_authorization_snapshots (app_id, version, snapshot, valid_until_ms) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(&tenant)
    .bind(projection.version as i64)
    .bind(serde_json::to_value(&projection).unwrap())
    .bind(projection.valid_until_ms)
    .execute(&pool)
    .await
    .unwrap();
    let preset = serde_json::json!({ PLUGIN_CARD_UNBOUND_KEY: "true" });
    insert_message(
        &pool,
        &tenant,
        &conversation,
        1,
        &card("m-bound", "example.poll", "poll-1"),
        preset.clone(),
    )
    .await;
    insert_message(
        &pool,
        &tenant,
        &conversation,
        2,
        &card("m-copy", "example.poll", "poll-1"),
        serde_json::json!({}),
    )
    .await;
    insert_message(
        &pool,
        &tenant,
        &conversation,
        3,
        &custom("m-custom", "flare.plugin.message.v1"),
        serde_json::json!({}),
    )
    .await;
    insert_message(&pool, &tenant, &conversation, 4, &text("m-text"), preset).await;
    sqlx::query(
        "INSERT INTO plugin_entity_bindings \
         (tenant_id, plugin_id, entity, server_msg_id, conversation_id, contribution_id) \
         VALUES ($1, 'example.poll', 'poll-1', 'm-bound', $2, 'example.poll.card')",
    )
    .bind(&tenant)
    .bind(&conversation)
    .execute(&pool)
    .await
    .unwrap();

    let storage = OptimizedPostgresMessageStorageImpl::new(
        PostgresBaseStorage {
            pool: pool.clone(),
            cache: None,
        },
        None,
        None,
    );
    let handler = MessageStorageQueryHandler::new(Arc::new(storage));
    let ctx: flare_im_contracts::Ctx = Arc::new(
        flare_server_core::Context::with_request_id("pcm-read")
            .with_tenant_id(&tenant)
            .with_user_id("alice"),
    );
    let read_conversation = conversation.clone();
    let outcome = async move {
        let conversation = read_conversation;
        let (page, _) = handler
            .handle_query_messages_by_seq(
                &ctx,
                QueryMessagesBySeqQuery {
                    conversation_id: conversation.clone(),
                    after_seq: 0,
                    before_seq: None,
                    limit: 10,
                    user_id: None,
                    include_burned_placeholder: false,
                },
            )
            .await
            .unwrap();
        let marks: Vec<(String, Option<String>)> = page
            .iter()
            .map(|m| {
                (
                    m.server_id.clone(),
                    m.extra.get(PLUGIN_CARD_UNBOUND_KEY).cloned(),
                )
            })
            .collect();
        assert_eq!(
            marks,
            vec![
                ("m-bound".into(), None),
                ("m-copy".into(), Some("true".into())),
                ("m-custom".into(), Some("true".into())),
                ("m-text".into(), None),
            ]
        );
        let windows = handler
            .handle_query_conversations_message_windows(
                &ctx,
                &[(conversation.clone(), 0)],
                None,
                10,
                true,
                false,
            )
            .await
            .unwrap();
        let copy = windows[0]
            .1
            .iter()
            .find(|m| m.server_id == "m-copy")
            .unwrap();
        assert_eq!(
            copy.extra.get(PLUGIN_CARD_UNBOUND_KEY).map(String::as_str),
            Some("true")
        );
        let bound = handler
            .handle_get_message(
                &ctx,
                GetMessageQuery {
                    message_id: "m-bound".into(),
                },
            )
            .await
            .unwrap()
            .unwrap();
        assert!(!bound.extra.contains_key(PLUGIN_CARD_UNBOUND_KEY));
    };
    let result = tokio::spawn(outcome).await;
    for table in [
        "DELETE FROM messages WHERE tenant_id = $1",
        "DELETE FROM plugin_entity_bindings WHERE tenant_id = $1",
        "DELETE FROM plugin_authorization_snapshots WHERE app_id = $1",
    ] {
        sqlx::query(table)
            .bind(&tenant)
            .execute(&pool)
            .await
            .unwrap();
    }
    pool.close().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error.into_panic());
    }
}
