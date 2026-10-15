//! Source transaction only: no export worker, object download, or broker delivery.
use super::optimized_postgres_store::OptimizedPostgresMessageStorageImpl;
use super::postgres_base::PostgresBaseStorage;
use crate::domain::{model::MessageExportTaskDraft, repository::MessageStorage};
use ed25519_dalek::{
    SigningKey,
    pkcs8::{EncodePrivateKey, EncodePublicKey},
};
use flare_im_service_kit::audit_effects::durable::StaffAudit;
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, SignedContextVerifier,
};
use sqlx::{PgPool, postgres::PgConnectOptions};
use std::str::FromStr;

fn staff() -> flare_im_contracts::Ctx {
    let key = SigningKey::from_bytes(&[89; 32]);
    let signer =
        SignedContextSigner::from_private_key(&key.to_pkcs8_pem(Default::default()).unwrap())
            .unwrap()
            .with_kid("gateway")
            .unwrap();
    let verifier = SignedContextVerifier::from_public_key(
        &key.verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap(),
    )
    .unwrap()
    .require_staff_kid(["gateway"]);
    let token = signer
        .sign(
            &SignedContextIdentity::new(
                SignedActorType::Staff,
                "employee",
                "app-a",
                "export-request",
            )
            .with_staff_grant(None, Some("core:messages:export".into()), 1),
        )
        .unwrap();
    let mut request = tonic::Request::new(());
    flare_server_core::set_signed_context_token(&mut request, &token).unwrap();
    flare_im_service_kit::staff_context::verify_staff_metadata(
        request.metadata(),
        &verifier,
        "core:messages:export",
    )
    .unwrap()
    .ctx
}

#[tokio::test]
#[ignore = "requires explicit owned-test FLARE_FUNCTIONAL_PG_ADMIN_URL"]
async fn export_intent_and_source_evidence_share_commit_rollback_and_replay() {
    let url = std::env::var("FLARE_FUNCTIONAL_PG_ADMIN_URL").unwrap();
    let admin = PgPool::connect(&url).await.unwrap();
    let name = format!("flare_export_audit_test_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE DATABASE {name}"))
        .execute(&admin)
        .await
        .unwrap();
    let pool = PgPool::connect_with(PgConnectOptions::from_str(&url).unwrap().database(&name))
        .await
        .unwrap();
    let run = pool.clone();
    let result = tokio::spawn(async move {
        check(run).await;
    })
    .await;
    pool.close().await;
    sqlx::query(&format!("DROP DATABASE {name} WITH (FORCE)"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
    if let Err(e) = result {
        std::panic::resume_unwind(e.into_panic());
    }
}

async fn check(pool: PgPool) {
    sqlx::raw_sql(include_str!(
        "../../../../../migrations/0012_20261015_core_audit_outbox.sql"
    ))
    .execute(&pool)
    .await
    .unwrap();
    let baseline = include_str!("../../../../../migrations/0001_baseline.sql");
    let start = baseline
        .find("CREATE TABLE message_export_tasks (")
        .unwrap();
    let end = start + baseline[start..].find("\n);").unwrap() + 3;
    sqlx::raw_sql(&baseline[start..end])
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
    let ctx = staff();
    let draft = MessageExportTaskDraft {
        task_id: "export-a".into(),
        tenant_id: "app-a".into(),
        conversation_id: "group:a".into(),
        start_time: 1,
        end_time: 1000,
        filters: serde_json::json!([]),
        requested_by: Some("employee".into()),
        request_id: "export-request".into(),
        trace_id: "trace".into(),
    };
    let save = |d| {
        storage
            .create_message_export_task(&ctx, d)
            .staff_audit(&ctx, "storage-reader")
    };
    sqlx::raw_sql("CREATE FUNCTION reject_audit() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'audit unavailable'; END$$; CREATE TRIGGER reject_audit BEFORE INSERT ON core_audit_outbox FOR EACH ROW EXECUTE FUNCTION reject_audit();").execute(&pool).await.unwrap();
    assert!(save(draft.clone()).await.is_err());
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM message_export_tasks")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    sqlx::query("DROP TRIGGER reject_audit ON core_audit_outbox")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(save(draft.clone()).await.unwrap(), "export-a");
    assert_eq!(save(draft.clone()).await.unwrap(), "export-a");
    let payloads: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT payload::jsonb FROM core_audit_outbox")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(payloads.len(), 1);
    assert_eq!(payloads[0]["request_id"], "export-request");
    assert_eq!(payloads[0]["actor_id"], "employee");
    assert_eq!(payloads[0]["action"], "enqueue");
    let mut other = draft;
    other.tenant_id = "app-b".into();
    assert!(save(other).await.is_err());
}
