//! Production queue decoder and PG archive owner; controlled queue publication, not broker E2E.
use super::*;
use crate::domain::repository::ArchiveStoreRepository;
use ed25519_dalek::{
    SigningKey,
    pkcs8::{EncodePrivateKey, EncodePublicKey},
};
use flare_im_service_kit::audit_effects::write_proof::{HEADER, ProofSigner, ProofVerifier};
use flare_im_service_kit::platform_send::{PLATFORM_API_KID, SYSTEM_MESSAGE_SCOPE};
use flare_proto::common::{MessageContent, SystemContent, message_content::Content};
use flare_server_core::context::Context;
use flare_server_core::mq::consumer::{ContentType, MessageContext};
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, SignedContextVerifier,
};
use prost::Message as _;
use sqlx::{PgPool, postgres::PgConnectOptions};
use std::str::FromStr;

fn keys() -> (ProofSigner, ProofVerifier, Ctx) {
    let key = SigningKey::from_bytes(&[81; 32]);
    let signer =
        SignedContextSigner::from_private_key(&key.to_pkcs8_pem(Default::default()).unwrap())
            .unwrap()
            .with_kid(PLATFORM_API_KID)
            .unwrap();
    let public = key
        .verifying_key()
        .to_public_key_pem(Default::default())
        .unwrap();
    let spec = serde_json::to_string(&std::collections::HashMap::from([(
        PLATFORM_API_KID,
        public,
    )]))
    .unwrap();
    let verifier = SignedContextVerifier::from_key_set(
        flare_server_core::auth::KeySet::from_sources(Some(&spec), None)
            .unwrap()
            .unwrap(),
    );
    let token = signer
        .sign(
            &SignedContextIdentity::new(
                SignedActorType::Service,
                "sa-1",
                "app-a",
                "machine-request",
            )
            .with_roles(vec![SYSTEM_MESSAGE_SCOPE.into()])
            .with_credential_id("AAAAAAAAAAAA"),
        )
        .unwrap();
    let mut request = tonic::Request::new(());
    flare_server_core::set_signed_context_token(&mut request, &token).unwrap();
    let ctx = flare_server_core::grpc::middleware::signed_context::verify_signed_context_metadata(
        request.metadata(),
        &verifier,
    )
    .unwrap();
    let key = SigningKey::from_bytes(&[82; 32]);
    (
        ProofSigner::from_pem(&key.to_pkcs8_pem(Default::default()).unwrap()).unwrap(),
        ProofVerifier::from_pem(
            &key.verifying_key()
                .to_public_key_pem(Default::default())
                .unwrap(),
        )
        .unwrap(),
        ctx,
    )
}
fn message(id: &str) -> flare_proto::common::Message {
    flare_proto::common::Message {
        server_id: id.into(),
        conversation_id: "group:group-a".into(),
        client_msg_id: format!("cli-{id}"),
        sender_id: "sa-1".into(),
        channel_id: "group-a".into(),
        source: 2,
        conversation_type: 2,
        message_type: 10,
        conversation_seq: 1,
        content: Some(MessageContent {
            content: Some(Content::System(SystemContent {
                event_kind: "platform.notice".into(),
                body: "secret body never in audit".into(),
                ..Default::default()
            })),
        }),
        ..Default::default()
    }
}
fn queued(message: &flare_proto::common::Message, token: &str) -> Message {
    let headers = std::collections::HashMap::from([
        ("x-tenant-id".into(), "app-a".into()),
        (HEADER.into(), token.into()),
    ]);
    let envelope = flare_im_contracts::event::mq_envelope_for_main_queue_message_with_headers(
        message,
        vec![],
        headers.clone(),
        false,
    );
    let mut context = MessageContext::new(Arc::new(Context::root()), "message.created".into());
    context.headers = headers;
    Message::new(envelope.encode_to_vec(), ContentType::Protobuf, context)
}

#[test]
fn proof_binds_maps_content_actor_and_app_and_rejects_unsigned_machine_queue_messages() {
    let (signer, verifier, ctx) = keys();
    let mut m = message("m-1");
    m.attributes.insert("z".into(), "2".into());
    m.attributes.insert("a".into(), "1".into());
    let token = signer.stamp(&ctx, &m).unwrap();
    let mut reordered = m.clone();
    reordered.attributes =
        std::collections::HashMap::from([("a".into(), "1".into()), ("z".into(), "2".into())]);
    assert!(verifier.verify(&token, "app-a", &reordered).is_ok());
    let mut tampered = m.clone();
    tampered.sender_id = "other".into();
    assert!(verifier.verify(&token, "app-a", &tampered).is_err());
    let mut tampered = m.clone();
    tampered.server_id = "other".into();
    assert!(verifier.verify(&token, "app-a", &tampered).is_err());
    let mut tampered = m.clone();
    tampered.conversation_seq += 1;
    assert!(verifier.verify(&token, "app-a", &tampered).is_err());
    let mut changed = m.clone();
    changed.content.as_mut().unwrap().content = Some(Content::System(SystemContent {
        event_kind: "platform.notice".into(),
        body: "changed".into(),
        ..Default::default()
    }));
    assert!(verifier.verify(&token, "app-a", &changed).is_err());
    assert!(verifier.verify(&token, "app-b", &m).is_err());
    assert!(MessageCreatedHandler::decode_store_message(&queued(&m, ""), Some(&verifier)).is_err());
    assert!(MessageCreatedHandler::decode_store_message(&queued(&m, &token), None).is_err());
    let decoded = MessageCreatedHandler::decode_store_message(&queued(&m, &token), Some(&verifier))
        .unwrap()
        .unwrap();
    assert!(!decoded.command.metadata.contains_key(HEADER));
    assert!(
        decoded
            .ctx
            .get_data::<flare_im_service_kit::audit_effects::write_proof::VerifiedWrite>()
            .is_some()
    );
}

#[tokio::test]
#[ignore = "requires explicit owned-test FLARE_FUNCTIONAL_PG_ADMIN_URL"]
async fn machine_archive_and_source_evidence_commit_once_or_roll_back_together() {
    let url = std::env::var("FLARE_FUNCTIONAL_PG_ADMIN_URL").unwrap();
    let admin = PgPool::connect(&url).await.unwrap();
    let name = format!("flare_writer_audit_test_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE DATABASE {name}"))
        .execute(&admin)
        .await
        .unwrap();
    let pool = PgPool::connect_with(PgConnectOptions::from_str(&url).unwrap().database(&name))
        .await
        .unwrap();
    let run = pool.clone();
    let result = tokio::spawn(async move {
        check_archive(run).await;
    })
    .await;
    pool.close().await;
    sqlx::query(&format!("DROP DATABASE {name} WITH (FORCE)"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error.into_panic());
    }
}
async fn check_archive(pool: PgPool) {
    let schema = include_str!("../../../../../migrations/0001_baseline.sql");
    let start = schema.find("CREATE TABLE messages (").unwrap();
    let end = schema.find("SELECT create_hypertable('messages'").unwrap();
    sqlx::raw_sql(&schema[start..end])
        .execute(&pool)
        .await
        .unwrap();
    let start = schema.find("CREATE TABLE message_write_ledger (").unwrap();
    let end = start + schema[start..].find("\n);").unwrap() + 3;
    sqlx::raw_sql(&schema[start..end])
        .execute(&pool)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!(
        "../../../../../migrations/0012_20261015_core_audit_outbox.sql"
    ))
    .execute(&pool)
    .await
    .unwrap();
    let store = ArchiveRepo::from_pool(pool.clone());
    let (signer, verifier, ctx) = keys();
    let prepare = |id: &str| {
        let m = message(id);
        let token = signer.stamp(&ctx, &m).unwrap();
        MessageCreatedHandler::decode_store_message(&queued(&m, &token), Some(&verifier))
            .unwrap()
            .unwrap()
    };
    let d = prepare("committed");
    let domain = flare_im_contracts::message::message_from_proto(&message("committed"));
    store.store_archive(&d.ctx, &domain).await.unwrap();
    store.store_archive(&d.ctx, &domain).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM messages")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM core_audit_outbox")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    let event: serde_json::Value = sqlx::query_scalar("SELECT payload FROM core_audit_outbox")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(event["actor_type"], "service");
    assert_eq!(event["actor_id"], "sa-1");
    assert_eq!(event["credential_id"], "AAAAAAAAAAAA");
    assert_eq!(event["request_id"], "machine-request");
    assert_eq!(event["aggregate_id"], "committed");
    assert_eq!(event["action"], "archive_commit");
    assert!(!event.to_string().contains("secret body"));
    sqlx::raw_sql("CREATE FUNCTION reject_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'owned audit fault'; END $$; CREATE TRIGGER reject_audit BEFORE INSERT ON core_audit_outbox FOR EACH ROW EXECUTE FUNCTION reject_audit()").execute(&pool).await.unwrap();
    let d = prepare("rollback");
    let m = flare_im_contracts::message::message_from_proto(&message("rollback"));
    assert!(store.store_archive(&d.ctx, &m).await.is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM messages WHERE server_id='rollback'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM message_write_ledger WHERE server_id='rollback'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    sqlx::raw_sql("DROP TRIGGER reject_audit ON core_audit_outbox; DROP FUNCTION reject_audit()")
        .execute(&pool)
        .await
        .unwrap();
    store
        .store_archive_batch(&d.ctx, &[m.clone(), m])
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM core_audit_outbox")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
}
