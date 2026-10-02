//! `PurgeConversationHistory` 逐 RPC 验签（REL-03 会话管理验签第 ② 步，flare-social
//! `docs/REL03-friendship-epoch.md`）：经 gRPC 处理器的入口，只收 `flare-social-bridge` 的服务签名，
//! 租户取签名里的，没配验证器时 fail closed。
//!
//! 请求故意带非法参数（负的 `before_epoch`）：通过验签的请求在领域层被参数校验拒绝
//! （INVALID_ARGUMENT），不碰数据库；没通过的在验签处就被拒。数据库只用惰性连接池，不连。

use std::sync::Arc;
use std::time::Duration;

use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
use flare_conversation::application::handlers::{
    ConversationCommandHandler, ConversationQueryHandler,
};
use flare_conversation::config::ConversationConfig;
use flare_conversation::domain::model::{
    ConflictResolutionPolicy, ConversationDomainConfig, ConversationPolicy,
};
use flare_conversation::domain::service::DefaultConversationDomainService;
use flare_conversation::infrastructure::persistence::PostgresConversationRepository;
use flare_conversation::infrastructure::persistence::redis_presence::RedisPresenceRepository;
use flare_conversation::interface::grpc::ConversationGrpcHandler;
use flare_grpc_proto::conversation::PurgeConversationHistoryRequest;
use flare_grpc_proto::conversation::conversation_manage_service_server::ConversationManageService;
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, SignedContextVerifier,
};
use sqlx::postgres::PgPoolOptions;

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
        default_policy: ConversationPolicy {
            conflict_resolution: ConflictResolutionPolicy::Coexist,
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

/// 处理器背后是从不连接的惰性连接池（端口 9 上没有数据库）。
fn handler(verifier: Option<SignedContextVerifier>) -> ConversationGrpcHandler {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_millis(300))
        .connect_lazy("postgres://flare@127.0.0.1:9/flare")
        .unwrap();
    let repo = Arc::new(PostgresConversationRepository::new(
        Arc::new(pool),
        test_config(),
    ));
    let presence = Arc::new(RedisPresenceRepository::new(
        Arc::new(redis::Client::open("redis://127.0.0.1:9/").unwrap()),
        test_config(),
    ));
    let domain: Arc<DefaultConversationDomainService> =
        Arc::new(DefaultConversationDomainService::new(
            repo.clone(),
            presence,
            None,
            ConversationDomainConfig::default(),
        ));
    ConversationGrpcHandler::new(
        Arc::new(ConversationCommandHandler::new(domain.clone())),
        Arc::new(ConversationQueryHandler::new(repo, None, domain)),
    )
    .with_purge_verifier(verifier)
}

fn keys(seed: u8) -> (SignedContextSigner, SignedContextVerifier) {
    let key = SigningKey::from_bytes(&[seed; 32]);
    let private = key.to_pkcs8_pem(Default::default()).unwrap();
    let public = key
        .verifying_key()
        .to_public_key_pem(Default::default())
        .unwrap();
    (
        SignedContextSigner::from_private_key(&private).unwrap(),
        SignedContextVerifier::from_public_key(&public).unwrap(),
    )
}

fn sign(signer: &SignedContextSigner, actor_type: SignedActorType, actor: &str) -> String {
    signer
        .sign(&SignedContextIdentity::new(
            actor_type,
            actor,
            "tenant-a",
            "req-purge-signed",
        ))
        .unwrap()
}

/// 与已部署的 Bridge 相同：带调用方元数据，另附（或不附）签名令牌。
fn request(token: Option<&str>) -> tonic::Request<PurgeConversationHistoryRequest> {
    let mut request = tonic::Request::new(PurgeConversationHistoryRequest {
        conversation_id: "single:alice:bob".into(),
        before_ms: 1,
        epoch_attribute: "flare.social.friendship_epoch".into(),
        before_epoch: -1,
    });
    let metadata = request.metadata_mut();
    metadata.insert("x-tenant-id", "tenant-a".parse().unwrap());
    metadata.insert("x-request-id", "req-purge-signed".parse().unwrap());
    if let Some(token) = token {
        metadata.insert("x-flare-ctx", token.parse().unwrap());
    }
    request
}

async fn purge(handler: &ConversationGrpcHandler, token: Option<&str>) -> tonic::Code {
    handler
        .purge_conversation_history(request(token))
        .await
        .expect_err("the request carries an invalid before_epoch")
        .code()
}

/// 无签名（只有调用方元数据）的清除被拒；以前它按元数据里的租户直接执行。
#[tokio::test]
async fn an_unsigned_purge_is_rejected() {
    let (_, verifier) = keys(61);
    assert_eq!(
        purge(&handler(Some(verifier)), None).await,
        tonic::Code::Unauthenticated
    );
}

/// 别的密钥签的、用户或其他服务身份签的都被拒。
#[tokio::test]
async fn a_wrong_key_or_identity_is_rejected() {
    let (signer, verifier) = keys(61);
    let (other_signer, _) = keys(62);
    let handler = handler(Some(verifier));

    let forged = sign(
        &other_signer,
        SignedActorType::Service,
        "flare-social-bridge",
    );
    assert_eq!(
        purge(&handler, Some(&forged)).await,
        tonic::Code::Unauthenticated
    );
    for (actor_type, actor) in [
        (SignedActorType::User, "alice"),
        (SignedActorType::Service, "flare-sync-orchestrator"),
    ] {
        let token = sign(&signer, actor_type, actor);
        assert_eq!(
            purge(&handler, Some(&token)).await,
            tonic::Code::PermissionDenied,
            "{actor_type:?} {actor}"
        );
    }
}

/// `flare-social-bridge` 的服务签名通过验签，请求到达领域层（这里被参数校验拒绝）。
#[tokio::test]
async fn the_bridge_signature_reaches_the_command() {
    let (signer, verifier) = keys(61);
    let bridge = sign(&signer, SignedActorType::Service, "flare-social-bridge");
    assert_eq!(
        purge(&handler(Some(verifier)), Some(&bridge)).await,
        tonic::Code::InvalidArgument
    );
}

/// 没配验证器时 fail closed，带了合法签名也一样。
#[tokio::test]
async fn without_a_verifier_purge_fails_closed() {
    let (signer, _) = keys(61);
    let bridge = sign(&signer, SignedActorType::Service, "flare-social-bridge");
    assert_eq!(
        purge(&handler(None), Some(&bridge)).await,
        tonic::Code::Unavailable
    );
    assert_eq!(purge(&handler(None), None).await, tonic::Code::Unavailable);
}
