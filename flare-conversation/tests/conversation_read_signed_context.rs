//! 会话读接口的身份边界（W10-D）：经 `signed_read_service` 起一个真的 gRPC 服务，带签名的
//! `x-flare-ctx` 决定租户与身份，与签名不一致的裸 `x-tenant-id`、`x-user-id` 被拒；打开
//! `FLARE_REQUIRE_SIGNED_READ_CTX`（边界要求签名）时未签名调用被拒，关掉时旧调用方照旧可用。
//!
//! 以前读接口只看调用方自报的元数据：内网里任何能连到端口的进程带上别人的 `x-user-id` 就能读。
//!
//! 请求用空的 `conversation_id`：通过边界的请求在处理器里先按身份取用户、再被参数校验拒绝，
//! 不碰数据库（只有惰性连接池，不连）。

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
use flare_conversation::interface::grpc::{ConversationGrpcHandler, signed_read_service};
use flare_grpc_proto::conversation::GetConversationDetailRequest;
use flare_grpc_proto::conversation::conversation_read_service_client::ConversationReadServiceClient;
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, SignedContextVerifier,
    SignedFirstContextLayer,
};
use sqlx::postgres::PgPoolOptions;
use tonic::Code;
use tonic::transport::server::TcpIncoming;

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
fn handler() -> ConversationGrpcHandler {
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
            "req-read-signed",
        ))
        .unwrap()
}

/// 起一个只挂会话读接口的服务（与 `service/mod.rs` 同一个装配函数）。
async fn serve(
    layer: SignedFirstContextLayer,
) -> ConversationReadServiceClient<tonic::transport::Channel> {
    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let port = incoming.local_addr().unwrap().port();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(signed_read_service(handler(), &layer))
            .serve_with_incoming(incoming),
    );
    ConversationReadServiceClient::connect(format!("http://127.0.0.1:{port}"))
        .await
        .unwrap()
}

async fn detail(
    client: &mut ConversationReadServiceClient<tonic::transport::Channel>,
    metadata: &[(&'static str, &str)],
) -> tonic::Status {
    let mut request = tonic::Request::new(GetConversationDetailRequest {
        conversation_id: String::new(),
    });
    for (key, value) in metadata {
        request.metadata_mut().append(*key, value.parse().unwrap());
    }
    client
        .get_conversation_detail(request)
        .await
        .expect_err("the request has no conversation id")
}

const UNSIGNED_VICTIM: [(&str, &str); 2] = [("x-tenant-id", "tenant-a"), ("x-user-id", "alice")];

/// 要求签名时：只带元数据（冒充 alice）的调用、别的密钥签的、匿名身份都被拒，到不了处理器。
#[tokio::test]
async fn unsigned_and_forged_reads_are_refused_when_signing_is_required() {
    let (signer, verifier) = keys(81);
    let (other_signer, _) = keys(82);
    let mut client = serve(SignedFirstContextLayer::new(Some(verifier)).require_signed(true)).await;

    let status = detail(&mut client, &UNSIGNED_VICTIM).await;
    assert_eq!(status.code(), Code::Unauthenticated, "{status:?}");
    assert_eq!(status.message(), "signed context required");

    let forged = sign(&other_signer, SignedActorType::User, "alice");
    let status = detail(&mut client, &[("x-flare-ctx", &forged)]).await;
    assert_eq!(status.code(), Code::Unauthenticated, "{status:?}");

    let guest = sign(&signer, SignedActorType::Guest, "anonymous");
    let status = detail(&mut client, &[("x-flare-ctx", &guest)]).await;
    assert_eq!(status.code(), Code::PermissionDenied, "{status:?}");
}

/// 签名与元数据不一致（签的是 mallory，元数据说是 alice 或别的租户）一律拒绝。
#[tokio::test]
async fn bare_identity_that_disagrees_with_the_signature_is_refused() {
    let (signer, verifier) = keys(81);
    for require in [true, false] {
        let mut client =
            serve(SignedFirstContextLayer::new(Some(verifier.clone())).require_signed(require))
                .await;
        let mallory = sign(&signer, SignedActorType::User, "mallory");
        for (name, value) in [
            ("x-user-id", "alice"),
            ("x-tenant-id", "tenant-b"),
            ("x-actor-type", "2"),
        ] {
            let status = detail(&mut client, &[("x-flare-ctx", &mallory), (name, value)]).await;
            assert_eq!(
                status.code(),
                Code::PermissionDenied,
                "require={require} {name}={value}: {status:?}"
            );
        }
    }
}

/// 正确签名通过边界，处理器拿到的是签名里的身份：用户签名过了取用户这一步、停在参数校验；
/// 服务签名没有用户，同一请求停在取用户。与签名一致的元数据不算冲突。
#[tokio::test]
async fn a_valid_signature_reaches_the_handler_with_the_signed_identity() {
    let (signer, verifier) = keys(81);
    let mut client = serve(SignedFirstContextLayer::new(Some(verifier)).require_signed(true)).await;

    let alice = sign(&signer, SignedActorType::User, "alice");
    for metadata in [
        vec![("x-flare-ctx", alice.as_str())],
        vec![
            ("x-flare-ctx", alice.as_str()),
            ("x-tenant-id", "tenant-a"),
            ("x-user-id", "alice"),
        ],
    ] {
        let status = detail(&mut client, &metadata).await;
        assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");
        assert!(
            status.message().contains("conversation_id is required"),
            "{status:?}"
        );
    }

    // 处理器自己取用户时才拒绝（不是边界的「缺签名」）：签名里没有用户，元数据也补不进来。
    let service = sign(&signer, SignedActorType::Service, "flare-orchestrator");
    let status = detail(&mut client, &[("x-flare-ctx", &service)]).await;
    assert_eq!(status.message(), "user_id is required", "{status:?}");
}

/// 开关关掉（仍有旧调用方）时，只带元数据的调用照旧按元数据取身份。
#[tokio::test]
async fn unsigned_reads_still_work_while_the_switch_is_off() {
    let (_, verifier) = keys(81);
    let mut client = serve(SignedFirstContextLayer::new(Some(verifier))).await;
    let status = detail(&mut client, &UNSIGNED_VICTIM).await;
    assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");
    assert!(
        status.message().contains("conversation_id is required"),
        "{status:?}"
    );
}
