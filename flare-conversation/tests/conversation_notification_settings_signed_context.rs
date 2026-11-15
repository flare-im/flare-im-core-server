//! 「调用者本人的用户级通知设置」的身份边界：经 `signed_notification_settings_service` 起一个真的
//! gRPC 服务。只放行用户身份：服务、租户管理员签的调用到不了处理器；开关要求签名时未签名的调用被拒。
//!
//! 通过边界的写入请求不带任何要改的项：处理器核对完身份后被参数校验拒绝，不碰数据库（只有惰性
//! 连接池，不连）。

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
use flare_conversation::interface::grpc::{
    ConversationGrpcHandler, signed_notification_settings_service,
};
use flare_grpc_proto::conversation::UpdateUserNotificationSettingsRequest;
use flare_grpc_proto::conversation::user_notification_settings_service_client::UserNotificationSettingsServiceClient;
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, SignedContextVerifier,
    SignedFirstContextLayer,
};
use sqlx::postgres::PgPoolOptions;
use tonic::Code;
use tonic::transport::server::TcpIncoming;

type SettingsClient = UserNotificationSettingsServiceClient<tonic::transport::Channel>;

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
            "req-notification-settings",
        ))
        .unwrap()
}

/// 起一个只挂这个服务的 gRPC 服务（与 `service/mod.rs` 同一个装配函数）。
async fn serve(layer: SignedFirstContextLayer) -> SettingsClient {
    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let port = incoming.local_addr().unwrap().port();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(signed_notification_settings_service(handler(), &layer))
            .serve_with_incoming(incoming),
    );
    SettingsClient::connect(format!("http://127.0.0.1:{port}"))
        .await
        .unwrap()
}

/// 一个什么都不改的写入：过了身份这一关就停在参数校验。
async fn empty_update(client: &mut SettingsClient, token: Option<&str>) -> tonic::Status {
    let mut request = tonic::Request::new(UpdateUserNotificationSettingsRequest::default());
    if let Some(token) = token {
        request
            .metadata_mut()
            .append("x-flare-ctx", token.parse().unwrap());
    }
    client
        .update_user_notification_settings(request)
        .await
        .expect_err("an empty update is never applied")
}

/// 要求签名时，没有签名的调用被拒。
#[tokio::test]
async fn an_unsigned_caller_is_refused_when_signing_is_required() {
    let (_, verifier) = keys(71);
    let mut client = serve(SignedFirstContextLayer::new(Some(verifier)).require_signed(true)).await;
    let status = empty_update(&mut client, None).await;
    assert_eq!(status.code(), Code::Unauthenticated, "{status:?}");
}

/// 用户签名到达处理器（停在参数校验）；服务与租户管理员都没有「本人」，在边界上就被拒。
#[tokio::test]
async fn only_a_signed_user_reaches_the_handler() {
    let (signer, verifier) = keys(71);
    let mut client = serve(SignedFirstContextLayer::new(Some(verifier))).await;

    let user = sign(&signer, SignedActorType::User, "alice");
    let status = empty_update(&mut client, Some(&user)).await;
    assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");

    for (actor_type, actor) in [
        (SignedActorType::Service, "flare-push-server"),
        (SignedActorType::TenantAdmin, "admin"),
    ] {
        let token = sign(&signer, actor_type, actor);
        let status = empty_update(&mut client, Some(&token)).await;
        assert_eq!(
            status.code(),
            Code::PermissionDenied,
            "{actor_type:?}: {status:?}"
        );
    }
}
