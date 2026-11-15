//! 「服务身份按用户读」的身份边界：经 `signed_internal_read_service` 起一个真的 gRPC 服务。
//! 这个服务按请求体里的 `user_id` 读别人有哪些未读会话，所以一律要求签名，并且只放行 Cell 内
//! 服务——不跟随 `FLARE_REQUIRE_SIGNED_READ_CTX`：开关关着时，网内进程带一个裸
//! `x-actor-type` 也读不到。
//!
//! 通过边界的请求用空的 `user_id`：处理器核对完身份后被参数校验拒绝，不碰数据库（只有惰性
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
use flare_conversation::interface::grpc::{ConversationGrpcHandler, signed_internal_read_service};
use flare_grpc_proto::conversation::conversation_internal_read_service_client::ConversationInternalReadServiceClient;
use flare_grpc_proto::conversation::{
    BatchGetUserNotificationSettingsRequest, ListNotificationSubscribersRequest,
    ListRecentUnreadConversationsRequest,
};
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, SignedContextVerifier,
    SignedFirstContextLayer,
};
use sqlx::postgres::PgPoolOptions;
use tonic::Code;
use tonic::transport::server::TcpIncoming;

type InternalReadClient = ConversationInternalReadServiceClient<tonic::transport::Channel>;

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
            "req-internal-read",
        ))
        .unwrap()
}

/// 起一个只挂这个服务的 gRPC 服务（与 `service/mod.rs` 同一个装配函数）。
async fn serve(layer: SignedFirstContextLayer) -> InternalReadClient {
    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let port = incoming.local_addr().unwrap().port();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(signed_internal_read_service(handler(), &layer))
            .serve_with_incoming(incoming),
    );
    InternalReadClient::connect(format!("http://127.0.0.1:{port}"))
        .await
        .unwrap()
}

async fn ask(
    client: &mut InternalReadClient,
    user_id: &str,
    metadata: &[(&'static str, &str)],
) -> tonic::Status {
    let mut request = tonic::Request::new(ListRecentUnreadConversationsRequest {
        user_id: user_id.into(),
        since: 1_760_000_000_000,
        limit: 5,
    });
    for (key, value) in metadata {
        request.metadata_mut().append(*key, value.parse().unwrap());
    }
    client
        .list_recent_unread_conversations(request)
        .await
        .expect_err("never answered from a database here")
}

/// 冒充内部服务的裸元数据：以前的读接口在开关关着时会认它。
const UNSIGNED_SERVICE: [(&str, &str); 3] = [
    ("x-tenant-id", "tenant-a"),
    ("x-actor-id", "flare-push-server"),
    ("x-actor-type", "2"),
];

/// 读接口的开关开着、关着，没有签名的调用都被拒；别的密钥签的也被拒；没有公钥时一律不可用。
#[tokio::test]
async fn an_unsigned_or_forged_caller_is_always_refused() {
    let (_, verifier) = keys(91);
    let (other_signer, _) = keys(92);
    for require in [true, false] {
        let mut client =
            serve(SignedFirstContextLayer::new(Some(verifier.clone())).require_signed(require))
                .await;

        let status = ask(&mut client, "alice", &UNSIGNED_SERVICE).await;
        assert_eq!(
            status.code(),
            Code::Unauthenticated,
            "require={require}: {status:?}"
        );
        assert_eq!(status.message(), "signed context required");

        let forged = sign(&other_signer, SignedActorType::Service, "flare-push-server");
        let status = ask(&mut client, "alice", &[("x-flare-ctx", &forged)]).await;
        assert_eq!(
            status.code(),
            Code::Unauthenticated,
            "require={require}: {status:?}"
        );
    }

    let mut client = serve(SignedFirstContextLayer::new(None)).await;
    let status = ask(&mut client, "alice", &UNSIGNED_SERVICE).await;
    assert_eq!(status.code(), Code::Unavailable, "{status:?}");
}

/// 签了名但不是服务身份的调用到不了处理器：用户读自己的也不行（那是 ConversationReadService
/// 的事），租户管理员、匿名身份同样被拒。
#[tokio::test]
async fn a_signed_caller_that_is_not_a_service_is_refused() {
    let (signer, verifier) = keys(91);
    let mut client = serve(SignedFirstContextLayer::new(Some(verifier))).await;

    for (actor_type, actor) in [
        (SignedActorType::User, "alice"),
        (SignedActorType::TenantAdmin, "admin"),
        (SignedActorType::Guest, "anonymous"),
    ] {
        let token = sign(&signer, actor_type, actor);
        let status = ask(&mut client, "alice", &[("x-flare-ctx", &token)]).await;
        assert_eq!(
            status.code(),
            Code::PermissionDenied,
            "{actor_type:?}: {status:?}"
        );
    }
}

/// 服务签名通过边界到达处理器：空的 user_id 停在参数校验；给了 user_id 的请求走到存储才失败
/// （这里没有数据库），说明身份这一关已经过了。
#[tokio::test]
async fn a_signed_service_reaches_the_handler() {
    let (signer, verifier) = keys(91);
    let mut client = serve(SignedFirstContextLayer::new(Some(verifier))).await;
    let push_server = sign(&signer, SignedActorType::Service, "flare-push-server");

    let status = ask(&mut client, "", &[("x-flare-ctx", &push_server)]).await;
    assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");
    assert!(
        status.message().contains("user_id is required"),
        "{status:?}"
    );

    let status = ask(&mut client, "alice", &[("x-flare-ctx", &push_server)]).await;
    assert!(
        !matches!(
            status.code(),
            Code::Unauthenticated | Code::PermissionDenied | Code::InvalidArgument
        ),
        "the identity and the arguments were accepted, only storage is missing: {status:?}"
    );
}

/// 推送判定用的两个读（批量读用户级通知设置、列会话通知订阅者）在同一道边界后面：用户签名的
/// 调用被拒，服务签名的调用到达处理器，停在参数校验（空的用户列表、空的会话 ID）。
#[tokio::test]
async fn the_notification_reads_sit_behind_the_same_boundary() {
    let (signer, verifier) = keys(91);
    let mut client = serve(SignedFirstContextLayer::new(Some(verifier))).await;
    let user = sign(&signer, SignedActorType::User, "alice");
    let push_server = sign(&signer, SignedActorType::Service, "flare-push-server");

    for (token, expected) in [
        (&user, Code::PermissionDenied),
        (&push_server, Code::InvalidArgument),
    ] {
        let mut request =
            tonic::Request::new(BatchGetUserNotificationSettingsRequest { user_ids: vec![] });
        request
            .metadata_mut()
            .append("x-flare-ctx", token.parse().unwrap());
        let status = client
            .batch_get_user_notification_settings(request)
            .await
            .expect_err("never answered from a database here");
        assert_eq!(status.code(), expected, "{status:?}");

        let mut request = tonic::Request::new(ListNotificationSubscribersRequest {
            conversation_id: String::new(),
            page_token: String::new(),
            page_size: 0,
        });
        request
            .metadata_mut()
            .append("x-flare-ctx", token.parse().unwrap());
        let status = client
            .list_notification_subscribers(request)
            .await
            .expect_err("never answered from a database here");
        assert_eq!(status.code(), expected, "{status:?}");
    }
}
