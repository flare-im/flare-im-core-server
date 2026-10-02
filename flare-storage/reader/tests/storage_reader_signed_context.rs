//! Storage Reader 读接口的身份边界（W10-D）：经 `signed_read_service` 起一个真的 gRPC 服务。
//! 消息、会话头等读以 `x-flare-ctx` 的签名为准，与签名不一致的裸 `x-tenant-id`、`x-user-id`
//! 被拒；边界要求签名时未签名调用被拒。管理检索不经这道边界，照旧只认 admin 令牌。
//!
//! 以前这些读只看调用方自报的元数据：内网里任何能连到端口的进程带上任意租户与用户就能读消息。
//!
//! 数据库是连不上的回环端口：通过边界的读在处理器里走到存储，回可重试的 UNAVAILABLE；
//! 被边界拒绝的是 UNAUTHENTICATED 或 PERMISSION_DENIED，到不了存储。

use std::sync::Arc;
use std::time::Duration;

use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
use flare_grpc_proto::storage::storage_reader_service_client::StorageReaderServiceClient;
use flare_grpc_proto::storage::{GetMessageRequest, QueryMessagesBySeqRequest};
use flare_im_service_kit::control_auth::{
    CONSOLE_ISSUER, CORE_ADMIN_AUDIENCE, ConsoleControlVerifier,
};
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, SignedContextVerifier,
    SignedFirstContextLayer, TokenService,
};
use flare_storage_reader::application::handlers::MessageStorageQueryHandler;
use flare_storage_reader::infrastructure::persistence::optimized_postgres_store::OptimizedPostgresMessageStorageImpl;
use flare_storage_reader::infrastructure::persistence::postgres_base::PostgresBaseStorage;
use flare_storage_reader::interface::grpc::{StorageReaderGrpcHandler, signed_read_service};
use tonic::Code;
use tonic::transport::{Channel, server::TcpIncoming};

type Client = StorageReaderServiceClient<Channel>;

fn cell_keys(seed: u8) -> (SignedContextSigner, SignedContextVerifier) {
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

/// Console 的测试密钥对（admin-gateway 原样转发的 admin 令牌由它签）。
fn console() -> (TokenService, ConsoleControlVerifier) {
    let key = SigningKey::from_bytes(&[47; 32]);
    let private = key.to_pkcs8_pem(Default::default()).unwrap();
    let public = key
        .verifying_key()
        .to_public_key_pem(Default::default())
        .unwrap();
    (
        TokenService::from_ed25519_private_key(&private, CONSOLE_ISSUER, 60).unwrap(),
        ConsoleControlVerifier::from_public_key(&public).unwrap(),
    )
}

fn sign(signer: &SignedContextSigner, actor_type: SignedActorType, actor: &str) -> String {
    signer
        .sign(&SignedContextIdentity::new(
            actor_type,
            actor,
            "tenant-a",
            "req-reader-signed",
        ))
        .unwrap()
}

/// 与 `service/mod.rs` 同一个装配函数，存储是连不上的回环端口。
async fn serve(layer: SignedFirstContextLayer, admin: Option<ConsoleControlVerifier>) -> Client {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(Duration::from_millis(500))
        .connect_lazy("postgres://reader@127.0.0.1:1/flare")
        .unwrap();
    let storage = Arc::new(OptimizedPostgresMessageStorageImpl::new(
        PostgresBaseStorage { pool, cache: None },
        None,
        None,
    ));
    let handler =
        StorageReaderGrpcHandler::new(Arc::new(MessageStorageQueryHandler::new(storage)), admin)
            .await
            .unwrap();
    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let port = incoming.local_addr().unwrap().port();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(signed_read_service(handler, &layer))
            .serve_with_incoming(incoming),
    );
    StorageReaderServiceClient::connect(format!("http://127.0.0.1:{port}"))
        .await
        .unwrap()
}

fn with_metadata<T>(payload: T, metadata: &[(&'static str, &str)]) -> tonic::Request<T> {
    let mut request = tonic::Request::new(payload);
    for (key, value) in metadata {
        request.metadata_mut().append(*key, value.parse().unwrap());
    }
    request
}

async fn read_messages(client: &mut Client, metadata: &[(&'static str, &str)]) -> tonic::Status {
    client
        .query_messages_by_seq(with_metadata(
            QueryMessagesBySeqRequest {
                conversation_id: "single:alice:bob".into(),
                after_seq: 0,
                limit: 10,
                user_id: "alice".into(),
                ..Default::default()
            },
            metadata,
        ))
        .await
        .expect_err("the store is unreachable")
}

const UNSIGNED_VICTIM: [(&str, &str); 2] = [("x-tenant-id", "tenant-a"), ("x-user-id", "alice")];

#[tokio::test]
async fn unsigned_reads_are_refused_when_signing_is_required() {
    let (signer, verifier) = cell_keys(91);
    let (other_signer, _) = cell_keys(92);
    let mut client = serve(
        SignedFirstContextLayer::new(Some(verifier)).require_signed(true),
        None,
    )
    .await;

    let status = read_messages(&mut client, &UNSIGNED_VICTIM).await;
    assert_eq!(status.code(), Code::Unauthenticated, "{status:?}");
    assert_eq!(status.message(), "signed context required");

    let forged = sign(&other_signer, SignedActorType::User, "alice");
    let status = read_messages(&mut client, &[("x-flare-ctx", &forged)]).await;
    assert_eq!(status.code(), Code::Unauthenticated, "{status:?}");

    let mallory = sign(&signer, SignedActorType::User, "mallory");
    for (name, value) in [("x-user-id", "alice"), ("x-tenant-id", "tenant-b")] {
        let status = read_messages(&mut client, &[("x-flare-ctx", &mallory), (name, value)]).await;
        assert_eq!(status.code(), Code::PermissionDenied, "{name}: {status:?}");
    }
}

/// 正确签名（用户或 Cell 服务）通过边界走到存储；关掉开关时旧调用方照旧走到存储。
#[tokio::test]
async fn valid_signatures_and_tolerated_legacy_calls_reach_the_store() {
    let (signer, verifier) = cell_keys(91);
    let mut required = serve(
        SignedFirstContextLayer::new(Some(verifier.clone())).require_signed(true),
        None,
    )
    .await;
    for token in [
        sign(&signer, SignedActorType::User, "alice"),
        sign(&signer, SignedActorType::Service, "flare-sync-orchestrator"),
    ] {
        let status = read_messages(&mut required, &[("x-flare-ctx", &token)]).await;
        assert_eq!(status.code(), Code::Unavailable, "{status:?}");
    }

    let mut tolerant = serve(SignedFirstContextLayer::new(Some(verifier)), None).await;
    let status = read_messages(&mut tolerant, &UNSIGNED_VICTIM).await;
    assert_eq!(status.code(), Code::Unavailable, "{status:?}");
}

/// 管理检索不要 `x-flare-ctx`：带 admin 令牌的调用越过读接口边界走到存储；不带的被处理器以
/// admin 令牌缺失拒绝，而不是以缺签名上下文拒绝。
#[tokio::test]
async fn admin_retrieval_keeps_its_admin_token_check() {
    let (_, verifier) = cell_keys(91);
    let (console, admin_verifier) = console();
    let mut client = serve(
        SignedFirstContextLayer::new(Some(verifier)).require_signed(true),
        Some(admin_verifier),
    )
    .await;
    let admin = format!(
        "Bearer {}",
        console
            .generate_with_ed25519("employee-1", Some("tenant-a"), "admin", CORE_ADMIN_AUDIENCE)
            .unwrap()
    );
    let get = |metadata: Vec<(&'static str, String)>| {
        let mut request = tonic::Request::new(GetMessageRequest {
            message_id: "m1".into(),
        });
        for (key, value) in metadata {
            request.metadata_mut().append(key, value.parse().unwrap());
        }
        request
    };
    let status = client
        .get_message(get(vec![("authorization", admin)]))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Unavailable, "{status:?}");

    let status = client
        .get_message(get(vec![("x-tenant-id", "tenant-a".into())]))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Unauthenticated, "{status:?}");
    assert_ne!(status.message(), "signed context required");
}
