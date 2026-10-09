//! Storage Reader 读接口的身份边界（W10-D）：经 `signed_read_service` 起一个真的 gRPC 服务。
//! 消息、会话头等读以 `x-flare-ctx` 的签名为准，与签名不一致的裸 `x-tenant-id`、`x-user-id`
//! 被拒；边界要求签名时未签名调用被拒。管理检索不经这道边界，只认转发的 Staff 上下文。
//!
//! 以前这些读只看调用方自报的元数据：内网里任何能连到端口的进程带上任意租户与用户就能读消息。
//! 管理检索（admin-gateway 转发的 Staff 上下文）由处理器自己验，见 `admin_retrieval_*`。
//!
//! 数据库是连不上的回环端口：通过边界的读在处理器里走到存储，回可重试的 UNAVAILABLE；
//! 被边界拒绝的是 UNAUTHENTICATED 或 PERMISSION_DENIED，到不了存储。

use std::sync::Arc;
use std::time::Duration;

use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
use flare_grpc_proto::storage::storage_reader_service_client::StorageReaderServiceClient;
use flare_grpc_proto::storage::{
    ConversationWindowTarget, GetMessageRequest, QueryConversationsMessageWindowsRequest,
    QueryMessagesBySeqRequest,
};
use flare_server_core::auth::kid;
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, SignedContextVerifier,
    SignedFirstContextLayer,
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

/// 网关 admin 分区的测试密钥对（admin-gateway 原样转发的 Staff 上下文由它按 `kid=gateway` 签），
/// 以及钉死 gateway 的验签器。
fn gateway() -> (SignedContextSigner, SignedContextVerifier) {
    let key = SigningKey::from_bytes(&[47; 32]);
    let private = key.to_pkcs8_pem(Default::default()).unwrap();
    let public = key
        .verifying_key()
        .to_public_key_pem(Default::default())
        .unwrap();
    (
        SignedContextSigner::from_private_key(&private)
            .unwrap()
            .with_kid(kid::GATEWAY)
            .unwrap(),
        SignedContextVerifier::from_public_key(&public)
            .unwrap()
            .require_staff_kid([kid::GATEWAY]),
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
async fn serve(layer: SignedFirstContextLayer, admin: Option<SignedContextVerifier>) -> Client {
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

/// 带可见性下限的两种读：按 seq 读与批量窗口，请求体的 `user_id` 取 `user_id`。
#[derive(Debug, Clone, Copy)]
enum FloorRead {
    BySeq,
    Windows,
}

impl FloorRead {
    async fn call(
        self,
        client: &mut Client,
        user_id: &str,
        metadata: &[(&'static str, &str)],
    ) -> tonic::Status {
        match self {
            FloorRead::BySeq => client
                .query_messages_by_seq(with_metadata(
                    QueryMessagesBySeqRequest {
                        conversation_id: "group:g1".into(),
                        after_seq: 0,
                        limit: 10,
                        user_id: user_id.into(),
                        ..Default::default()
                    },
                    metadata,
                ))
                .await
                .map(|_| ())
                .expect_err("the store is unreachable"),
            FloorRead::Windows => client
                .query_conversations_message_windows(with_metadata(
                    QueryConversationsMessageWindowsRequest {
                        targets: vec![ConversationWindowTarget {
                            conversation_id: "group:g1".into(),
                            after_seq: 0,
                        }],
                        per_conversation_limit: 10,
                        user_id: user_id.into(),
                        ..Default::default()
                    },
                    metadata,
                ))
                .await
                .map(|_| ())
                .expect_err("the store is unreachable"),
        }
    }
}

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

/// 按 seq 读与批量窗口的 `user_id` 是可见性下限（W13-R2）：签名的 User 指名他人时回
/// PERMISSION_DENIED、到不了存储，签名读开关打开与关闭都一样；指名自己、留空（按签名用户补上）
/// 与 Service 签名指名任意用户都走到存储。开关关闭时未签名的旧调用仍按请求体。修复前 mallory
/// 带自己的签名、请求体写 alice，就能按 alice 的下限读同租户的会话。
#[tokio::test]
async fn signed_users_cannot_read_with_another_users_visibility_floor() {
    let (signer, verifier) = cell_keys(91);
    let mallory = sign(&signer, SignedActorType::User, "mallory");
    let alice = sign(&signer, SignedActorType::User, "alice");
    let service = sign(&signer, SignedActorType::Service, "flare-sync-orchestrator");
    for require in [true, false] {
        let mut client = serve(
            SignedFirstContextLayer::new(Some(verifier.clone())).require_signed(require),
            None,
        )
        .await;
        for read in [FloorRead::BySeq, FloorRead::Windows] {
            let status = read
                .call(&mut client, "alice", &[("x-flare-ctx", &mallory)])
                .await;
            assert_eq!(
                status.code(),
                Code::PermissionDenied,
                "require={require} {read:?}: {status:?}"
            );
            assert_eq!(status.message(), "user_id does not match the signed user");

            for (token, user_id) in [
                (&alice, "alice"),
                (&alice, ""),
                (&mallory, ""),
                (&service, "alice"),
                (&service, ""),
            ] {
                let status = read
                    .call(&mut client, user_id, &[("x-flare-ctx", token)])
                    .await;
                assert_eq!(
                    status.code(),
                    Code::Unavailable,
                    "require={require} {read:?} user_id={user_id:?}: {status:?}"
                );
            }

            let status = read
                .call(
                    &mut client,
                    "alice",
                    &[("x-tenant-id", "tenant-a"), ("x-user-id", "mallory")],
                )
                .await;
            let expected = if require {
                Code::Unauthenticated
            } else {
                Code::Unavailable
            };
            assert_eq!(
                status.code(),
                expected,
                "require={require} {read:?} unsigned: {status:?}"
            );
        }
    }
}

/// 管理检索不经读接口边界：带网关签的 Staff 上下文的调用越过边界走到存储（边界的验签器是另一把
/// Cell 钥、也不放行 Staff，仍不碍事）；不带的被处理器以 Staff 上下文缺失拒绝，而不是以边界的
/// 缺签名上下文拒绝。
#[tokio::test]
async fn admin_retrieval_keeps_its_staff_context_check() {
    let (_, verifier) = cell_keys(91);
    let (gateway_signer, staff_verifier) = gateway();
    let mut client = serve(
        SignedFirstContextLayer::new(Some(verifier)).require_signed(true),
        Some(staff_verifier),
    )
    .await;
    let staff = gateway_signer
        .sign(
            &SignedContextIdentity::new(SignedActorType::Staff, "employee-1", "tenant-a", "req-1")
                .with_staff_grant(None, Some("core:messages:read".into()), 1),
        )
        .unwrap();
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
        .get_message(get(vec![("x-flare-ctx", staff)]))
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
