//! seq floor 自愈对真实 Storage Reader 签名边界的读（W14-R (e)）。
//!
//! 摄入按消息派生会话租户查 floor。以前原样转发入站签名：入站签名的租户（tenant-a）与派生的租户
//! （tenant-b）不同时，Reader 的 `SignedFirstContextLayer` 判为元数据与签名冲突，回
//! `PERMISSION_DENIED`，自愈失效（`message_ingest_seq_floor_tenant_conflict_total` 的来源）。现在摄入
//! 以自己的 Service 身份为派生租户签名，读通过边界、到达存储。
//!
//! 起一个真的 Storage Reader gRPC 服务（与 `service/mod.rs` 同一个装配函数），数据库是连不上的回环
//! 端口：通过边界的读在处理器里走到存储，回可重试的 `UNAVAILABLE`；被边界拒绝的是
//! `PERMISSION_DENIED` 或 `UNAUTHENTICATED`，到不了存储。

use std::sync::Arc;
use std::time::Duration;

use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
use flare_im_contracts::Ctx;
use flare_message_ingest::domain::repository::SeqFloorProvider;
use flare_message_ingest::infrastructure::persistence::storage_seq_floor_provider::StorageSeqFloorProvider;
use flare_server_core::error::ErrorCode;
use flare_server_core::grpc::middleware::signed_context::verify_signed_context_metadata;
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, SignedContextVerifier,
    SignedFirstContextLayer, set_signed_context_token,
};
use flare_storage_reader::application::handlers::MessageStorageQueryHandler;
use flare_storage_reader::infrastructure::persistence::optimized_postgres_store::OptimizedPostgresMessageStorageImpl;
use flare_storage_reader::infrastructure::persistence::postgres_base::PostgresBaseStorage;
use flare_storage_reader::interface::grpc::{StorageReaderGrpcHandler, signed_read_service};
use tonic::transport::{Channel, server::TcpIncoming};

fn cell_keys() -> (SignedContextSigner, SignedContextVerifier) {
    let key = SigningKey::from_bytes(&[53; 32]);
    (
        SignedContextSigner::from_private_key(&key.to_pkcs8_pem(Default::default()).unwrap())
            .unwrap(),
        SignedContextVerifier::from_public_key(
            &key.verifying_key()
                .to_public_key_pem(Default::default())
                .unwrap(),
        )
        .unwrap(),
    )
}

/// 签名优先、要求签名的 Storage Reader（单机模板的设置）；存储是连不上的回环端口。
async fn reader(verifier: SignedContextVerifier) -> Channel {
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
        StorageReaderGrpcHandler::new(Arc::new(MessageStorageQueryHandler::new(storage)), None)
            .await
            .unwrap();
    let layer = SignedFirstContextLayer::new(Some(verifier)).require_signed(true);
    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let port = incoming.local_addr().unwrap().port();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(signed_read_service(handler, &layer))
            .serve_with_incoming(incoming),
    );
    Channel::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect_lazy()
}

/// 入站验过签的发送者：租户 tenant-a 的用户 alice。
fn inbound_alice(signer: &SignedContextSigner, verifier: &SignedContextVerifier) -> Ctx {
    let token = signer
        .sign(&SignedContextIdentity::new(
            SignedActorType::User,
            "alice",
            "tenant-a",
            "req-floor-e2e",
        ))
        .unwrap();
    let mut inbound = tonic::Request::new(());
    set_signed_context_token(&mut inbound, &token).unwrap();
    verify_signed_context_metadata(inbound.metadata(), verifier).unwrap()
}

#[tokio::test]
async fn a_floor_read_for_the_derived_tenant_passes_the_signed_reader_boundary() {
    let (signer, verifier) = cell_keys();
    let channel = reader(verifier.clone()).await;
    let ctx = inbound_alice(&signer, &verifier);

    // 摄入持有 Cell 私钥：以自己的 Service 身份为派生租户签名，读通过边界、到达（连不上的）存储。
    let provider = StorageSeqFloorProvider::from_channel(channel.clone())
        .with_service_signer(Some(signer.clone()), "flare-message-ingest");
    let err = tokio::time::timeout(
        Duration::from_secs(10),
        provider.persisted_max_seq(&ctx, "tenant-b", "conv-floor"),
    )
    .await
    .expect("the reader answers")
    .expect_err("the store behind the boundary is unreachable");
    assert_eq!(err.code(), Some(ErrorCode::ServiceUnavailable), "{err}");

    // 同租户照样通过（服务身份，不是 alice）。
    let err = provider
        .persisted_max_seq(&ctx, "tenant-a", "conv-floor")
        .await
        .expect_err("the store behind the boundary is unreachable");
    assert_eq!(err.code(), Some(ErrorCode::ServiceUnavailable), "{err}");

    // 没有私钥：别的租户的签名不再转发，要求签名的 Reader 拒绝未签名的读（未迁移的部署，退化为普通
    // INCR）；同租户照旧转发入站签名，通过边界。
    let unsigned = StorageSeqFloorProvider::from_channel(channel);
    let err = unsigned
        .persisted_max_seq(&ctx, "tenant-b", "conv-floor")
        .await
        .expect_err("an unsigned read is refused");
    assert_eq!(err.code(), Some(ErrorCode::AuthenticationFailed), "{err}");
    let err = unsigned
        .persisted_max_seq(&ctx, "tenant-a", "conv-floor")
        .await
        .expect_err("the store behind the boundary is unreachable");
    assert_eq!(err.code(), Some(ErrorCode::ServiceUnavailable), "{err}");
}
