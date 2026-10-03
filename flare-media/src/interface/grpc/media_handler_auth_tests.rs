//! Media gRPC caller authentication over a real loopback server: the production
//! server composition, the Core client wrapper, and in-memory Media ports.
use super::*;
use crate::domain::model::{
    MEDIA_LIFECYCLE_SCOPE_METADATA_KEY, MESSAGE_MEDIA_LIFECYCLE_SCOPE, MediaAssetStatus,
    MediaDomainConfig, MediaFileMetadata,
};
use crate::domain::repository::MediaMetadataStore;
use crate::domain::service::MediaService;
use crate::domain::service::isolation_tests::{Files, Metadata, Sessions};
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
use flare_grpc_proto::media::media_service_client::MediaServiceClient;
use flare_grpc_proto::media::{GetFileInfoRequest, UploadFileMetadata};
use flare_im_service_kit::clients::{MediaCaller, MediaServiceClientWrapper};
use flare_server_core::context::Context;
use flare_server_core::{SignedContextIdentity, SignedContextSigner, set_signed_context_token};
use std::collections::HashMap;
use tonic::Code;
use tonic::transport::Channel;

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

struct Harness {
    channel: Channel,
    signer: SignedContextSigner,
    metadata: Arc<Metadata>,
    chunks: std::path::PathBuf,
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.chunks);
    }
}

async fn serve() -> Harness {
    let (signer, verifier) = keys(71);
    let metadata = Arc::new(Metadata::default());
    let chunks = std::env::temp_dir().join(format!("flare-media-auth-{}", uuid::Uuid::new_v4()));
    let service = Arc::new(MediaService::new(
        None,
        Some(metadata.clone()),
        None,
        None,
        Some(Arc::new(Sessions::default())),
        Some(Arc::new(Files::default())),
        MediaDomainConfig::new(3600, None, 60, chunks.clone(), 3600, 8 * 1024 * 1024),
    ));
    let handler = MediaGrpcHandler::new(
        Arc::new(MediaCommandHandler::new(service.clone())),
        Arc::new(MediaQueryHandler::new(service)),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(authenticated_media_service(handler, verifier))
            .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener)),
    );
    Harness {
        channel: Channel::from_shared(format!("http://{address}"))
            .unwrap()
            .connect_lazy(),
        signer,
        metadata,
        chunks,
    }
}

fn ctx() -> Ctx {
    Arc::new(Context::with_request_id("auth-test"))
}

fn token(signer: &SignedContextSigner, actor: SignedActorType, tenant: &str, id: &str) -> String {
    signer
        .sign(&SignedContextIdentity::new(actor, id, tenant, "auth-test"))
        .unwrap()
}

/// Raw request: optional signed token plus caller-chosen legacy identity metadata.
fn raw<T>(payload: T, signed: Option<&str>, legacy_tenant: &str, legacy_user: &str) -> Request<T> {
    let mut request = Request::new(payload);
    let metadata = request.metadata_mut();
    metadata.insert("x-tenant-id", legacy_tenant.parse().unwrap());
    metadata.insert("x-user-id", legacy_user.parse().unwrap());
    if let Some(token) = signed {
        set_signed_context_token(&mut request, token).unwrap();
    }
    request
}

async fn upload(h: &Harness, tenant: &str, user: &str) -> String {
    let caller = MediaCaller::user(ctx(), h.signer.clone(), tenant, user);
    let mut client = MediaServiceClientWrapper::from_channel(h.channel.clone(), None);
    client
        .upload_file(
            &caller,
            vec![
                UploadFileRequest {
                    request: Some(upload_file_request::Request::Metadata(UploadFileMetadata {
                        file_name: "a.txt".to_string(),
                        mime_type: "text/plain".to_string(),
                        file_size: 5,
                        ..Default::default()
                    })),
                },
                UploadFileRequest {
                    request: Some(upload_file_request::Request::ChunkData(b"bytes".to_vec())),
                },
            ],
        )
        .await
        .unwrap()
        .file_id
}

/// A private file owned by `owner` in `tenant-a`, written straight to the store.
async fn seed_private_file(h: &Harness) -> String {
    let file = MediaFileMetadata {
        owner_id: Some("owner".to_string()),
        file_id: "private-file".to_string(),
        file_name: "a.txt".to_string(),
        mime_type: "text/plain".to_string(),
        file_size: 5,
        storage_path: Some("tenant-a/private-file".to_string()),
        ..Default::default()
    };
    h.metadata
        .save_metadata(&Context::root().with_tenant_id("tenant-a"), &file)
        .await
        .unwrap();
    file.file_id
}

async fn file_info_code(h: &Harness, request: Request<GetFileInfoRequest>) -> Code {
    match MediaServiceClient::new(h.channel.clone())
        .get_file_info(request)
        .await
    {
        Ok(_) => Code::Ok,
        Err(status) => status.code(),
    }
}

#[tokio::test]
async fn unsigned_or_foreign_signed_callers_are_unauthenticated_despite_legacy_metadata() {
    let h = serve().await;
    let file_id = seed_private_file(&h).await;
    let get = || GetFileInfoRequest {
        file_id: file_id.clone(),
    };
    // Legacy metadata alone used to establish the tenant and owner.
    assert_eq!(
        file_info_code(&h, raw(get(), None, "tenant-a", "owner")).await,
        Code::Unauthenticated
    );
    let (rogue, _) = keys(72);
    let forged = token(&rogue, SignedActorType::User, "tenant-a", "owner");
    assert_eq!(
        file_info_code(&h, raw(get(), Some(&forged), "tenant-a", "owner")).await,
        Code::Unauthenticated
    );
    assert_eq!(
        file_info_code(&h, raw(get(), Some("not-a-token"), "tenant-a", "owner")).await,
        Code::Unauthenticated
    );
    let genuine = token(&h.signer, SignedActorType::User, "tenant-a", "owner");
    assert_eq!(
        file_info_code(&h, raw(get(), Some(&genuine), "tenant-a", "owner")).await,
        Code::Ok
    );
}

#[tokio::test]
async fn signed_caller_of_another_tenant_or_user_cannot_read_even_with_spoofed_metadata() {
    let h = serve().await;
    let file_id = seed_private_file(&h).await;
    let get = || GetFileInfoRequest {
        file_id: file_id.clone(),
    };
    let other_tenant = token(&h.signer, SignedActorType::User, "tenant-b", "owner");
    let code = file_info_code(&h, raw(get(), Some(&other_tenant), "tenant-a", "owner")).await;
    assert!(
        !matches!(code, Code::Ok | Code::Unauthenticated),
        "{code:?}"
    );
    let other_user = token(&h.signer, SignedActorType::User, "tenant-a", "intruder");
    let code = file_info_code(&h, raw(get(), Some(&other_user), "tenant-a", "owner")).await;
    assert!(
        !matches!(code, Code::Ok | Code::Unauthenticated),
        "{code:?}"
    );
}

/// 签名层给 TenantAdmin 填了 `user_id = actor_id`（员工 id）。员工 id 与 IM 用户 id 同号时，
/// 修复前员工能以属主身份读、删用户的私有文件。服务与系统主体同样不算属主。
#[tokio::test]
async fn employee_service_or_system_ids_never_act_as_the_im_owner() {
    let h = serve().await;
    let file_id = seed_private_file(&h).await;
    for actor in [
        SignedActorType::TenantAdmin,
        SignedActorType::Service,
        SignedActorType::System,
    ] {
        let impostor = token(&h.signer, actor, "tenant-a", "owner");
        let get = GetFileInfoRequest {
            file_id: file_id.clone(),
        };
        assert_eq!(
            file_info_code(&h, raw(get, Some(&impostor), "tenant-a", "owner")).await,
            Code::PermissionDenied,
            "{actor:?}"
        );
        let delete = raw(
            DeleteFileRequest {
                file_id: file_id.clone(),
                ..Default::default()
            },
            Some(&impostor),
            "tenant-a",
            "owner",
        );
        let code = MediaServiceClient::new(h.channel.clone())
            .delete_file(delete)
            .await
            .map(|_| Code::Ok)
            .unwrap_or_else(|status| status.code());
        assert_eq!(code, Code::PermissionDenied, "{actor:?}");
    }
    let owner = token(&h.signer, SignedActorType::User, "tenant-a", "owner");
    assert_eq!(
        file_info_code(
            &h,
            raw(
                GetFileInfoRequest { file_id },
                Some(&owner),
                "tenant-a",
                "owner"
            )
        )
        .await,
        Code::Ok
    );
}

#[tokio::test]
async fn core_client_wrapper_uploads_and_reads_back_as_the_signed_owner() {
    let h = serve().await;
    let file_id = upload(&h, "tenant-a", "owner").await;
    let owner = token(&h.signer, SignedActorType::User, "tenant-a", "owner");
    assert_eq!(
        file_info_code(
            &h,
            raw(
                GetFileInfoRequest { file_id },
                Some(&owner),
                "tenant-b",
                "someone"
            )
        )
        .await,
        Code::Ok
    );
}

#[tokio::test]
async fn orphan_cleanup_requires_signed_tenant_admin_and_stays_in_its_tenant() {
    let h = serve().await;
    let orphan = MediaFileMetadata {
        file_id: "orphan".to_string(),
        reference_count: 0,
        status: MediaAssetStatus::Pending,
        grace_expires_at: Some(chrono::Utc::now() - chrono::Duration::seconds(10)),
        storage_path: Some("tenant-a/orphan".to_string()),
        metadata: HashMap::from([(
            MEDIA_LIFECYCLE_SCOPE_METADATA_KEY.to_string(),
            MESSAGE_MEDIA_LIFECYCLE_SCOPE.to_string(),
        )]),
        ..Default::default()
    };
    let tenant_a = Context::root().with_tenant_id("tenant-a");
    h.metadata.save_metadata(&tenant_a, &orphan).await.unwrap();
    let cleanup = |signed: Option<String>, legacy_tenant: &str| {
        let request = raw(
            CleanupOrphanedAssetsRequest {
                limit: 10,
                dry_run: false,
            },
            signed.as_deref(),
            legacy_tenant,
            "admin",
        );
        let channel = h.channel.clone();
        async move {
            MediaServiceClient::new(channel)
                .cleanup_orphaned_assets(request)
                .await
                .map(|response| response.into_inner().file_ids)
                .map_err(|status| status.code())
        }
    };
    // Unsigned "any tenant" cleanup — the original SEC-05 gap.
    assert_eq!(cleanup(None, "tenant-a").await, Err(Code::Unauthenticated));
    // A validly signed end user is not an administrator.
    let user = token(&h.signer, SignedActorType::User, "tenant-a", "owner");
    assert_eq!(
        cleanup(Some(user), "tenant-a").await,
        Err(Code::PermissionDenied)
    );
    // An administrator of tenant B cannot reach tenant A through metadata.
    let admin_b = token(&h.signer, SignedActorType::TenantAdmin, "tenant-b", "admin");
    assert_eq!(cleanup(Some(admin_b), "tenant-a").await, Ok(vec![]));
    assert!(
        h.metadata
            .load_metadata(&tenant_a, "orphan")
            .await
            .unwrap()
            .is_some()
    );
    let admin_a = token(&h.signer, SignedActorType::TenantAdmin, "tenant-a", "admin");
    assert_eq!(
        cleanup(Some(admin_a), "tenant-b").await,
        Ok(vec!["orphan".to_string()])
    );
    assert!(
        h.metadata
            .load_metadata(&tenant_a, "orphan")
            .await
            .unwrap()
            .is_none()
    );
}

#[test]
fn handler_without_the_signed_layer_rejects_instead_of_trusting_metadata() {
    let mut request = raw(
        GetFileInfoRequest {
            file_id: "x".to_string(),
        },
        None,
        "tenant-a",
        "owner",
    );
    assert_eq!(
        verified_ctx(&request).unwrap_err().code(),
        Code::Unauthenticated
    );
    // An unverified Context (e.g. from the legacy ContextLayer) is not an identity either.
    let legacy: Ctx = Arc::new(
        Context::root()
            .with_tenant_id("tenant-a")
            .with_user_id("owner"),
    );
    request.extensions_mut().insert(legacy);
    assert_eq!(
        verified_ctx(&request).unwrap_err().code(),
        Code::Unauthenticated
    );
}

/// 上传的租户只取签名上下文（W12-T）：缺租户、空白或通配的租户回 `PERMISSION_DENIED`，不再按
/// 租户 "0" 写进上传元数据；显式的 "0" 照常。签名身份本身要求非空租户，这是入口的第二道防线。
#[test]
fn upload_tenant_never_defaults_to_tenant_zero() {
    let no_tenant: Ctx = Arc::new(Context::with_request_id("req").with_user_id("alice"));
    assert_eq!(
        upload_tenant(&no_tenant).unwrap_err().code(),
        Code::PermissionDenied
    );
    for tenant in ["", "  ", "*"] {
        let ctx: Ctx = Arc::new(Context::with_request_id("req").with_tenant_id(tenant));
        assert_eq!(
            upload_tenant(&ctx).unwrap_err().code(),
            Code::PermissionDenied,
            "{tenant:?}"
        );
    }
    for tenant in ["0", "tenant-a"] {
        let ctx: Ctx = Arc::new(Context::with_request_id("req").with_tenant_id(tenant));
        assert_eq!(upload_tenant(&ctx).unwrap(), tenant);
    }
}
