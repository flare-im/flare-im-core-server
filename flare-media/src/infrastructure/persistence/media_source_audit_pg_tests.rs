//! Production Media gRPC + owned fresh PG + real local filesystem. Not S3, Gateway HTTP,
//! broker delivery or container acceptance. Fixture rows are not an upload-flow proof.
use super::*;
use crate::{
    application::handlers::{MediaCommandHandler, MediaQueryHandler},
    domain::{
        model::{
            MEDIA_LIFECYCLE_SCOPE_METADATA_KEY, MESSAGE_MEDIA_LIFECYCLE_SCOPE, MediaDomainConfig,
        },
        repository::MediaLocalStore,
        service::MediaService,
    },
    infrastructure::local::filesystem::FilesystemMediaStore,
    interface::grpc::{MediaGrpcHandler, authenticated_media_service},
};
use ed25519_dalek::{
    SigningKey,
    pkcs8::{EncodePrivateKey, EncodePublicKey},
};
use flare_grpc_proto::media::{
    CleanupOrphanedAssetsRequest, media_service_client::MediaServiceClient,
};
use flare_im_service_kit::audit_effects::durable::StaffAudit;
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, SignedContextVerifier,
    context::{Context, Ctx},
};
use sqlx::postgres::PgConnectOptions;
use std::{future::Future, path::PathBuf, time::Duration};
use tonic::transport::Channel;
use tonic::{Code, Request};

fn keys() -> (SignedContextSigner, SignedContextVerifier) {
    let key = SigningKey::from_bytes(&[93; 32]);
    let private = key.to_pkcs8_pem(Default::default()).unwrap();
    let public = key
        .verifying_key()
        .to_public_key_pem(Default::default())
        .unwrap();
    (
        SignedContextSigner::from_private_key(private.as_str())
            .unwrap()
            .with_kid("gateway")
            .unwrap(),
        SignedContextVerifier::from_public_key(&public)
            .unwrap()
            .require_staff_kid(["gateway"]),
    )
}
fn request(app: &str, permission: &str, dry_run: bool) -> Request<CleanupOrphanedAssetsRequest> {
    let token = keys()
        .0
        .sign(
            &SignedContextIdentity::new(
                SignedActorType::Staff,
                "employee",
                app,
                "signed-media-request",
            )
            .with_staff_grant(None, Some(permission.into()), 1),
        )
        .unwrap();
    let mut request = Request::new(CleanupOrphanedAssetsRequest { limit: 10, dry_run });
    flare_server_core::set_signed_context_token(&mut request, &token).unwrap();
    request
        .metadata_mut()
        .insert("x-tenant-id", "spoofed-other-app".parse().unwrap());
    request
        .metadata_mut()
        .insert("x-user-id", "spoofed-owner".parse().unwrap());
    request
}
struct Files(PathBuf);
impl Drop for Files {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Running(tokio::task::JoinHandle<()>);
impl Drop for Running {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn database<F, Fut>(work: F)
where
    F: FnOnce(Arc<PgPool>, PathBuf) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let url =
        std::env::var("FLARE_FUNCTIONAL_PG_ADMIN_URL").expect("explicit owned-test PG required");
    let admin = PgPool::connect(&url)
        .await
        .expect("owned-test PG connection");
    let name = format!("flare_media_audit_test_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE DATABASE {name}"))
        .execute(&admin)
        .await
        .unwrap();
    let options = PgConnectOptions::from_str(&url).unwrap().database(&name);
    let pool = Arc::new(
        PgPoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await
            .unwrap(),
    );
    let root = std::env::temp_dir().join(format!("flare-media-source-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let files = Files(root.clone());
    let run = pool.clone();
    let outcome = tokio::spawn(async move {
        let baseline = include_str!("../../../../migrations/0001_baseline.sql");
        let start = baseline.find("CREATE TABLE media_assets (").unwrap();
        let end = baseline.find("CREATE TABLE ack_archive_records (").unwrap();
        // Assemble only the target Media schema in the new fixture; never alter a runtime DB.
        let schema = baseline[start..end].replacen(
            "file_id TEXT NOT NULL,",
            "file_id TEXT NOT NULL,\n    owner_id TEXT,",
            1,
        );
        sqlx::raw_sql(&schema).execute(&*run).await.unwrap();
        sqlx::raw_sql(include_str!(
            "../../../../migrations/0012_20261015_core_audit_outbox.sql"
        ))
        .execute(&*run)
        .await
        .unwrap();
        sqlx::raw_sql(include_str!(
            "../../../../migrations/0013_20261015_media_read_grants.sql"
        ))
        .execute(&*run)
        .await
        .unwrap();
        work(run, root).await;
    })
    .await;
    pool.close().await;
    sqlx::query(&format!("DROP DATABASE {name} WITH (FORCE)"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
    drop(files);
    if let Err(error) = outcome {
        std::panic::resume_unwind(error.into_panic());
    }
}
async fn seed(
    store: &PostgresMetadataStore,
    root: &std::path::Path,
    app: &str,
    id: &str,
    seconds: i64,
) {
    let path = format!("{app}/{id}");
    tokio::fs::create_dir_all(root.join(app)).await.unwrap();
    tokio::fs::write(root.join(&path), b"owned fixture bytes")
        .await
        .unwrap();
    store
        .save_metadata(
            &Context::root().with_tenant_id(app),
            &MediaFileMetadata {
                owner_id: Some("user".into()),
                file_id: id.into(),
                file_name: "private-file-name".into(),
                mime_type: "text/plain".into(),
                file_size: 19,
                status: MediaAssetStatus::Pending,
                grace_expires_at: Some(Utc::now() - chrono::Duration::seconds(seconds)),
                metadata: HashMap::from([
                    (
                        MEDIA_LIFECYCLE_SCOPE_METADATA_KEY.into(),
                        MESSAGE_MEDIA_LIFECYCLE_SCOPE.into(),
                    ),
                    (STORAGE_PATH_METADATA_KEY.into(), path),
                ]),
                ..Default::default()
            },
        )
        .await
        .unwrap();
}
async fn effects(pool: &PgPool) -> Vec<Value> {
    sqlx::query_scalar::<_, String>(
        "SELECT payload::text FROM core_audit_outbox ORDER BY payload->>'aggregate_id'",
    )
    .fetch_all(pool)
    .await
    .unwrap()
    .into_iter()
    .map(|value| serde_json::from_str(&value).unwrap())
    .collect()
}
async fn present(store: &PostgresMetadataStore, app: &str, id: &str) -> bool {
    store
        .load_metadata(&Context::root().with_tenant_id(app), id)
        .await
        .unwrap()
        .is_some()
}

#[tokio::test]
#[ignore = "requires explicit owned-test FLARE_FUNCTIONAL_PG_ADMIN_URL"]
async fn signed_media_cleanup_commits_each_source_effect_and_failures_preserve_retryable_facts() {
    database(check_cleanup).await;
}

async fn check_cleanup(pool: Arc<PgPool>, root: PathBuf) {
    let store = Arc::new(PostgresMetadataStore { pool: pool.clone() });
    let local = Arc::new(FilesystemMediaStore::new(&root, None).unwrap());
    let service = Arc::new(MediaService::new(
        None,
        Some(store.clone()),
        Some(store.clone()),
        None,
        None,
        Some(local.clone()),
        MediaDomainConfig::new(3600, None, 60, root.join("chunks"), 3600, 1024),
    ));
    let handler = MediaGrpcHandler::new(
        Arc::new(MediaCommandHandler::new(service.clone())),
        Arc::new(MediaQueryHandler::new(service)),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown, stopped) = tokio::sync::oneshot::channel::<()>();
    let running = Running(tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(authenticated_media_service(handler, keys().1))
            .serve_with_incoming_shutdown(
                tonic::transport::server::TcpIncoming::from(listener),
                async {
                    let _ = stopped.await;
                },
            )
            .await
            .unwrap();
    }));
    let channel = Channel::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = MediaServiceClient::new(channel);
    seed(&store, &root, "app-a", "first", 100).await;
    seed(&store, &root, "app-b", "sibling", 100).await;
    let before = effects(&pool).await;
    assert!(before.is_empty());

    // Direct Media callers cannot turn preview/read permissions into a destructive write.
    assert_eq!(
        client
            .cleanup_orphaned_assets(request("app-a", "core:medias:write", true))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(
        client
            .cleanup_orphaned_assets(request("app-a", "core:medias:read", false))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert!(present(&store, "app-a", "first").await && root.join("app-a/first").exists());
    assert!(effects(&pool).await.is_empty());

    sqlx::raw_sql("CREATE FUNCTION reject_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'owned audit fault'; END $$; CREATE TRIGGER reject_audit BEFORE INSERT ON core_audit_outbox FOR EACH ROW EXECUTE FUNCTION reject_audit()")
            .execute(&*pool).await.unwrap();
    let rejected = client
        .cleanup_orphaned_assets(request("app-a", "core:medias:write", false))
        .await
        .unwrap_err();
    assert!(!rejected.message().contains("owned audit fault"));
    assert!(
        present(&store, "app-a", "first").await && root.join("app-a/first").exists(),
        "audit failure occurs before external deletion"
    );
    assert!(effects(&pool).await.is_empty());
    sqlx::raw_sql("DROP TRIGGER reject_audit ON core_audit_outbox; DROP FUNCTION reject_audit()")
        .execute(&*pool)
        .await
        .unwrap();

    // Cancel/dropped owner claim has neither a metadata deletion nor an audit commit.
    let carrier = request("app-a", "core:medias:write", false);
    let verified = flare_im_service_kit::staff_context::verify_staff_metadata(
        carrier.metadata(),
        &keys().1,
        "core:medias:write",
    )
    .unwrap();
    let ctx: Ctx = verified.ctx;
    let claim = store
        .claim_orphaned_asset(&ctx, "first", Utc::now())
        .staff_audit(&ctx, "flare-media")
        .await
        .unwrap()
        .unwrap();
    drop(claim);
    tokio::time::timeout(Duration::from_secs(1), sqlx::query("SELECT file_id FROM media_assets WHERE tenant_id='app-a' AND file_id='first' FOR UPDATE").fetch_one(&*pool)).await.unwrap().unwrap();
    assert!(effects(&pool).await.is_empty());

    let response = client
        .cleanup_orphaned_assets(request("app-a", "core:medias:write", false))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.file_ids, ["first"]);
    assert!(!present(&store, "app-a", "first").await && !root.join("app-a/first").exists());
    assert!(present(&store, "app-b", "sibling").await && root.join("app-b/sibling").exists());
    assert!(
        client
            .cleanup_orphaned_assets(request("app-a", "core:medias:write", false))
            .await
            .unwrap()
            .get_ref()
            .file_ids
            .is_empty()
    );
    assert_eq!(effects(&pool).await.len(), 1);

    // First asset commits, then a real filesystem error: preserve the first effect,
    // roll back the failing claim, and retry just the remaining asset.
    seed(&store, &root, "app-a", "next", 400).await;
    seed(&store, &root, "app-a", "blocked", 200).await;
    tokio::fs::remove_file(root.join("app-a/blocked"))
        .await
        .unwrap();
    tokio::fs::create_dir(root.join("app-a/blocked"))
        .await
        .unwrap();
    assert!(
        client
            .cleanup_orphaned_assets(request("app-a", "core:medias:write", false))
            .await
            .is_err()
    );
    assert!(!present(&store, "app-a", "next").await && !root.join("app-a/next").exists());
    assert!(present(&store, "app-a", "blocked").await && root.join("app-a/blocked").is_dir());
    assert_eq!(effects(&pool).await.len(), 2);
    tokio::fs::remove_dir(root.join("app-a/blocked"))
        .await
        .unwrap();
    tokio::fs::write(root.join("app-a/blocked"), b"retry")
        .await
        .unwrap();
    assert_eq!(
        client
            .cleanup_orphaned_assets(request("app-a", "core:medias:write", false))
            .await
            .unwrap()
            .get_ref()
            .file_ids,
        ["blocked"]
    );
    let rows = effects(&pool).await;
    assert_eq!(rows.len(), 3);
    for row in rows {
        assert_eq!(row["actor_type"], "staff");
        assert_eq!(row["actor_id"], "employee");
        assert_eq!(row["request_id"], "signed-media-request");
        assert_eq!(row["permission"], "core:medias:write");
        assert_eq!(row["scope"], "app-a");
        assert_eq!(row["source"], "flare-media");
        assert_eq!(row["aggregate"], "media_asset_metadata");
        assert_eq!(row["action"], "reclaim");
        assert_eq!(row["outcome"], "committed");
        for field in [
            "file_name",
            "storage_path",
            "metadata",
            "headers",
            "token",
            "body",
        ] {
            assert!(row.get(field).is_none());
        }
    }
    assert!(local.read("app-b/sibling").await.is_ok());
    drop(client);
    let _ = shutdown.send(());
    // The test owns this server; never leave a listener on success or unwind.
    drop(running);
}

fn signed<T>(
    payload: T,
    app: &str,
    actor: SignedActorType,
    id: &str,
    permission: &str,
) -> Request<T> {
    let identity = SignedContextIdentity::new(actor, id, app, "media-acl-request");
    let identity = if actor == SignedActorType::Staff {
        identity.with_staff_grant(None, Some(permission.into()), 1)
    } else {
        identity
    };
    let mut r = Request::new(payload);
    flare_server_core::set_signed_context_token(&mut r, &keys().0.sign(&identity).unwrap())
        .unwrap();
    r
}

#[tokio::test]
#[ignore = "requires explicit owned-test FLARE_FUNCTIONAL_PG_ADMIN_URL"]
async fn acl_replacement_is_atomic_revokes_actual_reads_and_inventory_is_app_scoped() {
    database(check_acl).await;
}

async fn check_acl(pool: Arc<PgPool>, root: PathBuf) {
    use flare_grpc_proto::media::{
        AccessControlEntry, DeleteFileRequest, GetFileInfoRequest, ListObjectsRequest,
        SetObjectAclRequest,
    };
    let store = Arc::new(PostgresMetadataStore { pool: pool.clone() });
    let local = Arc::new(FilesystemMediaStore::new(&root, None).unwrap());
    let service = Arc::new(MediaService::new(
        None,
        Some(store.clone()),
        Some(store.clone()),
        None,
        None,
        Some(local),
        MediaDomainConfig::new(3600, None, 60, root.join("chunks"), 3600, 1024),
    ));
    let handler = MediaGrpcHandler::new(
        Arc::new(MediaCommandHandler::new(service.clone())),
        Arc::new(MediaQueryHandler::new(service)),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let running = Running(tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(authenticated_media_service(handler, keys().1))
            .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener))
            .await
            .unwrap();
    }));
    let mut client = MediaServiceClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    for (app, id) in [("app-a", "first"), ("app-a", "last"), ("app-b", "first")] {
        seed(&store, &root, app, id, 100).await;
    }
    let read = || {
        signed(
            GetFileInfoRequest {
                file_id: "first".into(),
            },
            "app-a",
            SignedActorType::User,
            "reader",
            "",
        )
    };
    let grants = |entries: Vec<AccessControlEntry>| {
        signed(
            SetObjectAclRequest {
                file_id: "first".into(),
                entries,
            },
            "app-a",
            SignedActorType::Staff,
            "employee",
            "core:medias:write",
        )
    };
    let entry = || AccessControlEntry {
        principal: "user:reader".into(),
        permissions: vec!["read".into()],
    };
    assert_eq!(
        client.get_file_info(read()).await.unwrap_err().code(),
        Code::PermissionDenied
    );
    client.set_object_acl(grants(vec![entry()])).await.unwrap();
    assert_eq!(
        client
            .get_file_info(read())
            .await
            .unwrap()
            .get_ref()
            .info
            .as_ref()
            .unwrap()
            .file_id,
        "first"
    );
    assert_eq!(
        client
            .delete_file(signed(
                DeleteFileRequest {
                    file_id: "first".into(),
                    ..Default::default()
                },
                "app-a",
                SignedActorType::User,
                "reader",
                ""
            ))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    client.set_object_acl(grants(vec![entry()])).await.unwrap();
    assert_eq!(
        effects(&pool).await.len(),
        1,
        "unchanged replacement does not duplicate evidence"
    );
    client.set_object_acl(grants(vec![])).await.unwrap();
    assert_eq!(
        client.get_file_info(read()).await.unwrap_err().code(),
        Code::PermissionDenied
    );
    assert_eq!(effects(&pool).await.len(), 2);
    sqlx::raw_sql("CREATE FUNCTION reject_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'owned audit fault'; END $$; CREATE TRIGGER reject_audit BEFORE INSERT ON core_audit_outbox FOR EACH ROW EXECUTE FUNCTION reject_audit()").execute(&*pool).await.unwrap();
    assert!(client.set_object_acl(grants(vec![entry()])).await.is_err());
    assert_eq!(
        client.get_file_info(read()).await.unwrap_err().code(),
        Code::PermissionDenied
    );
    assert_eq!(effects(&pool).await.len(), 2);
    sqlx::raw_sql("DROP TRIGGER reject_audit ON core_audit_outbox; DROP FUNCTION reject_audit()")
        .execute(&*pool)
        .await
        .unwrap();
    for permissions in [vec!["write".into()], vec!["read".into(), "delete".into()]] {
        assert_eq!(
            client
                .set_object_acl(grants(vec![AccessControlEntry {
                    principal: "user:reader".into(),
                    permissions
                }]))
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }
    assert!(
        client
            .set_object_acl(signed(
                SetObjectAclRequest {
                    file_id: "last".into(),
                    entries: vec![entry()]
                },
                "app-b",
                SignedActorType::Staff,
                "employee",
                "core:medias:write"
            ))
            .await
            .is_err()
    );
    let page = |cursor: &str| {
        signed(
            ListObjectsRequest {
                bucket: "media".into(),
                pagination: Some(flare_grpc_proto::Pagination {
                    cursor: cursor.into(),
                    limit: 1,
                    ..Default::default()
                }),
                ..Default::default()
            },
            "app-a",
            SignedActorType::Staff,
            "employee",
            "core:medias:read",
        )
    };
    let first = client.list_objects(page("")).await.unwrap().into_inner();
    assert_eq!(first.files.len(), 1);
    assert_eq!(first.files[0].file_id, "first");
    assert!(first.pagination.as_ref().unwrap().has_more);
    let last = client
        .list_objects(page(&first.pagination.unwrap().cursor))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(last.files.len(), 1);
    assert_eq!(last.files[0].file_id, "last");
    assert!(!last.pagination.unwrap().has_more);
    let mut wrong = page("");
    wrong.get_mut().bucket = "foreign-bucket".into();
    assert_eq!(
        client.list_objects(wrong).await.unwrap_err().code(),
        Code::PermissionDenied
    );
    let mut prefix = page("");
    prefix.get_mut().prefix = "../app-b".into();
    assert_eq!(
        client.list_objects(prefix).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    for row in effects(&pool).await {
        assert_eq!(row["aggregate"], "media_read_grants");
        assert_eq!(row["scope"], "app-a");
        assert_eq!(row["actor_type"], "staff");
    }
    drop(client);
    drop(running);
}
