//! Production gRPC receiver -> real isolated PG -> production runtime/key reader.
//! No full Core startup, Timescale/message path or socket-disconnect claim.
use ed25519_dalek::{
    SigningKey,
    pkcs8::{EncodePrivateKey, EncodePublicKey},
};
use flare_capability::{
    infrastructure::persistence::PostgresTenantProjectionRepository,
    interface::grpc::tenant_projection::TenantProjectionGrpcServer,
};
use flare_grpc_proto::control::{
    self, tenant_projection_client::TenantProjectionClient,
    tenant_projection_server::TenantProjectionServer,
};
use flare_im_service_kit::{
    control_auth::*,
    credential_projection::PostgresCredentialReader,
    tenant_runtime::{PgTenantRuntimeSource, TenantRuntimeSource},
};
use flare_server_core::auth::credential_verifier::LocalCredentialReader;
use futures_util::FutureExt;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{sync::Arc, time::Duration};
use tonic::{Code, Request};
struct Server(tokio::task::JoinHandle<Result<(), tonic::transport::Error>>);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}
fn request<T>(
    signer: &flare_server_core::TokenService,
    payload: T,
    app: &str,
    subject: &str,
) -> Request<T> {
    let token = signer
        .generate_with_ed25519(subject, Some(app), "control", CORE_PROJECTION_AUDIENCE)
        .expect("fixture control assertion");
    let mut r = Request::new(payload);
    r.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}")
            .parse()
            .expect("bounded fixture metadata"),
    );
    r
}
fn runtime(
    app: &str,
    status: control::TenantStatus,
    version: u64,
) -> control::TenantProjectionUpsert {
    control::TenantProjectionUpsert {
        organization_id: "991".into(),
        im_tenant_id: app.into(),
        name: "Owned functional application".into(),
        status: status as i32,
        quota: None,
        settings_json: b"{}".to_vec(),
        version,
    }
}
async fn exercise(pool: &PgPool) -> anyhow::Result<()> {
    // Only the three actual read-model CREATE blocks from the canonical Core
    // development DDL. Never execute its DROP/default seed/Timescale sections.
    let ddl = include_str!("../../deploy/init.sql");
    for name in [
        "tenants",
        "credential_projection_versions",
        "credential_keys",
    ] {
        let start = ddl
            .find(&format!("CREATE TABLE {name} ("))
            .expect("own canonical projection table");
        let end = ddl[start..].find("\n);").expect("complete table block") + start + 3;
        sqlx::raw_sql(&ddl[start..end]).execute(pool).await?;
    }
    let key = SigningKey::from_bytes(&[83; 32]);
    let private = key.to_pkcs8_pem(Default::default())?;
    let public = key.verifying_key().to_public_key_pem(Default::default())?;
    let signer =
        flare_server_core::TokenService::from_ed25519_private_key(&private, PLATFORM_ISSUER, 60)?;
    let service = TenantProjectionGrpcServer::new(
        Arc::new(PostgresTenantProjectionRepository::new(Arc::new(
            pool.clone(),
        ))),
        Some(PlatformAssertionVerifier::from_public_key(&public)?),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let incoming = Box::pin(futures_util::stream::unfold(
        listener,
        |listener| async move {
            let next = listener.accept().await.map(|p| p.0);
            Some((next, listener))
        },
    ));
    let _server = Server(tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(TenantProjectionServer::new(service))
            .serve_with_incoming(incoming),
    ));
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))?
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(3))
        .connect()
        .await?;
    let mut client = TenantProjectionClient::new(channel);
    assert_eq!(
        client
            .upsert_tenant(Request::new(runtime(
                "owned-a",
                control::TenantStatus::Active,
                1
            )))
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
    assert_eq!(
        client
            .upsert_tenant(request(
                &signer,
                runtime("owned-a", control::TenantStatus::Active, 1),
                "owned-b",
                PROJECTION_WORKER_SUBJECT
            ))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        client
            .upsert_tenant(request(
                &signer,
                runtime("owned-a", control::TenantStatus::Active, 1),
                "owned-a",
                SCHEDULER_SUBJECT
            ))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let source = PgTenantRuntimeSource::new(pool.clone());
    assert!(
        source
            .load("owned-a")
            .await
            .map_err(|_| anyhow::anyhow!("runtime read unavailable"))?
            .is_none()
    );
    let first = client
        .upsert_tenant(request(
            &signer,
            runtime("owned-a", control::TenantStatus::Active, 5),
            "owned-a",
            PROJECTION_WORKER_SUBJECT,
        ))
        .await?
        .into_inner();
    assert!(first.accepted);
    assert_eq!(first.current_version, 5);
    assert!(
        source
            .load("owned-a")
            .await
            .map_err(|_| anyhow::anyhow!("runtime read unavailable"))?
            .unwrap()
            .status
            .admits_connections()
    );
    client
        .upsert_tenant(request(
            &signer,
            runtime("owned-b", control::TenantStatus::Active, 7),
            "owned-b",
            PROJECTION_WORKER_SUBJECT,
        ))
        .await?;
    let suspended = client
        .upsert_tenant(request(
            &signer,
            runtime("owned-a", control::TenantStatus::Suspended, 6),
            "owned-a",
            PROJECTION_WORKER_SUBJECT,
        ))
        .await?
        .into_inner();
    assert!(suspended.accepted);
    assert!(
        !source
            .load("owned-a")
            .await
            .map_err(|_| anyhow::anyhow!("runtime read unavailable"))?
            .unwrap()
            .status
            .admits_connections()
    );
    let late = client
        .upsert_tenant(request(
            &signer,
            runtime("owned-a", control::TenantStatus::Active, 5),
            "owned-a",
            PROJECTION_WORKER_SUBJECT,
        ))
        .await?
        .into_inner();
    assert!(!late.accepted);
    assert_eq!(late.current_version, 6);
    assert!(
        source
            .load("owned-b")
            .await
            .map_err(|_| anyhow::anyhow!("runtime read unavailable"))?
            .unwrap()
            .status
            .admits_connections()
    );
    let versions = client
        .get_versions(request(
            &signer,
            control::GetVersionsRequest {
                im_tenant_ids: vec!["owned-a".into(), "owned-b".into()],
            },
            "*",
            PROJECTION_WORKER_SUBJECT,
        ))
        .await?
        .into_inner()
        .versions;
    assert_eq!(versions["owned-a"].tenant_version, 6);
    assert_eq!(versions["owned-b"].tenant_version, 7);
    // Real receiver and credential reader, including app-scoped empty revocation
    // watermarks. Hash-only fixture records; never issue or print a user secret.
    let snapshot = |app: &str, version, key_id: &str| control::CredentialsProjectionUpsert {
        app_id: app.into(),
        version,
        keys: vec![control::CredentialKey {
            key_id: key_id.into(),
            service_account_id: "account".into(),
            mode: 1,
            hash: vec![3; 32],
            pepper_fingerprint: vec![4; 32],
            scopes: vec!["platform.users.read".into()],
            expires_at_ms: 0,
            ip_allow: vec![],
            revoked: false,
            service_account_enabled: true,
        }],
    };
    let a = snapshot("owned-a", 9, "AAAAAAAAAAAA");
    client
        .upsert_credentials(request(
            &signer,
            a.clone(),
            "owned-a",
            PROJECTION_WORKER_SUBJECT,
        ))
        .await?;
    client
        .upsert_credentials(request(
            &signer,
            snapshot("owned-b", 8, "BBBBBBBBBBBB"),
            "owned-b",
            PROJECTION_WORKER_SUBJECT,
        ))
        .await?;
    let reader = PostgresCredentialReader::new(pool.clone());
    assert_eq!(reader.lookup("AAAAAAAAAAAA").await?.unwrap().version(), 9);
    let cleared = client
        .upsert_credentials(request(
            &signer,
            control::CredentialsProjectionUpsert {
                app_id: "owned-a".into(),
                version: 10,
                keys: vec![],
            },
            "owned-a",
            PROJECTION_WORKER_SUBJECT,
        ))
        .await?
        .into_inner();
    assert!(cleared.accepted);
    assert_eq!(cleared.current_version, 10);
    assert!(reader.lookup("AAAAAAAAAAAA").await?.is_none());
    assert_eq!(reader.lookup("BBBBBBBBBBBB").await?.unwrap().version(), 8);
    let stale = client
        .upsert_credentials(request(&signer, a, "owned-a", PROJECTION_WORKER_SUBJECT))
        .await?
        .into_inner();
    assert!(!stale.accepted);
    assert_eq!(stale.current_version, 10);
    assert!(reader.lookup("AAAAAAAAAAAA").await?.is_none());
    Ok(())
}
#[tokio::test]
#[ignore = "requires explicitly owned loopback PG admin, creates only nonce DB"]
async fn production_receiver_and_readers_on_owned_pg() {
    let options: PgConnectOptions = std::env::var("FLARE_FUNCTIONAL_PG_ADMIN_URL")
        .expect("owned fixture URL")
        .parse()
        .expect("fixture options");
    assert_eq!(options.get_host(), "127.0.0.1");
    assert_eq!(options.get_port(), 19886);
    assert_eq!(options.get_username(), "r4route_admin");
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options.clone())
        .await
        .expect("owned admin");
    let name = format!("flare_receiver_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE DATABASE {name}"))
        .execute(&admin)
        .await
        .expect("create own nonce DB");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options.database(&name))
        .await
        .expect("own nonce connection");
    let result = std::panic::AssertUnwindSafe(exercise(&pool))
        .catch_unwind()
        .await;
    pool.close().await;
    sqlx::query(&format!("DROP DATABASE {name} WITH (FORCE)"))
        .execute(&admin)
        .await
        .expect("drop own nonce DB");
    admin.close().await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(_)) => panic!("owned receiver functional check failed; no secret diagnostic"),
        Err(p) => std::panic::resume_unwind(p),
    }
}
