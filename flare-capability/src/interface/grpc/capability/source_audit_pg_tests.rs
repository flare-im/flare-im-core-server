//! Functional owner tests in a fresh nonce PG database. No existing store, migrations,
//! running services, broker, HTTP/gRPC transport or full-stack acceptance is implied.
use super::{CapabilityGrpcServer, CapabilityInvocationMetrics};
use crate::{
    infrastructure::{
        capability::{CapabilityExtensionRegistry, PluginRouteBook},
        config::CapabilityRuntimeConfig,
        persistence::{PostgresCapabilityPolicy, PostgresHookConfigRepository},
    },
    interface::grpc::HookServiceServer,
};
use ed25519_dalek::{
    SigningKey,
    pkcs8::{EncodePrivateKey, EncodePublicKey},
};
use flare_grpc_proto::capability::{capability_service_server::CapabilityService, *};
use flare_im_core_service_kit::audit_effects::durable::{EvidencePublisher, StaffAudit, drain_one};
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, SignedContextVerifier,
    auth::core_admin::{PLATFORM_SCOPE_TENANT, permission::HOOKS_PLATFORM_WRITE},
    context::{Context, Ctx},
};
use prost::Message;
use sqlx::{
    ConnectOptions, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{
    future::Future,
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tonic::{Code, Request};

fn keys() -> (SignedContextSigner, SignedContextVerifier) {
    let key = SigningKey::from_bytes(&[92; 32]);
    (
        SignedContextSigner::from_private_key(
            key.to_pkcs8_pem(Default::default()).unwrap().as_str(),
        )
        .unwrap()
        .with_kid("gateway")
        .unwrap(),
        SignedContextVerifier::from_public_key(
            &key.verifying_key()
                .to_public_key_pem(Default::default())
                .unwrap(),
        )
        .unwrap()
        .require_staff_kid(["gateway"]),
    )
}
fn request<T>(body: T, app: &str, permission: &str) -> Request<T> {
    let (signer, _) = keys();
    let token = signer
        .sign(
            &SignedContextIdentity::new(
                SignedActorType::Staff,
                "employee",
                app,
                "signed-owner-request",
            )
            .with_staff_grant(None, Some(permission.into()), 1),
        )
        .unwrap();
    let mut request = Request::new(body);
    flare_server_core::set_signed_context_token(&mut request, &token).unwrap();
    request
}
fn hook(operation: &str, body: impl Message) -> GenericRequest {
    GenericRequest {
        operation: format!("flare.extension.v1.hook_config.{operation}"),
        request_id: "untrusted-payload-id".into(),
        payload: Some(prost_types::Any {
            type_url: String::new(),
            value: body.encode_to_vec(),
        }),
        ..Default::default()
    }
}
fn create(name: &str) -> GenericRequest {
    hook(
        "create",
        // Application hooks cannot be created yet; the owner path is exercised on platform hooks.
        CreateHookConfigRequest {
            name: name.into(),
            hook_type: "pre_send".into(),
            transport: Some(HookTransport {
                r#type: "webhook".into(),
                endpoint: "http://127.0.0.1:9/hook".into(),
                timeout_ms: 500,
                ..Default::default()
            }),
            ..Default::default()
        },
    )
}
async fn database<F, Fut>(work: F)
where
    F: FnOnce(Arc<PgPool>, String) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let url =
        std::env::var("FLARE_FUNCTIONAL_PG_ADMIN_URL").expect("explicit owned-test PG required");
    let admin = PgPool::connect(&url)
        .await
        .expect("owned-test PG connection");
    let name = format!("flare_core_audit_test_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE DATABASE {name}"))
        .execute(&admin)
        .await
        .unwrap();
    let options = PgConnectOptions::from_str(&url).unwrap().database(&name);
    let url = options.to_url_lossy().to_string();
    let pool = Arc::new(
        PgPoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await
            .unwrap(),
    );
    let run = pool.clone();
    let outcome = tokio::spawn(async move {
        let baseline = include_str!("../../../../../migrations/0001_baseline.sql");
        let start = baseline.find("CREATE TABLE hook_configs").unwrap();
        let end = baseline
            .find("-- FLARE_EXTRACT:END_HOOK_CAPABILITY")
            .unwrap();
        sqlx::raw_sql(&baseline[start..end])
            .execute(&*run)
            .await
            .unwrap();
        sqlx::raw_sql(include_str!(
            "../../../../../migrations/0012_20261015_core_audit_outbox.sql"
        ))
        .execute(&*run)
        .await
        .unwrap();
        // 0024 gives hook_configs its current shape and the scope versions the repository bumps.
        sqlx::raw_sql(include_str!(
            "../../../../../migrations/0024_20261107_hook_runtime_unification.sql"
        ))
        .execute(&*run)
        .await
        .unwrap();
        work(run, url).await;
    })
    .await;
    pool.close().await;
    sqlx::query(&format!("DROP DATABASE {name} WITH (FORCE)"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
    if let Err(error) = outcome {
        std::panic::resume_unwind(error.into_panic());
    }
}
async fn server(pool: Arc<PgPool>, url: &str) -> CapabilityGrpcServer {
    let repository = Arc::new(PostgresHookConfigRepository::new(url).await.unwrap());
    let governance = Arc::new(HookServiceServer::new(repository));
    let runtime = CapabilityRuntimeConfig {
        max_payload_json_bytes: 1024,
        dispatch_timeout: Duration::from_secs(1),
        dispatch_max_per_minute: None,
        plugin_call_timeout: Duration::from_millis(10),
    };
    let (_, verifier) = keys();
    CapabilityGrpcServer::new(
        CapabilityExtensionRegistry::new(),
        Arc::new(PostgresCapabilityPolicy::new(pool.clone())),
        Arc::new(runtime),
        None,
        None,
        Arc::new(CapabilityInvocationMetrics::default()),
        Some(governance),
        Arc::new(PluginRouteBook::new()),
        Some(verifier.clone()),
        Some(verifier),
    )
}
async fn effects(pool: &PgPool) -> Vec<serde_json::Value> {
    sqlx::query_scalar::<_, String>("SELECT payload::text FROM core_audit_outbox ORDER BY event_id")
        .fetch_all(pool)
        .await
        .unwrap()
        .into_iter()
        .map(|value| serde_json::from_str(&value).unwrap())
        .collect()
}
async fn facts(pool: &PgPool) -> Vec<String> {
    let mut rows = vec![];
    for table in [
        "hook_configs",
        "capability_tenant_switches",
        "core_audit_outbox",
    ] {
        rows.push(sqlx::query_scalar(&format!("SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb)::text FROM {table} t")).fetch_one(pool).await.unwrap());
    }
    rows
}
async fn reject(pool: &PgPool, enabled: bool) {
    let ddl = if enabled {
        "CREATE FUNCTION reject_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'owned audit fault'; END $$; CREATE TRIGGER reject_audit BEFORE INSERT ON core_audit_outbox FOR EACH ROW EXECUTE FUNCTION reject_audit()"
    } else {
        "DROP TRIGGER reject_audit ON core_audit_outbox; DROP FUNCTION reject_audit()"
    };
    sqlx::raw_sql(ddl).execute(pool).await.unwrap();
}
async fn atomic_failure<T, E>(pool: &PgPool, work: impl Future<Output = Result<T, E>>) {
    reject(pool, true).await;
    let before = facts(pool).await;
    assert!(
        work.await.is_err(),
        "audit-tail failure must reject owner command"
    );
    assert!(
        facts(pool).await == before,
        "owner facts and evidence must roll back together"
    );
    reject(pool, false).await;
}

#[tokio::test]
#[ignore = "requires explicit owned-test FLARE_FUNCTIONAL_PG_ADMIN_URL"]
async fn normal_staff_rpc_writes_commit_source_evidence_and_tail_failures_roll_back() {
    database(|pool, url| async move {
        let server = server(pool.clone(), &url).await;
        let created = server
            .administer(request(create("first"), PLATFORM_SCOPE_TENANT, HOOKS_PLATFORM_WRITE))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(created.request_id, "signed-owner-request");
        let id = CreateHookConfigResponse::decode(created.payload.unwrap().value.as_slice())
            .unwrap()
            .config
            .unwrap()
            .hook_id;
        atomic_failure(
            &pool,
            server.administer(request(create("rejected"), PLATFORM_SCOPE_TENANT, HOOKS_PLATFORM_WRITE)),
        )
        .await;
        let update = || {
            request(
                hook(
                    "update",
                    UpdateHookConfigRequest {
                        hook_id: id.clone(),
                        name: "renamed".into(),
                        ..Default::default()
                    },
                ),
                PLATFORM_SCOPE_TENANT,
                HOOKS_PLATFORM_WRITE,
            )
        };
        atomic_failure(&pool, server.administer(update())).await;
        server.administer(update()).await.unwrap();
        let stored: String = sqlx::query_scalar("SELECT name FROM hook_configs WHERE id=$1")
            .bind(id.parse::<i64>().unwrap())
            .fetch_one(&*pool)
            .await
            .unwrap();
        assert_eq!(stored, "renamed");
        let status = || {
            request(
                hook(
                    "set_status",
                    SetHookStatusRequest {
                        hook_id: id.clone(),
                        enabled: false,
                    },
                ),
                PLATFORM_SCOPE_TENANT,
                HOOKS_PLATFORM_WRITE,
            )
        };
        atomic_failure(&pool, server.administer(status())).await;
        server.administer(status()).await.unwrap();
        let count = effects(&pool).await.len();
        server.administer(status()).await.unwrap();
        assert_eq!(
            effects(&pool).await.len(),
            count,
            "unchanged status is not another committed effect"
        );
        let before = facts(&pool).await;
        let denied = server
            .administer(request(
                hook(
                    "delete",
                    DeleteHookConfigRequest {
                        hook_id: id.clone(),
                    },
                ),
                "app-b",
                "core:hooks:write",
            ))
            .await
            .unwrap_err();
        assert_eq!(denied.code(), Code::PermissionDenied);
        assert!(before == facts(&pool).await);
        let remove = || {
            request(
                hook(
                    "delete",
                    DeleteHookConfigRequest {
                        hook_id: id.clone(),
                    },
                ),
                PLATFORM_SCOPE_TENANT,
                HOOKS_PLATFORM_WRITE,
            )
        };
        atomic_failure(&pool, server.administer(remove())).await;
        server.administer(remove()).await.unwrap();

        let switch = || {
            request(
                SetTenantCapabilitySwitchRequest {
                    tenant_id: "app-a".into(),
                    capability_id: "example.poll".into(),
                    enabled: true,
                },
                "app-a",
                "core:capabilities:write",
            )
        };
        atomic_failure(&pool, server.set_tenant_capability_switch(switch())).await;
        server.set_tenant_capability_switch(switch()).await.unwrap();

        let rows = effects(&pool).await;
        assert_eq!(rows.len(), 5);
        for row in rows {
            assert_eq!(row["request_id"], "signed-owner-request");
            assert_eq!(row["actor_id"], "employee");
            assert_eq!(row["actor_type"], "staff");
            assert_eq!(row["source"], "flare-capability");
            assert_eq!(row["outcome"], "committed");
            let (scope, permission) = match row["aggregate"].as_str().unwrap() {
                "hook_config" => (PLATFORM_SCOPE_TENANT, HOOKS_PLATFORM_WRITE),
                "capability_switch" => ("app-a", "core:capabilities:write"),
                _ => panic!("unexpected owner aggregate"),
            };
            assert_eq!(row["scope"], scope);
            assert_eq!(row["permission"], permission);
            for field in [
                "body",
                "headers",
                "metadata",
                "transport",
                "token",
                "secret",
                "reason",
            ] {
                assert!(row.get(field).is_none());
            }
        }
        // Capability only writes configuration; Message Ingest loads it (and rejects a bad row
        // there). A row the runtime cannot read is still a committed fact with its evidence.
        // Deliberately corrupt this private fixture's selector inside the source insert.
        sqlx::raw_sql("CREATE FUNCTION broken_selector() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.selector_config = '{\"tenants\":5}'::jsonb; RETURN NEW; END $$; CREATE TRIGGER broken_selector BEFORE INSERT ON hook_configs FOR EACH ROW EXECUTE FUNCTION broken_selector()")
            .execute(&*pool).await.unwrap();
        server.administer(request(create("reload-error"), PLATFORM_SCOPE_TENANT, HOOKS_PLATFORM_WRITE)).await.unwrap();
        let committed: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM hook_configs WHERE tenant_id IS NULL AND name='reload-error')").fetch_one(&*pool).await.unwrap();
        assert!(committed);
        assert_eq!(effects(&pool).await.len(), 6, "the five writes above and this one");
    })
    .await;
}

#[derive(Default)]
struct Publisher {
    fail: AtomicBool,
    pending: AtomicBool,
    attempts: Mutex<Vec<(String, String, Vec<u8>)>>,
}
#[async_trait::async_trait]
impl EvidencePublisher for Publisher {
    async fn publish(&self, id: &str, app: &str, payload: Vec<u8>) -> Result<(), ()> {
        self.attempts
            .lock()
            .unwrap()
            .push((id.into(), app.into(), payload));
        if self.pending.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        if self.fail.load(Ordering::SeqCst) {
            Err(())
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
#[ignore = "requires explicit owned-test FLARE_FUNCTIONAL_PG_ADMIN_URL"]
async fn source_scope_and_queue_survive_failure_cancellation_and_retry_without_identity_drift() {
    database(|pool, _url| async move {
        let carrier = request((), "app-a", "core:hooks:write");
        let staff = flare_im_core_service_kit::staff_context::verify_staff_metadata(
            carrier.metadata(),
            &keys().1,
            "core:hooks:write",
        )
        .unwrap();
        // Unsigned actors cannot fabricate Staff evidence.
        let forged: Ctx = Arc::new(
            Context::with_request_id("forged")
                .with_user_id("employee")
                .with_tenant_id("app-a"),
        );
        let enqueue = |pool: Arc<PgPool>, app: &'static str| async move {
            let mut tx = pool.begin().await.unwrap();
            let result = flare_im_core_service_kit::audit_effects::durable::record(
                &mut tx,
                app,
                "hook_config",
                "h1",
                "update",
            )
            .await;
            if result.is_ok() {
                tx.commit().await.unwrap();
            }
            result
        };
        enqueue(pool.clone(), "app-a")
            .staff_audit(&forged, "flare-capability")
            .await
            .unwrap();
        assert!(effects(&pool).await.is_empty());
        assert!(
            enqueue(pool.clone(), "app-b")
                .staff_audit(&staff.ctx, "flare-capability")
                .await
                .is_err()
        );
        assert!(effects(&pool).await.is_empty());
        async {
            assert!(
                tokio::spawn(enqueue(pool.clone(), "app-a"))
                    .await
                    .unwrap()
                    .is_ok()
            );
            assert!(
                effects(&pool).await.is_empty(),
                "spawned tasks do not inherit source attribution"
            );
            enqueue(pool.clone(), "app-a").await.unwrap();
        }
        .staff_audit(&staff.ctx, "flare-capability")
        .await;
        enqueue(pool.clone(), "app-a").await.unwrap();
        assert_eq!(
            effects(&pool).await.len(),
            1,
            "scope must end with the command"
        );

        let publisher = Publisher::default();
        publisher.fail.store(true, Ordering::SeqCst);
        assert!(drain_one(&pool, &publisher).await.unwrap());
        let pending: (i32, bool) =
            sqlx::query_as("SELECT attempts,delivered_at IS NULL FROM core_audit_outbox")
                .fetch_one(&*pool)
                .await
                .unwrap();
        assert_eq!(pending, (1, true));
        sqlx::query("UPDATE core_audit_outbox SET next_at=clock_timestamp()")
            .execute(&*pool)
            .await
            .unwrap();
        publisher.pending.store(true, Ordering::SeqCst);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), drain_one(&pool, &publisher))
                .await
                .is_err()
        );
        publisher.pending.store(false, Ordering::SeqCst);
        publisher.fail.store(false, Ordering::SeqCst);
        assert!(drain_one(&pool, &publisher).await.unwrap());
        assert!(!drain_one(&pool, &publisher).await.unwrap());
        {
            let attempts = publisher.attempts.lock().unwrap();
            assert_eq!(attempts.len(), 3);
            assert!(
                attempts.iter().all(|attempt| attempt == &attempts[0]),
                "same ID, scope and wire bytes on every attempt"
            );
        }
        let delivered: bool =
            sqlx::query_scalar("SELECT delivered_at IS NOT NULL FROM core_audit_outbox")
                .fetch_one(&*pool)
                .await
                .unwrap();
        assert!(delivered);
    })
    .await;
}
