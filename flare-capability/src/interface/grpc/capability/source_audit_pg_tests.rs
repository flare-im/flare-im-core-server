//! Functional owner tests in a fresh nonce PG database. No existing store, migrations,
//! running services, broker, HTTP/gRPC transport or full-stack acceptance is implied.
use super::{CapabilityGrpcServer, CapabilityInvocationMetrics};
use crate::{
    composition::hook_registry::CoreHookRegistry,
    infrastructure::{
        capability::{CapabilityExtensionRegistry, PluginRouteBook},
        config::{CapabilityRuntimeConfig, ConfigLoaderItem, ConfigWatcher, DatabaseConfigLoader},
        persistence::{
            PostgresCapabilityPolicy, PostgresHookConfigRepository,
            PostgresPluginEndpointRepository,
        },
    },
    interface::grpc::HookServiceServer,
};
use ed25519_dalek::{
    SigningKey,
    pkcs8::{EncodePrivateKey, EncodePublicKey},
};
use flare_grpc_proto::capability::{capability_service_server::CapabilityService, *};
use flare_im_service_kit::audit_effects::durable::{EvidencePublisher, StaffAudit, drain_one};
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, SignedContextVerifier,
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
        CreateHookConfigRequest {
            tenant_id: "app-a".into(),
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
fn grant(user: &str) -> GrantUserCapabilityRequest {
    GrantUserCapabilityRequest {
        tenant_id: "app-a".into(),
        user_id: user.into(),
        capability_id: "rtc.call".into(),
        ..Default::default()
    }
}
fn plugin(name: &str) -> RegisterPluginEndpointRequest {
    RegisterPluginEndpointRequest {
        tenant_id: "app-a".into(),
        plugin_id: name.into(),
        capability_id: "rtc.call".into(),
        grpc_authority: "http://127.0.0.1:9".into(),
        declared_operations: vec!["rtc.call".into()],
        ..Default::default()
    }
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
        for ddl in [
            include_str!("../../../../../migrations/0011_20261009_plugin_endpoints.sql"),
            include_str!("../../../../../migrations/0012_20261015_core_audit_outbox.sql"),
        ] {
            sqlx::raw_sql(ddl).execute(&*run).await.unwrap();
        }
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
    let watcher = Arc::new(ConfigWatcher::new(
        vec![Arc::new(ConfigLoaderItem::Database(
            DatabaseConfigLoader::new(repository.clone(), None),
        ))],
        Duration::from_secs(3600),
    ));
    watcher.reload().await.unwrap();
    let governance = Arc::new(HookServiceServer::new(
        repository,
        Arc::new(CoreHookRegistry::new(watcher)),
    ));
    let runtime = CapabilityRuntimeConfig {
        max_payload_json_bytes: 1024,
        dispatch_timeout: Duration::from_secs(1),
        dispatch_max_per_minute: None,
        plugin_discovery_endpoints: vec![],
        plugin_health_check_interval: Duration::from_secs(3600),
        plugin_call_timeout: Duration::from_millis(10),
        plugin_route_refresh_interval: Duration::from_secs(3600),
        plugin_route_ttl: Duration::from_secs(86400),
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
        Arc::new(PluginRouteBook::with_store(Arc::new(
            PostgresPluginEndpointRepository::new(pool),
        ))),
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
        "capability_user_grants",
        "capability_tenant_switches",
        "capability_plugin_endpoints",
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
        // Let the empty route-book health pass finish; no real plugin server is contacted.
        tokio::task::yield_now().await;
        let created = server
            .administer(request(create("first"), "app-a", "core:hooks:write"))
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
            server.administer(request(create("rejected"), "app-a", "core:hooks:write")),
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
                "app-a",
                "core:hooks:write",
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
                "app-a",
                "core:hooks:write",
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
                "app-a",
                "core:hooks:write",
            )
        };
        atomic_failure(&pool, server.administer(remove())).await;
        server.administer(remove()).await.unwrap();

        atomic_failure(
            &pool,
            server.grant_user_capability(request(grant("u1"), "app-a", "core:capabilities:write")),
        )
        .await;
        server
            .grant_user_capability(request(grant("u1"), "app-a", "core:capabilities:write"))
            .await
            .unwrap();
        let revoke = || {
            request(
                RevokeUserCapabilityRequest {
                    tenant_id: "app-a".into(),
                    user_id: "u1".into(),
                    capability_id: "rtc.call".into(),
                },
                "app-a",
                "core:capabilities:write",
            )
        };
        atomic_failure(&pool, server.revoke_user_capability(revoke())).await;
        server.revoke_user_capability(revoke()).await.unwrap();
        let count = effects(&pool).await.len();
        server.revoke_user_capability(revoke()).await.unwrap();
        assert_eq!(effects(&pool).await.len(), count);
        let switch = || {
            request(
                SetTenantCapabilitySwitchRequest {
                    tenant_id: "app-a".into(),
                    capability_id: "rtc.call".into(),
                    enabled: true,
                },
                "app-a",
                "core:capabilities:write",
            )
        };
        atomic_failure(&pool, server.set_tenant_capability_switch(switch())).await;
        server.set_tenant_capability_switch(switch()).await.unwrap();
        atomic_failure(
            &pool,
            server.register_plugin_endpoint(request(plugin("p1"), "app-a", "core:plugins:write")),
        )
        .await;
        let list = || {
            request(
                ListRegisteredPluginsRequest {
                    tenant_id: "app-a".into(),
                    ..Default::default()
                },
                "app-a",
                "core:plugins:read",
            )
        };
        assert!(
            server
                .list_registered_plugins(list())
                .await
                .unwrap()
                .into_inner()
                .instances
                .is_empty(),
            "failed source transaction cannot enter the route book"
        );
        server
            .register_plugin_endpoint(request(plugin("p1"), "app-a", "core:plugins:write"))
            .await
            .unwrap();
        let deregister = || {
            request(
                DeregisterPluginEndpointRequest {
                    tenant_id: "app-a".into(),
                    plugin_id: "p1".into(),
                    ..Default::default()
                },
                "app-a",
                "core:plugins:write",
            )
        };
        atomic_failure(&pool, server.deregister_plugin_endpoint(deregister())).await;
        assert_eq!(
            server
                .list_registered_plugins(list())
                .await
                .unwrap()
                .into_inner()
                .instances
                .len(),
            1,
            "failed delete cannot remove the in-memory route"
        );
        server
            .deregister_plugin_endpoint(deregister())
            .await
            .unwrap();
        let count = effects(&pool).await.len();
        server
            .deregister_plugin_endpoint(deregister())
            .await
            .unwrap();
        assert_eq!(effects(&pool).await.len(), count);

        let rows = effects(&pool).await;
        assert_eq!(rows.len(), 9);
        for row in rows {
            assert_eq!(row["request_id"], "signed-owner-request");
            assert_eq!(row["actor_id"], "employee");
            assert_eq!(row["actor_type"], "staff");
            assert_eq!(row["scope"], "app-a");
            assert_eq!(row["source"], "flare-capability");
            assert_eq!(row["outcome"], "committed");
            let expected = match row["aggregate"].as_str().unwrap() {
                "hook_config" => "core:hooks:write",
                "plugin_endpoint" => "core:plugins:write",
                "capability_grant" | "capability_switch" => "core:capabilities:write",
                _ => panic!("unexpected owner aggregate"),
            };
            assert_eq!(row["permission"], expected);
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
        // A post-commit runtime reload failure must not erase evidence of the committed fact.
        // Deliberately corrupt this private fixture's selector inside the source insert.
        sqlx::raw_sql("CREATE FUNCTION broken_selector() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.selector_config = '{\"tenants\":5}'::jsonb; RETURN NEW; END $$; CREATE TRIGGER broken_selector BEFORE INSERT ON hook_configs FOR EACH ROW EXECUTE FUNCTION broken_selector()")
            .execute(&*pool).await.unwrap();
        let reload = server.administer(request(create("reload-error"), "app-a", "core:hooks:write")).await.unwrap_err();
        assert_eq!(reload.code(), Code::Internal);
        let committed: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM hook_configs WHERE tenant_id='app-a' AND name='reload-error')").fetch_one(&*pool).await.unwrap();
        assert!(committed);
        assert_eq!(effects(&pool).await.len(), 10);
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
        let staff = flare_im_service_kit::staff_context::verify_staff_metadata(
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
            let result = flare_im_service_kit::audit_effects::durable::record(
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
