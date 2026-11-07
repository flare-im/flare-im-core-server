//! Production gRPC receiver -> real isolated PG -> tenant notice relay -> real Redis channel, and
//! the runtime cache gateways read. No full Core startup.
//!
//! `FLARE_CLIENT_POLICY_PG_ADMIN_URL` names a throwaway loopback server; each run creates and drops
//! its own database. `FLARE_CLIENT_POLICY_REDIS_URL` names a throwaway loopback Redis.
use ed25519_dalek::{
    SigningKey,
    pkcs8::{EncodePrivateKey, EncodePublicKey},
};
use flare_capability::{
    application::handler::tenant_notice_relay::TenantNoticeRelay,
    domain::repository::tenant_notice::TenantNotices,
    infrastructure::{
        adapters::redis_tenant_notices::RedisTenantNotices,
        persistence::{
            PostgresTenantProjectionRepository, postgres_client_policy::PostgresTenantNoticeOutbox,
        },
    },
    interface::grpc::tenant_projection::TenantProjectionGrpcServer,
};
use flare_grpc_proto::control::{
    BuildPolicy, BuildRange, ClientPolicyUpsert, GetVersionsRequest,
    tenant_projection_client::TenantProjectionClient,
    tenant_projection_server::TenantProjectionServer,
};
use flare_im_service_kit::{
    control_auth::{
        CORE_PROJECTION_AUDIENCE, PLATFORM_ISSUER, PROJECTION_WORKER_SUBJECT,
        PlatformAssertionVerifier,
    },
    tenant_notice::TenantNotice,
    tenant_runtime::{
        PgTenantRuntimeSource, TenantRuntimeCache, TenantRuntimeCacheOptions, TenantRuntimeSource,
    },
};
use futures_util::{FutureExt, StreamExt};
use sqlx::{
    PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tonic::{Code, Request, transport::Channel};

const SCHEMA: &str = include_str!("../../migrations/0023_20261105_tenant_client_policy.sql");
const APP: &str = "owned-a";

struct Server(tokio::task::JoinHandle<Result<(), tonic::transport::Error>>);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct Worker {
    signer: flare_server_core::TokenService,
    client: TenantProjectionClient<Channel>,
}

impl Worker {
    fn signed<T>(&self, payload: T, app: &str) -> Request<T> {
        let token = self
            .signer
            .generate_with_ed25519(
                PROJECTION_WORKER_SUBJECT,
                Some(app),
                "control",
                CORE_PROJECTION_AUDIENCE,
            )
            .expect("fixture control assertion");
        let mut request = Request::new(payload);
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {token}")
                .parse()
                .expect("bounded fixture metadata"),
        );
        request
    }

    async fn upsert(&self, body: ClientPolicyUpsert) -> Result<(bool, u64), tonic::Status> {
        let app = body.app_id.clone();
        let ack = self
            .client
            .clone()
            .upsert_client_policy(self.signed(body, &app))
            .await?
            .into_inner();
        Ok((ack.accepted, ack.current_version))
    }
}

fn policy(version: u64, serial: u64, generation: u64, min_build: u64) -> ClientPolicyUpsert {
    ClientPolicyUpsert {
        app_id: APP.into(),
        version,
        access_serial: serial,
        release_generation: generation,
        build_policies: HashMap::from([(
            "android".to_string(),
            BuildPolicy {
                min_build,
                min_build_effective_from_ms: 1_700_000_000_000,
                blocked: vec![BuildRange { from: 130, to: 135 }],
            },
        )]),
    }
}

async fn start(pool: &PgPool) -> anyhow::Result<(Server, Worker)> {
    let key = SigningKey::from_bytes(&[91; 32]);
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
    let server = Server(tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(TenantProjectionServer::new(service))
            .serve_with_incoming(incoming),
    ));
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))?
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(5))
        .connect()
        .await?;
    Ok((
        server,
        Worker {
            signer,
            client: TenantProjectionClient::new(channel),
        },
    ))
}

async fn outbox(pool: &PgPool) -> anyhow::Result<Vec<(String, i64)>> {
    Ok(sqlx::query_as(
        "SELECT topic, version FROM tenant_notice_outbox WHERE tenant_id = $1 ORDER BY topic",
    )
    .bind(APP)
    .fetch_all(pool)
    .await?)
}

async fn stored(pool: &PgPool) -> anyhow::Result<(i64, i64, i64, i64)> {
    let row = sqlx::query(
        "SELECT version, access_serial, release_generation, \
         (build_policies->'android'->>'min_build')::bigint AS min_build \
         FROM tenant_client_policy WHERE tenant_id = $1",
    )
    .bind(APP)
    .fetch_one(pool)
    .await?;
    Ok((
        row.get("version"),
        row.get("access_serial"),
        row.get("release_generation"),
        row.get("min_build"),
    ))
}

/// Version idempotence and the rule that a notice exists exactly when a watermark moved.
async fn idempotence(pool: &PgPool, worker: &Worker) -> anyhow::Result<()> {
    assert_eq!(worker.upsert(policy(5, 40, 2, 120)).await?, (true, 5));
    assert_eq!(stored(pool).await?, (5, 40, 2, 120));
    assert_eq!(
        outbox(pool).await?,
        vec![("flare.access".into(), 40), ("flare.release".into(), 2)]
    );

    // A replay of the version and an older one change nothing, whatever they carry.
    assert_eq!(worker.upsert(policy(5, 99, 9, 999)).await?, (false, 5));
    assert_eq!(worker.upsert(policy(4, 99, 9, 999)).await?, (false, 5));
    assert_eq!(stored(pool).await?, (5, 40, 2, 120));
    assert_eq!(outbox(pool).await?.len(), 2);

    sqlx::query("DELETE FROM tenant_notice_outbox")
        .execute(pool)
        .await?;

    // Only the build policies changed: stored, not announced.
    assert_eq!(worker.upsert(policy(6, 40, 2, 121)).await?, (true, 6));
    assert_eq!(stored(pool).await?, (6, 40, 2, 121));
    assert!(outbox(pool).await?.is_empty());

    // A newer version cannot move a watermark back.
    assert_eq!(worker.upsert(policy(7, 39, 1, 122)).await?, (true, 7));
    assert_eq!(stored(pool).await?, (7, 40, 2, 122));
    assert!(outbox(pool).await?.is_empty());

    // One watermark moved: one notice.
    assert_eq!(worker.upsert(policy(8, 41, 2, 122)).await?, (true, 8));
    assert_eq!(outbox(pool).await?, vec![("flare.access".into(), 41)]);

    // A newer version takes over the pending row.
    assert_eq!(worker.upsert(policy(9, 42, 2, 122)).await?, (true, 9));
    assert_eq!(outbox(pool).await?, vec![("flare.access".into(), 42)]);

    let versions = worker
        .client
        .clone()
        .get_versions(worker.signed(
            GetVersionsRequest {
                im_tenant_ids: vec![APP.into()],
            },
            APP,
        ))
        .await?
        .into_inner()
        .versions;
    assert_eq!(versions[APP].client_policy_version, 9);
    Ok(())
}

/// The projection and its notice commit together: no outbox, no projection.
async fn atomicity(pool: &PgPool, worker: &Worker) -> anyhow::Result<()> {
    let before = stored(pool).await?;
    sqlx::query("ALTER TABLE tenant_notice_outbox RENAME TO tenant_notice_outbox_gone")
        .execute(pool)
        .await?;
    let failed = worker.upsert(policy(10, 50, 2, 122)).await;
    sqlx::query("ALTER TABLE tenant_notice_outbox_gone RENAME TO tenant_notice_outbox")
        .execute(pool)
        .await?;
    assert_ne!(failed.unwrap_err().code(), Code::Ok);
    assert_eq!(stored(pool).await?, before, "the projection rolled back");

    // The worker's retry of the same version succeeds and owes the same notice.
    assert_eq!(worker.upsert(policy(10, 50, 2, 122)).await?, (true, 10));
    assert_eq!(outbox(pool).await?, vec![("flare.access".into(), 50)]);
    Ok(())
}

/// Concurrent first projections of one application end on the highest version and serial.
async fn racing_first_projections(pool: &PgPool, worker: &Worker) -> anyhow::Result<()> {
    const APP_B: &str = "owned-b";
    let body = |version: u64| ClientPolicyUpsert {
        app_id: APP_B.into(),
        version,
        access_serial: version * 10,
        release_generation: 0,
        build_policies: HashMap::new(),
    };
    let results =
        futures_util::future::join_all((1..=16).map(|version| worker.upsert(body(version)))).await;
    assert_eq!(results.iter().filter(|r| r.is_err()).count(), 0);
    let row = sqlx::query(
        "SELECT p.version, p.access_serial, o.version AS notice \
         FROM tenant_client_policy p JOIN tenant_notice_outbox o USING (tenant_id) \
         WHERE tenant_id = $1 AND o.topic = 'flare.access'",
    )
    .bind(APP_B)
    .fetch_one(pool)
    .await?;
    assert_eq!(row.get::<i64, _>("version"), 16);
    assert_eq!(row.get::<i64, _>("access_serial"), 160);
    assert_eq!(row.get::<i64, _>("notice"), 160);
    Ok(())
}

/// A channel that cannot be reached keeps the row and records why.
async fn relay_survives_a_channel_outage(pool: &PgPool) -> anyhow::Result<()> {
    let closed = redis::Client::open("redis://127.0.0.1:1/")?;
    let relay = TenantNoticeRelay::new(
        Arc::new(PostgresTenantNoticeOutbox {
            pool: Arc::new(pool.clone()),
        }),
        Arc::new(RedisTenantNotices::new(closed, "owned:notice".into())) as Arc<dyn TenantNotices>,
    );
    assert_eq!(relay.publish_due().await, 2);
    let rows: Vec<(i32, Option<String>, bool)> = sqlx::query_as(
        "SELECT attempts, last_error, next_attempt_at > CURRENT_TIMESTAMP \
         FROM tenant_notice_outbox ORDER BY tenant_id",
    )
    .fetch_all(pool)
    .await?;
    assert_eq!(rows.len(), 2);
    for (attempts, error, later) in rows {
        assert_eq!(attempts, 1);
        assert!(error.is_some());
        assert!(later);
    }
    assert_eq!(relay.publish_due().await, 0, "not due again yet");
    sqlx::query("UPDATE tenant_notice_outbox SET next_attempt_at = CURRENT_TIMESTAMP")
        .execute(pool)
        .await?;
    Ok(())
}

/// The relay reaches a subscriber of the real channel once per version and empties the outbox.
async fn relay_publishes(pool: &PgPool, redis_url: &str) -> anyhow::Result<()> {
    let channel = format!("owned-{}:notice", uuid::Uuid::new_v4().simple());
    let client = redis::Client::open(redis_url)?;
    let mut subscriber = client.get_async_pubsub().await?;
    subscriber.subscribe(&channel).await?;
    let notices: Arc<dyn TenantNotices> =
        Arc::new(RedisTenantNotices::new(client, channel.clone()));
    let relay = TenantNoticeRelay::new(
        Arc::new(PostgresTenantNoticeOutbox {
            pool: Arc::new(pool.clone()),
        }),
        notices,
    );
    let mut stream = subscriber.on_message();
    assert_eq!(relay.publish_due().await, 2);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tenant_notice_outbox")
        .fetch_one(pool)
        .await?;
    assert_eq!(count, 0);

    let mut received = Vec::new();
    for _ in 0..2 {
        let message = tokio::time::timeout(Duration::from_secs(3), stream.next())
            .await?
            .expect("a notice");
        let notice: TenantNotice = serde_json::from_str(&message.get_payload::<String>()?)?;
        received.push((notice.tenant_id, notice.topic, notice.version));
    }
    received.sort();
    assert_eq!(
        received,
        vec![
            (
                "owned-a".to_string(),
                "flare.access".to_string(),
                "50".to_string()
            ),
            (
                "owned-b".to_string(),
                "flare.access".to_string(),
                "160".to_string()
            ),
        ]
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(300), stream.next())
            .await
            .is_err(),
        "nothing else was published"
    );
    Ok(())
}

/// What a gateway reads: the projected row through the cache, within a refresh interval.
async fn gateways_read_the_projection(pool: &PgPool, worker: &Worker) -> anyhow::Result<()> {
    let cache = TenantRuntimeCache::with_source(
        Arc::new(PgTenantRuntimeSource::new(pool.clone())),
        TenantRuntimeCacheOptions {
            refresh_interval: Duration::from_millis(100),
            capacity: 16,
        },
    );
    assert_eq!(cache.access_serial(APP).await, 50);
    assert_eq!(cache.access_serial("never-projected").await, 0);
    let policy_row = cache.client_policy(APP).await.expect("projected row");
    assert_eq!(policy_row.version, 10);
    assert_eq!(policy_row.release_generation, 2);
    assert_eq!(policy_row.build_policies["android"].min_build, 122);
    assert_eq!(
        policy_row.build_policies["android"].min_build_effective_from_ms,
        1_700_000_000_000
    );
    assert_eq!(policy_row.build_policies["android"].blocked.len(), 1);

    assert_eq!(worker.upsert(policy(11, 51, 2, 122)).await?, (true, 11));
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(cache.access_serial(APP).await, 51);

    let source = PgTenantRuntimeSource::new(pool.clone());
    assert!(
        source
            .load_client_policy("never-projected")
            .await
            .map_err(anyhow::Error::msg)?
            .is_none()
    );
    Ok(())
}

async fn exercise(pool: &PgPool, redis_url: &str) -> anyhow::Result<()> {
    // GetVersions reads these read models too. Only their CREATE blocks from the development DDL:
    // never its DROP, seed or Timescale sections.
    let ddl = include_str!("../../deploy/init.sql");
    for name in [
        "tenants",
        "credential_projection_versions",
        "plugin_authorization_snapshots",
    ] {
        let start = ddl
            .find(&format!("CREATE TABLE {name} ("))
            .or_else(|| ddl.find(&format!("CREATE TABLE IF NOT EXISTS {name} (")))
            .expect("canonical read model table");
        let end = ddl[start..].find("\n);").expect("complete table block") + start + 3;
        sqlx::raw_sql(&ddl[start..end]).execute(pool).await?;
    }
    sqlx::raw_sql(SCHEMA).execute(pool).await?;
    let (_server, worker) = start(pool).await?;
    idempotence(pool, &worker).await?;
    atomicity(pool, &worker).await?;
    racing_first_projections(pool, &worker).await?;
    relay_survives_a_channel_outage(pool).await?;
    relay_publishes(pool, redis_url).await?;
    gateways_read_the_projection(pool, &worker).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a throwaway loopback PG admin and Redis; creates only a nonce DB"]
async fn client_policy_projection_on_owned_pg_and_redis() {
    let options: PgConnectOptions = std::env::var("FLARE_CLIENT_POLICY_PG_ADMIN_URL")
        .expect("owned fixture URL")
        .parse()
        .expect("fixture options");
    assert_eq!(options.get_host(), "127.0.0.1");
    let redis_url = std::env::var("FLARE_CLIENT_POLICY_REDIS_URL").expect("owned Redis URL");
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options.clone())
        .await
        .expect("owned admin");
    let name = format!("flare_client_policy_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE DATABASE {name}"))
        .execute(&admin)
        .await
        .expect("create own nonce DB");
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect_with(options.database(&name))
        .await
        .expect("own nonce connection");
    let result = std::panic::AssertUnwindSafe(exercise(&pool, &redis_url))
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
        Ok(Err(error)) => panic!("owned client policy check failed: {error}"),
        Err(p) => std::panic::resume_unwind(p),
    }
}
