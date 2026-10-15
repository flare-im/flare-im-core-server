//! Real PG/owned Redis plus controlled receiver ACK. Physical transport is tested by
//! the gateway's receiver test, and only final R6 exercises actual client sockets.
use super::*;
use ed25519_dalek::{
    SigningKey,
    pkcs8::{EncodePrivateKey, EncodePublicKey},
};
use flare_im_service_kit::audit_effects::durable::StaffAudit;
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, SignedContextVerifier,
};
use futures_util::StreamExt;
use sqlx::{PgPool, postgres::PgConnectOptions};
use std::{
    path::PathBuf,
    process::{Child, Command, Stdio},
    str::FromStr,
};

struct OwnedRedis {
    child: Child,
    root: PathBuf,
}
impl Drop for OwnedRedis {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
fn staff() -> flare_im_contracts::Ctx {
    let key = SigningKey::from_bytes(&[88; 32]);
    let signer =
        SignedContextSigner::from_private_key(&key.to_pkcs8_pem(Default::default()).unwrap())
            .unwrap()
            .with_kid("gateway")
            .unwrap();
    let verifier = SignedContextVerifier::from_public_key(
        &key.verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap(),
    )
    .unwrap()
    .require_staff_kid(["gateway"]);
    let token = signer
        .sign(
            &SignedContextIdentity::new(
                SignedActorType::Staff,
                "employee",
                "app-a",
                "online-request",
            )
            .with_staff_grant(None, Some("core:users:kick".into()), 1),
        )
        .unwrap();
    let mut r = tonic::Request::new(());
    flare_server_core::set_signed_context_token(&mut r, &token).unwrap();
    flare_im_service_kit::staff_context::verify_staff_metadata(
        r.metadata(),
        &verifier,
        "core:users:kick",
    )
    .unwrap()
    .ctx
}
#[tokio::test]
#[ignore = "requires explicit owned-test PG and redis-server on PATH"]
async fn command_survives_missing_receiver_tail_failure_and_keeps_new_physical_session() {
    let url = std::env::var("FLARE_FUNCTIONAL_PG_ADMIN_URL").unwrap();
    let root = PathBuf::from("/tmp").join(format!("flare-online-audit-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let socket = root.join("redis.sock");
    let child = Command::new("redis-server")
        .args([
            "--port",
            "0",
            "--save",
            "",
            "--appendonly",
            "no",
            "--unixsocket",
        ])
        .arg(&socket)
        .args(["--unixsocketperm", "700"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let owned = OwnedRedis { child, root };
    let redis_url = format!("redis+unix://{}", socket.display());
    let client = Arc::new(redis::Client::open(redis_url.clone()).unwrap());
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if client.get_multiplexed_async_connection().await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let admin = PgPool::connect(&url).await.unwrap();
    let name = format!("flare_online_audit_test_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE DATABASE {name}"))
        .execute(&admin)
        .await
        .unwrap();
    let pool = Arc::new(
        PgPool::connect_with(PgConnectOptions::from_str(&url).unwrap().database(&name))
            .await
            .unwrap(),
    );
    let run = pool.clone();
    let result = tokio::spawn(async move {
        check_commands(run, client, redis_url).await;
    })
    .await;
    pool.close().await;
    sqlx::query(&format!("DROP DATABASE {name} WITH (FORCE)"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
    drop(owned);
    if let Err(error) = result {
        std::panic::resume_unwind(error.into_panic());
    }
}
async fn check_commands(pool: Arc<PgPool>, client: Arc<redis::Client>, url: String) {
    sqlx::raw_sql(include_str!(
        "../../../../../../migrations/0012_20261015_core_audit_outbox.sql"
    ))
    .execute(&*pool)
    .await
    .unwrap();
    sqlx::raw_sql(include_str!(
        "../../../../../../migrations/0014_20261015_online_kick_commands.sql"
    ))
    .execute(&*pool)
    .await
    .unwrap();
    let config = Arc::new(OnlineConfig {
        redis_url: url.clone(),
        kick_redis_url: url,
        kick_channel: "owned:kick".into(),
        presence_prefix: "owned:presence".into(),
        redis_ttl_seconds: 3600,
        tenant_runtime_postgres_url: None,
        tenant_policy: flare_im_service_kit::tenant_runtime::TenantAccessPolicy::Lenient,
        max_online_devices_per_user: 10,
    });
    let repo = RedisConversationRepository::new(client.clone(), config.clone())
        .with_command_pool(pool.clone());
    let mut record = RedisConnectionRecord {
        tenant_id: "app-a".into(),
        conversation_id: "11111111-1111-4111-8111-111111111111".into(),
        gateway_id: "gateway-a".into(),
        server_id: "server-a".into(),
        device_id: "device".into(),
        device_platform: "desktop".into(),
        connection_id: "physical-old".into(),
        created_at: 1,
        last_seen: 1,
        device_priority: 2,
        token_version: 1,
    };
    let mut redis = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis
        .hset(
            RedisConversationRepository::connection_key("app-a", "user"),
            "11111111-1111-4111-8111-111111111111",
            serde_json::to_string(&record).unwrap(),
        )
        .await
        .unwrap();
    let ctx = staff();
    // Source intent failure must precede any irreversible publication or projection removal.
    sqlx::raw_sql("CREATE FUNCTION reject_command() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'owned command fault'; END $$; CREATE TRIGGER reject_command BEFORE INSERT ON online_kick_commands FOR EACH ROW EXECUTE FUNCTION reject_command()").execute(&*pool).await.unwrap();
    // Explicit values below keep the borrowed IDs alive across awaited command scopes.
    let session = ConnectionId::from_string("11111111-1111-4111-8111-111111111111".into()).unwrap();
    let user = UserId::new("user".into()).unwrap();
    assert!(
        repo.remove_connection_for_kick("app-a", &session, &user, "DEVICE_KICKED")
            .staff_audit(&ctx, "signaling-online")
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM online_kick_commands")
            .fetch_one(&*pool)
            .await
            .unwrap(),
        0
    );
    sqlx::raw_sql(
        "DROP TRIGGER reject_command ON online_kick_commands; DROP FUNCTION reject_command()",
    )
    .execute(&*pool)
    .await
    .unwrap();
    assert!(
        repo.remove_connection_for_kick("app-a", &session, &user, "DEVICE_KICKED")
            .staff_audit(&ctx, "signaling-online")
            .await
            .is_err()
    );
    let id: String = sqlx::query_scalar("SELECT id::text FROM online_kick_commands")
        .fetch_one(&*pool)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM core_audit_outbox")
            .fetch_one(&*pool)
            .await
            .unwrap(),
        0,
        "no ACK is not a confirmed effect"
    );
    let still: Option<String> = redis
        .hget(
            RedisConversationRepository::connection_key("app-a", "user"),
            "11111111-1111-4111-8111-111111111111",
        )
        .await
        .unwrap();
    assert!(still.is_some());
    // Reconstruct owner after an interrupted request. Addressed receiver returns only
    // the stored old physical target, not the user's replacement login.
    drop(repo);
    record.connection_id = "physical-new".into();
    let _: () = redis
        .hset(
            RedisConversationRepository::connection_key("app-a", "user"),
            "11111111-1111-4111-8111-111111111111",
            serde_json::to_string(&record).unwrap(),
        )
        .await
        .unwrap();
    let repo = Arc::new(
        RedisConversationRepository::new(client.clone(), config).with_command_pool(pool.clone()),
    );
    let mut sub = client.get_async_pubsub().await.unwrap();
    sub.subscribe("owned:kick").await.unwrap();
    let receiver_client = client.clone();
    let receiver = tokio::spawn(async move {
        let mut stream = sub.on_message();
        let msg = stream.next().await.unwrap();
        let payload: String = msg.get_payload().unwrap();
        let value: Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(value["connection_id"], "physical-old");
        assert_eq!(value["target_gateway"], "gateway-a");
        let mut conn = receiver_client
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let _: () = conn
            .set_ex(
                format!(
                    "owned:kick:receipt:{}",
                    value["command_id"].as_str().unwrap()
                ),
                "physical-old",
                86400,
            )
            .await
            .unwrap();
    });
    sqlx::raw_sql("CREATE FUNCTION reject_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'owned audit fault'; END $$; CREATE TRIGGER reject_audit BEFORE INSERT ON core_audit_outbox FOR EACH ROW EXECUTE FUNCTION reject_audit()").execute(&*pool).await.unwrap();
    assert!(repo.advance_command(&id).await.is_err());
    receiver.await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM online_kick_commands WHERE completed_at IS NULL"
        )
        .fetch_one(&*pool)
        .await
        .unwrap(),
        1
    );
    sqlx::raw_sql("DROP TRIGGER reject_audit ON core_audit_outbox; DROP FUNCTION reject_audit()")
        .execute(&*pool)
        .await
        .unwrap();
    sqlx::query("UPDATE online_kick_commands SET next_at=clock_timestamp()")
        .execute(&*pool)
        .await
        .unwrap();
    let (stop, shutdown) = tokio::sync::oneshot::channel();
    let runner = repo.clone();
    let run_pool = pool.clone();
    let task = tokio::spawn(async move {
        runner
            .run_commands(
                run_pool,
                AuditEffects::disabled("signaling-online"),
                async {
                    let _ = shutdown.await;
                },
            )
            .await;
    });
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            if sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM online_kick_commands WHERE completed_at IS NOT NULL",
            )
            .fetch_one(&*pool)
            .await
            .unwrap()
                == 1
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .unwrap();
    stop.send(()).unwrap();
    task.await.unwrap();
    assert!(repo.advance_command(&id).await.unwrap());
    let value: Value = sqlx::query_scalar("SELECT payload FROM core_audit_outbox")
        .fetch_one(&*pool)
        .await
        .unwrap();
    assert_eq!(value["aggregate_id"], "physical-old");
    assert_eq!(value["actor_id"], "employee");
    assert_eq!(value["permission"], "core:users:kick");
    assert_eq!(value["request_id"], "online-request");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM core_audit_outbox")
            .fetch_one(&*pool)
            .await
            .unwrap(),
        1
    );
    let current: String = redis
        .hget(
            RedisConversationRepository::connection_key("app-a", "user"),
            "11111111-1111-4111-8111-111111111111",
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&current).unwrap()["connection_id"],
        "physical-new"
    );
}
