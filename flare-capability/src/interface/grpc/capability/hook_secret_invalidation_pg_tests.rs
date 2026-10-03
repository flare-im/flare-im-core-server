//! 真库：Webhook 签名密钥只写不读（W11-S1 F5）、Administer 写入跨实例生效（W11-H 余项）。
//!
//! 需要 `FLARE_TENANT_TEST_DATABASE_URL`（用 `deploy/init.sql` 初始化的库），用例标了 `#[ignore]`，
//! 约定同 `tenant_hooks_pg_tests`。只写随机租户与随机名的行，结束时删除。
//!
//! - 修复前 create、get、list 的应答原样带回签名密钥；现在都为空，只在 `transport.metadata`
//!   说明已设置。更新时密钥留空即沿用（照读回的配置写回不会关掉签名），传新值即换钥；投递
//!   始终按库里的密钥签名。整个过程的日志（含 TRACE）里也没有密钥。
//! - 修复前多个实例时只有收到写入的实例重载，其余实例等定时刷新（60 s）；现在写入经
//!   PostgreSQL `LISTEN/NOTIFY` 通知其余实例，按租户（全局行则全量）重载。

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use flare_grpc_proto::capability::{
    CreateHookConfigRequest, CreateHookConfigResponse, DeleteHookConfigRequest, GenericRequest,
    GenericResponse, GetHookConfigRequest, GetHookConfigResponse, HookConfig as ProtoHookConfig,
    HookRetryPolicy, HookSelector, HookTransport, ListHookConfigsRequest, ListHookConfigsResponse,
    SetHookStatusRequest, UpdateHookConfigRequest, UpdateHookConfigResponse,
};
use flare_im_hooks::PreSendDecision;
use flare_im_hooks::hooks::adapters::{
    WEBHOOK_SIGNATURE_TOLERANCE_SECS, WebhookSignatureError, verify_webhook_signature,
};
use flare_im_service_kit::control_auth::{ControlActorKind, VerifiedControlActor};
use flare_server_core::SignedActorType;
use prost::Message;

use super::administer::dispatch_hook_administer;
use crate::application::handler::HookCommandHandler;
use crate::composition::hook_registry::CoreHookRegistry;
use crate::domain::model::HookTransportConfig;
use crate::domain::service::HookOrchestrationService;
use crate::infrastructure::adapters::HookAdapterFactory;
use crate::infrastructure::capability::CapabilityExtensionRegistry;
use crate::infrastructure::config::{
    ConfigLoaderItem, ConfigWatcher, DatabaseConfigLoader, HookConfigInvalidation,
};
use crate::infrastructure::persistence::PostgresHookConfigRepository;
use crate::interface::grpc::hooks::test_support::{
    CellKeys, WebhookReceiver, cell_keys, ingest_pre_send, serve_hook_plugin, signed_ctx,
    webhook_hook,
};
use crate::interface::grpc::{HookServiceServer, ImHookPluginServer};

const ENV_DATABASE_URL: &str = "FLARE_TENANT_TEST_DATABASE_URL";
const SECRET_KEY: &str = "signing_secret_configured";

fn database_url() -> String {
    std::env::var(ENV_DATABASE_URL).unwrap_or_else(|_| {
        panic!("set {ENV_DATABASE_URL} to a database initialised with deploy/init.sql")
    })
}

fn suffix() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..12].to_string()
}

fn employee(tenant: &str) -> VerifiedControlActor {
    VerifiedControlActor {
        subject: "employee-w12w".into(),
        tenant_id: tenant.into(),
        kind: ControlActorKind::Employee,
    }
}

fn administer(operation: &str, type_url: &str, value: Vec<u8>) -> GenericRequest {
    GenericRequest {
        operation: format!("flare.extension.v1.hook_config.{operation}"),
        metadata: Default::default(),
        payload: Some(prost_types::Any {
            type_url: format!("type.googleapis.com/flare.capability.v1.{type_url}"),
            value,
        }),
        request_id: format!("w12w-{operation}-{}", suffix()),
    }
}

fn webhook_transport(endpoint: &str, secret: &str) -> HookTransport {
    HookTransport {
        r#type: "webhook".into(),
        endpoint: endpoint.into(),
        secret: secret.into(),
        timeout_ms: 2000,
        ..Default::default()
    }
}

fn create(tenant: &str, name: &str, endpoint: &str, secret: &str) -> GenericRequest {
    administer(
        "create",
        "CreateHookConfigRequest",
        CreateHookConfigRequest {
            name: name.into(),
            hook_type: "pre_send".into(),
            tenant_id: tenant.into(),
            priority: 900,
            transport: Some(webhook_transport(endpoint, secret)),
            selector: Some(HookSelector {
                tenants: vec![tenant.into()],
                ..Default::default()
            }),
            retry_policy: Some(HookRetryPolicy {
                max_retries: 0,
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode_to_vec(),
    )
}

fn update_transport(hook_id: &str, transport: HookTransport) -> GenericRequest {
    administer(
        "update",
        "UpdateHookConfigRequest",
        UpdateHookConfigRequest {
            hook_id: hook_id.into(),
            transport: Some(transport),
            ..Default::default()
        }
        .encode_to_vec(),
    )
}

fn get(hook_id: &str) -> GenericRequest {
    administer(
        "get",
        "GetHookConfigRequest",
        GetHookConfigRequest {
            hook_id: hook_id.into(),
        }
        .encode_to_vec(),
    )
}

fn list(tenant: &str) -> GenericRequest {
    administer(
        "list",
        "ListHookConfigsRequest",
        ListHookConfigsRequest {
            tenant_id: tenant.into(),
            ..Default::default()
        }
        .encode_to_vec(),
    )
}

fn set_status(hook_id: &str, enabled: bool) -> GenericRequest {
    administer(
        "set_status",
        "SetHookStatusRequest",
        SetHookStatusRequest {
            hook_id: hook_id.into(),
            enabled,
        }
        .encode_to_vec(),
    )
}

fn delete(hook_id: &str) -> GenericRequest {
    administer(
        "delete",
        "DeleteHookConfigRequest",
        DeleteHookConfigRequest {
            hook_id: hook_id.into(),
        }
        .encode_to_vec(),
    )
}

async fn call(
    governance: &HookServiceServer,
    request: GenericRequest,
    tenant: &str,
) -> GenericResponse {
    let response = dispatch_hook_administer(governance, request, &employee(tenant))
        .await
        .unwrap()
        .into_inner();
    assert!(response.ok, "{response:?}");
    response
}

fn contains(haystack: &[u8], needle: &str) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle.as_bytes())
}

/// 应答里的一条配置：不带密钥，只说明已设置；整个应答的编码字节里也找不到任一密钥。
fn assert_write_only(response: &GenericResponse, config: &ProtoHookConfig, secrets: &[&str]) {
    let transport = config.transport.as_ref().unwrap();
    assert!(
        transport.secret.is_empty(),
        "{} echoes its secret",
        config.hook_id
    );
    assert_eq!(
        transport.metadata.get(SECRET_KEY).map(String::as_str),
        Some("true")
    );
    let encoded = response.encode_to_vec();
    for secret in secrets {
        assert!(
            !contains(&encoded, secret),
            "a signing secret is in the response"
        );
    }
}

fn watcher(repository: &Arc<PostgresHookConfigRepository>) -> Arc<ConfigWatcher> {
    Arc::new(ConfigWatcher::new(
        vec![Arc::new(ConfigLoaderItem::Database(
            DatabaseConfigLoader::new(Arc::clone(repository), None),
        ))],
        // 只有写入（与通知）触发重载：不靠定时刷新。
        Duration::from_secs(3600),
    ))
}

/// 以 `tenant` 的签名用户发一次 PreSend，返回唯一一次投递。
async fn deliver_once(
    endpoint: &str,
    keys: &CellKeys,
    receiver: &WebhookReceiver,
    tenant: &str,
    marker: &str,
) -> crate::interface::grpc::hooks::test_support::Delivery {
    let ctx = signed_ctx(keys, SignedActorType::User, "u-w12w", tenant);
    match ingest_pre_send(endpoint, &ctx, marker).await {
        PreSendDecision::Continue => {}
        PreSendDecision::Reject { error } => panic!("pre-send rejected: {error}"),
    }
    let deliveries = receiver.wait_for(marker, 1).await;
    assert_eq!(deliveries.len(), 1, "{deliveries:?}");
    deliveries.into_iter().next().unwrap()
}

fn verify(
    delivery: &crate::interface::grpc::hooks::test_support::Delivery,
    secret: &str,
) -> Result<u64, WebhookSignatureError> {
    let header = delivery.header_values("flare-signature");
    assert_eq!(header.len(), 1, "{:?}", delivery.headers);
    assert!(delivery.header_values("x-hook-signature").is_empty());
    verify_webhook_signature(
        secret.as_bytes(),
        &delivery.raw,
        header[0],
        delivery.received,
        WEBHOOK_SIGNATURE_TOLERANCE_SECS,
    )
}

/// 测试期间的全部日志（TRACE 起）写进内存，事后检查有没有密钥。
#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl Write for CapturedLogs {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl CapturedLogs {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).to_string()
    }
}

#[tokio::test]
#[ignore = "requires FLARE_TENANT_TEST_DATABASE_URL pointing at a database initialised with deploy/init.sql"]
async fn webhook_signing_secrets_are_write_only_and_deliveries_stay_signed() {
    let logs = CapturedLogs::default();
    let writer = logs.clone();
    let _logging = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish(),
    );

    let repository = Arc::new(
        PostgresHookConfigRepository::new(&database_url())
            .await
            .unwrap(),
    );
    let run = suffix();
    let tenant = format!("tw{run}");
    let first_secret = format!("w12w-first-{run}");
    let rotated_secret = format!("w12w-rotated-{run}");
    let receiver = WebhookReceiver::start().await;

    let watcher = watcher(&repository);
    watcher.start().await.unwrap();
    let registry = Arc::new(CoreHookRegistry::new(Arc::clone(&watcher)));
    let governance = HookServiceServer::new(Arc::clone(&repository), Arc::clone(&registry));
    let keys = cell_keys(95);
    let endpoint = serve_hook_plugin(ImHookPluginServer::new(
        Arc::new(HookCommandHandler::new(Arc::new(HookOrchestrationService))),
        Arc::clone(&registry),
        Arc::new(HookAdapterFactory::new()),
        CapabilityExtensionRegistry::new(),
        Some(keys.verifier.clone()),
    ))
    .await;
    let secrets = [first_secret.as_str(), rotated_secret.as_str()];

    // create：应答不带密钥。
    let created = call(
        &governance,
        create(
            &tenant,
            &format!("w12w-{run}"),
            &receiver.url("/w"),
            &first_secret,
        ),
        &tenant,
    )
    .await;
    let config =
        CreateHookConfigResponse::decode(created.payload.as_ref().unwrap().value.as_slice())
            .unwrap()
            .config
            .unwrap();
    assert_write_only(&created, &config, &secrets);
    let hook_id = config.hook_id.clone();

    // get、list：同样不带。
    let got = call(&governance, get(&hook_id), &tenant).await;
    let config = GetHookConfigResponse::decode(got.payload.as_ref().unwrap().value.as_slice())
        .unwrap()
        .config
        .unwrap();
    assert_write_only(&got, &config, &secrets);
    let listed = call(&governance, list(&tenant), &tenant).await;
    let configs =
        ListHookConfigsResponse::decode(listed.payload.as_ref().unwrap().value.as_slice())
            .unwrap()
            .configs;
    let ours: Vec<_> = configs.iter().filter(|c| c.hook_id == hook_id).collect();
    assert_eq!(ours.len(), 1);
    assert_write_only(&listed, ours[0], &secrets);
    assert!(
        configs
            .iter()
            .filter_map(|c| c.transport.as_ref())
            .all(|t| t.secret.is_empty()),
        "list returns some webhook secret"
    );

    // 投递按登记的密钥签名。
    let first = deliver_once(&endpoint, &keys, &receiver, &tenant, &format!("{run}-1")).await;
    assert!(verify(&first, &first_secret).is_ok());

    // 照读回的配置写回（密钥为空）：沿用原密钥，投递仍按它签名，没有变成不签名。
    let read_back = config.transport.clone().unwrap();
    assert!(read_back.secret.is_empty());
    let updated = call(&governance, update_transport(&hook_id, read_back), &tenant).await;
    let config =
        UpdateHookConfigResponse::decode(updated.payload.as_ref().unwrap().value.as_slice())
            .unwrap()
            .config
            .unwrap();
    assert_write_only(&updated, &config, &secrets);
    let kept = deliver_once(&endpoint, &keys, &receiver, &tenant, &format!("{run}-2")).await;
    assert!(verify(&kept, &first_secret).is_ok());

    // 换钥：传新的非空值。
    let rotated = call(
        &governance,
        update_transport(
            &hook_id,
            webhook_transport(&receiver.url("/w"), &rotated_secret),
        ),
        &tenant,
    )
    .await;
    let config =
        UpdateHookConfigResponse::decode(rotated.payload.as_ref().unwrap().value.as_slice())
            .unwrap()
            .config
            .unwrap();
    assert_write_only(&rotated, &config, &secrets);
    let after = deliver_once(&endpoint, &keys, &receiver, &tenant, &format!("{run}-3")).await;
    assert!(verify(&after, &rotated_secret).is_ok());
    assert_eq!(
        verify(&after, &first_secret),
        Err(WebhookSignatureError::Mismatch)
    );
    // 密钥照常存在库里，只是不读回。
    let (_, stored) = repository
        .get_by_id(hook_id.parse().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        stored.transport,
        HookTransportConfig::Webhook { secret: Some(ref s), .. } if *s == rotated_secret
    ));

    call(&governance, delete(&hook_id), &tenant).await;

    // 按租户重载在 INFO 级记下租户与条数；整个过程的日志里没有任何密钥。
    let text = logs.text();
    assert!(
        text.lines().any(|line| line.contains("INFO")
            && line.contains("Reloaded tenant hook config")
            && line.contains(&tenant)
            && line.contains("hooks_count=1")),
        "no INFO tenant reload line"
    );
    for secret in secrets {
        assert!(!text.contains(secret), "a signing secret was logged");
    }
}

/// 等到 `registry` 中 `tenant` 的 PreSend Hook 名单满足 `expected`（或 5 s 超时）。
async fn wait_for_names(
    registry: &CoreHookRegistry,
    tenant: &str,
    expected: impl Fn(&[String]) -> bool,
) -> Vec<String> {
    let mut names = Vec::new();
    for _ in 0..100 {
        names = registry
            .get_pre_send_hooks(tenant)
            .await
            .unwrap()
            .into_iter()
            .map(|hook| hook.name)
            .collect();
        if expected(&names) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    names
}

#[tokio::test]
#[ignore = "requires FLARE_TENANT_TEST_DATABASE_URL pointing at a database initialised with deploy/init.sql"]
async fn a_write_on_one_instance_reloads_the_other_instances_without_the_refresh() {
    let url = database_url();
    // 两个实例：各自的连接池、监听器与快照（同两个进程）。第三个不监听，代表修复前的行为。
    let repository_a = Arc::new(PostgresHookConfigRepository::new(&url).await.unwrap());
    let repository_b = Arc::new(PostgresHookConfigRepository::new(&url).await.unwrap());
    let repository_c = Arc::new(PostgresHookConfigRepository::new(&url).await.unwrap());
    let (watcher_a, watcher_b, watcher_c) = (
        watcher(&repository_a),
        watcher(&repository_b),
        watcher(&repository_c),
    );
    for watcher in [&watcher_a, &watcher_b, &watcher_c] {
        watcher.start().await.unwrap();
    }
    let invalidation_a = Arc::new(HookConfigInvalidation::new(repository_a.connection_pool()));
    let invalidation_b = Arc::new(HookConfigInvalidation::new(repository_b.connection_pool()));
    let (task_a, ready_a) = invalidation_a.spawn_listener(Arc::clone(&watcher_a));
    let (task_b, ready_b) = invalidation_b.spawn_listener(Arc::clone(&watcher_b));
    tokio::time::timeout(Duration::from_secs(10), ready_a)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), ready_b)
        .await
        .unwrap()
        .unwrap();

    let registry_a = Arc::new(CoreHookRegistry::new(Arc::clone(&watcher_a)));
    let registry_b = CoreHookRegistry::new(Arc::clone(&watcher_b));
    let registry_c = CoreHookRegistry::new(Arc::clone(&watcher_c));
    let governance_a = HookServiceServer::new(Arc::clone(&repository_a), registry_a)
        .with_invalidation(Arc::clone(&invalidation_a));

    let run = suffix();
    let tenant = format!("tx{run}");
    let name = format!("w12w-x-{run}");

    // 租户经实例 A 登记：B 不等定时刷新即装上，不监听的 C 仍是旧的。
    let created = call(
        &governance_a,
        create(&tenant, &name, "http://127.0.0.1:9/hook", ""),
        &tenant,
    )
    .await;
    let hook_id = CreateHookConfigResponse::decode(created.payload.unwrap().value.as_slice())
        .unwrap()
        .config
        .unwrap()
        .hook_id;
    let on_b = wait_for_names(&registry_b, &tenant, |names| names.contains(&name)).await;
    assert!(
        on_b.contains(&name),
        "instance B never loaded the write: {on_b:?}"
    );
    let on_c = wait_for_names(&registry_c, &tenant, |names| names.contains(&name)).await;
    assert!(
        !on_c.contains(&name),
        "instance C has no listener: {on_c:?}"
    );

    // 停用与删除同样传到 B。
    call(&governance_a, set_status(&hook_id, false), &tenant).await;
    let on_b = wait_for_names(&registry_b, &tenant, |names| !names.contains(&name)).await;
    assert!(!on_b.contains(&name), "B kept a disabled hook: {on_b:?}");
    call(&governance_a, set_status(&hook_id, true), &tenant).await;
    let on_b = wait_for_names(&registry_b, &tenant, |names| names.contains(&name)).await;
    assert!(on_b.contains(&name), "B missed the re-enable: {on_b:?}");
    call(&governance_a, delete(&hook_id), &tenant).await;
    let on_b = wait_for_names(&registry_b, &tenant, |names| !names.contains(&name)).await;
    assert!(!on_b.contains(&name), "B kept a deleted hook: {on_b:?}");

    // 全局行的变化（不经 Administer 的租户写入，例如平台运维写库后通知）让其余实例全量重载。
    // 这条全局 Hook 的端点不可达：`selector.tenants` 只列本用例的租户，同库并行的其他用例
    // 即使在这段时间全量重载也不会执行它。
    let global = format!("w12w-global-{run}");
    let mut global_hook = webhook_hook(&global, "http://127.0.0.1:9/global");
    global_hook.selector.tenants = vec![tenant.clone()];
    repository_a
        .save(None, "pre_send", &global_hook, None)
        .await
        .unwrap();
    invalidation_a.publish(None).await.unwrap();
    let on_b = wait_for_names(&registry_b, &tenant, |names| names.contains(&global)).await;
    assert!(on_b.contains(&global), "B missed a global change: {on_b:?}");
    assert!(
        repository_a
            .delete(None, "pre_send", &global)
            .await
            .unwrap()
    );
    invalidation_a.publish(None).await.unwrap();
    let on_b = wait_for_names(&registry_b, &tenant, |names| !names.contains(&global)).await;
    assert!(
        !on_b.contains(&global),
        "B kept a deleted global hook: {on_b:?}"
    );

    task_a.abort();
    task_b.abort();
}
