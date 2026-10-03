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
//! - 自定义 Webhook 头与 gRPC metadata 的取值同样只写不读（读回只有键名与掩码），照读回写回
//!   沿用已存的值；`clear_signing_secret` 显式清除签名密钥、回到不签名（W13-H）。
//! - `LISTEN` 用独立连接：仓储连接池只有一条连接（`CAPABILITY_POSTGRES_MAX_CONNECTIONS=1`）时
//!   仓储查询、`pg_notify` 与收到通知后的重载都不用等取连接超时；停机后监听连接关闭（W13-H）。

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
use std::collections::HashMap;
use std::time::Instant;

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
const CLEAR_KEY: &str = "clear_signing_secret";
const MASK: &str = "<redacted>";

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
    // 进程里只有一个订阅者时，tracing 按「第一次走到某个日志调用点的那个线程」的订阅者决定该调用点
    // 开不开：同一测试进程里没有订阅者的其他用例先走到 `info!` 调用点，它就此关闭，本用例的 INFO
    // 断言偶发失败、日志里的密钥检索也形同虚设。再注册一个订阅者（不输出），调用点就按所有订阅者
    // 决定，事件照常交给各线程自己的订阅者。
    let _second_dispatch = tracing::Dispatch::new(tracing_subscriber::registry());
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
    let mut listener_a = invalidation_a.spawn_listener(Arc::clone(&watcher_a));
    let mut listener_b = invalidation_b.spawn_listener(Arc::clone(&watcher_b));
    assert!(listener_a.wait_subscribed(Duration::from_secs(10)).await);
    assert!(listener_b.wait_subscribed(Duration::from_secs(10)).await);

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

    listener_a.shutdown().await;
    listener_b.shutdown().await;
}

fn create_with(tenant: &str, name: &str, transport: HookTransport) -> GenericRequest {
    administer(
        "create",
        "CreateHookConfigRequest",
        CreateHookConfigRequest {
            name: name.into(),
            hook_type: "pre_send".into(),
            tenant_id: tenant.into(),
            priority: 900,
            transport: Some(transport),
            selector: Some(HookSelector {
                tenants: vec![tenant.into()],
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode_to_vec(),
    )
}

fn decoded_config(response: &GenericResponse) -> ProtoHookConfig {
    let bytes = response.payload.as_ref().unwrap().value.as_slice();
    // create、get、update 的应答都只有 `config = 1` 一个字段，按 Get 的类型解码即可。
    GetHookConfigResponse::decode(bytes)
        .unwrap()
        .config
        .unwrap()
}

/// 读回里的头、metadata 只有键名与掩码，应答编码字节里没有任何取值。
fn assert_values_masked(
    response: &GenericResponse,
    values: &HashMap<String, String>,
    secrets: &[&str],
) {
    for value in values.values() {
        assert_eq!(value, MASK);
    }
    let encoded = response.encode_to_vec();
    for secret in secrets {
        assert!(
            !contains(&encoded, secret),
            "a header, metadata or signing value is read back"
        );
    }
}

#[tokio::test]
#[ignore = "requires FLARE_TENANT_TEST_DATABASE_URL pointing at a database initialised with deploy/init.sql"]
async fn header_and_metadata_values_are_write_only_and_the_signing_secret_can_be_cleared() {
    let repository = Arc::new(
        PostgresHookConfigRepository::new(&database_url())
            .await
            .unwrap(),
    );
    let run = suffix();
    let tenant = format!("th{run}");
    let secret = format!("w13h-secret-{run}");
    let authorization = format!("Bearer w13h-authorization-{run}");
    let note = format!("w13h-note-{run}");
    let api_key = format!("w13h-api-key-{run}");
    let values = [
        secret.as_str(),
        authorization.as_str(),
        note.as_str(),
        api_key.as_str(),
    ];
    let receiver = WebhookReceiver::start().await;

    let watcher = watcher(&repository);
    watcher.start().await.unwrap();
    let registry = Arc::new(CoreHookRegistry::new(Arc::clone(&watcher)));
    let governance = HookServiceServer::new(Arc::clone(&repository), Arc::clone(&registry));
    let keys = cell_keys(96);
    let endpoint = serve_hook_plugin(ImHookPluginServer::new(
        Arc::new(HookCommandHandler::new(Arc::new(HookOrchestrationService))),
        Arc::clone(&registry),
        Arc::new(HookAdapterFactory::new()),
        CapabilityExtensionRegistry::new(),
        Some(keys.verifier.clone()),
    ))
    .await;

    // create：带两个自定义头与签名密钥；应答只有头名与掩码。
    let mut transport = webhook_transport(&receiver.url("/h"), &secret);
    transport.headers = HashMap::from([
        ("Authorization".to_string(), authorization.clone()),
        ("X-Tenant-Note".to_string(), note.clone()),
    ]);
    let created = call(
        &governance,
        create_with(&tenant, &format!("w13h-h-{run}"), transport),
        &tenant,
    )
    .await;
    let config = decoded_config(&created);
    let read_back = config.transport.clone().unwrap();
    assert_values_masked(&created, &read_back.headers, &values);
    assert_eq!(read_back.headers.len(), 2);
    let hook_id = config.hook_id.clone();

    // get、list：同样只有键名与掩码。
    let got = call(&governance, get(&hook_id), &tenant).await;
    assert_values_masked(
        &got,
        &decoded_config(&got).transport.unwrap().headers,
        &values,
    );
    let listed = call(&governance, list(&tenant), &tenant).await;
    let configs =
        ListHookConfigsResponse::decode(listed.payload.as_ref().unwrap().value.as_slice())
            .unwrap()
            .configs;
    for config in &configs {
        assert_values_masked(
            &listed,
            &config.transport.as_ref().unwrap().headers,
            &values,
        );
    }

    // 投递带登记的真实取值，并按密钥签名。
    let first = deliver_once(&endpoint, &keys, &receiver, &tenant, &format!("{run}-1")).await;
    assert_eq!(
        first.header_values("authorization"),
        [authorization.as_str()]
    );
    assert_eq!(first.header_values("x-tenant-note"), [note.as_str()]);
    assert!(verify(&first, &secret).is_ok());

    // 照读回写回（头为掩码、一个改成空串，密钥为空，metadata 带读回的标志）：取值与签名都不变。
    let mut written_back = read_back.clone();
    written_back
        .headers
        .insert("X-Tenant-Note".into(), String::new());
    let updated = call(
        &governance,
        update_transport(&hook_id, written_back),
        &tenant,
    )
    .await;
    let after_update = decoded_config(&updated).transport.unwrap();
    assert_values_masked(&updated, &after_update.headers, &values);
    assert_eq!(
        after_update.metadata.get(SECRET_KEY).map(String::as_str),
        Some("true")
    );
    let kept = deliver_once(&endpoint, &keys, &receiver, &tenant, &format!("{run}-2")).await;
    assert_eq!(
        kept.header_values("authorization"),
        [authorization.as_str()]
    );
    assert_eq!(kept.header_values("x-tenant-note"), [note.as_str()]);
    assert!(verify(&kept, &secret).is_ok());

    // 掩码值的新键没有已存值可沿用：INVALID_ARGUMENT，错误里只有键名。
    let mut unknown = read_back.clone();
    unknown.headers.insert("X-Unknown".into(), MASK.into());
    let status = dispatch_hook_administer(
        &governance,
        update_transport(&hook_id, unknown),
        &employee(&tenant),
    )
    .await
    .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status:?}");
    assert!(status.message().contains("X-Unknown"));
    assert!(values.iter().all(|value| !status.message().contains(value)));

    // 清除标志与新密钥同时出现：INVALID_ARGUMENT，什么都不改。
    let mut contradictory = read_back.clone();
    contradictory.secret = format!("w13h-other-{run}");
    contradictory
        .metadata
        .insert(CLEAR_KEY.into(), "true".into());
    let status = dispatch_hook_administer(
        &governance,
        update_transport(&hook_id, contradictory),
        &employee(&tenant),
    )
    .await
    .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status:?}");

    // 显式清除：回到不签名，头照旧。
    let mut clear = read_back.clone();
    clear.metadata.insert(CLEAR_KEY.into(), "true".into());
    let cleared = call(&governance, update_transport(&hook_id, clear), &tenant).await;
    let after_clear = decoded_config(&cleared).transport.unwrap();
    assert_eq!(
        after_clear.metadata.get(SECRET_KEY).map(String::as_str),
        Some("false")
    );
    assert!(!after_clear.metadata.contains_key(CLEAR_KEY));
    let unsigned = deliver_once(&endpoint, &keys, &receiver, &tenant, &format!("{run}-3")).await;
    assert!(unsigned.header_values("flare-signature").is_empty());
    assert_eq!(
        unsigned.header_values("authorization"),
        [authorization.as_str()]
    );
    let (_, stored) = repository
        .get_by_id(hook_id.parse().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        stored.transport,
        HookTransportConfig::Webhook { secret: None, ref headers, .. }
            if headers.get("Authorization") == Some(&authorization)
                && headers.get("X-Tenant-Note") == Some(&note)
    ));

    // 再设新密钥即恢复签名。
    let mut resign = read_back.clone();
    resign.secret = secret.clone();
    call(&governance, update_transport(&hook_id, resign), &tenant).await;
    let resigned = deliver_once(&endpoint, &keys, &receiver, &tenant, &format!("{run}-4")).await;
    assert!(verify(&resigned, &secret).is_ok());
    call(&governance, delete(&hook_id), &tenant).await;

    // gRPC metadata：读回只有键名与掩码，照读回写回不改已存的值。这条 Hook 停用、不执行。
    let grpc = HookTransport {
        r#type: "grpc".into(),
        endpoint: "http://127.0.0.1:9".into(),
        metadata: HashMap::from([("x-api-key".to_string(), api_key.clone())]),
        timeout_ms: 2000,
        ..Default::default()
    };
    let created = call(
        &governance,
        create_with(&tenant, &format!("w13h-g-{run}"), grpc),
        &tenant,
    )
    .await;
    let config = decoded_config(&created);
    let grpc_id = config.hook_id.clone();
    call(&governance, set_status(&grpc_id, false), &tenant).await;
    let grpc_read_back = config.transport.unwrap();
    assert_values_masked(&created, &grpc_read_back.metadata, &values);
    let got = call(&governance, get(&grpc_id), &tenant).await;
    assert_values_masked(
        &got,
        &decoded_config(&got).transport.unwrap().metadata,
        &values,
    );
    let updated = call(
        &governance,
        update_transport(&grpc_id, grpc_read_back),
        &tenant,
    )
    .await;
    assert_values_masked(
        &updated,
        &decoded_config(&updated).transport.unwrap().metadata,
        &values,
    );
    let (_, stored) = repository
        .get_by_id(grpc_id.parse().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        stored.transport,
        HookTransportConfig::Grpc { ref metadata, .. }
            if metadata.get("x-api-key") == Some(&api_key)
    ));
    call(&governance, delete(&grpc_id), &tenant).await;
}

/// `url` 加上 `application_name`，用来在 `pg_stat_activity` 里认出本用例的连接。
fn with_application_name(url: &str, name: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}application_name={name}")
}

/// 本用例（`application_name`）在库里的连接数，以及其中最后一条语句是 `LISTEN` 的连接数。
async fn backends(repository: &PostgresHookConfigRepository, name: &str) -> (i64, i64) {
    sqlx::query_as::<_, (i64, i64)>(
        "SELECT count(*), count(*) FILTER (WHERE query ILIKE 'LISTEN%') \
         FROM pg_stat_activity WHERE application_name = $1",
    )
    .bind(name)
    .fetch_one(repository.connection_pool().as_ref())
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "requires FLARE_TENANT_TEST_DATABASE_URL pointing at a database initialised with deploy/init.sql"]
async fn the_listener_does_not_take_a_repository_connection() {
    let url = database_url();
    let run = suffix();
    let application = format!("w13h-{run}");
    // 仓储池只有一条连接、取连接 2 s 超时（同 CAPABILITY_POSTGRES_MAX_CONNECTIONS=1）。
    let repository = Arc::new(
        PostgresHookConfigRepository::new_with_pool_profile(
            &with_application_name(&url, &application),
            1,
            1,
            2,
            300,
            1800,
        )
        .await
        .unwrap(),
    );
    let watcher_a = watcher(&repository);
    watcher_a.start().await.unwrap();
    let invalidation = HookConfigInvalidation::new(repository.connection_pool());
    let mut listener = invalidation.spawn_listener(Arc::clone(&watcher_a));
    assert!(listener.wait_subscribed(Duration::from_secs(10)).await);

    // 仓储查询与发通知都立即拿到连接（修复前监听占着唯一一条，要等 2 s 后报取连接超时）。
    let tenant = format!("tl{run}");
    for _ in 0..3 {
        let started = Instant::now();
        repository.load_by_tenant().await.unwrap();
        invalidation.publish(Some(&tenant)).await.unwrap();
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(1),
            "repository query and pg_notify waited {elapsed:?}"
        );
    }
    // 两条连接：仓储池一条、监听一条（监听不在仓储池里）。
    assert_eq!(backends(&repository, &application).await, (2, 1));

    // 另一个实例写入并通知：本实例经监听收到后按租户重载（重载走的仍是那一条仓储连接）。
    let other = Arc::new(PostgresHookConfigRepository::new(&url).await.unwrap());
    let other_invalidation = HookConfigInvalidation::new(other.connection_pool());
    let name = format!("w13h-l-{run}");
    let mut hook = webhook_hook(&name, "http://127.0.0.1:9/hook");
    hook.selector.tenants = vec![tenant.clone()];
    other
        .save(Some(&tenant), "pre_send", &hook, None)
        .await
        .unwrap();
    other_invalidation.publish(Some(&tenant)).await.unwrap();
    let registry_a = CoreHookRegistry::new(Arc::clone(&watcher_a));
    let names = wait_for_names(&registry_a, &tenant, |names| names.contains(&name)).await;
    assert!(
        names.contains(&name),
        "the listener never reloaded: {names:?}"
    );
    assert!(
        other
            .delete(Some(&tenant), "pre_send", &name)
            .await
            .unwrap()
    );

    // 停机：任务结束、监听连接关闭，仓储池照常可用。
    listener.shutdown().await;
    let mut remaining = (0, 0);
    for _ in 0..50 {
        remaining = backends(&repository, &application).await;
        if remaining == (1, 0) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        remaining,
        (1, 0),
        "the listening connection outlived shutdown"
    );
}
