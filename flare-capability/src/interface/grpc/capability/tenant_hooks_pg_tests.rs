//! 真库：租户经 `CapabilityService/Administer` 登记的 Webhook 不重启即生效，且只对本租户的事件
//! 触发；全局行（`tenant_id IS NULL`）对所有租户生效；固定了 `TENANT_ID` 的进程行为不变。
//!
//! 需要 `FLARE_TENANT_TEST_DATABASE_URL`（用 `deploy/init.sql` 初始化的库）。用例标了 `#[ignore]`：
//! 默认的 `cargo test` 只记为 ignored，`-- --ignored` 显式运行时变量必须已设置，否则直接失败。
//! 只写随机租户与随机名的行，结束时删除。修复前多租户进程只加载 `tenant_id IS NULL` 的行，租户
//! 登记的 Webhook 一次都不执行（W10-S2 F1）。全局 Hook 的名字为平台保留：租户登记同类型同名的
//! Hook 被拒，绕过 Administer 写进库的同名租户行也顶替不了全局 Hook。
//!
//! 用例中途失败也不留下会拖垮同库其他用例的行（W13-H）：[`RowCleanup`] 在用例结束时（含断言
//! 失败的 unwind）删掉本次写入的全部行；全局行的 `selector.tenants` 只列本用例的三个租户，进程被
//! 强杀、守卫没来得及跑时，留下的全局行（端点已随用例关闭）也不会让别的租户的 PreSend 都回
//! `SERVICE_UNAVAILABLE`。

use std::sync::Arc;
use std::time::Duration;

use flare_grpc_proto::capability::{
    CreateHookConfigRequest, CreateHookConfigResponse, DeleteHookConfigRequest, GenericRequest,
    GenericResponse, HookRetryPolicy, HookSelector, HookTransport, UpdateHookConfigRequest,
};
use flare_im_hooks::PreSendDecision;
use flare_server_core::SignedActorType;
use prost::Message;

use super::administer::{HookAdministrator, dispatch_hook_administer};
use crate::application::handler::HookCommandHandler;
use crate::composition::hook_registry::CoreHookRegistry;
use crate::domain::service::HookOrchestrationService;
use crate::infrastructure::adapters::HookAdapterFactory;
use crate::infrastructure::capability::CapabilityExtensionRegistry;
use crate::infrastructure::config::{ConfigLoaderItem, ConfigWatcher, DatabaseConfigLoader};
use crate::infrastructure::persistence::PostgresHookConfigRepository;
use crate::interface::grpc::hooks::test_support::{
    CellKeys, WebhookReceiver, cell_keys, ingest_pre_send, serve_hook_plugin, signed_ctx,
    webhook_hook,
};
use crate::interface::grpc::{HookServiceServer, ImHookPluginServer};

const ENV_DATABASE_URL: &str = "FLARE_TENANT_TEST_DATABASE_URL";

fn suffix() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..12].to_string()
}

/// 用例写入的行名都形如 `w11h-…-{run}`；守卫被丢弃时（正常结束或断言失败的 unwind）把它们全部
/// 删掉。`Drop` 里不能 await、测试的运行时里也不能 `block_on`，所以另起一个线程，用自己的运行时
/// 与连接删，删完再返回。
struct RowCleanup {
    url: String,
    run: String,
}

impl RowCleanup {
    fn pattern(run: &str) -> String {
        format!("w11h-%{run}")
    }
}

impl Drop for RowCleanup {
    fn drop(&mut self) {
        let url = self.url.clone();
        let pattern = Self::pattern(&self.run);
        let outcome = std::thread::spawn(move || -> Result<u64, String> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            runtime.block_on(async move {
                let pool = sqlx::postgres::PgPoolOptions::new()
                    .max_connections(1)
                    .connect(&url)
                    .await
                    .map_err(|e| e.to_string())?;
                let deleted = sqlx::query("DELETE FROM hook_configs WHERE name LIKE $1")
                    .bind(&pattern)
                    .execute(&pool)
                    .await
                    .map_err(|e| e.to_string())?
                    .rows_affected();
                pool.close().await;
                Ok(deleted)
            })
        })
        .join();
        match outcome {
            Ok(Ok(deleted)) if deleted > 0 => {
                eprintln!("RowCleanup: removed {deleted} leftover hook_configs row(s)");
            }
            Ok(Ok(_)) => {}
            Ok(Err(error)) => eprintln!("RowCleanup failed: {error}"),
            Err(_) => eprintln!("RowCleanup thread panicked"),
        }
    }
}

fn employee(tenant: &str) -> HookAdministrator {
    super::administer::test_employee("employee-w11h", tenant, "request-w11h")
}

fn create(tenant: &str, name: &str, endpoint: &str) -> GenericRequest {
    let request = CreateHookConfigRequest {
        name: name.into(),
        hook_type: "pre_send".into(),
        tenant_id: tenant.into(),
        priority: 900,
        transport: Some(HookTransport {
            r#type: "webhook".into(),
            endpoint: endpoint.into(),
            timeout_ms: 2000,
            ..Default::default()
        }),
        selector: Some(HookSelector {
            tenants: vec![tenant.into()],
            ..Default::default()
        }),
        retry_policy: Some(HookRetryPolicy {
            max_retries: 0,
            ..Default::default()
        }),
        ..Default::default()
    };
    GenericRequest {
        operation: "flare.extension.v1.hook_config.create".into(),
        metadata: Default::default(),
        payload: Some(prost_types::Any {
            type_url: "type.googleapis.com/flare.capability.v1.CreateHookConfigRequest".into(),
            value: request.encode_to_vec(),
        }),
        request_id: format!("w11h-create-{name}"),
    }
}

fn delete(hook_id: &str) -> GenericRequest {
    GenericRequest {
        operation: "flare.extension.v1.hook_config.delete".into(),
        metadata: Default::default(),
        payload: Some(prost_types::Any {
            type_url: "type.googleapis.com/flare.capability.v1.DeleteHookConfigRequest".into(),
            value: DeleteHookConfigRequest {
                hook_id: hook_id.into(),
            }
            .encode_to_vec(),
        }),
        request_id: format!("w11h-delete-{hook_id}"),
    }
}

fn rename(hook_id: &str, name: &str) -> GenericRequest {
    GenericRequest {
        operation: "flare.extension.v1.hook_config.update".into(),
        metadata: Default::default(),
        payload: Some(prost_types::Any {
            type_url: "type.googleapis.com/flare.capability.v1.UpdateHookConfigRequest".into(),
            value: UpdateHookConfigRequest {
                hook_id: hook_id.into(),
                name: name.into(),
                ..Default::default()
            }
            .encode_to_vec(),
        }),
        request_id: format!("w11h-rename-{hook_id}"),
    }
}

fn created_id(response: GenericResponse) -> String {
    assert!(response.ok, "{response:?}");
    let config = CreateHookConfigResponse::decode(response.payload.unwrap().value.as_slice())
        .unwrap()
        .config
        .unwrap();
    assert!(!config.hook_id.is_empty());
    config.hook_id
}

fn watcher(
    repository: &Arc<PostgresHookConfigRepository>,
    pinned: Option<&str>,
) -> Arc<ConfigWatcher> {
    Arc::new(ConfigWatcher::new(
        vec![Arc::new(ConfigLoaderItem::Database(
            DatabaseConfigLoader::new(Arc::clone(repository), pinned.map(str::to_string)),
        ))],
        // 只有 Administer 的写入触发重载：证明不需要等定时刷新，更不需要重启。
        Duration::from_secs(3600),
    ))
}

/// 以租户 `tenant` 的签名用户发一次 PreSend（同 Message Ingest），返回投到的路径（已排序）。
async fn trigger(
    endpoint: &str,
    keys: &CellKeys,
    receiver: &WebhookReceiver,
    tenant: &str,
    marker: &str,
    expected: usize,
) -> Vec<String> {
    let ctx = signed_ctx(keys, SignedActorType::User, "u-w11h", tenant);
    match ingest_pre_send(endpoint, &ctx, marker).await {
        PreSendDecision::Continue => {}
        PreSendDecision::Reject { error } => panic!("pre-send for {tenant} rejected: {error}"),
    }
    receiver.wait_for(marker, expected).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let deliveries = receiver.with_marker(marker);
    assert!(
        deliveries.iter().all(|d| d.tenant() == Some(tenant)),
        "{deliveries:?}"
    );
    let mut paths: Vec<String> = deliveries.into_iter().map(|d| d.path).collect();
    paths.sort();
    paths
}

#[tokio::test]
#[ignore = "requires FLARE_TENANT_TEST_DATABASE_URL pointing at a database initialised with deploy/init.sql"]
async fn tenant_webhooks_registered_through_administer_run_only_for_their_tenant_without_restart() {
    let url = std::env::var(ENV_DATABASE_URL).unwrap_or_else(|_| {
        panic!("set {ENV_DATABASE_URL} to a database initialised with deploy/init.sql")
    });
    let repository = Arc::new(PostgresHookConfigRepository::new(&url).await.unwrap());
    let run = suffix();
    let _cleanup = RowCleanup {
        url: url.clone(),
        run: run.clone(),
    };
    let (tenant_a, tenant_b) = (format!("ta{run}"), format!("tb{run}"));
    let tenant_c = format!("tc{run}");
    // 全局行只作用于本用例的租户（见模块说明）；对这三个租户而言与不限租户的全局行相同。
    let test_tenants = vec![tenant_a.clone(), tenant_b.clone(), tenant_c.clone()];
    let receiver = WebhookReceiver::start().await;

    // 全局行：tenant_id IS NULL，对（选择器列出的）所有租户生效，包括没有登记任何 Hook 的 C。
    let global_name = format!("w11h-global-{run}");
    let mut global = webhook_hook(&global_name, &receiver.url("/global"));
    global.selector.tenants = test_tenants.clone();
    repository
        .save(None, "pre_send", &global, None)
        .await
        .unwrap();

    let watcher = watcher(&repository, None);
    watcher.start().await.unwrap();
    let registry = Arc::new(CoreHookRegistry::new(Arc::clone(&watcher)));
    let governance = HookServiceServer::new(Arc::clone(&repository), Arc::clone(&registry));
    let keys = cell_keys(91);
    let endpoint = serve_hook_plugin(ImHookPluginServer::new(
        Arc::new(HookCommandHandler::new(Arc::new(HookOrchestrationService))),
        Arc::clone(&registry),
        Arc::new(HookAdapterFactory::new()),
        CapabilityExtensionRegistry::new(),
        Some(keys.verifier.clone()),
    ))
    .await;

    assert_eq!(
        trigger(
            &endpoint,
            &keys,
            &receiver,
            &tenant_a,
            &format!("{run}-before"),
            1
        )
        .await,
        ["/global"]
    );

    let hook_a = created_id(
        dispatch_hook_administer(
            &governance,
            create(&tenant_a, &format!("w11h-a-{run}"), &receiver.url("/a")),
            &employee(&tenant_a),
        )
        .await
        .unwrap()
        .into_inner(),
    );
    let hook_b = created_id(
        dispatch_hook_administer(
            &governance,
            create(&tenant_b, &format!("w11h-b-{run}"), &receiver.url("/b")),
            &employee(&tenant_b),
        )
        .await
        .unwrap()
        .into_inner(),
    );
    // 员工只能登记本租户的。
    let status = dispatch_hook_administer(
        &governance,
        create(
            &tenant_b,
            &format!("w11h-cross-{run}"),
            &receiver.url("/cross"),
        ),
        &employee(&tenant_a),
    )
    .await
    .unwrap_err();
    assert_eq!(status.code(), tonic::Code::PermissionDenied, "{status:?}");

    assert_eq!(
        trigger(
            &endpoint,
            &keys,
            &receiver,
            &tenant_a,
            &format!("{run}-a"),
            2
        )
        .await,
        ["/a", "/global"]
    );
    assert_eq!(
        trigger(
            &endpoint,
            &keys,
            &receiver,
            &tenant_b,
            &format!("{run}-b"),
            2
        )
        .await,
        ["/b", "/global"]
    );
    assert_eq!(
        trigger(
            &endpoint,
            &keys,
            &receiver,
            &tenant_c,
            &format!("{run}-c"),
            1
        )
        .await,
        ["/global"]
    );

    // 全局 Hook 的名字为平台保留：租户登记或改名成同类型同名的被拒，停用的全局行也算。
    let off_name = format!("w11h-global-off-{run}");
    let mut off = webhook_hook(&off_name, &receiver.url("/global-off"));
    off.enabled = false;
    off.selector.tenants = test_tenants.clone();
    repository.save(None, "pre_send", &off, None).await.unwrap();
    for name in [&global_name, &off_name] {
        let status = dispatch_hook_administer(
            &governance,
            create(&tenant_a, name, &receiver.url("/a-noop")),
            &employee(&tenant_a),
        )
        .await
        .unwrap_err();
        assert_eq!(status.code(), tonic::Code::AlreadyExists, "{status:?}");
    }
    let status = dispatch_hook_administer(
        &governance,
        rename(&hook_a, &global_name),
        &employee(&tenant_a),
    )
    .await
    .unwrap_err();
    assert_eq!(status.code(), tonic::Code::AlreadyExists, "{status:?}");

    // 绕过 Administer 写进库的同名租户行（例如修复前登记的）顶替不了全局 Hook：全局的照常
    // 投递，租户那条不装。
    repository
        .save(
            Some(&tenant_a),
            "pre_send",
            &webhook_hook(&global_name, &receiver.url("/a-noop")),
            None,
        )
        .await
        .unwrap();
    registry.reload_tenant(&tenant_a).await.unwrap();
    assert_eq!(
        trigger(
            &endpoint,
            &keys,
            &receiver,
            &tenant_a,
            &format!("{run}-a-shadow"),
            2
        )
        .await,
        ["/a", "/global"]
    );
    watcher.reload().await.unwrap();
    assert_eq!(
        trigger(
            &endpoint,
            &keys,
            &receiver,
            &tenant_a,
            &format!("{run}-a-shadow-full"),
            2
        )
        .await,
        ["/a", "/global"]
    );
    assert!(
        repository
            .delete(Some(&tenant_a), "pre_send", &global_name)
            .await
            .unwrap()
    );

    // 固定 TENANT_ID 的进程不做这项限制（同以前）。
    let pinned_watcher = self::watcher(&repository, Some(&tenant_a));
    pinned_watcher.reload().await.unwrap();
    let pinned_governance = HookServiceServer::new(
        Arc::clone(&repository),
        Arc::new(CoreHookRegistry::new(pinned_watcher)),
    );
    let pinned_hook = created_id(
        dispatch_hook_administer(
            &pinned_governance,
            create(&tenant_a, &global_name, &receiver.url("/a-pinned")),
            &employee(&tenant_a),
        )
        .await
        .unwrap()
        .into_inner(),
    );
    let deleted = dispatch_hook_administer(
        &pinned_governance,
        delete(&pinned_hook),
        &employee(&tenant_a),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(deleted.ok, "{deleted:?}");
    assert!(
        repository
            .delete(None, "pre_send", &off_name)
            .await
            .unwrap()
    );

    // 全量重载（定时刷新走的路径）同样按租户分桶。
    watcher.reload().await.unwrap();
    assert_eq!(
        trigger(
            &endpoint,
            &keys,
            &receiver,
            &tenant_b,
            &format!("{run}-b-full"),
            2
        )
        .await,
        ["/b", "/global"]
    );

    // 固定 TENANT_ID 的单租户进程：全局行与该租户的行都加载，别的租户的行不加载（同以前）。
    let pinned = watcher_names(&repository, &tenant_a).await;
    assert!(pinned.contains(&global_name), "{pinned:?}");
    assert!(pinned.contains(&format!("w11h-a-{run}")), "{pinned:?}");
    assert!(!pinned.contains(&format!("w11h-b-{run}")), "{pinned:?}");

    // 删除后不重启即不再投递。
    let deleted = dispatch_hook_administer(&governance, delete(&hook_a), &employee(&tenant_a))
        .await
        .unwrap()
        .into_inner();
    assert!(deleted.ok, "{deleted:?}");
    assert_eq!(
        trigger(
            &endpoint,
            &keys,
            &receiver,
            &tenant_a,
            &format!("{run}-a-after"),
            1
        )
        .await,
        ["/global"]
    );
    assert_eq!(
        trigger(
            &endpoint,
            &keys,
            &receiver,
            &tenant_b,
            &format!("{run}-b-after"),
            2
        )
        .await,
        ["/b", "/global"]
    );

    let deleted = dispatch_hook_administer(&governance, delete(&hook_b), &employee(&tenant_b))
        .await
        .unwrap()
        .into_inner();
    assert!(deleted.ok, "{deleted:?}");
    assert!(
        repository
            .delete(None, "pre_send", &global_name)
            .await
            .unwrap()
    );
}

/// 固定 `pinned` 租户的监听器加载到的 PreSend Hook 名。
async fn watcher_names(
    repository: &Arc<PostgresHookConfigRepository>,
    pinned: &str,
) -> Vec<String> {
    let watcher = watcher(repository, Some(pinned));
    watcher.reload().await.unwrap();
    CoreHookRegistry::new(watcher)
        .get_pre_send_hooks(pinned)
        .await
        .unwrap()
        .into_iter()
        .map(|hook| hook.name)
        .collect()
}
