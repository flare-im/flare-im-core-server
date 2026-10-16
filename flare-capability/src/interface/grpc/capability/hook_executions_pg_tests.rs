//! 真库：Hook 执行证据写进 `hook_executions`（源 DDL 0015 的结构），`hook_config.get_statistics` 与
//! `hook_config.query_executions` 经 Administer 读回，按签名租户与 Hook 限定，计数按采样权重加权；
//! 保留期清理分批删最旧的行。
//!
//! 需要 `FLARE_TENANT_TEST_DATABASE_URL`（用 `deploy/init.sql` 或 `migrations/` 初始化的库）。用例标了
//! `#[ignore]`：默认的 `cargo test` 只记为 ignored，`-- --ignored` 显式运行时变量必须已设置，否则直接
//! 失败。只写随机租户与随机名的行，结束时（含断言失败的 unwind）删除；保留期用例把自己的行写在
//! 2000 年，截止时间取 2001 年，删不到别人的行。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use flare_grpc_proto::capability::{
    CreateHookConfigRequest, CreateHookConfigResponse, GenericRequest, GenericResponse,
    GetHookStatisticsRequest, GetHookStatisticsResponse, HookSelector, HookStatistics,
    HookTransport, QueryHookExecutionsRequest, QueryHookExecutionsResponse,
};
use flare_im_hooks::PreSendDecision;
use flare_proto::common::{Pagination, TimeRange};
use flare_server_core::SignedActorType;
use prost::Message;
use sqlx::PgPool;
use tokio::sync::oneshot;

use super::administer::{HookAdministrator, dispatch_hook_administer};
use crate::application::handler::{HookCommandHandler, HookQueryHandler};
use crate::composition::hook_registry::CoreHookRegistry;
use crate::domain::model::{
    HOOK_CIRCUIT_OPEN_CODE, HOOK_TIMEOUT_CODE, HookExecutionFilter, HookExecutionOutcome,
    HookExecutionRecord, HookExecutionScope, HookExecutionTimeRange,
};
use crate::domain::repository::HookExecutionRepository;
use crate::domain::service::{HookCircuitPolicy, HookExecutionGuard, HookOrchestrationService};
use crate::infrastructure::adapters::HookAdapterFactory;
use crate::infrastructure::capability::CapabilityExtensionRegistry;
use crate::infrastructure::config::{ConfigLoaderItem, ConfigWatcher, DatabaseConfigLoader};
use crate::infrastructure::hook_execution::{
    HookExecutionBuffer, HookExecutionBufferSettings, HookExecutionMetrics, run_writer,
};
use crate::infrastructure::persistence::{
    PostgresHookConfigRepository, PostgresHookExecutionRepository,
};
use crate::interface::grpc::hooks::test_support::{
    WebhookReceiver, cell_keys, ingest_pre_send, serve_hook_plugin, signed_ctx,
};
use crate::interface::grpc::{HookServiceServer, ImHookPluginServer};

const ENV_DATABASE_URL: &str = "FLARE_TENANT_TEST_DATABASE_URL";

fn database_url() -> String {
    std::env::var(ENV_DATABASE_URL).unwrap_or_else(|_| {
        panic!(
            "set {ENV_DATABASE_URL} to a database initialised with deploy/init.sql or migrations/"
        )
    })
}

fn run_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..12].to_string()
}

/// 删掉本用例租户的执行记录与本用例名字的 Hook 配置；`Drop` 里不能 await，另起线程与运行时。
struct RowCleanup {
    url: String,
    tenants: Vec<String>,
    name_pattern: Option<String>,
}

impl Drop for RowCleanup {
    fn drop(&mut self) {
        let url = self.url.clone();
        let tenants = self.tenants.clone();
        let pattern = self.name_pattern.clone();
        let outcome = std::thread::spawn(move || -> Result<(), String> {
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
                sqlx::query("DELETE FROM hook_executions WHERE tenant_id = ANY($1)")
                    .bind(&tenants)
                    .execute(&pool)
                    .await
                    .map_err(|e| e.to_string())?;
                if let Some(pattern) = pattern {
                    sqlx::query("DELETE FROM hook_configs WHERE name LIKE $1")
                        .bind(&pattern)
                        .execute(&pool)
                        .await
                        .map_err(|e| e.to_string())?;
                }
                pool.close().await;
                Ok(())
            })
        })
        .join();
        if !matches!(outcome, Ok(Ok(()))) {
            eprintln!("hook execution test rows were not cleaned up: {outcome:?}");
        }
    }
}

async fn pool(url: &str) -> Arc<PgPool> {
    Arc::new(
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(url)
            .await
            .unwrap(),
    )
}

fn record(
    tenant: &str,
    name: &str,
    outcome: HookExecutionOutcome,
    latency_ms: u64,
    weight: u32,
    executed_at: SystemTime,
    message_id: &str,
) -> HookExecutionRecord {
    HookExecutionRecord {
        execution_id: uuid::Uuid::new_v4().to_string(),
        tenant_id: tenant.into(),
        hook_type: "pre_send".into(),
        hook_name: name.into(),
        hook_version: Some("v7".into()),
        message_id: Some(message_id.into()),
        request_id: Some("req-r75a".into()),
        trace_id: None,
        outcome,
        latency: Duration::from_millis(latency_ms),
        executed_at,
        sample_weight: weight,
    }
}

fn scope(tenant: &str, name: &str) -> HookExecutionScope {
    HookExecutionScope {
        tenant_id: tenant.into(),
        hook_type: "pre_send".into(),
        hook_name: name.into(),
    }
}

/// 一批写入一条语句；统计按租户与 Hook 限定、按 `sample_weight` 加权（失败、拒绝、成功、熔断短路
/// 各自计数，耗时不含熔断短路，p99 加权）；查询从新到旧、按消息号与只看成功过滤；别的租户同名
/// Hook 的行不进统计也查不到。
#[tokio::test]
#[ignore = "requires FLARE_TENANT_TEST_DATABASE_URL pointing at a database initialised with deploy/init.sql or migrations/"]
async fn the_repository_writes_batches_and_reads_weighted_tenant_scoped_statistics() {
    let url = database_url();
    let run = run_id();
    let (tenant, other) = (format!("r75a-{run}"), format!("r75a-o-{run}"));
    let _cleanup = RowCleanup {
        url: url.clone(),
        tenants: vec![tenant.clone(), other.clone()],
        name_pattern: None,
    };
    let repository = PostgresHookExecutionRepository::new(pool(&url).await);
    let base = SystemTime::now() - Duration::from_secs(60);
    let at = |seconds: u64| base + Duration::from_secs(seconds);
    let rejected = HookExecutionOutcome::Rejected {
        reason: "SPAM".into(),
    };
    let records = vec![
        record(
            &tenant,
            "dlp",
            HookExecutionOutcome::TimedOut,
            100,
            1,
            at(1),
            "m1",
        ),
        record(
            &tenant,
            "dlp",
            HookExecutionOutcome::TimedOut,
            200,
            1,
            at(2),
            "m2",
        ),
        record(
            &tenant,
            "dlp",
            HookExecutionOutcome::TimedOut,
            300,
            1,
            at(3),
            "m3",
        ),
        record(
            &tenant,
            "dlp",
            HookExecutionOutcome::Allowed,
            10,
            10,
            at(4),
            "m4",
        ),
        record(
            &tenant,
            "dlp",
            HookExecutionOutcome::Allowed,
            10,
            10,
            at(5),
            "m5",
        ),
        record(&tenant, "dlp", rejected, 20, 1, at(6), "m6"),
        record(
            &tenant,
            "dlp",
            HookExecutionOutcome::CircuitOpen,
            0,
            10,
            at(7),
            "m7",
        ),
        record(
            &tenant,
            "other-hook",
            HookExecutionOutcome::TimedOut,
            900,
            1,
            at(8),
            "m8",
        ),
        record(
            &other,
            "dlp",
            HookExecutionOutcome::TimedOut,
            900,
            1,
            at(9),
            "m9",
        ),
    ];
    assert_eq!(repository.insert_batch(&records).await.unwrap(), 9);
    // 同一批重放（写入任务不会这样做，主键冲突只跳过）不产生重复行。
    assert_eq!(repository.insert_batch(&records[..2]).await.unwrap(), 0);

    let range = HookExecutionTimeRange {
        from: base,
        to: SystemTime::now() + Duration::from_secs(1),
    };
    let stats = repository
        .statistics(&scope(&tenant, "dlp"), &range)
        .await
        .unwrap();
    assert_eq!(stats.total_executions, 34);
    assert_eq!(stats.success_count, 21, "allowed 20 + rejected 1");
    assert_eq!(stats.failure_count, 13, "timeouts 3 + circuit open 10");
    assert_eq!(stats.circuit_open_count, 10);
    assert_eq!(
        stats.error_count_by_code,
        HashMap::from([
            (HOOK_TIMEOUT_CODE.to_string(), 3),
            (HOOK_CIRCUIT_OPEN_CODE.to_string(), 10),
        ])
    );
    // (100 + 200 + 300 + 10*10*2 + 20) / (3 + 20 + 1)
    assert!(
        (stats.avg_latency_ms - 820.0 / 24.0).abs() < 1e-9,
        "{stats:?}"
    );
    assert_eq!(stats.p99_latency_ms, 300.0);

    // 时间范围之外的不计。
    let early = HookExecutionTimeRange {
        from: base,
        to: at(3),
    };
    let stats = repository
        .statistics(&scope(&tenant, "dlp"), &early)
        .await
        .unwrap();
    assert_eq!(stats.total_executions, 2);

    let filter = |message_id: Option<&str>, success_only: bool, limit: u32| HookExecutionFilter {
        scope: scope(&tenant, "dlp"),
        range,
        message_id: message_id.map(str::to_string),
        success_only,
        limit,
    };
    let newest = repository.query(&filter(None, false, 3)).await.unwrap();
    let messages: Vec<_> = newest
        .iter()
        .map(|e| e.message_id.clone().unwrap())
        .collect();
    assert_eq!(messages, ["m7", "m6", "m5"]);
    assert_eq!(
        newest[0].error_code.as_deref(),
        Some(HOOK_CIRCUIT_OPEN_CODE)
    );
    assert_eq!(newest[0].sample_weight, 10);
    assert_eq!(newest[1].error_code.as_deref(), Some("HOOK_REJECTED"));
    assert_eq!(newest[1].error_message.as_deref(), Some("SPAM"));
    assert!(newest[1].success);
    let successes = repository.query(&filter(None, true, 100)).await.unwrap();
    assert_eq!(successes.len(), 3);
    assert!(successes.iter().all(|e| e.success));
    let one = repository
        .query(&filter(Some("m2"), false, 100))
        .await
        .unwrap();
    assert_eq!(one.len(), 1);
    assert_eq!(one[0].latency_ms, Some(200));
    // 别的租户的同名 Hook 不出现。
    assert!(
        repository
            .query(&filter(Some("m9"), false, 100))
            .await
            .unwrap()
            .is_empty()
    );
    let other_stats = repository
        .statistics(&scope(&other, "dlp"), &range)
        .await
        .unwrap();
    assert_eq!(other_stats.total_executions, 1);
}

/// 保留期清理按执行时间从旧到新分批删，只删早于截止时间的行。
#[tokio::test]
#[ignore = "requires FLARE_TENANT_TEST_DATABASE_URL pointing at a database initialised with deploy/init.sql or migrations/"]
async fn the_retention_sweep_deletes_only_expired_rows_in_batches() {
    let url = database_url();
    let run = run_id();
    let tenant = format!("r75a-ret-{run}");
    let _cleanup = RowCleanup {
        url: url.clone(),
        tenants: vec![tenant.clone()],
        name_pattern: None,
    };
    let pool = pool(&url).await;
    let repository = PostgresHookExecutionRepository::new(Arc::clone(&pool));
    let year_2000 = SystemTime::UNIX_EPOCH + Duration::from_secs(946_684_800);
    let mut records: Vec<_> = (0..20)
        .map(|i| {
            record(
                &tenant,
                "dlp",
                HookExecutionOutcome::TimedOut,
                1,
                1,
                year_2000 + Duration::from_secs(i),
                &format!("old-{i:02}"),
            )
        })
        .collect();
    records.push(record(
        &tenant,
        "dlp",
        HookExecutionOutcome::TimedOut,
        1,
        1,
        SystemTime::now(),
        "fresh",
    ));
    repository.insert_batch(&records).await.unwrap();
    let cutoff = year_2000 + Duration::from_secs(366 * 86_400);
    // 一批至多 limit 行：修复前删除写成 `IN (SELECT .. LIMIT .. FOR UPDATE)`，规划器做成每个外层行
    // 重扫一次的半连接，一条语句删光了全部过期行。
    assert_eq!(
        repository.delete_executed_before(cutoff, 2).await.unwrap(),
        2
    );
    let left: Vec<String> = sqlx::query_scalar(
        "SELECT message_id FROM hook_executions WHERE tenant_id = $1 ORDER BY executed_at",
    )
    .bind(&tenant)
    .fetch_all(pool.as_ref())
    .await
    .unwrap();
    assert_eq!(left.len(), 19);
    assert_eq!(left[..2], ["old-02", "old-03"], "oldest first");
    assert_eq!(left.last().map(String::as_str), Some("fresh"));
    assert_eq!(
        repository.delete_executed_before(cutoff, 5).await.unwrap(),
        5
    );
    assert_eq!(
        repository
            .delete_executed_before(cutoff, 100)
            .await
            .unwrap(),
        13
    );
    assert_eq!(
        repository.delete_executed_before(cutoff, 10).await.unwrap(),
        0
    );
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM hook_executions WHERE tenant_id = $1")
        .bind(&tenant)
        .fetch_one(pool.as_ref())
        .await
        .unwrap();
    assert_eq!(left, 1);
}

fn employee(tenant: &str) -> HookAdministrator {
    super::administer::test_employee("employee-r75a", tenant, "request-r75a")
}

fn generic(operation: &str, type_name: &str, value: Vec<u8>) -> GenericRequest {
    GenericRequest {
        operation: format!("flare.extension.v1.hook_config.{operation}"),
        metadata: Default::default(),
        payload: Some(prost_types::Any {
            type_url: format!("type.googleapis.com/flare.capability.v1.{type_name}"),
            value,
        }),
        request_id: format!("r75a-{operation}"),
    }
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
        ..Default::default()
    };
    generic("create", "CreateHookConfigRequest", request.encode_to_vec())
}

fn created_id(response: GenericResponse) -> String {
    assert!(response.ok, "{response:?}");
    CreateHookConfigResponse::decode(response.payload.unwrap().value.as_slice())
        .unwrap()
        .config
        .unwrap()
        .hook_id
}

async fn statistics(
    governance: &HookServiceServer,
    tenant: &str,
    hook_id: &str,
) -> Result<HookStatistics, tonic::Status> {
    let request = GetHookStatisticsRequest {
        hook_id: hook_id.into(),
        time_range: None,
    };
    let response = dispatch_hook_administer(
        governance,
        generic(
            "get_statistics",
            "GetHookStatisticsRequest",
            request.encode_to_vec(),
        ),
        &employee(tenant),
    )
    .await?
    .into_inner();
    Ok(
        GetHookStatisticsResponse::decode(response.payload.unwrap().value.as_slice())
            .unwrap()
            .statistics
            .unwrap(),
    )
}

async fn executions(
    governance: &HookServiceServer,
    tenant: &str,
    hook_id: &str,
    message_id: &str,
    success_only: bool,
    limit: i32,
) -> QueryHookExecutionsResponse {
    let request = QueryHookExecutionsRequest {
        hook_id: hook_id.into(),
        message_id: message_id.into(),
        success_only,
        time_range: Some(TimeRange {
            start_time: Some(0),
            end_time: None,
        }),
        pagination: Some(Pagination {
            limit,
            ..Default::default()
        }),
    };
    let response = dispatch_hook_administer(
        governance,
        generic(
            "query_executions",
            "QueryHookExecutionsRequest",
            request.encode_to_vec(),
        ),
        &employee(tenant),
    )
    .await
    .unwrap()
    .into_inner();
    QueryHookExecutionsResponse::decode(response.payload.unwrap().value.as_slice()).unwrap()
}

async fn closed_port_url() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    format!("http://{address}/hook")
}

/// 端到端：两个租户各经 Administer 登记一条同名 PreSend Webhook（A 的放行，B 的连不上），按
/// Message Ingest 的方式发 PreSend；执行守卫按 (租户, Hook) 熔断，记录经缓冲批量写库；员工经
/// Administer 读到的统计与执行记录只是本租户这条 Hook 的，读别的租户的 Hook 被拒。
#[tokio::test]
#[ignore = "requires FLARE_TENANT_TEST_DATABASE_URL pointing at a database initialised with deploy/init.sql or migrations/"]
async fn administer_reads_the_executions_recorded_from_hook_plugin_calls() {
    let url = database_url();
    let run = run_id();
    let (tenant_a, tenant_b) = (format!("r75a-a{run}"), format!("r75a-b{run}"));
    let name = format!("r75a-dlp-{run}");
    let _cleanup = RowCleanup {
        url: url.clone(),
        tenants: vec![tenant_a.clone(), tenant_b.clone()],
        name_pattern: Some(format!("r75a-%{run}")),
    };
    let config_repository = Arc::new(PostgresHookConfigRepository::new(&url).await.unwrap());
    let execution_repository: Arc<dyn HookExecutionRepository> = Arc::new(
        PostgresHookExecutionRepository::new(config_repository.connection_pool()),
    );
    let watcher = Arc::new(ConfigWatcher::new(
        vec![Arc::new(ConfigLoaderItem::Database(
            DatabaseConfigLoader::new(Arc::clone(&config_repository), None),
        ))],
        Duration::from_secs(3600),
    ));
    watcher.start().await.unwrap();
    let registry = Arc::new(CoreHookRegistry::new(Arc::clone(&watcher)));
    let governance = HookServiceServer::new(Arc::clone(&config_repository), Arc::clone(&registry))
        .with_executions(Arc::new(HookQueryHandler::new(Arc::clone(
            &execution_repository,
        ))));

    // 记录全量（采样 1/1），攒满 1 条即写；熔断阈值 2。
    let buffer = Arc::new(HookExecutionBuffer::new(
        HookExecutionBufferSettings {
            capacity: 1_000,
            batch_size: 1,
            sample_every: 1,
        },
        Arc::new(HookExecutionMetrics::new()),
    ));
    let (stop_writer, writer_shutdown) = oneshot::channel();
    let writer = tokio::spawn(run_writer(
        Arc::clone(&buffer),
        Arc::clone(&execution_repository),
        Duration::from_millis(50),
        writer_shutdown,
    ));
    let guard = Arc::new(HookExecutionGuard::new(
        HookCircuitPolicy {
            enabled: true,
            failure_threshold: 2,
            failure_window: Duration::from_secs(30),
            open_for: Duration::from_secs(600),
        },
        buffer.clone(),
    ));
    let keys = cell_keys(75);
    let endpoint = serve_hook_plugin(
        ImHookPluginServer::new(
            Arc::new(HookCommandHandler::new(Arc::new(HookOrchestrationService))),
            Arc::clone(&registry),
            Arc::new(HookAdapterFactory::new()),
            CapabilityExtensionRegistry::new(),
            Some(keys.verifier.clone()),
        )
        .with_execution_guard(guard),
    )
    .await;

    let receiver = WebhookReceiver::start().await;
    let hook_a = created_id(
        dispatch_hook_administer(
            &governance,
            create(&tenant_a, &name, &receiver.url("/a")),
            &employee(&tenant_a),
        )
        .await
        .unwrap()
        .into_inner(),
    );
    let hook_b = created_id(
        dispatch_hook_administer(
            &governance,
            create(&tenant_b, &name, &closed_port_url().await),
            &employee(&tenant_b),
        )
        .await
        .unwrap()
        .into_inner(),
    );

    for i in 0..2 {
        let ctx = signed_ctx(&keys, SignedActorType::User, "u-r75a", &tenant_a);
        match ingest_pre_send(&endpoint, &ctx, &format!("{run}-a{i}")).await {
            PreSendDecision::Continue => {}
            PreSendDecision::Reject { error } => panic!("tenant A rejected: {error}"),
        }
    }
    for i in 0..3 {
        let ctx = signed_ctx(&keys, SignedActorType::User, "u-r75a", &tenant_b);
        match ingest_pre_send(&endpoint, &ctx, &format!("{run}-b{i}")).await {
            PreSendDecision::Reject { error } => assert_eq!(
                error.code(),
                Some(flare_server_core::error::ErrorCode::ServiceUnavailable),
                "{error}"
            ),
            PreSendDecision::Continue => panic!("tenant B's required hook is unreachable"),
        }
    }
    assert_eq!(receiver.count(), 2, "only A's hook reaches the receiver");

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let (stats_a, stats_b) = loop {
        let a = statistics(&governance, &tenant_a, &hook_a).await.unwrap();
        let b = statistics(&governance, &tenant_b, &hook_b).await.unwrap();
        if a.total_executions == 2 && b.total_executions == 3 {
            break (a, b);
        }
        assert!(
            std::time::Instant::now() < deadline,
            "rows not written: {a:?} {b:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(stats_a.hook_id, hook_a);
    assert_eq!((stats_a.success_count, stats_a.failure_count), (2, 0));
    assert_eq!((stats_b.success_count, stats_b.failure_count), (0, 3));
    assert_eq!(stats_b.circuit_break_count, 1);
    assert_eq!(
        stats_b.error_count_by_code.get(HOOK_CIRCUIT_OPEN_CODE),
        Some(&1)
    );
    assert_eq!(
        stats_b.error_count_by_code.values().sum::<i64>(),
        3,
        "{stats_b:?}"
    );

    let page = executions(&governance, &tenant_a, &hook_a, "", false, 1).await;
    assert_eq!(page.executions.len(), 1);
    assert!(page.pagination.as_ref().unwrap().has_more);
    assert_eq!(
        page.executions[0].message_id,
        format!("{run}-a1"),
        "newest first"
    );
    assert_eq!(page.executions[0].hook_id, hook_a);
    assert!(page.executions[0].success);
    let page = executions(
        &governance,
        &tenant_a,
        &hook_a,
        &format!("{run}-a0"),
        false,
        10,
    )
    .await;
    assert_eq!(page.executions.len(), 1);
    assert!(!page.pagination.unwrap().has_more);
    let page = executions(&governance, &tenant_b, &hook_b, "", true, 10).await;
    assert!(page.executions.is_empty(), "B has no successful execution");
    let page = executions(&governance, &tenant_b, &hook_b, "", false, 10).await;
    assert_eq!(page.executions.len(), 3);
    assert!(
        page.executions
            .iter()
            .all(|e| !e.success && !e.error_code.is_empty())
    );
    assert!(
        page.executions
            .iter()
            .all(|e| !e.error_message.contains("127.0.0.1")),
        "no downstream error text: {:?}",
        page.executions
    );

    // 时间超出 9999 年末（换算时间戳会 panic 的值）：两个读接口都回 INVALID_ARGUMENT。
    let huge = TimeRange {
        start_time: None,
        end_time: Some(9_000_000_000_000_000),
    };
    for (operation, type_name, payload) in [
        (
            "get_statistics",
            "GetHookStatisticsRequest",
            GetHookStatisticsRequest {
                hook_id: hook_a.clone(),
                time_range: Some(huge),
            }
            .encode_to_vec(),
        ),
        (
            "query_executions",
            "QueryHookExecutionsRequest",
            QueryHookExecutionsRequest {
                hook_id: hook_a.clone(),
                time_range: Some(huge),
                ..Default::default()
            }
            .encode_to_vec(),
        ),
    ] {
        let status = dispatch_hook_administer(
            &governance,
            generic(operation, type_name, payload),
            &employee(&tenant_a),
        )
        .await
        .unwrap_err();
        assert_eq!(
            status.code(),
            tonic::Code::InvalidArgument,
            "{operation}: {status:?}"
        );
    }

    // 员工读不到别的租户的 Hook。
    let status = statistics(&governance, &tenant_a, &hook_b)
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::PermissionDenied, "{status:?}");

    // 直接用领域查询核对：A 的行只在 A 名下。
    let rows = HookQueryHandler::new(Arc::clone(&execution_repository))
        .executions(&HookExecutionFilter {
            scope: HookExecutionScope {
                tenant_id: tenant_b.clone(),
                hook_type: "pre_send".into(),
                hook_name: name.clone(),
            },
            range: HookExecutionTimeRange {
                from: SystemTime::UNIX_EPOCH,
                to: SystemTime::now() + Duration::from_secs(1),
            },
            message_id: Some(format!("{run}-a0")),
            success_only: false,
            limit: 10,
        })
        .await
        .unwrap();
    assert!(rows.is_empty());

    stop_writer.send(()).unwrap();
    writer.await.unwrap();
    assert!(buffer.is_empty());
}
