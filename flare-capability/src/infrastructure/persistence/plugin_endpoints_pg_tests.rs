//! 真库：经 `RegisterPluginEndpoint` 登记的插件端点写进 `capability_plugin_endpoints`，另一个
//! 实例重读后看得到、注销后看不到，重启后载回，超过 TTL 不健康的登记被清理（配置发现的端点不受影响）。
//!
//! 需要 `FLARE_TENANT_TEST_DATABASE_URL`（用 `deploy/init.sql` 或 `migrations/` 初始化的库）。用例标了
//! `#[ignore]`：默认的 `cargo test` 只记为 ignored，`-- --ignored` 显式运行时变量必须已设置，否则直接
//! 失败。只写随机租户的行，结束时（含断言失败的 unwind）删除；TTL 用例把自己的行回拨到 2000 年，
//! 清理的截止时间取 2001 年，删不到别人的行。

use std::sync::Arc;

use flare_grpc_proto::capability::RegisteredPluginInstance;
use sqlx::PgPool;

use super::PostgresPluginEndpointRepository;
use crate::domain::repository::PluginEndpointRepository;
use crate::infrastructure::capability::PluginRouteBook;
use crate::infrastructure::capability::routing::RouteOrigin;

const ENV_DATABASE_URL: &str = "FLARE_TENANT_TEST_DATABASE_URL";

/// 删掉本用例租户的全部行；`Drop` 里不能 await，另起线程与运行时。
struct RowCleanup {
    url: String,
    tenants: Vec<String>,
}

impl Drop for RowCleanup {
    fn drop(&mut self) {
        let url = self.url.clone();
        let tenants = self.tenants.clone();
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
                sqlx::query("DELETE FROM capability_plugin_endpoints WHERE tenant_id = ANY($1)")
                    .bind(&tenants)
                    .execute(&pool)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(())
            })
        })
        .join();
        if !matches!(outcome, Ok(Ok(()))) {
            eprintln!("plugin endpoint test rows were not cleaned up: {outcome:?}");
        }
    }
}

struct Fixture {
    pool: Arc<PgPool>,
    tenant: String,
    other_tenant: String,
    _cleanup: RowCleanup,
}

async fn fixture() -> Fixture {
    let url = std::env::var(ENV_DATABASE_URL).unwrap_or_else(|_| {
        panic!(
            "set {ENV_DATABASE_URL} to a database initialised with deploy/init.sql or migrations/"
        )
    });
    let pool = Arc::new(
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await
            .unwrap(),
    );
    let run = uuid::Uuid::new_v4().simple().to_string()[..12].to_string();
    let (tenant, other_tenant) = (format!("pe-a-{run}"), format!("pe-b-{run}"));
    Fixture {
        pool,
        _cleanup: RowCleanup {
            url,
            tenants: vec![tenant.clone(), other_tenant.clone()],
        },
        tenant,
        other_tenant,
    }
}

fn book(pool: &Arc<PgPool>) -> PluginRouteBook {
    PluginRouteBook::with_store(Arc::new(PostgresPluginEndpointRepository::new(Arc::clone(
        pool,
    ))))
}

fn instance(plugin_id: &str, declared: &[&str]) -> RegisteredPluginInstance {
    RegisteredPluginInstance {
        plugin_id: plugin_id.to_string(),
        capability_id: "echo.call".to_string(),
        grpc_authority: "http://127.0.0.1:9".to_string(),
        labels: [("region".to_string(), "eu \"west\"".to_string())].into(),
        plugin_version: "1.2.0".to_string(),
        api_version: "v2".to_string(),
        manifest_sha256: "ab".repeat(32),
        declared_operations: declared.iter().map(|op| op.to_string()).collect(),
        unverified: declared.is_empty(),
    }
}

fn ids(book: &PluginRouteBook, tenant: &str) -> Vec<String> {
    book.list_filtered(tenant, "")
        .into_iter()
        .map(|instance| instance.plugin_id)
        .collect()
}

#[tokio::test]
#[ignore = "requires FLARE_TENANT_TEST_DATABASE_URL pointing at a database initialised with deploy/init.sql or migrations/"]
async fn registrations_persist_converge_across_instances_and_survive_a_restart() {
    let f = fixture().await;
    let first = book(&f.pool);
    let second = book(&f.pool);
    let registered = instance("plugin-1", &["echo.call", "echo.hangup"]);
    first.register(&f.tenant, registered.clone()).await.unwrap();
    first
        .register(&f.other_tenant, instance("plugin-2", &[]))
        .await
        .unwrap();

    // Register persists, every field round-trips.
    let repository = PostgresPluginEndpointRepository::new(Arc::clone(&f.pool));
    let stored: Vec<_> = repository
        .list_all()
        .await
        .unwrap()
        .into_iter()
        .filter(|endpoint| endpoint.tenant_id == f.tenant)
        .collect();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].instance, registered);
    assert_eq!(
        first.origin(&f.tenant, "plugin-1"),
        Some(RouteOrigin::Registered)
    );

    // A second instance sees it after its refresh, tenant by tenant.
    assert!(ids(&second, &f.tenant).is_empty());
    second.reload().await.unwrap();
    assert_eq!(ids(&second, &f.tenant), ["plugin-1"]);
    assert_eq!(ids(&second, &f.other_tenant), ["plugin-2"]);
    assert_eq!(second.list_filtered(&f.tenant, "echo.hangup").len(), 1);

    // Re-registration updates the row in place.
    let mut moved = registered.clone();
    moved.grpc_authority = "http://127.0.0.1:10".to_string();
    second.register(&f.tenant, moved.clone()).await.unwrap();
    first.reload().await.unwrap();
    assert_eq!(first.list_filtered(&f.tenant, "")[0], moved);

    // Deregister removes the row; the other instance drops it on its next refresh.
    assert!(first.deregister(&f.tenant, "plugin-1").await.unwrap());
    assert!(!first.deregister(&f.tenant, "plugin-1").await.unwrap());
    assert!(ids(&first, &f.tenant).is_empty());
    assert_eq!(ids(&second, &f.tenant), ["plugin-1"]);
    second.reload().await.unwrap();
    assert!(ids(&second, &f.tenant).is_empty());

    // A restarted instance (a new, empty book) loads what is left.
    let restarted = book(&f.pool);
    restarted.reload().await.unwrap();
    assert!(ids(&restarted, &f.tenant).is_empty());
    assert_eq!(ids(&restarted, &f.other_tenant), ["plugin-2"]);
    assert_eq!(
        restarted.origin(&f.other_tenant, "plugin-2"),
        Some(RouteOrigin::Registered)
    );
}

#[tokio::test]
#[ignore = "requires FLARE_TENANT_TEST_DATABASE_URL pointing at a database initialised with deploy/init.sql or migrations/"]
async fn a_refresh_keeps_config_endpoints_and_newer_local_writes() {
    let f = fixture().await;
    let routes = book(&f.pool);
    // Config-discovered endpoints are never written to the table and survive every refresh.
    routes.upsert(&f.tenant, instance("discovered", &[]));
    routes.reload().await.unwrap();
    assert_eq!(ids(&routes, &f.tenant), ["discovered"]);
    assert_eq!(
        routes.origin(&f.tenant, "discovered"),
        Some(RouteOrigin::Config)
    );
    let stored = PostgresPluginEndpointRepository::new(Arc::clone(&f.pool))
        .list_all()
        .await
        .unwrap();
    assert!(!stored.iter().any(|endpoint| endpoint.tenant_id == f.tenant));

    // A registration of the same key replaces the configured one and is persisted.
    routes
        .register(&f.tenant, instance("discovered", &["echo.call"]))
        .await
        .unwrap();
    routes.reload().await.unwrap();
    assert_eq!(
        routes.origin(&f.tenant, "discovered"),
        Some(RouteOrigin::Registered)
    );
}

#[tokio::test]
#[ignore = "requires FLARE_TENANT_TEST_DATABASE_URL pointing at a database initialised with deploy/init.sql or migrations/"]
async fn registrations_not_healthy_within_the_ttl_are_cleaned_up() {
    let f = fixture().await;
    let routes = book(&f.pool);
    for plugin in ["stale", "healthy", "fresh"] {
        routes
            .register(&f.tenant, instance(plugin, &[]))
            .await
            .unwrap();
    }
    routes.upsert(&f.tenant, instance("discovered", &[]));
    // Back-date two registrations to 2000; the TTL below puts the cutoff at 2001, so only rows of
    // this test are old enough. "fresh" keeps its current updated_at.
    sqlx::query(
        "UPDATE capability_plugin_endpoints
         SET registered_at = '2000-01-01T00:00:00Z', updated_at = '2000-01-01T00:00:00Z'
         WHERE tenant_id = $1 AND plugin_id IN ('stale', 'healthy')",
    )
    .bind(&f.tenant)
    .execute(f.pool.as_ref())
    .await
    .unwrap();
    // The health round records "healthy" (and ignores the config endpoint, which has no row).
    let recorded = routes
        .persist_health(&[
            (f.tenant.clone(), "healthy".to_string()),
            (f.tenant.clone(), "discovered".to_string()),
        ])
        .await
        .unwrap();
    assert_eq!(recorded, 1);

    let since_2001 = (chrono::Utc::now()
        - "2001-01-01T00:00:00Z"
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap())
    .to_std()
    .unwrap();
    assert_eq!(routes.cleanup_stale(since_2001).await.unwrap(), 1);
    assert_eq!(ids(&routes, &f.tenant), ["discovered", "fresh", "healthy"]);

    // Another instance converges on its refresh; a second pass finds nothing left to remove.
    let other = book(&f.pool);
    other.reload().await.unwrap();
    assert_eq!(ids(&other, &f.tenant), ["fresh", "healthy"]);
    assert_eq!(other.cleanup_stale(since_2001).await.unwrap(), 0);
    assert_eq!(ids(&other, &f.tenant), ["fresh", "healthy"]);
}

#[tokio::test]
#[ignore = "requires FLARE_TENANT_TEST_DATABASE_URL pointing at a database initialised with deploy/init.sql or migrations/"]
async fn the_table_refuses_rows_the_rpc_would_refuse() {
    let f = fixture().await;
    for (column, value) in [
        ("tenant_id", "'*'"),
        ("plugin_id", "''"),
        ("grpc_authority", "''"),
        ("labels", "'[]'::jsonb"),
        ("unverified", "TRUE"),
    ] {
        let mut values = vec![
            ("tenant_id", format!("'{}'", f.tenant)),
            ("plugin_id", "'p'".to_string()),
            ("capability_id", "'c'".to_string()),
            ("grpc_authority", "'http://p:9'".to_string()),
            ("labels", "'{}'::jsonb".to_string()),
            ("declared_operations", "ARRAY['c']".to_string()),
            ("unverified", "FALSE".to_string()),
        ];
        for entry in &mut values {
            if entry.0 == column {
                entry.1 = value.to_string();
            }
        }
        let sql = format!(
            "INSERT INTO capability_plugin_endpoints ({}) VALUES ({})",
            values
                .iter()
                .map(|(c, _)| *c)
                .collect::<Vec<_>>()
                .join(", "),
            values
                .iter()
                .map(|(_, v)| v.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        let error = sqlx::query(&sql)
            .execute(f.pool.as_ref())
            .await
            .expect_err(column);
        assert!(
            error.to_string().contains("violates check constraint"),
            "{column}: {error}"
        );
    }
}
