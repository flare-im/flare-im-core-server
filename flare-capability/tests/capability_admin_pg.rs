//! 管理员只读数据源的真库校验：租户运行时投影读回与租户能力开关列表。
//!
//! 需要 `FLARE_TENANT_TEST_DATABASE_URL`（已建 `tenants` 与 `capability_tenant_switches` 的库，
//! 例如用 `deploy/init.sql` 初始化）。用例标了 `#[ignore]`：默认的 `cargo test` 只记为 ignored，
//! `-- --ignored` 显式运行时变量必须已设置，否则直接失败。用例只写随机租户，结束时删除。

use std::sync::Arc;

use flare_capability::domain::capability::CapabilityPolicyBackend;
use flare_capability::domain::model::TenantProjectionRecord;
use flare_capability::domain::repository::{TenantProjectionRepository, TenantRuntimeReader};
use flare_capability::infrastructure::persistence::{
    PostgresCapabilityPolicy, PostgresTenantProjectionRepository,
};
use flare_grpc_proto::control::TenantStatus as ProtoStatus;
use flare_grpc_proto::control::{CoreQuota, TenantProjectionUpsert, TenantQuota as ProtoQuota};
use flare_im_contracts::domain::tenant::TenantStatus;
use sqlx::postgres::PgPoolOptions;

const ENV_DATABASE_URL: &str = "FLARE_TENANT_TEST_DATABASE_URL";

fn tenant() -> String {
    format!("t{}", &uuid::Uuid::new_v4().simple().to_string()[..12])
}

#[tokio::test]
#[ignore = "requires FLARE_TENANT_TEST_DATABASE_URL pointing at a database initialised with deploy/init.sql"]
async fn tenant_runtime_and_switches_read_back_per_tenant() {
    let url = std::env::var(ENV_DATABASE_URL).unwrap_or_else(|_| {
        panic!("set {ENV_DATABASE_URL} to a database initialised with deploy/init.sql")
    });
    let pool = Arc::new(
        PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .expect("connect tenant test database"),
    );
    let (tenant_a, tenant_b) = (tenant(), tenant());
    let projection = PostgresTenantProjectionRepository::new(Arc::clone(&pool));
    let policy = PostgresCapabilityPolicy::new(Arc::clone(&pool));

    assert!(
        projection
            .tenant_runtime(&tenant_a)
            .await
            .unwrap()
            .is_none()
    );

    let record = TenantProjectionRecord::from_proto(&TenantProjectionUpsert {
        organization_id: Default::default(),
        im_tenant_id: tenant_a.clone(),
        name: "Tenant A".to_string(),
        status: ProtoStatus::Suspended as i32,
        quota: Some(ProtoQuota {
            max_users: 10,
            max_groups: 0,
            max_group_members: 0,
            core: Some(CoreQuota {
                send_qps: 7,
                sync_pull_qps: 0,
                max_online_devices_per_user: 2,
            }),
        }),
        settings_json: br#"{"locale":"zh-CN"}"#.to_vec(),
        version: 5,
    })
    .unwrap();
    assert!(projection.upsert_if_newer(&record).await.unwrap().accepted);

    let view = projection.tenant_runtime(&tenant_a).await.unwrap().unwrap();
    assert_eq!(view.tenant_id, tenant_a);
    assert_eq!(view.name, "Tenant A");
    assert_eq!(view.status, TenantStatus::Suspended);
    assert_eq!(view.version, 5);
    assert_eq!(view.quota.core.send_qps, 7);
    assert_eq!(view.quota.core.max_online_devices_per_user, 2);
    let settings: serde_json::Value = serde_json::from_str(&view.settings_json).unwrap();
    assert_eq!(settings["locale"], "zh-CN");
    assert!(view.projected_at.is_some());
    // 别的租户读不到这一行。
    assert!(
        projection
            .tenant_runtime(&tenant_b)
            .await
            .unwrap()
            .is_none()
    );

    policy
        .set_tenant_capability(&tenant_a, "rtc.call", true)
        .await
        .unwrap();
    policy
        .set_tenant_capability(&tenant_a, "ai.summary", false)
        .await
        .unwrap();
    policy
        .set_tenant_capability(&tenant_b, "social.moments", true)
        .await
        .unwrap();
    let switches = policy.list_tenant_switches(&tenant_a).await.unwrap();
    let rows: Vec<_> = switches
        .iter()
        .map(|s| (s.tenant_id.as_str(), s.capability_id.as_str(), s.enabled))
        .collect();
    assert_eq!(
        rows,
        vec![
            (tenant_a.as_str(), "ai.summary", false),
            (tenant_a.as_str(), "rtc.call", true)
        ]
    );
    assert!(switches.iter().all(|s| s.updated_at.is_some()));
    assert_eq!(
        policy.list_tenant_switches(&tenant_b).await.unwrap().len(),
        1
    );
    assert!(
        policy
            .list_tenant_switches(&tenant())
            .await
            .unwrap()
            .is_empty()
    );

    for tenant in [&tenant_a, &tenant_b] {
        sqlx::query("DELETE FROM capability_tenant_switches WHERE tenant_id = $1")
            .bind(tenant)
            .execute(&*pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM tenants WHERE tenant_id = $1")
            .bind(tenant)
            .execute(&*pool)
            .await
            .unwrap();
    }
}
