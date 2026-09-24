//! 租户投影的真库校验：写 `tenants` 表的幂等 SQL、`GetVersions` 与网关侧 `PgTenantRuntimeSource` 读回。
//!
//! 只在设置了 `FLARE_TENANT_TEST_DATABASE_URL`（一个已用 `deploy/init.sql` 初始化的库）时运行，
//! 否则直接通过。用法：
//!
//! ```bash
//! docker exec flare-postgres psql -U flare -d postgres -c "CREATE DATABASE flare_d0_check"
//! docker exec -i flare-postgres psql -U flare -d flare_d0_check < deploy/init.sql
//! FLARE_TENANT_TEST_DATABASE_URL=postgres://flare:flare123@localhost:25432/flare_d0_check \
//!   cargo test -p flare-capability --test tenant_projection_pg
//! ```

use std::sync::Arc;

use flare_capability::domain::model::TenantProjectionRecord;
use flare_capability::domain::repository::TenantProjectionRepository;
use flare_capability::infrastructure::persistence::PostgresTenantProjectionRepository;
use flare_grpc_proto::control::TenantStatus as ProtoStatus;
use flare_grpc_proto::control::{CoreQuota, TenantProjectionUpsert, TenantQuota as ProtoQuota};
use flare_im_contracts::domain::tenant::TenantStatus;
use flare_im_service_kit::tenant_runtime::{PgTenantRuntimeSource, TenantRuntimeSource};
use sqlx::postgres::PgPoolOptions;

const ENV_DATABASE_URL: &str = "FLARE_TENANT_TEST_DATABASE_URL";

async fn pool() -> Option<sqlx::PgPool> {
    let url = std::env::var(ENV_DATABASE_URL).ok()?;
    Some(
        PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .expect("connect tenant test database"),
    )
}

fn upsert(
    tenant: &str,
    status: ProtoStatus,
    version: u64,
    send_qps: u32,
) -> TenantProjectionRecord {
    TenantProjectionRecord::from_proto(&TenantProjectionUpsert {
        im_tenant_id: tenant.to_string(),
        name: format!("tenant {tenant}"),
        status: status as i32,
        quota: Some(ProtoQuota {
            max_users: 10,
            max_groups: 0,
            max_group_members: 0,
            core: Some(CoreQuota {
                send_qps,
                sync_pull_qps: send_qps / 2,
                max_online_devices_per_user: 0,
            }),
        }),
        settings_json: br#"{"locale":"zh-CN"}"#.to_vec(),
        version,
    })
    .unwrap()
}

#[tokio::test]
async fn projection_round_trips_through_postgres() {
    let Some(pool) = pool().await else {
        eprintln!("{ENV_DATABASE_URL} not set; skipping");
        return;
    };
    // 租户 ID 上限 32 字符：取 uuid 的前 12 个十六进制位即可唯一。
    let tenant = format!("t{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
    let repo = PostgresTenantProjectionRepository::new(Arc::new(pool.clone()));
    let source = PgTenantRuntimeSource::new(pool.clone());

    // 未投影：读回 None。
    assert!(source.load(&tenant).await.unwrap().is_none());

    // 首次投影生效。
    let first = repo
        .upsert_if_newer(&upsert(&tenant, ProtoStatus::Active, 5, 40))
        .await
        .unwrap();
    assert!(first.accepted);
    assert_eq!(first.current_version, 5);

    let snapshot = source.load(&tenant).await.unwrap().expect("projected row");
    assert_eq!(snapshot.status, TenantStatus::Active);
    assert_eq!(snapshot.version, 5);
    assert_eq!(snapshot.quota.max_users, 10);
    assert_eq!(snapshot.quota.core.send_qps, 40);
    assert_eq!(snapshot.quota.core.sync_pull_qps, 20);

    // 同版本 / 旧版本：丢弃，行不变。
    for stale in [5, 4] {
        let ack = repo
            .upsert_if_newer(&upsert(&tenant, ProtoStatus::Suspended, stale, 1))
            .await
            .unwrap();
        assert!(!ack.accepted, "version {stale} must be ignored");
        assert_eq!(ack.current_version, 5);
    }
    let unchanged = source.load(&tenant).await.unwrap().unwrap();
    assert_eq!(unchanged.status, TenantStatus::Active);
    assert_eq!(unchanged.quota.core.send_qps, 40);

    // 更高版本：状态与配额都换掉，config 透传。
    let newer = repo
        .upsert_if_newer(&upsert(&tenant, ProtoStatus::Suspended, 6, 7))
        .await
        .unwrap();
    assert!(newer.accepted);
    let suspended = source.load(&tenant).await.unwrap().unwrap();
    assert_eq!(suspended.status, TenantStatus::Suspended);
    assert_eq!(suspended.version, 6);
    assert_eq!(suspended.quota.core.send_qps, 7);
    assert!(!suspended.status.admits_connections());

    let (config, projected_at): (serde_json::Value, Option<chrono::DateTime<chrono::Utc>>) =
        sqlx::query_as("SELECT config, projected_at FROM tenants WHERE tenant_id = $1")
            .bind(&tenant)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(config["locale"], "zh-CN");
    assert!(projected_at.is_some());

    // GetVersions：按 id 过滤与全量都含它；init.sql 预置的 "0" 也在全量里。
    let filtered = repo.versions(std::slice::from_ref(&tenant)).await.unwrap();
    assert_eq!(filtered, vec![(tenant.clone(), 6)]);
    let all = repo.versions(&[]).await.unwrap();
    assert!(all.iter().any(|(id, v)| id == &tenant && *v == 6));
    assert!(all.iter().any(|(id, v)| id == "0" && *v == 0));

    sqlx::query("DELETE FROM tenants WHERE tenant_id = $1")
        .bind(&tenant)
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn seeded_default_tenant_is_active_and_unversioned() {
    let Some(pool) = pool().await else {
        eprintln!("{ENV_DATABASE_URL} not set; skipping");
        return;
    };
    let source = PgTenantRuntimeSource::new(pool);
    let zero = source
        .load("0")
        .await
        .unwrap()
        .expect("init.sql seeds tenant 0");
    assert_eq!(zero.status, TenantStatus::Active);
    assert_eq!(zero.version, 0);
    assert_eq!(zero.quota.core.send_qps, 0, "unlimited by default");
}
