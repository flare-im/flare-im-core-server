//! Hook 运行时的 PostgreSQL 端：读 `hook_configs`、批量写 `hook_executions`、写部署摘要。
//!
//! `hook_config_versions` 每个范围一行（`scope` 为应用，空串为平台），Capability 写配置时在同一事务
//! 里递增；这里约每秒查一次有没有版本晚于已加载版本的范围，只取这些范围的行。Hook 配置与消息同在
//! Core 库，连 `core_runtime`。

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use flare_im_core_hooks::hooks::execution::{HookExecutionRecord, HookExecutionSink};
use flare_im_core_hooks::hooks::{
    DeploymentHookSummary, HookConfigStore, HookScope, HookScopeChange, PRE_SEND_HOOK_TYPE,
    StoredHook,
};
use flare_server_core::error::{FlareError, Result};
use sqlx::postgres::PgPoolOptions;
use sqlx::{FromRow, PgPool};

const MAX_CONNECTIONS: u32 = 3;
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(2);
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// 平台范围在 `hook_config_versions.scope` 里的写法。
const PLATFORM_SCOPE: &str = "";

const CURRENT_VERSION_SQL: &str =
    "SELECT COALESCE(max(version), 0)::int8 FROM hook_config_versions";

const CHANGED_SCOPES_SQL: &str =
    "SELECT scope, version FROM hook_config_versions WHERE version > $1";

const LOAD_ALL_SQL: &str = "
SELECT id, tenant_id, name, description, hook_order, timeout_ms, require_success,
       selector_config, transport_config, metadata
  FROM hook_configs
 WHERE hook_type = $1 AND enabled
 ORDER BY hook_order ASC, id ASC";

const LOAD_SCOPES_SQL: &str = "
SELECT id, tenant_id, name, description, hook_order, timeout_ms, require_success,
       selector_config, transport_config, metadata
  FROM hook_configs
 WHERE hook_type = $1 AND enabled
   AND (tenant_id = ANY($2) OR ($3 AND tenant_id IS NULL))
 ORDER BY hook_order ASC, id ASC";

const INSERT_EXECUTIONS_SQL: &str = "
INSERT INTO hook_executions (
    execution_id, hook_id, hook_name, hook_type, tenant_id, message_id, request_id, trace_id,
    success, latency_ms, error_code, error_message, executed_at, sample_weight)
SELECT * FROM UNNEST(
    $1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[], $8::text[],
    $9::bool[], $10::int4[], $11::text[], $12::text[], $13::timestamptz[], $14::int4[])
ON CONFLICT (execution_id) DO NOTHING";

const UPSERT_SUMMARY_SQL: &str = "
INSERT INTO hook_deployment_summary (service, source, hooks, instance_id)
VALUES ($1, $2, $3, $4)
ON CONFLICT (service) DO UPDATE
   SET source = EXCLUDED.source,
       hooks = EXCLUDED.hooks,
       instance_id = EXCLUDED.instance_id,
       loaded_at = CURRENT_TIMESTAMP";

#[derive(FromRow)]
struct HookRow {
    id: i64,
    tenant_id: Option<String>,
    name: String,
    description: Option<String>,
    hook_order: i32,
    timeout_ms: i64,
    require_success: bool,
    selector_config: serde_json::Value,
    transport_config: serde_json::Value,
    metadata: Option<serde_json::Value>,
}

impl From<HookRow> for StoredHook {
    fn from(row: HookRow) -> Self {
        Self {
            id: row.id,
            tenant_id: row.tenant_id,
            name: row.name,
            description: row.description,
            order: row.hook_order,
            timeout_ms: row.timeout_ms,
            require_success: row.require_success,
            selector: row.selector_config,
            transport: row.transport_config,
            metadata: row.metadata,
        }
    }
}

fn scope_from_column(scope: String) -> HookScope {
    if scope == PLATFORM_SCOPE {
        HookScope::Platform
    } else {
        HookScope::Application(scope)
    }
}

/// 存储错误只带操作名：连接串、SQL 与行内容不进错误文本。
fn storage_error(operation: &'static str, error: sqlx::Error) -> FlareError {
    tracing::debug!(operation, %error, "hook storage failed");
    FlareError::system(format!("hook storage unavailable: {operation}"))
}

pub struct PostgresHookStore {
    pool: Arc<PgPool>,
}

impl PostgresHookStore {
    /// 懒连接：只解析连接串，第一次查询才建连。
    pub fn connect_lazy(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(MAX_CONNECTIONS)
            .min_connections(0)
            .acquire_timeout(ACQUIRE_TIMEOUT)
            .idle_timeout(IDLE_TIMEOUT)
            .connect_lazy(database_url)
            .map_err(|_| FlareError::system("invalid hook store postgres url"))?;
        Ok(Self {
            pool: Arc::new(pool),
        })
    }

    /// 写一行已装载的部署 Hook 摘要，控制面只读展示它。每个服务一行，后启动的实例覆盖。
    pub async fn write_deployment_summary(
        &self,
        service: &str,
        source: &str,
        hooks: &[DeploymentHookSummary],
        instance_id: Option<&str>,
    ) -> Result<()> {
        let hooks = serde_json::to_value(hooks)
            .map_err(|_| FlareError::system("hook summary not serializable"))?;
        sqlx::query(UPSERT_SUMMARY_SQL)
            .bind(service)
            .bind(source)
            .bind(hooks)
            .bind(instance_id)
            .execute(self.pool.as_ref())
            .await
            .map_err(|error| storage_error("write deployment summary", error))?;
        Ok(())
    }
}

#[async_trait]
impl HookConfigStore for PostgresHookStore {
    async fn current_version(&self) -> Result<i64> {
        sqlx::query_scalar(CURRENT_VERSION_SQL)
            .fetch_one(self.pool.as_ref())
            .await
            .map_err(|error| storage_error("read hook config version", error))
    }

    async fn changed_scopes(&self, after: i64) -> Result<Vec<HookScopeChange>> {
        let rows: Vec<(String, i64)> = sqlx::query_as(CHANGED_SCOPES_SQL)
            .bind(after)
            .fetch_all(self.pool.as_ref())
            .await
            .map_err(|error| storage_error("read changed hook scopes", error))?;
        Ok(rows
            .into_iter()
            .map(|(scope, version)| HookScopeChange {
                scope: scope_from_column(scope),
                version,
            })
            .collect())
    }

    async fn load(&self, scopes: Option<&[HookScope]>) -> Result<Vec<StoredHook>> {
        let rows: Vec<HookRow> = match scopes {
            None => {
                sqlx::query_as::<_, HookRow>(LOAD_ALL_SQL)
                    .bind(PRE_SEND_HOOK_TYPE)
                    .fetch_all(self.pool.as_ref())
                    .await
            }
            Some(scopes) => {
                let applications: Vec<&str> = scopes
                    .iter()
                    .filter_map(|scope| match scope {
                        HookScope::Application(app) => Some(app.as_str()),
                        HookScope::Platform => None,
                    })
                    .collect();
                sqlx::query_as::<_, HookRow>(LOAD_SCOPES_SQL)
                    .bind(PRE_SEND_HOOK_TYPE)
                    .bind(applications)
                    .bind(scopes.contains(&HookScope::Platform))
                    .fetch_all(self.pool.as_ref())
                    .await
            }
        }
        .map_err(|error| storage_error("read hook configs", error))?;
        Ok(rows.into_iter().map(StoredHook::from).collect())
    }
}

#[async_trait]
impl HookExecutionSink for PostgresHookStore {
    async fn write_batch(&self, records: &[HookExecutionRecord]) -> Result<u64> {
        if records.is_empty() {
            return Ok(0);
        }
        let mut execution_ids = Vec::with_capacity(records.len());
        let mut hook_ids = Vec::with_capacity(records.len());
        let mut hook_names = Vec::with_capacity(records.len());
        let mut hook_types = Vec::with_capacity(records.len());
        let mut tenant_ids = Vec::with_capacity(records.len());
        let mut message_ids = Vec::with_capacity(records.len());
        let mut request_ids = Vec::with_capacity(records.len());
        let mut trace_ids = Vec::with_capacity(records.len());
        let mut successes = Vec::with_capacity(records.len());
        let mut latencies = Vec::with_capacity(records.len());
        let mut error_codes = Vec::with_capacity(records.len());
        let mut error_messages = Vec::with_capacity(records.len());
        let mut executed_ats = Vec::with_capacity(records.len());
        let mut weights = Vec::with_capacity(records.len());
        for record in records {
            execution_ids.push(record.execution_id.clone());
            hook_ids.push(record.hook_id());
            hook_names.push(record.hook_name.clone());
            hook_types.push(record.hook_type.clone());
            tenant_ids.push(record.tenant_id.clone());
            message_ids.push(record.message_id.clone());
            request_ids.push(record.request_id.clone());
            trace_ids.push(record.trace_id.clone());
            successes.push(record.outcome.answered());
            latencies.push(record.latency_ms());
            error_codes.push(record.outcome.error_code().map(str::to_string));
            error_messages.push(record.outcome.error_message());
            executed_ats.push(DateTime::<Utc>::from(record.executed_at));
            weights.push(i32::try_from(record.sample_weight.max(1)).unwrap_or(i32::MAX));
        }
        let result = sqlx::query(INSERT_EXECUTIONS_SQL)
            .bind(execution_ids)
            .bind(hook_ids)
            .bind(hook_names)
            .bind(hook_types)
            .bind(tenant_ids)
            .bind(message_ids)
            .bind(request_ids)
            .bind(trace_ids)
            .bind(successes)
            .bind(latencies)
            .bind(error_codes)
            .bind(error_messages)
            .bind(executed_ats)
            .bind(weights)
            .execute(self.pool.as_ref())
            .await
            .map_err(|error| storage_error("write hook executions", error))?;
        Ok(result.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_scope_is_the_platform() {
        assert_eq!(scope_from_column(String::new()), HookScope::Platform);
        assert_eq!(
            scope_from_column("acme".into()),
            HookScope::Application("acme".into())
        );
    }

    /// 连接串解析失败的错误不带连接串。
    #[test]
    fn a_bad_url_is_not_echoed() {
        let error =
            match PostgresHookStore::connect_lazy("postgres://user:hook-pw@host:not-a-port/db") {
                Ok(_) => panic!("a malformed url must be refused"),
                Err(error) => error,
            };
        assert!(!format!("{error} {error:?}").contains("hook-pw"));
    }

    /// 真库：迁移到最新（0024 起）的 Core 库上跑这里的每条 SQL。库名必须以 `flare_hook_store_test_`
    /// 开头，用例会清空三张 Hook 表。
    #[tokio::test]
    #[ignore = "requires FLARE_HOOK_STORE_TEST_DATABASE_URL: a disposable flare_hook_store_test_* Core database at the latest migration"]
    async fn the_store_reads_scopes_and_writes_executions_on_postgres() {
        use flare_im_core_hooks::hooks::execution::HookExecutionOutcome;
        use std::time::{Duration, SystemTime};

        let url = std::env::var("FLARE_HOOK_STORE_TEST_DATABASE_URL").expect("database url");
        let store = PostgresHookStore::connect_lazy(&url).unwrap();
        let pool = store.pool.as_ref();
        let name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(pool)
            .await
            .unwrap();
        assert!(
            name.starts_with("flare_hook_store_test_"),
            "refusing {name}"
        );
        sqlx::raw_sql(
            "TRUNCATE hook_configs, hook_config_versions, hook_deployment_summary;
             DELETE FROM hook_executions;",
        )
        .execute(pool)
        .await
        .unwrap();
        assert_eq!(store.current_version().await.unwrap(), 0);
        assert!(store.changed_scopes(0).await.unwrap().is_empty());

        sqlx::raw_sql(
            r#"INSERT INTO hook_configs(tenant_id, hook_type, name, description, enabled, timeout_ms,
                   require_success, selector_config, transport_config, metadata, hook_order)
               VALUES
               (NULL,  'pre_send', 'platform-audit', 'audit', TRUE, 800, TRUE,
                '{"message_types":["text"]}', '{"type":"grpc","endpoint":"http://127.0.0.1:9"}',
                '{"policy":"audit"}', 5),
               ('app-a','pre_send', 'a-filter', NULL, TRUE, 500, FALSE,
                '{}', '{"type":"webhook","endpoint":"https://hooks.example/a"}', NULL, 1),
               ('app-a','pre_send', 'a-off', NULL, FALSE, 500, FALSE,
                '{}', '{"type":"webhook","endpoint":"https://hooks.example/off"}', NULL, 0),
               ('app-b','pre_send', 'b-filter', NULL, TRUE, 500, TRUE,
                '{}', '{"type":"grpc","service_name":"b-hooks"}', NULL, 0);
               INSERT INTO hook_config_versions(scope, version) VALUES ('', 1), ('app-a', 2), ('app-b', 3);"#,
        )
        .execute(pool)
        .await
        .unwrap();
        // A second platform row with the same name is refused by the 0024 partial index.
        assert!(
            sqlx::raw_sql(
                "INSERT INTO hook_configs(tenant_id, hook_type, name) VALUES (NULL, 'pre_send', 'platform-audit')"
            )
            .execute(pool)
            .await
            .is_err()
        );

        assert_eq!(store.current_version().await.unwrap(), 3);
        let mut changed = store.changed_scopes(1).await.unwrap();
        changed.sort_by_key(|change| change.version);
        assert_eq!(
            changed
                .iter()
                .map(|change| (change.scope.clone(), change.version))
                .collect::<Vec<_>>(),
            vec![
                (HookScope::Application("app-a".into()), 2),
                (HookScope::Application("app-b".into()), 3)
            ]
        );

        let all = store.load(None).await.unwrap();
        assert_eq!(
            all.iter()
                .map(|hook| hook.name.as_str())
                .collect::<Vec<_>>(),
            ["b-filter", "a-filter", "platform-audit"],
            "enabled rows only, by hook_order then id"
        );
        for hook in &all {
            hook.definition().unwrap();
        }
        let platform = store.load(Some(&[HookScope::Platform])).await.unwrap();
        assert_eq!(platform.len(), 1);
        assert_eq!(platform[0].scope(), HookScope::Platform);
        assert_eq!(platform[0].order, 5);
        assert_eq!(platform[0].timeout_ms, 800);
        let app_a = store
            .load(Some(&[HookScope::Application("app-a".into())]))
            .await
            .unwrap();
        assert_eq!(
            app_a
                .iter()
                .map(|hook| hook.name.as_str())
                .collect::<Vec<_>>(),
            ["a-filter"]
        );
        assert!(
            store
                .load(Some(&[HookScope::Application("app-c".into())]))
                .await
                .unwrap()
                .is_empty()
        );

        let record = |id: &str, outcome| HookExecutionRecord {
            execution_id: id.into(),
            tenant_id: "app-a".into(),
            hook_id: "pre_send:a-filter".into(),
            hook_type: PRE_SEND_HOOK_TYPE.into(),
            hook_name: "a-filter".into(),
            message_id: Some("m1".into()),
            request_id: None,
            trace_id: Some("t1".into()),
            outcome,
            latency: Duration::from_millis(12),
            executed_at: SystemTime::now(),
            sample_weight: 0,
        };
        let batch = [
            record("e1", HookExecutionOutcome::Allowed),
            record(
                "e2",
                HookExecutionOutcome::Rejected {
                    reason: "spam".into(),
                },
            ),
            record("e3", HookExecutionOutcome::TimedOut),
        ];
        assert_eq!(store.write_batch(&batch).await.unwrap(), 3);
        assert_eq!(store.write_batch(&batch).await.unwrap(), 0, "idempotent");
        assert_eq!(store.write_batch(&[]).await.unwrap(), 0);
        let rows: Vec<(String, String, bool, i32, Option<String>, i32)> = sqlx::query_as(
            "SELECT execution_id, hook_id, success, latency_ms, error_code, sample_weight
               FROM hook_executions ORDER BY execution_id",
        )
        .fetch_all(pool)
        .await
        .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].1, "pre_send:a-filter");
        assert!(rows[0].2 && rows[1].2 && !rows[2].2);
        assert_eq!(rows[0].3, 12);
        assert_eq!(rows[0].4, None);
        assert!(rows[1].4.is_some() && rows[2].4.is_some());
        assert!(rows.iter().all(|row| row.5 == 1), "weight is at least 1");

        store
            .write_deployment_summary("message-ingest", "config/hooks.toml", &[], Some("i-1"))
            .await
            .unwrap();
        store
            .write_deployment_summary("message-ingest", "config/hooks.toml", &[], Some("i-2"))
            .await
            .unwrap();
        let summary: Vec<(String, Option<String>, serde_json::Value)> =
            sqlx::query_as("SELECT service, instance_id, hooks FROM hook_deployment_summary")
                .fetch_all(pool)
                .await
                .unwrap();
        assert_eq!(
            summary,
            vec![(
                "message-ingest".to_string(),
                Some("i-2".to_string()),
                serde_json::json!([])
            )]
        );
    }
}
