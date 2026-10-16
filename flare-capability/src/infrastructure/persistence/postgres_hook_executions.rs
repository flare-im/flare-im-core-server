//! `hook_executions`（源 DDL 0015 加了 `sample_weight`、`hook_version` 与两条索引）：Hook 执行证据。
//!
//! 写入一批一条语句（`UNNEST`）；统计与查询都限定租户与 Hook（走
//! `idx_hook_executions_tenant_hook_executed`），计数按 `sample_weight` 加权；保留期清理按执行时间
//! 从旧到新分批删（走 `idx_hook_executions_executed_at`），每批先物化再删，多实例同时清理时互相
//! 跳过已锁的行。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use chrono::{DateTime, Utc};
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError, Result};
use sqlx::{PgPool, Row};

use crate::domain::model::{
    HOOK_CIRCUIT_OPEN_CODE, HookExecutionFilter, HookExecutionRecord, HookExecutionScope,
    HookExecutionStatistics, HookExecutionTimeRange, StoredHookExecution,
};
use crate::domain::repository::HookExecutionRepository;

/// 统计里的加权 p99：按耗时排序累加权重，取累计权重首次达到总权重 99% 的耗时。熔断短路没有调用
/// Hook，不计入耗时。
const STATISTICS_SQL: &str = "
WITH scoped AS (
    SELECT success, latency_ms, error_code, sample_weight::int8 AS w
      FROM hook_executions
     WHERE tenant_id = $1 AND hook_type = $2 AND hook_name = $3
       AND executed_at >= $4 AND executed_at < $5
), called AS (
    SELECT latency_ms,
           sum(w) OVER (ORDER BY latency_ms ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum,
           sum(w) OVER () AS called_total,
           w
      FROM scoped
     WHERE latency_ms IS NOT NULL AND error_code IS DISTINCT FROM $6
)
SELECT
    (SELECT COALESCE(sum(w), 0) FROM scoped)::int8 AS total,
    (SELECT COALESCE(sum(w) FILTER (WHERE success), 0) FROM scoped)::int8 AS succeeded,
    (SELECT COALESCE(sum(w) FILTER (WHERE NOT success), 0) FROM scoped)::int8 AS failed,
    (SELECT COALESCE(sum(w) FILTER (WHERE error_code = $6), 0) FROM scoped)::int8 AS circuit_open,
    (SELECT sum(latency_ms::float8 * w) / NULLIF(sum(w), 0) FROM called)::float8 AS avg_latency,
    (SELECT min(latency_ms) FROM called WHERE cum >= 0.99 * called_total)::float8 AS p99_latency";

const ERROR_CODES_SQL: &str = "
SELECT error_code, sum(sample_weight)::int8 AS count
  FROM hook_executions
 WHERE tenant_id = $1 AND hook_type = $2 AND hook_name = $3
   AND executed_at >= $4 AND executed_at < $5
   AND NOT success AND error_code IS NOT NULL
 GROUP BY error_code";

const INSERT_SQL: &str = "
INSERT INTO hook_executions (
    execution_id, hook_id, hook_name, hook_type, tenant_id, hook_version, message_id,
    request_id, trace_id, success, latency_ms, error_code, error_message, executed_at,
    sample_weight)
SELECT * FROM UNNEST(
    $1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[],
    $8::text[], $9::text[], $10::bool[], $11::int4[], $12::text[], $13::text[],
    $14::timestamptz[], $15::int4[])
ON CONFLICT (execution_id) DO NOTHING";

/// 这一批先在物化的 CTE 里选定并加锁，只算一次。不能写成 `WHERE execution_id IN (SELECT ...
/// LIMIT .. FOR UPDATE)`：规划器可把它做成每个外层行重扫一次的半连接，重扫时跳过本语句已删的行、
/// 换下一批，一条语句就删光全部过期行，批大小形同虚设。
const DELETE_SQL: &str = "
WITH batch AS MATERIALIZED (
    SELECT execution_id FROM hook_executions
     WHERE executed_at < $1
     ORDER BY executed_at
     LIMIT $2
       FOR UPDATE SKIP LOCKED
)
DELETE FROM hook_executions AS h
 USING batch
 WHERE h.execution_id = batch.execution_id";

const QUERY_SQL: &str = "
SELECT execution_id, message_id, success, latency_ms, error_code, error_message, executed_at,
       sample_weight
  FROM hook_executions
 WHERE tenant_id = $1 AND hook_type = $2 AND hook_name = $3
   AND executed_at >= $4 AND executed_at < $5
   AND ($6::text IS NULL OR message_id = $6)
   AND (NOT $7 OR success)
 ORDER BY executed_at DESC, execution_id DESC
 LIMIT $8";

/// `hook_executions` 仓储（与 Hook 配置共用连接池）。
pub struct PostgresHookExecutionRepository {
    pool: Arc<PgPool>,
}

impl PostgresHookExecutionRepository {
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self { pool }
    }
}

/// 存储错误只带固定说明与操作名；连接串、SQL 与行内容不进错误文本。
fn storage_error(operation: &'static str, error: sqlx::Error) -> FlareError {
    tracing::warn!(operation, error = %error, "hook execution storage failed");
    ErrorBuilder::new(
        ErrorCode::DatabaseError,
        "hook execution storage is unavailable",
    )
    .details(operation)
    .build_error()
}

/// `SystemTime` 换成库里的时间戳（保留到纳秒，库存微秒）。chrono 的 `From<SystemTime>` 越界直接
/// panic，这里越界返回无效参数：管理接口的时间在领域层已限到 9999 年末，这是第二道防线。
fn utc(time: SystemTime) -> Result<DateTime<Utc>> {
    let converted = match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(after) => i64::try_from(after.as_secs())
            .ok()
            .and_then(|secs| DateTime::from_timestamp(secs, after.subsec_nanos())),
        Err(before) => {
            let before = before.duration();
            i64::try_from(before.as_secs())
                .ok()
                .and_then(|secs| match before.subsec_nanos() {
                    0 => DateTime::from_timestamp(secs.checked_neg()?, 0),
                    nanos => DateTime::from_timestamp(
                        secs.checked_neg()?.checked_sub(1)?,
                        1_000_000_000 - nanos,
                    ),
                })
        }
    };
    converted.ok_or_else(|| {
        ErrorBuilder::new(ErrorCode::InvalidParameter, "time is out of range").build_error()
    })
}

#[async_trait::async_trait]
impl HookExecutionRepository for PostgresHookExecutionRepository {
    async fn insert_batch(&self, records: &[HookExecutionRecord]) -> Result<u64> {
        if records.is_empty() {
            return Ok(0);
        }
        let mut execution_ids = Vec::with_capacity(records.len());
        let mut hook_ids = Vec::with_capacity(records.len());
        let mut hook_names = Vec::with_capacity(records.len());
        let mut hook_types = Vec::with_capacity(records.len());
        let mut tenant_ids = Vec::with_capacity(records.len());
        let mut hook_versions = Vec::with_capacity(records.len());
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
            hook_versions.push(record.hook_version.clone());
            message_ids.push(record.message_id.clone());
            request_ids.push(record.request_id.clone());
            trace_ids.push(record.trace_id.clone());
            successes.push(record.outcome.answered());
            latencies.push(record.latency_ms());
            error_codes.push(record.outcome.error_code().map(str::to_string));
            error_messages.push(record.outcome.error_message());
            executed_ats.push(utc(record.executed_at)?);
            weights.push(i32::try_from(record.sample_weight.max(1)).unwrap_or(i32::MAX));
        }
        let result = sqlx::query(INSERT_SQL)
            .bind(execution_ids)
            .bind(hook_ids)
            .bind(hook_names)
            .bind(hook_types)
            .bind(tenant_ids)
            .bind(hook_versions)
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
            .map_err(|e| storage_error("insert", e))?;
        Ok(result.rows_affected())
    }

    async fn delete_executed_before(&self, cutoff: SystemTime, limit: u32) -> Result<u64> {
        let result = sqlx::query(DELETE_SQL)
            .bind(utc(cutoff)?)
            .bind(i64::from(limit.max(1)))
            .execute(self.pool.as_ref())
            .await
            .map_err(|e| storage_error("delete_expired", e))?;
        Ok(result.rows_affected())
    }

    async fn statistics(
        &self,
        scope: &HookExecutionScope,
        range: &HookExecutionTimeRange,
    ) -> Result<HookExecutionStatistics> {
        let row = sqlx::query(STATISTICS_SQL)
            .bind(&scope.tenant_id)
            .bind(&scope.hook_type)
            .bind(&scope.hook_name)
            .bind(utc(range.from)?)
            .bind(utc(range.to)?)
            .bind(HOOK_CIRCUIT_OPEN_CODE)
            .fetch_one(self.pool.as_ref())
            .await
            .map_err(|e| storage_error("statistics", e))?;
        let codes = sqlx::query(ERROR_CODES_SQL)
            .bind(&scope.tenant_id)
            .bind(&scope.hook_type)
            .bind(&scope.hook_name)
            .bind(utc(range.from)?)
            .bind(utc(range.to)?)
            .fetch_all(self.pool.as_ref())
            .await
            .map_err(|e| storage_error("statistics_codes", e))?;
        let mut error_count_by_code = HashMap::with_capacity(codes.len());
        for code in codes {
            error_count_by_code.insert(
                code.try_get::<String, _>("error_code")
                    .map_err(|e| storage_error("statistics_codes", e))?,
                code.try_get::<i64, _>("count")
                    .map_err(|e| storage_error("statistics_codes", e))?,
            );
        }
        let read = |column: &str| -> Result<i64> {
            row.try_get::<i64, _>(column)
                .map_err(|e| storage_error("statistics", e))
        };
        let float = |column: &str| -> Result<f64> {
            Ok(row
                .try_get::<Option<f64>, _>(column)
                .map_err(|e| storage_error("statistics", e))?
                .unwrap_or(0.0))
        };
        Ok(HookExecutionStatistics {
            total_executions: read("total")?,
            success_count: read("succeeded")?,
            failure_count: read("failed")?,
            avg_latency_ms: float("avg_latency")?,
            p99_latency_ms: float("p99_latency")?,
            circuit_open_count: read("circuit_open")?,
            error_count_by_code,
        })
    }

    async fn query(&self, filter: &HookExecutionFilter) -> Result<Vec<StoredHookExecution>> {
        let rows = sqlx::query(QUERY_SQL)
            .bind(&filter.scope.tenant_id)
            .bind(&filter.scope.hook_type)
            .bind(&filter.scope.hook_name)
            .bind(utc(filter.range.from)?)
            .bind(utc(filter.range.to)?)
            .bind(filter.message_id.as_deref())
            .bind(filter.success_only)
            .bind(i64::from(filter.limit))
            .fetch_all(self.pool.as_ref())
            .await
            .map_err(|e| storage_error("query", e))?;
        let mut executions = Vec::with_capacity(rows.len());
        for row in rows {
            let executed_at: DateTime<Utc> = row
                .try_get("executed_at")
                .map_err(|e| storage_error("query", e))?;
            executions.push(StoredHookExecution {
                execution_id: row
                    .try_get("execution_id")
                    .map_err(|e| storage_error("query", e))?,
                message_id: row
                    .try_get("message_id")
                    .map_err(|e| storage_error("query", e))?,
                success: row
                    .try_get("success")
                    .map_err(|e| storage_error("query", e))?,
                latency_ms: row
                    .try_get("latency_ms")
                    .map_err(|e| storage_error("query", e))?,
                error_code: row
                    .try_get("error_code")
                    .map_err(|e| storage_error("query", e))?,
                error_message: row
                    .try_get("error_message")
                    .map_err(|e| storage_error("query", e))?,
                executed_at: SystemTime::from(executed_at),
                sample_weight: u32::try_from(
                    row.try_get::<i32, _>("sample_weight")
                        .map_err(|e| storage_error("query", e))?,
                )
                .unwrap_or(1),
            });
        }
        Ok(executions)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// 可表示的时间与 chrono 自己的换算逐纳秒一致（含纪元之前）；越界的返回无效参数，不 panic。
    #[test]
    fn timestamps_convert_exactly_and_out_of_range_times_are_rejected() {
        let epoch = SystemTime::UNIX_EPOCH;
        for time in [
            epoch,
            SystemTime::now(),
            epoch + Duration::new(253_402_300_799, 999_999_999),
            epoch - Duration::new(86_400, 0),
            epoch - Duration::new(86_400, 250),
        ] {
            assert_eq!(utc(time).unwrap(), DateTime::<Utc>::from(time), "{time:?}");
        }
        for huge in [
            epoch + Duration::from_millis(9_000_000_000_000_000),
            epoch + Duration::from_secs(i64::MAX as u64),
            epoch - Duration::from_secs(1 << 60),
        ] {
            let error = utc(huge).unwrap_err();
            assert_eq!(error.code(), Some(ErrorCode::InvalidParameter), "{huge:?}");
        }
    }

    /// 绕过领域层直接给越界的时间：统计与查询回无效参数，不 panic，也不发查询（连接池是惰性的，
    /// 指向的端口没有库）。
    #[tokio::test]
    async fn out_of_range_times_fail_before_any_query() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://flare@127.0.0.1:1/none")
            .unwrap();
        let repository = PostgresHookExecutionRepository::new(Arc::new(pool));
        let scope = HookExecutionScope {
            tenant_id: "t1".into(),
            hook_type: "pre_send".into(),
            hook_name: "dlp".into(),
        };
        let range = HookExecutionTimeRange {
            from: SystemTime::UNIX_EPOCH,
            to: SystemTime::UNIX_EPOCH + Duration::from_millis(9_000_000_000_000_000),
        };
        let error = repository.statistics(&scope, &range).await.unwrap_err();
        assert_eq!(error.code(), Some(ErrorCode::InvalidParameter), "{error}");
        let error = repository
            .query(&HookExecutionFilter {
                scope,
                range,
                message_id: None,
                success_only: false,
                limit: 10,
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), Some(ErrorCode::InvalidParameter), "{error}");
    }
}
