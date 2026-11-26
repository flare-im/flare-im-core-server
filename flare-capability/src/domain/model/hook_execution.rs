//! Hook 执行证据的查询模型：`hook_config.get_statistics` 与 `hook_config.query_executions` 读
//! `hook_executions`。写入在 Ingest（见 `flare_im_core_hooks::hooks::execution`）。

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

/// 一条 Hook：统计与查询都限定在这里。`tenant_id` 空是平台或部署 Hook，它们跑在所有应用的消息上，
/// 不按应用限定；名字在这两个来源和应用之间不重复（应用不能占用）。`hook_id` 是执行记录里的 Hook
/// 身份（Ingest 的 `execution_hook_id`）：配置的 Hook 是行号，删掉后同名重建的 Hook 不继承旧记录；
/// 部署 Hook 是 `hook_type:name`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookExecutionScope {
    pub tenant_id: Option<String>,
    pub hook_type: String,
    pub hook_name: String,
    pub hook_id: String,
}

/// 管理接口接受的最晚时间：9999-12-31T23:59:59.999Z（Unix 毫秒）。更晚的值不是有效的查询时间，
/// 换算成库里的时间戳时还会溢出（chrono 从 `SystemTime` 换算越界直接 panic），一律当作无效参数拒绝。
pub const MAX_QUERY_TIME_MILLIS: i64 = 253_402_300_799_999;

/// 左闭右开的执行时间范围。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HookExecutionTimeRange {
    pub from: SystemTime,
    pub to: SystemTime,
}

impl HookExecutionTimeRange {
    /// 管理接口的毫秒时间范围：没给范围取 `now` 之前的 `default_span`；只给一端时另一端取 `now`
    /// 或 `end - default_span`。`end` 是闭区间的毫秒，换成开区间加 1 ms。两端都须在 0 到
    /// [`MAX_QUERY_TIME_MILLIS`] 之间。
    pub fn from_millis(
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        now: SystemTime,
        default_span: Duration,
    ) -> Result<Self, &'static str> {
        let to = match end_ms {
            // 上限内加 1 ms 仍可表示（10000-01-01T00:00:00Z）。
            Some(end) => millis_to_time(end)? + Duration::from_millis(1),
            None => now,
        };
        let from = match start_ms {
            Some(start) => millis_to_time(start)?,
            None => to
                .checked_sub(default_span)
                .unwrap_or(SystemTime::UNIX_EPOCH),
        };
        if to < from {
            return Err(if end_ms.is_some() {
                "end_time must not be earlier than start_time"
            } else {
                "start_time must not be in the future when end_time is omitted"
            });
        }
        Ok(Self { from, to })
    }
}

fn millis_to_time(ms: i64) -> Result<SystemTime, &'static str> {
    if ms > MAX_QUERY_TIME_MILLIS {
        return Err("time must not be later than 9999-12-31T23:59:59.999Z");
    }
    let ms = u64::try_from(ms).map_err(|_| "time must not be negative")?;
    Ok(SystemTime::UNIX_EPOCH + Duration::from_millis(ms))
}

/// 执行记录查询条件（结果按执行时间从新到旧）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookExecutionFilter {
    pub scope: HookExecutionScope,
    pub range: HookExecutionTimeRange,
    pub message_id: Option<String>,
    pub success_only: bool,
    pub limit: u32,
}

/// 库里的一条执行记录（查询结果）。
#[derive(Debug, Clone, PartialEq)]
pub struct StoredHookExecution {
    pub execution_id: String,
    pub message_id: Option<String>,
    pub success: bool,
    pub latency_ms: Option<i32>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub executed_at: SystemTime,
    pub sample_weight: u32,
}

/// 一个租户的一条 Hook 在一段时间里的执行统计；计数都按 `sample_weight` 加权（估计值）。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct HookExecutionStatistics {
    pub total_executions: i64,
    pub success_count: i64,
    pub failure_count: i64,
    /// 真正调用了 Hook 的执行（不含熔断短路）的加权平均耗时。
    pub avg_latency_ms: f64,
    /// 同上的加权 p99 耗时。
    pub p99_latency_ms: f64,
    /// 熔断短路次数。
    pub circuit_open_count: i64,
    /// 失败（含超时、熔断短路）按错误码计数；拒绝不在其中。
    pub error_count_by_code: HashMap<String, i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_ranges_default_to_the_last_span_and_include_the_end_millisecond() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let day = Duration::from_secs(86_400);
        let range = HookExecutionTimeRange::from_millis(None, None, now, day).unwrap();
        assert_eq!(range.to, now);
        assert_eq!(range.from, now - day);

        let range =
            HookExecutionTimeRange::from_millis(Some(1_000), Some(2_000), now, day).unwrap();
        assert_eq!(
            range.from,
            SystemTime::UNIX_EPOCH + Duration::from_millis(1_000)
        );
        assert_eq!(
            range.to,
            SystemTime::UNIX_EPOCH + Duration::from_millis(2_001)
        );

        let range = HookExecutionTimeRange::from_millis(None, Some(100_000_000), now, day).unwrap();
        assert_eq!(range.to - day, range.from);

        assert_eq!(
            HookExecutionTimeRange::from_millis(Some(2_000), Some(1_000), now, day),
            Err("end_time must not be earlier than start_time")
        );
        assert_eq!(
            HookExecutionTimeRange::from_millis(Some(2_000_000_000), None, now, day),
            Err("start_time must not be in the future when end_time is omitted"),
            "no end_time was given, so the message must not blame it"
        );
        assert!(HookExecutionTimeRange::from_millis(Some(-1), None, now, day).is_err());
        assert!(HookExecutionTimeRange::from_millis(None, Some(-1), now, day).is_err());
    }

    /// 时间上限是 9999 年末：再晚的毫秒（包括会让时间戳换算 panic 的值）是无效参数，不进存储层。
    #[test]
    fn times_beyond_year_9999_are_rejected() {
        let now = SystemTime::now();
        let day = Duration::from_secs(86_400);
        let range =
            HookExecutionTimeRange::from_millis(Some(0), Some(MAX_QUERY_TIME_MILLIS), now, day)
                .unwrap();
        assert_eq!(
            range.to,
            SystemTime::UNIX_EPOCH + Duration::from_secs(253_402_300_800),
            "10000-01-01T00:00:00Z, the exclusive end"
        );
        for huge in [MAX_QUERY_TIME_MILLIS + 1, 9_000_000_000_000_000, i64::MAX] {
            assert_eq!(
                HookExecutionTimeRange::from_millis(None, Some(huge), now, day),
                Err("time must not be later than 9999-12-31T23:59:59.999Z"),
                "{huge}"
            );
            assert!(
                HookExecutionTimeRange::from_millis(Some(huge), None, now, day).is_err(),
                "{huge}"
            );
        }
    }
}
