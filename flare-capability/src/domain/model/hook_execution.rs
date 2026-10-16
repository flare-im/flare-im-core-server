//! Hook 执行证据：每次调用 Hook 的结果，写进 `hook_executions`（源 DDL 0015 加了 `sample_weight` 与
//! `hook_version`），`hook_config.get_statistics` 与 `hook_config.query_executions` 读它。
//!
//! 记录不带消息正文，也不带下游的错误原文（其中可能有内部地址）：失败只记错误码与固定说明，拒绝只记
//! Hook 给的拒绝原因码。放行与熔断短路按比例采样（一行代表 `sample_weight` 次），拒绝、调用失败与
//! 超时全量记录。

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

/// Hook 明确拒绝。
pub const HOOK_REJECTED_CODE: &str = "HOOK_REJECTED";
/// 调用超过 Hook 的超时。
pub const HOOK_TIMEOUT_CODE: &str = "HOOK_TIMEOUT";
/// 熔断打开，没有调用 Hook。
pub const HOOK_CIRCUIT_OPEN_CODE: &str = "HOOK_CIRCUIT_OPEN";
/// 调用失败、错误没有错误码时的代码。
pub const HOOK_CALL_FAILED_CODE: &str = "HOOK_CALL_FAILED";

/// 存进库里的拒绝原因码、消息号、请求号、追踪号的长度上限（字符）。
pub const MAX_RECORDED_FIELD_CHARS: usize = 256;

/// 一次 Hook 调用的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HookExecutionOutcome {
    /// Hook 放行（事件类 Hook：调用成功）。按比例采样。
    Allowed,
    /// Hook 明确拒绝，`reason` 是 Hook 给的拒绝原因码。全量记录。
    Rejected { reason: String },
    /// 调用失败（连不上、对端报错、应答解不开），`code` 为错误码。全量记录。
    Failed { code: &'static str },
    /// 超过 Hook 的超时。全量记录。
    TimedOut,
    /// 熔断打开，没有调用 Hook。按比例采样：熔断本就是为了在故障期间卸载，每次短路都写一行会把
    /// 一次故障变成一场写库风暴。
    CircuitOpen,
}

/// 记录的取舍：采样的一类在缓冲满时先丢。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HookExecutionClass {
    /// 放行与熔断短路：按比例采样，缓冲满时先丢。
    Sampled,
    /// 拒绝、失败、超时：全量记录。
    Always,
}

impl HookExecutionClass {
    /// 指标标签取值（固定）。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sampled => "sampled",
            Self::Always => "always",
        }
    }
}

impl HookExecutionOutcome {
    pub fn class(&self) -> HookExecutionClass {
        match self {
            Self::Allowed | Self::CircuitOpen => HookExecutionClass::Sampled,
            Self::Rejected { .. } | Self::Failed { .. } | Self::TimedOut => {
                HookExecutionClass::Always
            }
        }
    }

    /// Hook 给出了结论（放行或拒绝）。存为 `success`；熔断按它判断 Hook 是否健康。
    pub fn answered(&self) -> bool {
        matches!(self, Self::Allowed | Self::Rejected { .. })
    }

    /// 存进 `error_code` 的代码；放行没有。
    pub fn error_code(&self) -> Option<&str> {
        match self {
            Self::Allowed => None,
            Self::Rejected { .. } => Some(HOOK_REJECTED_CODE),
            Self::Failed { code } => Some(code),
            Self::TimedOut => Some(HOOK_TIMEOUT_CODE),
            Self::CircuitOpen => Some(HOOK_CIRCUIT_OPEN_CODE),
        }
    }

    /// 存进 `error_message` 的说明：拒绝是 Hook 的拒绝原因码，其余是固定文字（不带下游原文）。
    pub fn error_message(&self) -> Option<String> {
        match self {
            Self::Allowed => None,
            Self::Rejected { reason } if reason.is_empty() => None,
            Self::Rejected { reason } => Some(bounded(reason)),
            Self::Failed { .. } => Some("hook call failed".to_string()),
            Self::TimedOut => Some("hook timed out".to_string()),
            Self::CircuitOpen => Some("hook circuit open; hook not called".to_string()),
        }
    }
}

/// 截到 [`MAX_RECORDED_FIELD_CHARS`] 个字符，控制字符换成空格（这些字段会被员工在控制台读到）。
pub fn bounded(value: &str) -> String {
    value
        .chars()
        .take(MAX_RECORDED_FIELD_CHARS)
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect()
}

/// 一次 Hook 调用的记录（写入 `hook_executions` 的一行）。
#[derive(Debug, Clone, PartialEq)]
pub struct HookExecutionRecord {
    pub execution_id: String,
    pub tenant_id: String,
    pub hook_type: String,
    pub hook_name: String,
    pub hook_version: Option<String>,
    pub message_id: Option<String>,
    pub request_id: Option<String>,
    pub trace_id: Option<String>,
    pub outcome: HookExecutionOutcome,
    /// 从发起调用到得到结果；熔断短路为 0。
    pub latency: Duration,
    pub executed_at: SystemTime,
    /// 这一行代表几次执行：全量记录的为 1，按 1/N 采样留下的为 N。
    pub sample_weight: u32,
}

impl HookExecutionRecord {
    /// 存进 `hook_id` 的值：`hook_type:name`（与管理接口接受的 hook_id 写法相同）。
    pub fn hook_id(&self) -> String {
        format!("{}:{}", self.hook_type, self.hook_name)
    }

    pub fn latency_ms(&self) -> i32 {
        i32::try_from(self.latency.as_millis()).unwrap_or(i32::MAX)
    }
}

/// 一个租户的一条 Hook：统计与查询都限定在这里。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookExecutionScope {
    pub tenant_id: String,
    pub hook_type: String,
    pub hook_name: String,
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
    fn outcomes_map_to_success_class_and_codes() {
        let rejected = HookExecutionOutcome::Rejected {
            reason: "SPAM".into(),
        };
        let failed = HookExecutionOutcome::Failed {
            code: "SERVICE_UNAVAILABLE",
        };
        let cases = [
            (
                HookExecutionOutcome::Allowed,
                true,
                HookExecutionClass::Sampled,
                None,
            ),
            (
                rejected.clone(),
                true,
                HookExecutionClass::Always,
                Some(HOOK_REJECTED_CODE),
            ),
            (
                failed.clone(),
                false,
                HookExecutionClass::Always,
                Some("SERVICE_UNAVAILABLE"),
            ),
            (
                HookExecutionOutcome::TimedOut,
                false,
                HookExecutionClass::Always,
                Some(HOOK_TIMEOUT_CODE),
            ),
            (
                HookExecutionOutcome::CircuitOpen,
                false,
                HookExecutionClass::Sampled,
                Some(HOOK_CIRCUIT_OPEN_CODE),
            ),
        ];
        for (outcome, answered, class, code) in cases {
            assert_eq!(outcome.answered(), answered, "{outcome:?}");
            assert_eq!(outcome.class(), class, "{outcome:?}");
            assert_eq!(outcome.error_code(), code, "{outcome:?}");
        }
        assert_eq!(rejected.error_message().as_deref(), Some("SPAM"));
        assert_eq!(
            failed.error_message().as_deref(),
            Some("hook call failed"),
            "never the downstream error text"
        );
        assert_eq!(HookExecutionOutcome::Allowed.error_message(), None);
    }

    #[test]
    fn recorded_text_is_bounded_and_free_of_control_characters() {
        let long = "r".repeat(MAX_RECORDED_FIELD_CHARS + 10);
        assert_eq!(bounded(&long).chars().count(), MAX_RECORDED_FIELD_CHARS);
        assert_eq!(bounded("a\nb\u{1b}c"), "a b c");
    }

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
