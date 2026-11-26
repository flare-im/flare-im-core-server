//! Hook 执行证据：每次调用 Hook 的结果，由 Ingest 批量写进 `hook_executions`，Capability 的
//! `hook_config.get_statistics` 与 `hook_config.query_executions` 读它。
//!
//! 记录不带消息正文，也不带下游的错误原文（其中可能有内部地址）：失败只记错误码与固定说明，拒绝只记
//! Hook 给的拒绝原因码。放行与熔断短路按比例采样（一行代表 `sample_weight` 次），拒绝、调用失败与
//! 超时全量记录。

use std::time::{Duration, SystemTime};

use crate::hooks::circuit::HookCircuitResult;

/// Hook 明确拒绝。
pub const HOOK_REJECTED_CODE: &str = "HOOK_REJECTED";
/// 调用超过 Hook 的超时。
pub const HOOK_TIMEOUT_CODE: &str = "HOOK_TIMEOUT";
/// 熔断打开，没有调用 Hook。
pub const HOOK_CIRCUIT_OPEN_CODE: &str = "HOOK_CIRCUIT_OPEN";
/// 调用失败、错误没有错误码时的代码。
pub const HOOK_CALL_FAILED_CODE: &str = "HOOK_CALL_FAILED";
/// Hook 放行了，但改写的正文解不开或为空。
pub const HOOK_REWRITE_INVALID_CODE: &str = "HOOK_REWRITE_INVALID";
/// 这一步开始前或进行中，PreSend 总时限耗尽。
pub const HOOK_BUDGET_EXHAUSTED_CODE: &str = "HOOK_BUDGET_EXHAUSTED";

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
    /// PreSend 总时限耗尽：这一步没发出，或被总时限截断。不是这条 Hook 的错，不计入熔断，按比例
    /// 采样；真正耗时的那条 Hook 自己留有超时记录。
    BudgetExhausted,
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
            Self::Allowed | Self::CircuitOpen | Self::BudgetExhausted => {
                HookExecutionClass::Sampled
            }
            Self::Rejected { .. } | Self::Failed { .. } | Self::TimedOut => {
                HookExecutionClass::Always
            }
        }
    }

    /// Hook 给出了结论（放行或拒绝）。存为 `success`。
    pub fn answered(&self) -> bool {
        matches!(self, Self::Allowed | Self::Rejected { .. })
    }

    /// 这次调用对熔断的意义：给出结论算健康，总时限耗尽说明不了 Hook 的健康。
    pub fn circuit_result(&self) -> HookCircuitResult {
        match self {
            Self::Allowed | Self::Rejected { .. } => HookCircuitResult::Healthy,
            Self::BudgetExhausted => HookCircuitResult::Neutral,
            Self::Failed { .. } | Self::TimedOut | Self::CircuitOpen => {
                HookCircuitResult::Unhealthy
            }
        }
    }

    /// 存进 `error_code` 的代码；放行没有。
    pub fn error_code(&self) -> Option<&str> {
        match self {
            Self::Allowed => None,
            Self::Rejected { .. } => Some(HOOK_REJECTED_CODE),
            Self::Failed { code } => Some(code),
            Self::TimedOut => Some(HOOK_TIMEOUT_CODE),
            Self::CircuitOpen => Some(HOOK_CIRCUIT_OPEN_CODE),
            Self::BudgetExhausted => Some(HOOK_BUDGET_EXHAUSTED_CODE),
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
            Self::BudgetExhausted => Some("pre-send budget exhausted".to_string()),
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

/// 存进 `hook_executions.hook_id` 的 Hook 身份，也是管理接口查统计用的 hook_id：配置的 Hook 是它的
/// `hook_configs` 行号，删掉后同名重建的 Hook 号不同、统计不继承；部署文件的 Hook 没有行号，是
/// `hook_type:name`。
pub fn execution_hook_id(hook_type: &str, hook_name: &str, config_id: Option<i64>) -> String {
    match config_id {
        Some(id) => id.to_string(),
        None => format!("{hook_type}:{hook_name}"),
    }
}

/// 一次 Hook 调用的记录（写入 `hook_executions` 的一行）。
#[derive(Debug, Clone, PartialEq)]
pub struct HookExecutionRecord {
    pub execution_id: String,
    pub tenant_id: String,
    /// 见 [`execution_hook_id`]。
    pub hook_id: String,
    pub hook_type: String,
    pub hook_name: String,
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
    /// 存进 `hook_id` 的值（见 [`execution_hook_id`]）。
    pub fn hook_id(&self) -> String {
        self.hook_id.clone()
    }

    pub fn latency_ms(&self) -> i32 {
        i32::try_from(self.latency.as_millis()).unwrap_or(i32::MAX)
    }
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
            (
                HookExecutionOutcome::BudgetExhausted,
                false,
                HookExecutionClass::Sampled,
                Some(HOOK_BUDGET_EXHAUSTED_CODE),
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
}
