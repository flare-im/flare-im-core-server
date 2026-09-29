//! 日志与 trace 初始化
//!
//! 实现落在 [flare_server_core::telemetry]；本模块把 `flare_im_service_kit::config::LoggingConfig`
//! 与标准环境变量映射为 subscriber 选项，是 Core 各服务进程唯一的初始化入口：
//!
//! * `FLARE_LOG_FORMAT`（`text` | `json`）优先于 `[logging].format`；
//! * `OTEL_EXPORTER_OTLP_ENDPOINT` 非空时启用 OTLP trace exporter，服务名取
//!   `OTEL_SERVICE_NAME`，未设置时用可执行文件名。
//!
//! JSON 日志与 OTLP 是同一个全局 subscriber 上的两个 layer，可同时开启。
//! 返回的 [TracingGuard] 须持有到服务退出：释放时导出批处理队列里最后一批 span。

use std::time::Duration;

use flare_server_core::error::{FlareError, Result};
use flare_server_core::{LogFormat, LoggingSubscriberOptions, OtlpTracingOptions};

const OTLP_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

const LOG_FORMAT_ENV: &str = "FLARE_LOG_FORMAT";
const OTLP_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
const OTLP_SERVICE_NAME_ENV: &str = "OTEL_SERVICE_NAME";

/// 从日志配置与环境变量初始化全局 subscriber。
///
/// 显式配置错误（未知日志格式、OTLP exporter 无法构建）返回错误，由服务入口拒绝启动；
/// 已存在全局 subscriber 时静默跳过，重复调用安全。
pub fn init_tracing_from_config(
    logging_config: Option<&crate::config::LoggingConfig>,
) -> Result<TracingGuard> {
    let plan = resolve_tracing_plan(logging_config, |key| std::env::var(key).ok())?;
    flare_server_core::init_tracing_subscriber(Some(&plan.logging), plan.otlp.as_ref())
        .map_err(|error| FlareError::system(format!("tracing initialization failed: {error}")))?;
    if let Some(otlp) = &plan.otlp {
        tracing::info!(
            service_name = %otlp.service_name,
            endpoint = %otlp.endpoint,
            format = %plan.logging.format,
            "tracing initialized with OTLP exporter"
        );
    }
    Ok(TracingGuard {
        otlp_enabled: plan.otlp.is_some(),
    })
}

/// 服务生命周期内持有；释放时关闭 OTLP exporter 并导出剩余 span（最多等待 5 秒）。
#[must_use = "hold the guard until the service stops so the last spans are exported"]
#[derive(Debug)]
pub struct TracingGuard {
    otlp_enabled: bool,
}

impl Drop for TracingGuard {
    fn drop(&mut self) {
        if !self.otlp_enabled {
            return;
        }
        let shutdown = || flare_server_core::shutdown_tracing(OTLP_SHUTDOWN_TIMEOUT);
        // exporter 的 gRPC 通道由 tokio 运行时驱动：多线程运行时里让出当前工作线程，
        // 否则（单线程或无运行时）直接调用，最坏情况按超时返回，不会挂死退出流程。
        let result = match tokio::runtime::Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(shutdown)
            }
            _ => shutdown(),
        };
        if let Err(error) = result {
            tracing::warn!(%error, "OTLP exporter shutdown did not complete");
        }
    }
}

#[derive(Debug, Clone)]
struct TracingPlan {
    logging: LoggingSubscriberOptions,
    otlp: Option<OtlpTracingOptions>,
}

fn resolve_tracing_plan<F>(
    logging_config: Option<&crate::config::LoggingConfig>,
    env_value: F,
) -> Result<TracingPlan>
where
    F: Fn(&str) -> Option<String>,
{
    let non_empty = |key: &str| {
        env_value(key)
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    };

    let mut logging = logging_config
        .map(|c| LoggingSubscriberOptions {
            level: c.level.clone(),
            with_target: c.with_target,
            with_thread_ids: c.with_thread_ids,
            with_file: c.with_file,
            with_line_number: c.with_line_number,
            with_ansi: c.with_ansi,
            format: LogFormat::Text,
        })
        .unwrap_or_default();

    let configured_format = logging_config
        .and_then(|c| c.format.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    if let Some(format) = non_empty(LOG_FORMAT_ENV).or(configured_format) {
        logging.format = format.parse().map_err(|error| {
            FlareError::system(format!("{LOG_FORMAT_ENV}/[logging].format: {error}"))
        })?;
    }

    let otlp = non_empty(OTLP_ENDPOINT_ENV).map(|endpoint| {
        let service_name = non_empty(OTLP_SERVICE_NAME_ENV).unwrap_or_else(executable_name);
        OtlpTracingOptions::new(service_name, endpoint)
    });

    Ok(TracingPlan { logging, otlp })
}

fn executable_name() -> String {
    std::env::current_exe()
        .ok()
        .and_then(|path| {
            path.file_stem()
                .map(|stem| stem.to_string_lossy().into_owned())
        })
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "flare-im-core".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LoggingConfig;

    fn env_of(pairs: &'static [(&'static str, &'static str)]) -> impl Fn(&str) -> Option<String> {
        move |key| {
            pairs
                .iter()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| value.to_string())
        }
    }

    #[test]
    fn defaults_to_text_without_otlp() {
        let plan = resolve_tracing_plan(None, env_of(&[])).expect("plan");
        assert_eq!(plan.logging.format, LogFormat::Text);
        assert!(plan.otlp.is_none());
    }

    #[test]
    fn env_format_overrides_config_format() {
        let config = LoggingConfig {
            level: "info".to_string(),
            format: Some("text".to_string()),
            ..LoggingConfig::default()
        };
        let plan =
            resolve_tracing_plan(Some(&config), env_of(&[(LOG_FORMAT_ENV, "json")])).expect("plan");
        assert_eq!(plan.logging.format, LogFormat::Json);
        assert_eq!(plan.logging.level, "info");
    }

    #[test]
    fn config_format_applies_without_env() {
        let config = LoggingConfig {
            format: Some("JSON".to_string()),
            ..LoggingConfig::default()
        };
        let plan = resolve_tracing_plan(Some(&config), env_of(&[])).expect("plan");
        assert_eq!(plan.logging.format, LogFormat::Json);
    }

    #[test]
    fn unknown_format_is_a_startup_error() {
        let error = resolve_tracing_plan(None, env_of(&[(LOG_FORMAT_ENV, "logfmt")]))
            .expect_err("unknown format must fail");
        assert!(error.to_string().contains("logfmt"));
    }

    #[test]
    fn otlp_endpoint_enables_exporter_with_service_name() {
        let plan = resolve_tracing_plan(
            None,
            env_of(&[
                (OTLP_ENDPOINT_ENV, " http://otel-collector:4317 "),
                (OTLP_SERVICE_NAME_ENV, "message-ingest"),
            ]),
        )
        .expect("plan");
        let otlp = plan.otlp.expect("otlp enabled");
        assert_eq!(otlp.endpoint, "http://otel-collector:4317");
        assert_eq!(otlp.service_name, "message-ingest");
    }

    #[test]
    fn blank_otlp_endpoint_keeps_exporter_off() {
        let plan = resolve_tracing_plan(None, env_of(&[(OTLP_ENDPOINT_ENV, "  ")])).expect("plan");
        assert!(plan.otlp.is_none());
    }

    // 未安装 OTLP provider 时关闭是空操作；这里只证明释放守卫在两种运行时与无运行时下都不会 panic。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn guard_release_is_safe_on_multi_thread_runtime() {
        drop(TracingGuard { otlp_enabled: true });
    }

    #[tokio::test]
    async fn guard_release_is_safe_on_current_thread_runtime() {
        drop(TracingGuard { otlp_enabled: true });
    }

    #[test]
    fn guard_release_is_safe_without_runtime() {
        drop(TracingGuard { otlp_enabled: true });
        drop(TracingGuard {
            otlp_enabled: false,
        });
    }

    #[test]
    fn otlp_service_name_falls_back_to_executable() {
        let plan = resolve_tracing_plan(None, env_of(&[(OTLP_ENDPOINT_ENV, "http://c:4317")]))
            .expect("plan");
        assert!(!plan.otlp.expect("otlp enabled").service_name.is_empty());
    }
}
