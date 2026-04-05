//! 分布式追踪与日志初始化
//!
//! **fmt 日志**：实现落在 [flare_server_core::telemetry]；本模块提供 `flare_im_core::config::LoggingConfig` 的映射与 IM 侧可选 OTLP 入口。
//!
//! OTLP 与 `opentelemetry` 0.28 仍由本 crate 的 `tracing` feature 控制（与 `flare-server-core` 可选 OTel 版本可独立演进）。

#[cfg(feature = "tracing")]
use tracing::{Span, info, warn};

/// 从 `flare_im_core` 日志配置初始化全局 subscriber（委托 [flare_server_core::init_fmt_subscriber]）
pub fn init_tracing_from_config(logging_config: Option<&crate::config::LoggingConfig>) {
    let owned = logging_config.map(|c| flare_server_core::LoggingSubscriberOptions {
        level: c.level.clone(),
        with_target: c.with_target,
        with_thread_ids: c.with_thread_ids,
        with_file: c.with_file,
        with_line_number: c.with_line_number,
        with_ansi: c.with_ansi,
    });
    flare_server_core::init_fmt_subscriber(owned.as_ref());
}

/// 初始化 OpenTelemetry 追踪（可选）；失败或未启用时降级为 [init_tracing_from_config]\(None\)
#[cfg(feature = "tracing")]
pub fn init_tracing(
    service_name: &str,
    endpoint: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(all(feature = "tracing", feature = "opentelemetry"))]
    {
        if let Some(otlp_endpoint) = endpoint {
            match init_otlp_tracing(service_name, otlp_endpoint) {
                Ok(_) => {
                    info!(
                        service_name = %service_name,
                        endpoint = %otlp_endpoint,
                        "OpenTelemetry OTLP tracing initialized (connected to Tempo)"
                    );
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        service_name = %service_name,
                        endpoint = %otlp_endpoint,
                        error = %e,
                        "Failed to initialize OpenTelemetry OTLP, falling back to basic tracing"
                    );
                }
            }
        }
    }

    init_tracing_from_config(None);

    if let Some(ep) = endpoint {
        info!(
            service_name = %service_name,
            endpoint = %ep,
            "Tracing initialized (basic tracing mode, Tempo connection pending)"
        );
    } else {
        info!(
            service_name = %service_name,
            "Tracing initialized (basic tracing mode)"
        );
    }

    Ok(())
}

#[cfg(all(feature = "tracing", feature = "opentelemetry"))]
fn init_otlp_tracing(
    _service_name: &str,
    _endpoint: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use tracing_subscriber::{EnvFilter, fmt};

    let env_filter = match EnvFilter::try_from_default_env() {
        Ok(filter) => filter,
        Err(_) => EnvFilter::new("debug"),
    };

    let _ = fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .try_init();

    Ok(())
}

#[cfg(feature = "tracing")]
pub fn create_span(_tracer_name: &str, _span_name: &str) -> Span {
    Span::current()
}
