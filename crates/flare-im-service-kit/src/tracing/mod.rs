//! 日志初始化
//!
//! **fmt 日志**：实现落在 [flare_server_core::telemetry]；本模块提供
//! `flare_im_service_kit::config::LoggingConfig` 的映射。OTLP trace exporter 由
//! [flare_server_core::telemetry] 统一接线。

use tracing::info;

/// 从 `flare_im_service_kit` 日志配置初始化全局 subscriber（委托 [flare_server_core::init_fmt_subscriber]）
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

/// 初始化全局日志 subscriber。
pub fn init_tracing(
    service_name: &str,
    endpoint: Option<&str>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let otlp = endpoint.map(|ep| flare_server_core::OtlpTracingOptions::new(service_name, ep));
    flare_server_core::init_tracing_subscriber(None, otlp.as_ref())?;

    match endpoint {
        Some(ep) => info!(
            service_name = %service_name,
            endpoint = %ep,
            "Tracing initialized with OTLP exporter"
        ),
        None => info!(
            service_name = %service_name,
            "Tracing initialized (fmt only)"
        ),
    }

    Ok(())
}
