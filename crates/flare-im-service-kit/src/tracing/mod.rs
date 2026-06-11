//! 日志初始化
//!
//! **fmt 日志**：实现落在 [flare_server_core::telemetry]；本模块提供
//! `flare_im_service_kit::config::LoggingConfig` 的映射。OTLP 导出尚未实现。

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
///
/// OTLP 导出器未实现：`endpoint` 仅记录提示，不建立任何连接。
pub fn init_tracing(
    service_name: &str,
    endpoint: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    init_tracing_from_config(None);

    match endpoint {
        Some(ep) => info!(
            service_name = %service_name,
            endpoint = %ep,
            "Tracing initialized (fmt only; OTLP exporter not implemented, endpoint ignored)"
        ),
        None => info!(
            service_name = %service_name,
            "Tracing initialized (fmt only)"
        ),
    }

    Ok(())
}
