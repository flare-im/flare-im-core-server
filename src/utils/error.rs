//! Flare IM Core 错误工具模块
//!
//! - 统一对外暴露 `flare-server-core` 定义的错误类型
//! - 为基础设施层提供便捷的错误转换工具
//! - 提供各服务共用的错误映射辅助（to_system_err / to_system_err_with）

pub use flare_server_core::error::{
    ErrorBuilder, ErrorCategory, ErrorCode, FlareError, FlareServerError, InfraResult, InfraResultExt,
    LocalizedError, Result, map_infra_error,
};

/// 将内部错误映射为系统错误，供各服务 handler 层统一使用（如 `.map_err(to_system_err)?`）。
#[inline]
pub fn to_system_err(e: impl std::fmt::Display) -> FlareError {
    FlareError::system(format!("Internal error: {}", e))
}

/// 带上下文的系统错误映射，供发布 Kafka/推送 等调用统一使用。
#[inline]
pub fn to_system_err_with(e: impl std::fmt::Display, context: &str) -> FlareError {
    FlareError::system(format!("{}: {}", context, e))
}

/// 便捷宏：将基础设施错误映射为指定业务错误并提前返回
#[macro_export]
macro_rules! bail_infra {
    ($err:expr, $code:expr, $msg:expr) => {
        return Err($crate::error::map_infra_error($err, $code, $msg))
    };
}

/// 便捷宏：从返回 `InfraResult` 的表达式中直接转换为业务层 `Result`
#[macro_export]
macro_rules! try_infra {
    ($expr:expr, $code:expr, $msg:expr) => {
        $expr.into_flare($code, $msg)?
    };
}
