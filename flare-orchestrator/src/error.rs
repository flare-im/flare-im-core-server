//! 消息编排服务错误处理模块
//!
//! 统一处理消息操作相关的错误，遵循FlareError标准。
//! 重新导出 flare_im_core 的 to_system_err / to_system_err_with 供本服务统一使用。

pub use flare_server_core::error::{
    LocalizedError,
    ErrorBuilder, ErrorCode, FlareError, Result,
    map_infra_error, InfraResultExt,
};
pub use flare_server_core::{flare_err, flare_err_details};
pub use flare_server_core::error::grpc::IntoGrpc;


/// 统一错误映射（委托 flare_im_core）
pub use flare_im_core::error::{to_system_err, to_system_err_with};

// 重新导出FlareError相关类型
pub use flare_server_core::error::FlareError as MessageOrchestratorError;

/// 消息操作相关的错误代码
#[derive(Debug, Clone, Copy)]
pub enum MessageOperationErrorCode {
    /// 消息不存在
    MessageNotFound,
    /// 权限不足
    PermissionDenied,
    /// 撤回超时
    RecallTimeout,
    /// 状态不允许的操作
    InvalidStateTransition,
    /// 操作参数无效
    InvalidOperationParameter,
}

impl MessageOperationErrorCode {
    /// 转换为通用错误代码
    pub fn to_error_code(&self) -> ErrorCode {
        match self {
            MessageOperationErrorCode::MessageNotFound => ErrorCode::MessageNotFound,
            MessageOperationErrorCode::PermissionDenied => ErrorCode::PermissionDenied,
            MessageOperationErrorCode::RecallTimeout => ErrorCode::OperationFailed,
            MessageOperationErrorCode::InvalidStateTransition => ErrorCode::InvalidParameter,
            MessageOperationErrorCode::InvalidOperationParameter => ErrorCode::InvalidParameter,
        }
    }
}

/// 消息操作错误构建器
pub struct MessageOperationErrorBuilder;

impl MessageOperationErrorBuilder {
    /// 构建消息不存在错误
    pub fn message_not_found(message_id: &str) -> FlareError {
        ErrorBuilder::new(ErrorCode::MessageNotFound, "Message not found")
            .param("message_id", message_id)
            .build_error()
    }

    /// 构建权限不足错误
    pub fn permission_denied(operation: &str, user_id: &str) -> FlareError {
        ErrorBuilder::new(ErrorCode::PermissionDenied, "Permission denied")
            .param("operation", operation)
            .param("user_id", user_id)
            .build_error()
    }

    /// 构建撤回超时错误
    pub fn recall_timeout(message_id: &str) -> FlareError {
        ErrorBuilder::new(ErrorCode::OperationFailed, "Message recall timeout")
            .param("message_id", message_id)
            .build_error()
    }

    /// 构建无效状态转换错误
    pub fn invalid_state_transition(from: &str, to: &str) -> FlareError {
        ErrorBuilder::new(
            ErrorCode::InvalidParameter,
            format!("Invalid state transition from {} to {}", from, to),
        )
        .param("from_state", from)
        .param("to_state", to)
        .build_error()
    }
}
