//! 错误建立在 `flare_server_core::error` 之上：本工具只向操作者报告失败，
//! 所以全部是带步骤说明的 `FlareError::system`。说明里从不带秘密内容。

pub use flare_server_core::error::{FlareError, Result};

/// 命令行输出用的说明文字（去掉 `FlareError` 的类别前缀）。
pub fn message(error: &FlareError) -> String {
    match error {
        FlareError::System(text) | FlareError::Io(text) => text.clone(),
        other => other.to_string(),
    }
}

/// 由格式串构造一个系统错误。
macro_rules! err {
    ($($arg:tt)*) => {
        $crate::error::FlareError::system(format!($($arg)*))
    };
}

/// 以系统错误提前返回。
macro_rules! fail {
    ($($arg:tt)*) => {
        return Err($crate::error::err!($($arg)*))
    };
}

/// 条件不成立时以系统错误提前返回。
macro_rules! ensure {
    ($cond:expr, $($arg:tt)*) => {
        if !($cond) {
            $crate::error::fail!($($arg)*);
        }
    };
}

pub(crate) use {ensure, err, fail};
