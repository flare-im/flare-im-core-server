//! 领域服务（Domain Service）

pub mod device_manager_service;
pub mod online_status_service;
pub mod subscription_service;
pub mod user_service;

pub use device_manager_service::DeviceManagerService;
pub use online_status_service::{DefaultOnlineStatusService, OnlineStatusService};
pub use subscription_service::SubscriptionService;
pub use user_service::UserService;

use std::borrow::Cow;

/// 踢线原因进日志的形式：控制字符（`\n`、`\r`、`\t` 等）按 Rust 转义写出，其余字符（中文等）
/// 原样，与 admin-gateway `admin_audit` 日志里的 `audit_reason` 写法相同。
///
/// 原因是运维自由文本：admin-gateway 转来的是解码后的 `x-audit-reason`，可以含换行；调度器与
/// 用户自己的踢线请求同样不受约束。原样写进文本日志就能伪造日志行。只改日志里的写法，请求里
/// 的原因不变，连接端收到的始终是类型化原因。
pub(crate) fn loggable_reason(reason: &str) -> Cow<'_, str> {
    if !reason.chars().any(char::is_control) {
        return Cow::Borrowed(reason);
    }
    let mut escaped = String::with_capacity(reason.len() + 8);
    for c in reason.chars() {
        if c.is_control() {
            escaped.extend(c.escape_default());
        } else {
            escaped.push(c);
        }
    }
    Cow::Owned(escaped)
}

#[cfg(test)]
mod tests {
    use super::loggable_reason;
    use std::borrow::Cow;

    #[test]
    fn loggable_reason_escapes_control_characters_and_keeps_the_rest() {
        assert!(matches!(
            loggable_reason("清理违规 spam"),
            Cow::Borrowed("清理违规 spam")
        ));
        let escaped = loggable_reason("清理\nINFO forged\r\n\tx\u{1b}[31m\u{85}\u{7f}");
        assert_eq!(
            escaped,
            "清理\\nINFO forged\\r\\n\\tx\\u{1b}[31m\\u{85}\\u{7f}"
        );
        assert!(!escaped.contains(['\n', '\r']));
        assert!(!escaped.chars().any(char::is_control));
    }
}
