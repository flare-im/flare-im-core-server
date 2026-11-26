//! Hook 名称的登记规则与日志写法。
//!
//! 名称来自 Admin 接口（`CapabilityService/Administer` 的创建与更新）、配置文件和库里的行，每次执行
//! Hook 都会写进日志。新登记或改名时按 [`validate_hook_name`] 校验；规则之前已经存下或写在配置文件
//! 里的名称照常加载执行（已有部署不受影响），日志与错误信息里一律经 [`LoggableName`] 转义，带换行的
//! 名称伪造不了日志行。

use std::fmt::{self, Write as _};

/// 名称的长度上限（字符）。
pub const MAX_HOOK_NAME_CHARS: usize = 128;

/// 创建与改名时的规则：非空、至多 [`MAX_HOOK_NAME_CHARS`] 个字符、不含控制字符（`char::is_control`：
/// 换行、回车、制表符、ESC、DEL 与 C1）。返回的说明不带取值。
pub fn validate_hook_name(name: &str) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("name is required");
    }
    if name.chars().count() > MAX_HOOK_NAME_CHARS {
        return Err("name must be at most 128 characters");
    }
    if name.chars().any(char::is_control) {
        return Err("name must not contain control characters");
    }
    Ok(())
}

/// 日志与错误信息里的 Hook 名称：控制字符按 Rust 转义写出（`\n`、`\u{1b}`），其余字符原样。
/// 只在事件真正输出时格式化，不额外分配。
pub struct LoggableName<'a>(pub &'a str);

impl fmt::Display for LoggableName<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for c in self.0.chars() {
            if c.is_control() {
                write!(f, "{}", c.escape_default())?;
            } else {
                f.write_char(c)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_non_empty_short_and_free_of_control_characters() {
        for valid in [
            "dlp",
            "audit-hook.v2",
            "内容审核",
            "pre send: profanity",
            "n".repeat(MAX_HOOK_NAME_CHARS).as_str(),
            "审".repeat(MAX_HOOK_NAME_CHARS).as_str(),
        ] {
            assert_eq!(validate_hook_name(valid), Ok(()), "{valid:?}");
        }
        for (invalid, rule) in [
            ("", "required"),
            ("n".repeat(MAX_HOOK_NAME_CHARS + 1).as_str(), "128"),
            ("dlp\nINFO forged line", "control"),
            ("dlp\r", "control"),
            ("dlp\t", "control"),
            ("dlp\u{1b}[31m", "control"),
            ("dlp\u{7f}", "control"),
            ("dlp\u{85}", "control"),
            ("dlp\u{0}", "control"),
        ] {
            let error = validate_hook_name(invalid).unwrap_err();
            assert!(error.contains(rule), "{invalid:?}: {error}");
        }
    }

    #[test]
    fn loggable_names_escape_control_characters_and_keep_the_rest() {
        assert_eq!(LoggableName("内容审核 v2").to_string(), "内容审核 v2");
        assert_eq!(
            LoggableName("dlp\nINFO forged\r\n\u{1b}[31m\u{85}").to_string(),
            "dlp\\nINFO forged\\r\\n\\u{1b}[31m\\u{85}"
        );
        assert!(!LoggableName("a\nb\rc").to_string().contains(['\n', '\r']));
    }
}
