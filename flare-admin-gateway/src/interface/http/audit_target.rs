//! `admin_audit` 事件里 `target` 字段的统一转义。
//!
//! 目标取自 axum 百分号解码后的路径参数（媒资 ACL 取自请求体的 `file_id`），可以是任意 UTF-8：
//! 换行与控制字符能在不转义的日志后端伪造出新的一行，`/` 会让「用户/设备」这类复合目标有两种
//! 读法，超长输入会撑大每条审计事件。
//!
//! 规则：只原样保留 RFC 3986 的 unreserved 字符（`A-Z a-z 0-9 - . _ ~`），其余每个 UTF-8 字节写成
//! 大写 `%XX`。复合目标各段分别转义后再用 `/` 连接，所以 `/` 只会是分隔符，逐段百分号解码即得
//! 原值。一段转义后超过 [`MAX_SEGMENT_BYTES`] 字节时，截在完整的字符或 `%XX` 之后，再追加
//! `!truncated:<原始字节数>`；`!` 与 `:` 不是 unreserved 字符，正文里一定已被转义，所以标记不会
//! 与内容混淆。截断后的目标不再唯一，只作定位线索。
use std::fmt::Write as _;

/// 每一段转义后的上限（字节，不含截断标记）。
pub(super) const MAX_SEGMENT_BYTES: usize = 256;

/// 已转义的审计目标。只能经 [`AuditTarget::id`] 或 [`AuditTarget::pair`] 构造，审计函数也只收
/// 这个类型，因此未转义的原值进不了 `admin_audit` 事件。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AuditTarget(String);

impl AuditTarget {
    /// 单个标识：会话、用户、租户或文件。
    pub(super) fn id(raw: &str) -> Self {
        let mut escaped = String::new();
        push_segment(&mut escaped, raw);
        Self(escaped)
    }

    /// 两段的复合目标，例如踢设备的 `用户/设备`。
    pub(super) fn pair(first: &str, second: &str) -> Self {
        let mut escaped = String::new();
        push_segment(&mut escaped, first);
        escaped.push('/');
        push_segment(&mut escaped, second);
        Self(escaped)
    }

    pub(super) fn as_str(&self) -> &str {
        &self.0
    }
}

fn is_unreserved(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')
}

fn push_segment(out: &mut String, raw: &str) {
    let start = out.len();
    for byte in raw.bytes() {
        let width = if is_unreserved(byte) { 1 } else { 3 };
        if out.len() - start + width > MAX_SEGMENT_BYTES {
            // 写入 String 不会失败。
            let _ = write!(out, "!truncated:{}", raw.len());
            return;
        }
        if is_unreserved(byte) {
            out.push(char::from(byte));
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 逐段百分号解码，用来证明转义可逆。
    fn decode(segment: &str) -> String {
        let bytes = segment.as_bytes();
        let mut out = Vec::new();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'%' {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap();
                out.push(u8::from_str_radix(hex, 16).unwrap());
                i += 3;
            } else {
                out.push(bytes[i]);
                i += 1;
            }
        }
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn ordinary_ids_are_unchanged() {
        for id in [
            "u1",
            "1200000000000000638",
            "tenant-a",
            "f-1",
            "c_1.v2~x",
            "6f9619ff-8b86-d011-b42d-00cf4fc964ff",
        ] {
            assert_eq!(AuditTarget::id(id).as_str(), id);
        }
        assert_eq!(AuditTarget::pair("u1", "d1").as_str(), "u1/d1");
    }

    #[test]
    fn newline_and_control_characters_cannot_start_a_new_log_line() {
        let forged = "u1\nINFO admin_audit access=write route=tenant.kick_all";
        let target = AuditTarget::id(forged);
        assert_eq!(
            target.as_str(),
            "u1%0AINFO%20admin_audit%20access%3Dwrite%20route%3Dtenant.kick_all"
        );
        assert_eq!(AuditTarget::id("a\r\nb").as_str(), "a%0D%0Ab");
        assert_eq!(
            AuditTarget::id("\0\t\u{1b}[31m\u{7f}").as_str(),
            "%00%09%1B%5B31m%7F"
        );
        // U+2028/U+2029 也被某些日志查看器当成换行；非 ASCII 一律按字节转义。
        assert_eq!(AuditTarget::id("a\u{2028}b").as_str(), "a%E2%80%A8b");
        assert_eq!(AuditTarget::id("用户").as_str(), "%E7%94%A8%E6%88%B7");
        for raw in [forged, "a\r\nb", "\0\t\u{1b}[31m\u{7f}", "\"quoted\" = 1"] {
            let escaped = AuditTarget::id(raw);
            assert!(
                escaped
                    .as_str()
                    .bytes()
                    .all(|b| is_unreserved(b) || b == b'%'),
                "{escaped:?}"
            );
            assert_eq!(decode(escaped.as_str()), raw);
        }
    }

    #[test]
    fn slashes_in_a_segment_cannot_shift_the_pair_boundary() {
        // 解码前这两组都会拼成 `u1/d1/x`。
        let user_slash = AuditTarget::pair("u1/d1", "x");
        let device_slash = AuditTarget::pair("u1", "d1/x");
        assert_eq!(user_slash.as_str(), "u1%2Fd1/x");
        assert_eq!(device_slash.as_str(), "u1/d1%2Fx");
        assert_ne!(user_slash, device_slash);
        // 单段里的 `/` 与 `%` 也被转义，`/` 只作分隔符，`%` 只作转义前缀。
        assert_eq!(AuditTarget::id("a/b%2Fc").as_str(), "a%2Fb%252Fc");
        for (first, second) in [("u1/d1", "x"), ("u1", "d1/x"), ("%2F", "/")] {
            let target = AuditTarget::pair(first, second);
            let (a, b) = target.as_str().split_once('/').unwrap();
            assert!(!b.contains('/'), "{target:?}");
            assert_eq!((decode(a).as_str(), decode(b).as_str()), (first, second));
        }
    }

    #[test]
    fn overlong_input_is_cut_at_a_whole_escape_and_marked() {
        let at_limit = "a".repeat(MAX_SEGMENT_BYTES);
        assert_eq!(AuditTarget::id(&at_limit).as_str(), at_limit);

        let long = "a".repeat(10_000);
        let target = AuditTarget::id(&long);
        assert_eq!(
            target.as_str(),
            format!("{}!truncated:10000", "a".repeat(MAX_SEGMENT_BYTES))
        );

        // 每个字节转义成 3 个字符：256 字节内放得下 85 个完整的 `%XX`，不会切开半个。
        let wide = "\n".repeat(1_000);
        let target = AuditTarget::id(&wide);
        let (body, marker) = target.as_str().split_once('!').unwrap();
        assert_eq!(body, "%0A".repeat(MAX_SEGMENT_BYTES / 3));
        assert_eq!(marker, "truncated:1000");

        // 多字节字符同样按字节截在 `%XX` 边界上，结果仍是 ASCII。
        let target = AuditTarget::id(&"用".repeat(500));
        assert!(target.as_str().is_ascii());
        assert!(target.as_str().ends_with("!truncated:1500"));
        let body = target.as_str().split_once('!').unwrap().0;
        assert_eq!(body.len() % 3, 0);
        assert!(body.len() <= MAX_SEGMENT_BYTES);

        // 复合目标每段各自截断，第二段不会被第一段挤掉。
        let pair = AuditTarget::pair(&long, "d1");
        assert!(pair.as_str().ends_with("!truncated:10000/d1"), "{pair:?}");
        // 正文里的 `!` 一定被转义，截断标记不会与内容混淆。
        assert_eq!(
            AuditTarget::id("x!truncated:3").as_str(),
            "x%21truncated%3A3"
        );
    }
}
