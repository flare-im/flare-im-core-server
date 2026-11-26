//! 客户端上报的应用版本作指标标签。
//!
//! 应用版本来自客户端（推送绑定登记、到达回执），取值不受服务端控制。直接当标签用，
//! 一个乱报版本的客户端就能把时序基数撑大。这里两道关：
//!
//! - 形状：至多 [`APP_VERSION_MAX_LEN`] 个 `[0-9A-Za-z._+-]` 字符（去掉首尾空白），
//!   其余（含空）记为 [`OTHER_APP_VERSION_LABEL`]。决策链等非指标字段只用这一道
//!   （[`sanitized_app_version`]）；
//! - 个数：每个进程最多记住 [`MAX_APP_VERSION_LABELS`] 个不同的取值，先到先得，之后的新取值
//!   一律记为 `other`（[`app_version_label`]）。进程重启后重新计。Push Worker 的
//!   `flare_push_vendor_sent_total` 与 Push Proxy 的 `flare_push_receipts_total` 共用这一份规则，
//!   到达率按版本相除时两边的标签对得上；超出上限的版本两边都落进 `other`。

use std::collections::HashSet;
use std::sync::{Mutex, PoisonError};

use once_cell::sync::Lazy;

/// 应用版本最长多少个字符。
pub const APP_VERSION_MAX_LEN: usize = 32;

/// 不合形状或超出个数上限的应用版本的标签取值。
pub const OTHER_APP_VERSION_LABEL: &str = "other";

/// 每个进程单独列出的应用版本个数上限。
pub const MAX_APP_VERSION_LABELS: usize = 64;

/// 形状合法的应用版本（去掉首尾空白），否则 `other`。不限个数：给决策链这类不进时序库的字段用。
pub fn sanitized_app_version(raw: &str) -> &str {
    let version = raw.trim();
    let valid = !version.is_empty()
        && version.len() <= APP_VERSION_MAX_LEN
        && version
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'+' | b'-'));
    if valid {
        version
    } else {
        OTHER_APP_VERSION_LABEL
    }
}

/// 有上限的应用版本标签集合：记住先出现的 `capacity` 个取值。
#[derive(Debug)]
pub struct AppVersionLabels {
    seen: Mutex<HashSet<String>>,
    capacity: usize,
}

impl AppVersionLabels {
    pub fn new(capacity: usize) -> Self {
        Self {
            seen: Mutex::new(HashSet::new()),
            capacity,
        }
    }

    /// `raw` 作标签的取值：形状不合或集合已满且没见过的，是 `other`。
    pub fn label<'a>(&self, raw: &'a str) -> &'a str {
        let version = sanitized_app_version(raw);
        if version == OTHER_APP_VERSION_LABEL {
            return OTHER_APP_VERSION_LABEL;
        }
        let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        if seen.contains(version) {
            return version;
        }
        if seen.len() >= self.capacity {
            return OTHER_APP_VERSION_LABEL;
        }
        seen.insert(version.to_string());
        version
    }
}

static PROCESS_LABELS: Lazy<AppVersionLabels> =
    Lazy::new(|| AppVersionLabels::new(MAX_APP_VERSION_LABELS));

/// 应用版本作指标标签的取值：形状合法、且是本进程先出现的 [`MAX_APP_VERSION_LABELS`] 个之一，
/// 否则 `other`。
pub fn app_version_label(raw: &str) -> &str {
    PROCESS_LABELS.label(raw)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_outside_the_label_alphabet_are_other() {
        assert_eq!(sanitized_app_version(" 1.4.0 "), "1.4.0");
        assert_eq!(
            sanitized_app_version("2.0.0-rc.1+build_7"),
            "2.0.0-rc.1+build_7"
        );
        let too_long = "9".repeat(APP_VERSION_MAX_LEN + 1);
        for bad in ["", "  ", "1.4 beta", "版本", too_long.as_str(), "1.4/0"] {
            assert_eq!(
                sanitized_app_version(bad),
                OTHER_APP_VERSION_LABEL,
                "{bad:?}"
            );
        }
    }

    #[test]
    fn only_the_first_values_up_to_the_capacity_get_a_label_of_their_own() {
        let labels = AppVersionLabels::new(2);
        assert_eq!(labels.label("1.0.0"), "1.0.0");
        assert_eq!(labels.label(" 1.1.0 "), "1.1.0");
        // 集合满了：新取值记为 other，见过的照旧。
        assert_eq!(labels.label("1.2.0"), OTHER_APP_VERSION_LABEL);
        assert_eq!(labels.label("1.0.0"), "1.0.0");
        assert_eq!(labels.label("1.1.0"), "1.1.0");
        // 形状不合的取值不占集合里的位置。
        let labels = AppVersionLabels::new(1);
        assert_eq!(labels.label("1.4 beta"), OTHER_APP_VERSION_LABEL);
        assert_eq!(labels.label("2.0.0"), "2.0.0");
    }

    #[test]
    fn the_process_set_is_bounded() {
        for index in 0..(MAX_APP_VERSION_LABELS * 2) {
            let version = format!("0.0.{index}-bounded");
            let label = app_version_label(&version);
            assert!(label == version || label == OTHER_APP_VERSION_LABEL);
        }
        let seen = PROCESS_LABELS
            .seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len();
        assert!(seen <= MAX_APP_VERSION_LABELS, "{seen}");
    }
}
