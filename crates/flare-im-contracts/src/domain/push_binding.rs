//! Push binding fence keys. The authority is singleton Redis/Dragonfly, not Redis Cluster.
use sha2::{Digest, Sha256};
pub const PUSH_BINDING_MAX_COMMAND_MS: i64 = 600_000;
pub const PUSH_BINDING_TOMBSTONE_SECS: i64 = 86_400;
/// 绑定多少天没活跃就视为不存在。客户端每次回到前台都会核对一次自己的绑定，
/// 两个月都没核对过的设备多半已经卸载或换机，再往它的令牌上推只是白占厂商配额。
pub const PUSH_BINDING_INACTIVE_DAYS_DEFAULT: u32 = 60;
/// 绑定能声明的能力。`dataMessages`：Android 能收 FCM 数据消息；`serviceExtension`：iOS 带通知扩展。
pub const PUSH_BINDING_CAPABILITIES: [&str; 2] = ["dataMessages", "serviceExtension"];
/// APNs 绑定的环境。开发构建的令牌只在沙盒有效，发错环境 Apple 回 BadDeviceToken。
pub const APNS_ENVIRONMENT_SANDBOX: &str = "sandbox";
pub const APNS_ENVIRONMENT_PRODUCTION: &str = "production";
const DAY_MS: i64 = 86_400_000;

/// 一条绑定最近一次活跃的时间：设备最近核对绑定的时间，没核对过就是登记时间。
pub fn push_binding_last_active_ms(registered_at_ms: i64, last_seen_ms: i64) -> i64 {
    registered_at_ms.max(last_seen_ms)
}

/// 最近活跃早于这个时刻（毫秒）的绑定已经失效。
pub fn push_binding_inactive_cutoff_ms(now_ms: i64, inactive_days: u32) -> i64 {
    now_ms.saturating_sub(i64::from(inactive_days).saturating_mul(DAY_MS))
}

/// 绑定在 `now_ms` 时是否已经失效：最近活跃早于 `inactive_days` 天前。
pub fn push_binding_inactive(last_active_ms: i64, now_ms: i64, inactive_days: u32) -> bool {
    last_active_ms < push_binding_inactive_cutoff_ms(now_ms, inactive_days)
}
pub fn binding_id_valid(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
pub fn binding_hash(values: &[&str]) -> String {
    let mut hash = Sha256::new();
    for value in values {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    format!("{:x}", hash.finalize())
}
pub fn binding_id(secret: &str) -> Option<String> {
    binding_id_valid(secret).then(|| format!("{:x}", Sha256::digest(secret.as_bytes())))
}
pub fn push_binding_slot(prefix: &str, app: &str, provider: &str, device: &str) -> String {
    format!(
        "{}:binding-slot:{}",
        prefix.trim_end_matches(':'),
        binding_hash(&[app, provider, device])
    )
}
pub fn push_binding_command(prefix: &str, id: &str) -> String {
    format!("{}{id}", push_binding_command_prefix(prefix))
}
/// Everything of a command key but the binding id. A script that walks a user's
/// registry learns the ids only inside Redis, so it appends them to this itself.
pub fn push_binding_command_prefix(prefix: &str) -> String {
    format!("{}:binding-command:", prefix.trim_end_matches(':'))
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: i64 = 86_400_000;

    #[test]
    fn a_binding_expires_sixty_days_after_it_was_last_seen_or_registered() {
        let now = 1_800_000_000_000;
        let days = PUSH_BINDING_INACTIVE_DAYS_DEFAULT;
        // 从没核对过：按登记时间算。
        let registered = now - 61 * DAY;
        assert!(push_binding_inactive(
            push_binding_last_active_ms(registered, 0),
            now,
            days
        ));
        // 登记得早，但最近核对过：仍然有效。
        assert!(!push_binding_inactive(
            push_binding_last_active_ms(registered, now - DAY),
            now,
            days
        ));
        // 恰好 60 天不算过期，再早 1 毫秒才算。
        assert!(!push_binding_inactive(now - 60 * DAY, now, days));
        assert!(push_binding_inactive(now - 60 * DAY - 1, now, days));
        assert_eq!(push_binding_inactive_cutoff_ms(now, days), now - 60 * DAY);
    }
}
