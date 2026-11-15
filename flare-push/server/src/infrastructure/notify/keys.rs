//! 推送 Redis 里闸门的键，以及到期队列、展示索引成员的编码。
//!
//! 用户与设备标识可能含冒号，键里的每一段都按「字节数:值」分帧，("a:b", "c") 与 ("a", "b:c")
//! 不会落到同一个键上。会话只放 SHA-256 摘要（与会话合并键同一个算法），键长与会话标识无关。
//!
//! | 键 | 值 | 保留 |
//! |---|---|---|
//! | `{prefix}:dev:{设备}` | 设备预算：规则的 169 字节 + JSON 尾巴 | 1 天 |
//! | `{prefix}:slot:{设备}:{会话摘要}` | 会话槽位：规则的 58 字节 + JSON 尾巴 | 槽位空闲过期 + 10 分钟 |
//! | `{prefix}:delay:{设备}:{会话摘要}` | 推迟的提示（hash） | 推迟时长 + 10 分钟 |
//! | `{prefix}:badge:{设备}` | 角标表（hash：`b` 基线、`s` 贡献之和、`m` 采用的基线时刻、`c:{会话摘要}` 各会话贡献） | 7 天 |
//! | `{prefix}:shown:{租户}:{会话摘要}:{消息摘要}` | 展示过这条消息的设备（set，成员是分帧的用户与设备） | 同槽位 |
//! | `{prefix}:wave:{租户}:{会话摘要}` | 会话波次（hash：`v` 版本，`s` 状态 JSON，`m` 最新一条消息的正文） | @所有人 配额间隔与最长波次间隔中较长的 + 10 分钟 |
//! | `{prefix}:due:{分片}` | 到期队列（zset，分数是到期毫秒，成员见 [`due_member`] 与 [`wave_due_member`]） | 不过期 |
//!
//! 「设备」一段是分帧的租户、用户、设备标识；「租户」一段是分帧的租户标识。

use std::fmt::Write as _;

use flare_im_contracts::domain::push_policy::conversation_collapse_key;

use crate::domain::model::{DueKind, GateDevice, GateSlotKey, WaveKey};

/// 键前缀的缺省值。
pub const DEFAULT_KEY_PREFIX: &str = "push:gate";

/// 到期队列的缺省分片数。
pub const DEFAULT_DUE_SHARDS: u32 = 16;

fn frame(out: &mut String, value: &str) {
    let _ = write!(out, "{}:{}", value.len(), value);
}

/// 读出一段分帧的值，并吃掉其后的分隔冒号。
fn take_frame<'a>(input: &mut &'a str) -> Option<&'a str> {
    let (len, rest) = input.split_once(':')?;
    let len: usize = len.parse().ok()?;
    if rest.len() < len || !rest.is_char_boundary(len) {
        return None;
    }
    let (value, rest) = rest.split_at(len);
    *input = rest.strip_prefix(':').unwrap_or(rest);
    Some(value)
}

/// 会话一段：分帧的租户与会话。
fn wave_scope(key: &WaveKey) -> String {
    let mut out = String::with_capacity(key.tenant_id.len() + key.conversation_id.len() + 8);
    frame(&mut out, &key.tenant_id);
    out.push(':');
    frame(&mut out, &key.conversation_id);
    out
}

/// 设备一段：分帧的租户、用户、设备。
fn device_scope(device: &GateDevice) -> String {
    let mut out = String::with_capacity(
        device.tenant_id.len() + device.user_id.len() + device.device_id.len() + 12,
    );
    frame(&mut out, &device.tenant_id);
    out.push(':');
    frame(&mut out, &device.user_id);
    out.push(':');
    frame(&mut out, &device.device_id);
    out
}

/// 会话摘要：会话合并键去掉 `c:` 的 32 个十六进制字符。
pub fn conversation_hash(tenant_id: &str, conversation_id: &str) -> String {
    conversation_collapse_key(tenant_id, conversation_id)[2..].to_string()
}

/// 消息摘要：SHA-256 前 8 字节的十六进制。撞车只会让撤回时多核对一个槽位。
fn message_hash(message_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(message_id.as_bytes());
    let mut out = String::with_capacity(16);
    for byte in &digest[..8] {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// FNV-1a 32 位：到期队列的分片只需在所有实例之间一致。
fn fnv1a(bytes: &[u8]) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for byte in bytes {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

/// 闸门在推送 Redis 里的键。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateKeys {
    prefix: String,
    due_shards: u32,
}

impl GateKeys {
    pub fn new(prefix: impl Into<String>, due_shards: u32) -> Self {
        let prefix = prefix.into();
        let prefix = prefix.trim().trim_end_matches(':');
        Self {
            prefix: if prefix.is_empty() {
                DEFAULT_KEY_PREFIX.to_string()
            } else {
                prefix.to_string()
            },
            due_shards: due_shards.max(1),
        }
    }

    pub fn device(&self, device: &GateDevice) -> String {
        format!("{}:dev:{}", self.prefix, device_scope(device))
    }

    pub fn slot(&self, key: &GateSlotKey) -> String {
        format!(
            "{}:slot:{}:{}",
            self.prefix,
            device_scope(&key.device),
            conversation_hash(&key.device.tenant_id, &key.conversation_id)
        )
    }

    pub fn delay(&self, key: &GateSlotKey) -> String {
        format!(
            "{}:delay:{}:{}",
            self.prefix,
            device_scope(&key.device),
            conversation_hash(&key.device.tenant_id, &key.conversation_id)
        )
    }

    pub fn badge(&self, device: &GateDevice) -> String {
        format!("{}:badge:{}", self.prefix, device_scope(device))
    }

    pub fn shown(&self, tenant_id: &str, conversation_id: &str, message_id: &str) -> String {
        let mut tenant = String::new();
        frame(&mut tenant, tenant_id);
        format!(
            "{}:shown:{}:{}:{}",
            self.prefix,
            tenant,
            conversation_hash(tenant_id, conversation_id),
            message_hash(message_id)
        )
    }

    pub fn wave(&self, key: &WaveKey) -> String {
        let mut tenant = String::new();
        frame(&mut tenant, &key.tenant_id);
        format!(
            "{}:wave:{}:{}",
            self.prefix,
            tenant,
            conversation_hash(&key.tenant_id, &key.conversation_id)
        )
    }

    /// 这台设备的到期条目落在哪个分片。
    pub fn due_shard(&self, device: &GateDevice) -> String {
        let shard = fnv1a(device_scope(device).as_bytes()) % self.due_shards;
        format!("{}:due:{}", self.prefix, shard)
    }

    /// 这个会话的波次到期条目落在哪个分片。
    pub fn wave_due_shard(&self, key: &WaveKey) -> String {
        let shard = fnv1a(wave_scope(key).as_bytes()) % self.due_shards;
        format!("{}:due:{}", self.prefix, shard)
    }

    /// 全部分片，到期循环逐个领取。
    pub fn due_shard_keys(&self) -> Vec<String> {
        (0..self.due_shards)
            .map(|shard| format!("{}:due:{}", self.prefix, shard))
            .collect()
    }
}

/// 角标表里一个会话的贡献字段。
pub fn badge_field(tenant_id: &str, conversation_id: &str) -> String {
    format!("c:{}", conversation_hash(tenant_id, conversation_id))
}

/// 展示索引的成员：分帧的用户与设备（租户在键里）。
pub fn shown_member(device: &GateDevice) -> String {
    let mut out = String::new();
    frame(&mut out, &device.user_id);
    out.push(':');
    frame(&mut out, &device.device_id);
    out
}

pub fn parse_shown_member(tenant_id: &str, member: &str) -> Option<GateDevice> {
    let mut rest = member;
    let user_id = take_frame(&mut rest)?;
    let device_id = take_frame(&mut rest)?;
    rest.is_empty()
        .then(|| GateDevice::new(tenant_id, user_id, device_id))
}

/// 到期队列的成员：`t:`（尾部刷新）或 `d:`（推迟的提示），接分帧的租户、用户、设备、会话。
/// 同一个（种类，槽位）只有一个成员，重复登记只改分数。
pub fn due_member(kind: DueKind, key: &GateSlotKey) -> String {
    let mut out = String::from(match kind {
        DueKind::Tail => "t:",
        DueKind::Delay => "d:",
    });
    out.push_str(&device_scope(&key.device));
    out.push(':');
    frame(&mut out, &key.conversation_id);
    out
}

pub fn parse_due_member(member: &str) -> Option<(DueKind, GateSlotKey)> {
    let (kind, mut rest) = member.split_once(':')?;
    let kind = match kind {
        "t" => DueKind::Tail,
        "d" => DueKind::Delay,
        _ => return None,
    };
    let tenant_id = take_frame(&mut rest)?;
    let user_id = take_frame(&mut rest)?;
    let device_id = take_frame(&mut rest)?;
    let conversation_id = take_frame(&mut rest)?;
    if !rest.is_empty() {
        return None;
    }
    Some((
        kind,
        GateSlotKey::new(
            GateDevice::new(tenant_id, user_id, device_id),
            conversation_id,
        ),
    ))
}

/// 到期队列里会话波次的成员：`w:` 接分帧的租户与会话。同一个会话只有一个成员，重复登记只改分数。
pub fn wave_due_member(key: &WaveKey) -> String {
    let mut out = String::from("w:");
    out.push_str(&wave_scope(key));
    out
}

pub fn parse_wave_due_member(member: &str) -> Option<WaveKey> {
    let mut rest = member.strip_prefix("w:")?;
    let tenant_id = take_frame(&mut rest)?;
    let conversation_id = take_frame(&mut rest)?;
    rest.is_empty()
        .then(|| WaveKey::new(tenant_id, conversation_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(tenant: &str, user: &str, device: &str, conversation: &str) -> GateSlotKey {
        GateSlotKey::new(GateDevice::new(tenant, user, device), conversation)
    }

    #[test]
    fn due_members_round_trip_even_with_colons() {
        for kind in [DueKind::Tail, DueKind::Delay] {
            for key in [
                key("t1", "u1", "phone", "conv-1"),
                key("t1", "u:1", "pad:with:colons", "c:2"),
                key("t1", "用户", "设备", ""),
            ] {
                let member = due_member(kind, &key);
                assert_eq!(parse_due_member(&member), Some((kind, key)), "{member}");
            }
        }
        for broken in [
            "",
            "x:1:a",
            "t:1:a",
            "t:9:a:1:b:1:c:1:d",
            "t:1:a:1:b:1:c:1:dX",
        ] {
            assert_eq!(parse_due_member(broken), None, "{broken:?}");
        }
    }

    #[test]
    fn framing_keeps_neighbouring_ids_apart() {
        let keys = GateKeys::new("push:gate", 16);
        assert_ne!(
            keys.device(&GateDevice::new("t", "a:b", "c")),
            keys.device(&GateDevice::new("t", "a", "b:c"))
        );
        let device = GateDevice::new("t1", "u:1", "phone");
        assert_eq!(
            parse_shown_member("t1", &shown_member(&device)),
            Some(device.clone())
        );
        assert!(
            keys.slot(&key("t1", "u1", "phone", "conv"))
                .starts_with("push:gate:slot:2:t1:2:u1:5:phone:")
        );
        assert_eq!(
            keys.slot(&key("t1", "u1", "phone", "conv")).len(),
            "push:gate:slot:2:t1:2:u1:5:phone:".len() + 32
        );
    }

    /// 波次成员与槽位成员共用一个到期队列，各自只认自己的前缀。
    #[test]
    fn wave_members_round_trip_and_never_parse_as_slot_members() {
        let keys = GateKeys::new("push:gate", 16);
        for key in [
            WaveKey::new("t1", "g-1"),
            WaveKey::new("t:1", "group:with:colons"),
            WaveKey::new("t1", ""),
        ] {
            let member = wave_due_member(&key);
            assert_eq!(
                parse_wave_due_member(&member),
                Some(key.clone()),
                "{member}"
            );
            assert_eq!(parse_due_member(&member), None, "{member}");
            assert!(keys.due_shard_keys().contains(&keys.wave_due_shard(&key)));
        }
        let slot = due_member(DueKind::Tail, &key("t1", "u1", "phone", "g-1"));
        assert_eq!(parse_wave_due_member(&slot), None);
        for broken in ["w:", "w:2:t1", "w:2:t1:3:g-1X", "x:2:t1:3:g-1"] {
            assert_eq!(parse_wave_due_member(broken), None, "{broken:?}");
        }
        assert!(
            keys.wave(&WaveKey::new("t1", "g-1"))
                .starts_with("push:gate:wave:2:t1:")
        );
        assert_ne!(
            keys.wave(&WaveKey::new("t1", "g-1")),
            keys.wave(&WaveKey::new("t2", "g-1"))
        );
    }

    #[test]
    fn a_device_always_lands_on_the_same_listed_shard() {
        let keys = GateKeys::new("push:gate:", 4);
        let device = GateDevice::new("t1", "u1", "phone");
        let shard = keys.due_shard(&device);
        assert_eq!(shard, keys.due_shard(&device));
        assert!(keys.due_shard_keys().contains(&shard));
        assert_eq!(keys.due_shard_keys().len(), 4);
        assert_eq!(
            GateKeys::new("", 0).due_shard_keys(),
            vec!["push:gate:due:0"]
        );
    }
}
