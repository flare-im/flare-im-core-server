//! 单聊会话 ID，与 flare-core `generate_single_chat_conversation_id` 相同：
//! SHA256("DM:v1:{min}:{max}") 取前 10 字节 → Crockford Base32 → 加前缀 "1A"。

use sha2::{Digest, Sha256};

const CROCKFORD: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

pub fn single_chat_conversation_id(first: &str, second: &str) -> String {
    let (low, high) = if first <= second {
        (first, second)
    } else {
        (second, first)
    };
    let digest = Sha256::digest(format!("DM:v1:{low}:{high}").as_bytes());
    let mut bits: u128 = 0;
    for byte in &digest[..10] {
        bits = (bits << 8) | u128::from(*byte);
    }
    let mut id = String::with_capacity(18);
    id.push_str("1A");
    for shift in (0..=75).rev().step_by(5) {
        id.push(char::from(CROCKFORD[((bits >> shift) & 31) as usize]));
    }
    id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_the_reference_used_by_the_http_smoke() {
        // 与 scripts/test_smoke_message_flow.py 的 reference_cid 计算结果一致。
        assert_eq!(
            single_chat_conversation_id("10001", "10002"),
            "1AS05Q2E4KQHG1W05P"
        );
        assert_eq!(
            single_chat_conversation_id("10002", "10001"),
            "1AS05Q2E4KQHG1W05P"
        );
        assert_eq!(
            single_chat_conversation_id("smoke-user-a", "smoke-user-b"),
            "1ARP7W7B43JV8QJ0TF"
        );
        assert_eq!(
            single_chat_conversation_id("7312345678901234568", "7312345678901234567"),
            "1AVTWR148CV2CDZSNB"
        );
    }
}
