//! 用户 sync 收件箱：给「还没有会话」的业务通知用的投递通道。
//!
//! 好友申请这类通知发生在双方尚未成为好友、单聊会话还不存在的时候。发进真实单聊会话
//! 会占用该会话的 seq 却不落库（非持久化通知），制造 seq 空洞并破坏收件人时间线物化；
//! 所以业务侧改投 `sync:<user_id>`——ingest 对它跳过 ensure、不占真实会话 seq，
//! 客户端按前缀过滤，不污染会话列表。
//!
//! 前缀此前被分别硬编码在 ingest 与 sync-orchestrator 里，网关又漏了订阅，
//! 结果这类通知没有订阅者、被静默丢弃。统一收在这里，避免再写第四遍。

pub const SYNC_INBOX_CONVERSATION_PREFIX: &str = "sync:";

/// 某用户的 sync 收件箱会话 ID。
pub fn sync_inbox_conversation_id(user_id: &str) -> String {
    format!("{SYNC_INBOX_CONVERSATION_PREFIX}{}", user_id.trim())
}

/// 是否为 sync 收件箱会话（非真实会话，不该出现在会话列表 / 不该占 seq）。
pub fn is_sync_inbox_conversation_id(conversation_id: &str) -> bool {
    conversation_id.starts_with(SYNC_INBOX_CONVERSATION_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_carries_the_prefix_and_is_recognized() {
        let id = sync_inbox_conversation_id("42");
        assert_eq!(id, "sync:42");
        assert!(is_sync_inbox_conversation_id(&id));
    }

    #[test]
    fn real_conversation_ids_are_not_sync_inboxes() {
        assert!(!is_sync_inbox_conversation_id("2AB7E2CE84KN4K377P"));
    }
}
