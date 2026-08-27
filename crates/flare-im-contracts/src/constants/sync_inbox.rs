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

/// 从 sync 收件箱会话 ID 反解出收件人；不是 sync 收件箱则返回 `None`。
///
/// 收件人就写在 ID 里，**不需要也不能**去会话服务查参与者：
/// sync 收件箱不是真实会话、没有参与者行，查了必然 NOT_FOUND，
/// 消息随之被丢弃（线上实测建 499 人群时有 8 个成员因此收不到系统通知）。
pub fn sync_inbox_recipient(conversation_id: &str) -> Option<&str> {
    let rest = conversation_id
        .strip_prefix(SYNC_INBOX_CONVERSATION_PREFIX)?
        .trim();
    (!rest.is_empty()).then_some(rest)
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

    #[test]
    fn recipient_is_read_back_from_the_id() {
        assert_eq!(sync_inbox_recipient("sync:42"), Some("42"));
        assert_eq!(
            sync_inbox_recipient(&sync_inbox_conversation_id("u-7")),
            Some("u-7")
        );
    }

    #[test]
    fn non_sync_or_empty_ids_have_no_recipient() {
        assert_eq!(sync_inbox_recipient("2AB7E2CE84KN4K377P"), None);
        assert_eq!(sync_inbox_recipient("sync:"), None);
        assert_eq!(sync_inbox_recipient("sync:   "), None);
    }
}
