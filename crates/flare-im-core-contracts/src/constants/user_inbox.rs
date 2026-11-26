//! 用户收件箱（user inbox）：每个用户一条的投递通道，承载「还没有会话」的通知。
//!
//! 有些通知发生时，收件人与发送方之间还不存在任何会话。若发进一个真实会话，会占用该
//! 会话的 seq 却不落库（非持久化通知），制造 seq 空洞并破坏收件人时间线物化；所以这类
//! 通知改投 `inbox:<user_id>`——ingest 对它跳过 ensure、不占真实会话 seq，网关在连接
//! 建立时就为用户订阅自己的收件箱，客户端按前缀过滤，不进会话列表。
//!
//! 前缀与解析只在这里定义一次，ingest、sync-orchestrator、网关与收件人解析共用。

pub const USER_INBOX_CONVERSATION_PREFIX: &str = "inbox:";

/// 某用户的收件箱会话 ID。
pub fn user_inbox_conversation_id(user_id: &str) -> String {
    format!("{USER_INBOX_CONVERSATION_PREFIX}{}", user_id.trim())
}

/// 是否为用户收件箱（非真实会话，不该出现在会话列表 / 不该占 seq）。
pub fn is_user_inbox_conversation_id(conversation_id: &str) -> bool {
    conversation_id.starts_with(USER_INBOX_CONVERSATION_PREFIX)
}

/// 从用户收件箱 ID 反解出收件人；不是用户收件箱则返回 `None`。
///
/// 收件人就写在 ID 里，**不需要也不能**去会话服务查参与者：
/// 收件箱不是真实会话、没有参与者行，查了必然 NOT_FOUND，消息随之被丢弃。
pub fn user_inbox_recipient(conversation_id: &str) -> Option<&str> {
    let rest = conversation_id
        .strip_prefix(USER_INBOX_CONVERSATION_PREFIX)?
        .trim();
    (!rest.is_empty()).then_some(rest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_carries_the_prefix_and_is_recognized() {
        let id = user_inbox_conversation_id("42");
        assert_eq!(id, "inbox:42");
        assert!(is_user_inbox_conversation_id(&id));
    }

    #[test]
    fn real_conversation_ids_are_not_user_inboxes() {
        assert!(!is_user_inbox_conversation_id("2AB7E2CE84KN4K377P"));
    }

    #[test]
    fn recipient_is_read_back_from_the_id() {
        assert_eq!(user_inbox_recipient("inbox:42"), Some("42"));
        assert_eq!(
            user_inbox_recipient(&user_inbox_conversation_id("u-7")),
            Some("u-7")
        );
    }

    #[test]
    fn non_inbox_or_empty_ids_have_no_recipient() {
        assert_eq!(user_inbox_recipient("2AB7E2CE84KN4K377P"), None);
        assert_eq!(user_inbox_recipient("inbox:"), None);
        assert_eq!(user_inbox_recipient("inbox:   "), None);
    }
}
