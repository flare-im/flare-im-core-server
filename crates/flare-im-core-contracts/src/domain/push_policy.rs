//! Mobile alerts are optional display, never a substitute for client state synchronization.
use flare_proto::common::{
    AlertClass, Event, EventType, Message, event::Payload, message_content::Content,
};

pub fn message_has_mobile_alert(message: &Message) -> bool {
    match message.content.as_ref().and_then(|c| c.content.as_ref()) {
        Some(Content::Notification(notice)) => {
            !(notice.title.trim().is_empty()
                && notice.body.trim().is_empty()
                && !notice.show_in_list
                && !notice.show_badge
                && !notice.play_sound)
        }
        _ => true,
    }
}

pub fn event_has_mobile_alert(event: &Event) -> bool {
    event.r#type == EventType::EventMessage as i32
        && matches!(&event.payload,Some(Payload::Message(message)) if message_has_mobile_alert(message))
}

/// 一条消息对「没被点名的普通接收者」的提示等级。
///
/// 是否点名因接收者而异，由推送路由在逐个接收者时升到 [`AlertClass::Mention`]；这里不看提及。
/// 通知类内容：要响铃的是普通消息，只计角标或只进列表的静默更新，三者皆无则不提示。
pub fn message_alert_class(message: &Message) -> AlertClass {
    match message.content.as_ref().and_then(|c| c.content.as_ref()) {
        Some(Content::Notification(notice)) => {
            if notice.play_sound {
                AlertClass::Normal
            } else if message_has_mobile_alert(message) {
                AlertClass::Silent
            } else {
                AlertClass::None
            }
        }
        _ => AlertClass::Normal,
    }
}

/// 事件的提示等级：只有承载消息的事件可能提示，其余（撤回、编辑、已读、表情回应等）一律不提示。
pub fn event_alert_class(event: &Event) -> AlertClass {
    if event.r#type != EventType::EventMessage as i32 {
        return AlertClass::None;
    }
    match &event.payload {
        Some(Payload::Message(message)) => message_alert_class(message),
        _ => AlertClass::None,
    }
}

/// 需要网关跟踪送达确认、确认超时要转推送的等级。`None` 不跟踪。
pub fn alert_class_needs_delivery_watch(class: AlertClass) -> bool {
    !matches!(class, AlertClass::Unspecified | AlertClass::None)
}

/// 同一会话的通知在设备上互相覆盖所用的合并键，也用作分组键。
///
/// 厂商对长度有上限（APNs 的 `apns-collapse-id` 最多 64 字节），会话 ID 又可能含不宜出现在
/// 头部里的字符，所以取租户与会话的 SHA-256 前 16 字节的十六进制，固定 34 字节。
/// 客户端核心用同一算法给本地通知取标识，链路与推送两条路径的通知才能互相覆盖。
pub fn conversation_collapse_key(tenant_id: &str, conversation_id: &str) -> String {
    use sha2::{Digest, Sha256};
    use std::fmt::Write as _;

    let mut hasher = Sha256::new();
    hasher.update(tenant_id.as_bytes());
    hasher.update([0u8]);
    hasher.update(conversation_id.as_bytes());
    let digest = hasher.finalize();
    let mut key = String::with_capacity(34);
    key.push_str("c:");
    for byte in &digest[..16] {
        let _ = write!(key, "{byte:02x}");
    }
    key
}

/// 一台设备上摘要通知（「N 个会话有 M 条新消息」）的合并键，也用作分组键。
///
/// 摘要跨会话，一台设备只有一条：取租户、用户与设备的 SHA-256 前 16 字节的十六进制，前缀 `d:`，
/// 固定 34 字节，与会话合并键（`c:`）同样的长度与字符集，永远不会与任何会话的键相同。
pub fn device_digest_collapse_key(tenant_id: &str, user_id: &str, device_id: &str) -> String {
    use sha2::{Digest, Sha256};
    use std::fmt::Write as _;

    let mut hasher = Sha256::new();
    hasher.update(tenant_id.as_bytes());
    hasher.update([0u8]);
    hasher.update(user_id.as_bytes());
    hasher.update([0u8]);
    hasher.update(device_id.as_bytes());
    let digest = hasher.finalize();
    let mut key = String::with_capacity(34);
    key.push_str("d:");
    for byte in &digest[..16] {
        let _ = write!(key, "{byte:02x}");
    }
    key
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_proto::common::{MessageContent, NotificationContent};
    #[test]
    fn sync_facts_keep_their_carrier_but_cannot_become_mobile_alerts() {
        let mut message = Message {
            content: Some(MessageContent {
                content: Some(Content::Notification(NotificationContent::default())),
            }),
            ..Default::default()
        };
        assert!(!message_has_mobile_alert(&message));
        if let Some(Content::Notification(notice)) =
            message.content.as_mut().and_then(|c| c.content.as_mut())
        {
            notice.body = "Visible optional notice".into();
        }
        assert!(message_has_mobile_alert(&message));
        let mut event = Event {
            r#type: EventType::EventReadReceipt as i32,
            payload: Some(Payload::Message(message)),
            ..Default::default()
        };
        assert!(!event_has_mobile_alert(&event));
        event.r#type = EventType::EventMessage as i32;
        assert!(event_has_mobile_alert(&event));
        event.payload = None;
        assert!(!event_has_mobile_alert(&event));
    }

    fn notice_message(notice: NotificationContent) -> Message {
        Message {
            content: Some(MessageContent {
                content: Some(Content::Notification(notice)),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn notification_content_maps_to_normal_silent_or_none() {
        assert_eq!(
            message_alert_class(&Message::default()),
            AlertClass::Normal,
            "非通知类内容一律是普通消息"
        );
        assert_eq!(
            message_alert_class(&notice_message(NotificationContent::default())),
            AlertClass::None
        );
        assert_eq!(
            message_alert_class(&notice_message(NotificationContent {
                show_badge: true,
                ..Default::default()
            })),
            AlertClass::Silent
        );
        assert_eq!(
            message_alert_class(&notice_message(NotificationContent {
                body: "x".into(),
                play_sound: true,
                ..Default::default()
            })),
            AlertClass::Normal
        );
    }

    #[test]
    fn only_message_events_can_alert() {
        let mut event = Event {
            r#type: EventType::EventReadReceipt as i32,
            payload: Some(Payload::Message(Message::default())),
            ..Default::default()
        };
        assert_eq!(event_alert_class(&event), AlertClass::None);
        assert!(!alert_class_needs_delivery_watch(event_alert_class(&event)));
        event.r#type = EventType::EventMessage as i32;
        assert_eq!(event_alert_class(&event), AlertClass::Normal);
        assert!(alert_class_needs_delivery_watch(event_alert_class(&event)));
        event.payload = None;
        assert_eq!(event_alert_class(&event), AlertClass::None);
    }

    #[test]
    fn collapse_key_is_stable_short_and_tenant_scoped() {
        let key = conversation_collapse_key("0", "conv-1");
        assert_eq!(key.len(), 34);
        assert!(key.starts_with("c:"));
        assert!(key[2..].bytes().all(|b| b.is_ascii_hexdigit()));
        assert_eq!(key, conversation_collapse_key("0", "conv-1"));
        assert_ne!(key, conversation_collapse_key("1", "conv-1"));
        assert_ne!(key, conversation_collapse_key("0", "conv-2"));
        // 分隔符参与摘要：("a", "bc") 与 ("ab", "c") 不能撞。
        assert_ne!(
            conversation_collapse_key("a", "bc"),
            conversation_collapse_key("ab", "c")
        );
    }

    #[test]
    fn digest_key_is_per_device_and_never_a_conversation_key() {
        let key = device_digest_collapse_key("0", "u1", "phone");
        assert_eq!(key.len(), 34);
        assert!(key.starts_with("d:"));
        assert!(key[2..].bytes().all(|b| b.is_ascii_hexdigit()));
        assert_eq!(key, device_digest_collapse_key("0", "u1", "phone"));
        assert_ne!(key, device_digest_collapse_key("0", "u1", "pad"));
        assert_ne!(key, device_digest_collapse_key("0", "u2", "phone"));
        assert_ne!(key, device_digest_collapse_key("1", "u1", "phone"));
        assert_ne!(
            device_digest_collapse_key("0", "a", "bc"),
            device_digest_collapse_key("0", "ab", "c")
        );
        assert_ne!(key[2..], conversation_collapse_key("0", "u1")[2..]);
    }
}
