//! Mobile alerts are optional display, never a substitute for client state synchronization.
use flare_proto::common::{Event, EventType, Message, event::Payload, message_content::Content};

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
}
