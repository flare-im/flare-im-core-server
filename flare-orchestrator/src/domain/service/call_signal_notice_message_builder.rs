use flare_proto::common::call_signal_event::Signal;
use flare_proto::common::message_content::Content;
use flare_proto::common::{
    Event, Message, MessageContent, MessageSource, MessageStatus, MessageType, NotificationContent,
    event::Payload as EventPayload,
};
use prost::Message as _;

fn normalize_reason_code(raw: Option<i32>) -> String {
    match raw.unwrap_or_default() {
        1 => "user_hangup".to_string(),
        2 => "rejected".to_string(),
        3 => "cancelled".to_string(),
        4 => "no_answer_timeout".to_string(),
        5 => "busy".to_string(),
        6 => "failed".to_string(),
        _ => String::new(),
    }
}

fn normalize_visibility_scope(raw: Option<i32>) -> String {
    match raw.unwrap_or_default() {
        1 => "all_participants".to_string(),
        2 => "self_only".to_string(),
        _ => String::new(),
    }
}

fn is_machine_reason(reason: &str) -> bool {
    !reason.is_empty()
        && reason
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn infer_mode_from_ext(ext: &std::collections::HashMap<String, String>) -> Option<&'static str> {
    let from_call_mode = ext
        .get("call_mode")
        .or_else(|| ext.get("callMode"))
        .map(|s| s.trim().to_ascii_lowercase());
    if let Some(v) = from_call_mode {
        if v == "video" {
            return Some("video");
        }
        if v == "audio" {
            return Some("audio");
        }
    }
    None
}

fn infer_mode(call: &flare_proto::common::CallSignalEvent, signal: &Signal) -> &'static str {
    if let Some(v) = infer_mode_from_ext(&call.ext) {
        return v;
    }
    match signal {
        Signal::Invite(invite) => {
            let has_video = invite
                .offered_media
                .as_ref()
                .map(|m| m.types.iter().any(|x| *x == 2))
                .unwrap_or(false);
            if has_video { "video" } else { "audio" }
        }
        Signal::Accept(accept) => {
            let has_video = accept
                .accepted_media
                .as_ref()
                .map(|m| m.types.iter().any(|x| *x == 2))
                .unwrap_or(false);
            if has_video { "video" } else { "audio" }
        }
        _ => "audio",
    }
}

fn format_duration(seconds: i32) -> String {
    let safe = seconds.max(0);
    let mm = safe / 60;
    let ss = safe % 60;
    format!("{mm:02}:{ss:02}")
}

/// 将终态 `EVENT_CALL_SIGNAL` 转为可落库可同步的通知消息。
///
/// 仅转换用户可感知结果态：
/// - reject / busy
/// - hangup（取消、时长结束、异常中断等）
///
/// 协商过程（invite/accept/ringing/ice/...）返回 `None`。
pub fn build_call_signal_notice_message(event: &Event) -> Option<Message> {
    let call = match event.payload.as_ref()? {
        EventPayload::CallSignal(call) => call,
        _ => return None,
    };
    let signal = call.signal.as_ref()?;
    let mode = infer_mode(call, signal).to_string();
    let mut data = std::collections::HashMap::new();
    let (variant, body) = match signal {
        Signal::Invite(_) | Signal::Accept(_) => return None,
        Signal::Reject(_) => ("reject", "已拒绝".to_string()),
        Signal::Busy(_) => ("busy", "忙线中".to_string()),
        Signal::Hangup(h) => {
            let reason_code = normalize_reason_code(h.reason_code);
            let visibility_scope = normalize_visibility_scope(h.visibility_scope);
            let timeout_seconds = h.timeout_seconds.unwrap_or_default();
            let duration_seconds = h.duration_seconds.unwrap_or_default();
            if !reason_code.is_empty() {
                data.insert("reasonCode".to_string(), reason_code.clone());
            }
            if !visibility_scope.is_empty() {
                data.insert("visibilityScope".to_string(), visibility_scope.clone());
            }
            if timeout_seconds > 0 {
                data.insert("timeoutSeconds".to_string(), timeout_seconds.to_string());
            }
            if duration_seconds > 0 {
                data.insert("durationSeconds".to_string(), duration_seconds.to_string());
                data.insert(
                    "durationText".to_string(),
                    format_duration(duration_seconds),
                );
            }

            let body = if reason_code == "no_answer_timeout" && visibility_scope == "self_only" {
                let secs = if timeout_seconds > 0 {
                    timeout_seconds
                } else {
                    60
                };
                format!("对方无应答（{secs}s，仅自己可见）")
            } else if reason_code == "cancelled" {
                "通话已取消".to_string()
            } else if reason_code == "rejected" {
                "通话已拒绝".to_string()
            } else if reason_code == "busy" {
                "对端忙线".to_string()
            } else if reason_code == "failed" {
                "通话异常中断".to_string()
            } else {
                let reason = h.reason.trim();
                if reason.is_empty() || is_machine_reason(reason) {
                    "通话已结束".to_string()
                } else {
                    format!("通话结束：{reason}")
                }
            };
            ("hangup", body)
        }
        _ => return None,
    };
    data.insert("variant".to_string(), variant.to_string());
    data.insert("mode".to_string(), mode);
    data.insert("callId".to_string(), call.call_id.clone());

    let seq = if event.event_seq.unwrap_or_default() > 0 {
        event.event_seq.unwrap_or_default()
    } else {
        event.seq
    };
    let event_key = if event.event_id.trim().is_empty() {
        format!("{}-{}-{}", event.conversation_id, seq, call.call_id)
    } else {
        event.event_id.clone()
    };
    let msg_id = format!("call-signal-notice:{event_key}");

    let content = MessageContent {
        content: Some(Content::Notification(NotificationContent {
            title: "通话".to_string(),
            body,
            notification_type: "call_signal".to_string(),
            data,
            target_user_ids: Vec::new(),
            target_role_id: String::new(),
            notify_all: false,
            persistent: true,
            show_in_list: true,
            show_badge: false,
            play_sound: false,
        })),
    };

    let mut message = Message {
        server_id: msg_id.clone(),
        conversation_id: event.conversation_id.clone(),
        client_msg_id: msg_id,
        sender_id: call.from_user_id.clone(),
        source: MessageSource::System as i32,
        seq,
        timestamp: event.created_at.clone(),
        conversation_type: 0,
        message_type: MessageType::Notification as i32,
        channel_id: String::new(),
        sender_name: String::new(),
        sender_avatar: String::new(),
        content: content.encode_to_vec(),
        status: MessageStatus::Sent as i32,
        offline_push_info: None,
        extra: std::collections::HashMap::new(),
        extensions: std::collections::HashMap::new(),
    };
    message
        .extra
        .insert("message_type".to_string(), "notification".to_string());
    message
        .extra
        .insert("notification_type".to_string(), "call_signal".to_string());
    if !event.event_id.trim().is_empty() {
        message
            .extra
            .insert("event_id".to_string(), event.event_id.clone());
    }
    Some(message)
}

#[cfg(test)]
mod tests {
    use super::build_call_signal_notice_message;
    use flare_proto::common::call_signal_event::Signal;
    use flare_proto::common::{
        CallHangup, CallReject, CallSignalEvent, Event, EventType, NotificationContent,
        event::Payload as EventPayload, message_content::Content as MessageContentInner,
    };
    use prost::Message as _;

    fn make_event(signal: Signal) -> Event {
        Event {
            conversation_id: "c1".to_string(),
            seq: 9,
            r#type: EventType::EventCallSignal as i32,
            event_id: "evt-1".to_string(),
            payload: Some(EventPayload::CallSignal(CallSignalEvent {
                call_id: "call-1".to_string(),
                conversation_id: "c1".to_string(),
                from_user_id: "u2".to_string(),
                signal: Some(signal),
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    fn parse_notification(msg: &flare_proto::common::Message) -> NotificationContent {
        let content = flare_proto::common::MessageContent::decode(msg.content.as_slice())
            .expect("decode message content");
        match content.content {
            Some(MessageContentInner::Notification(n)) => n,
            _ => panic!("expect notification content"),
        }
    }

    #[test]
    fn invite_should_not_build_notice_message() {
        let ev = make_event(Signal::Invite(Default::default()));
        assert!(build_call_signal_notice_message(&ev).is_none());
    }

    #[test]
    fn reject_should_build_notice_message() {
        let ev = make_event(Signal::Reject(CallReject {
            reason: "reject".to_string(),
            code: 0,
        }));
        let msg = build_call_signal_notice_message(&ev).expect("reject should build");
        assert_eq!(
            msg.message_type,
            flare_proto::common::MessageType::Notification as i32
        );
        assert_eq!(
            msg.extra.get("notification_type").map(String::as_str),
            Some("call_signal")
        );
    }

    #[test]
    fn hangup_with_duration_should_include_duration_text() {
        let ev = make_event(Signal::Hangup(CallHangup {
            reason: "normal".to_string(),
            duration_seconds: Some(125),
            reason_code: Some(1),
            ..Default::default()
        }));
        let msg = build_call_signal_notice_message(&ev).expect("hangup should build");
        let n = parse_notification(&msg);
        assert_eq!(n.notification_type, "call_signal");
        assert_eq!(
            n.data.get("durationText").map(String::as_str),
            Some("02:05")
        );
    }

    #[test]
    fn hangup_reason_and_visibility_rules_should_match_product_expectation() {
        let cases = vec![
            (
                "failed_reason_code_should_show_abnormal_break",
                CallHangup {
                    reason_code: Some(6),
                    reason: String::new(),
                    ..Default::default()
                },
                "通话异常中断",
            ),
            (
                "self_only_no_answer_should_show_timeout_hint",
                CallHangup {
                    reason_code: Some(4),
                    visibility_scope: Some(2),
                    timeout_seconds: Some(45),
                    ..Default::default()
                },
                "对方无应答（45s，仅自己可见）",
            ),
            (
                "human_reason_should_be_exposed",
                CallHangup {
                    reason_code: None,
                    reason: "网络切换导致断开".to_string(),
                    ..Default::default()
                },
                "通话结束：网络切换导致断开",
            ),
            (
                "machine_reason_should_be_hidden",
                CallHangup {
                    reason_code: None,
                    reason: "user_hangup".to_string(),
                    ..Default::default()
                },
                "通话已结束",
            ),
        ];

        for (name, hangup, expected_body) in cases {
            let ev = make_event(Signal::Hangup(hangup));
            let msg = build_call_signal_notice_message(&ev).expect(name);
            let n = parse_notification(&msg);
            assert_eq!(n.body, expected_body, "{name}");
            assert_eq!(
                n.data.get("variant").map(String::as_str),
                Some("hangup"),
                "{name}"
            );
        }
    }

    #[test]
    fn hangup_with_call_mode_ext_should_keep_video_mode() {
        let mut ev = make_event(Signal::Hangup(CallHangup {
            reason_code: Some(1),
            duration_seconds: Some(10),
            ..Default::default()
        }));
        if let Some(EventPayload::CallSignal(call)) = ev.payload.as_mut() {
            call.ext
                .insert("call_mode".to_string(), "video".to_string());
        }
        let msg = build_call_signal_notice_message(&ev).expect("hangup should build");
        let n = parse_notification(&msg);
        assert_eq!(n.data.get("mode").map(String::as_str), Some("video"));
    }
}
