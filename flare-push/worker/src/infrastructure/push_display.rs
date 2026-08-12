//! 厂商无关的推送展示内容解码。
//!
//! 把 `PushTaskEnvelope` 解成「标题 + 正文」是所有厂商通道共用的一步。
//! 它此前私有在个推实现里，新增 FCM/APNs 时若各抄一份，三家对
//! 「消息被撤回/内容不可见时该显示什么」的判断就会各自漂移 ——
//! 而那正是最容易泄露内容的地方。

use flare_grpc_proto::access_gateway::{
    PushEventRequest, PushMessageRequest, PushNotificationRequest,
};
use flare_proto::PushTaskEnvelope;
use flare_proto::common::event::Payload as EventPayload;
use flare_proto::common::{ContentVisibility, PushTaskPayloadKind};
use prost::Message as _;

// E2EE 占位符判定与个推实现共用：内容加密时不能把密文当正文推出去。
use crate::infrastructure::getui_push::message_has_e2ee_placeholder;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushDisplay {
    pub title: String,
    pub body: String,
}

pub fn notification_display(envelope: &PushTaskEnvelope) -> PushDisplay {
    match PushTaskPayloadKind::try_from(envelope.payload_kind)
        .unwrap_or(PushTaskPayloadKind::Unspecified)
    {
        PushTaskPayloadKind::Notification => {
            PushNotificationRequest::decode(envelope.push_payload.as_slice())
                .ok()
                .and_then(|request| request.notification)
                .map(|notification| PushDisplay {
                    title: non_empty_or(notification.title, "Flare IM"),
                    body: non_empty_or(notification.body, "你收到一条通知"),
                })
                .unwrap_or_else(default_notification_display)
        }
        PushTaskPayloadKind::Message => {
            PushMessageRequest::decode(envelope.push_payload.as_slice())
                .ok()
                .and_then(|request| request.messages.into_iter().next())
                .map(|message| {
                    if message_requires_generic_push_display(&message) {
                        return default_message_display();
                    }
                    message
                        .offline_push_info
                        .map(|offline| PushDisplay {
                            title: non_empty_or(offline.title, "Flare IM"),
                            body: non_empty_or(offline.body, "你收到一条新消息"),
                        })
                        .unwrap_or_else(default_message_display)
                })
                .unwrap_or_else(default_message_display)
        }
        // 内联事件（群消息默认走这条）里带着整条消息，能渲染出和单聊一样的真实文案。
        // 落到通用文案只该发生在拿不到消息时——每条群消息都显示「你收到一条新消息」
        // 不是隐私保护，是信息缺失。真正需要遮蔽的情况由下面同一个判定负责。
        PushTaskPayloadKind::Event => PushEventRequest::decode(envelope.push_payload.as_slice())
            .ok()
            .and_then(|request| {
                request
                    .events
                    .into_iter()
                    .find_map(|event| match event.payload {
                        Some(EventPayload::Message(message)) => Some(message),
                        _ => None,
                    })
            })
            .map(|message| {
                if message_requires_generic_push_display(&message) {
                    return default_message_display();
                }
                message
                    .offline_push_info
                    .map(|offline| PushDisplay {
                        title: non_empty_or(offline.title, "Flare IM"),
                        body: non_empty_or(offline.body, "你收到一条新消息"),
                    })
                    .unwrap_or_else(default_message_display)
            })
            .unwrap_or_else(default_message_display),
        PushTaskPayloadKind::Custom => PushDisplay {
            title: "Flare IM".to_string(),
            body: "你收到一条业务通知".to_string(),
        },
        _ => default_message_display(),
    }
}

fn default_message_display() -> PushDisplay {
    PushDisplay {
        title: "Flare IM".to_string(),
        body: "你收到一条新消息".to_string(),
    }
}

fn default_notification_display() -> PushDisplay {
    PushDisplay {
        title: "Flare IM".to_string(),
        body: "你收到一条通知".to_string(),
    }
}

fn non_empty_or(value: String, fallback: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        fallback.to_string()
    } else {
        trimmed.to_string()
    }
}

fn message_requires_generic_push_display(message: &flare_proto::common::Message) -> bool {
    let content_hidden = message
        .retention_state
        .as_ref()
        .and_then(|state| ContentVisibility::try_from(state.content_visibility).ok())
        .is_some_and(|visibility| {
            matches!(
                visibility,
                ContentVisibility::Hidden | ContentVisibility::Redacted | ContentVisibility::Purged
            )
        });
    content_hidden || message_has_e2ee_placeholder(message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_proto::common::{Event, Message, MessageRetentionState, OfflinePushInfo};

    fn event_task(message: Message) -> PushTaskEnvelope {
        let req = PushEventRequest {
            events: vec![Event {
                payload: Some(EventPayload::Message(message)),
                ..Default::default()
            }],
            ..Default::default()
        };
        PushTaskEnvelope {
            payload_kind: PushTaskPayloadKind::Event as i32,
            push_payload: req.encode_to_vec(),
            ..Default::default()
        }
    }

    fn message_with_push_info(title: &str, body: &str) -> Message {
        Message {
            offline_push_info: Some(OfflinePushInfo {
                title: title.to_string(),
                body: body.to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// 群消息走内联事件，事件里带着整条消息——离线推送该显示真实文案，
    /// 而不是每条都「你收到一条新消息」。
    #[test]
    fn inline_event_renders_the_message_push_info() {
        let task = event_task(message_with_push_info("项目群", "小明: 今天几点开会"));
        assert_eq!(
            notification_display(&task),
            PushDisplay {
                title: "项目群".into(),
                body: "小明: 今天几点开会".into(),
            }
        );
    }

    /// 内容不可见（撤回/清除）时必须退回通用文案：推送是内容最容易漏出去的地方。
    #[test]
    fn hidden_content_falls_back_to_generic_display() {
        let mut message = message_with_push_info("项目群", "不该出现的正文");
        message.retention_state = Some(MessageRetentionState {
            content_visibility: ContentVisibility::Hidden as i32,
            ..Default::default()
        });
        let display = notification_display(&event_task(message));
        assert_eq!(display, default_message_display());
        assert!(!display.body.contains("不该出现"));
    }

    /// 事件里没有消息（纯 ping）时不该崩，也不该显示空白。
    #[test]
    fn ping_without_message_uses_generic_display() {
        let req = PushEventRequest {
            events: vec![Event::default()],
            ..Default::default()
        };
        let task = PushTaskEnvelope {
            payload_kind: PushTaskPayloadKind::Event as i32,
            push_payload: req.encode_to_vec(),
            ..Default::default()
        };
        assert_eq!(notification_display(&task), default_message_display());
    }
}
