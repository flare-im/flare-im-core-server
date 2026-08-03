//! 厂商无关的推送展示内容解码。
//!
//! 把 `PushTaskEnvelope` 解成「标题 + 正文」是所有厂商通道共用的一步。
//! 它此前私有在个推实现里，新增 FCM/APNs 时若各抄一份，三家对
//! 「消息被撤回/内容不可见时该显示什么」的判断就会各自漂移 ——
//! 而那正是最容易泄露内容的地方。

use flare_grpc_proto::access_gateway::{PushMessageRequest, PushNotificationRequest};
use flare_proto::PushTaskEnvelope;
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
