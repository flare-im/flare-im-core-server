//! Read-only, transport-owned message inspection. No Social policy or client URLs.
use flare_proto::common::{
    ContentVisibility, ImageInfo, Message, MessageRetentionLifecycle, MessageStatus,
    message_content::Content,
};
use serde::Serialize;
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminMessageAttachmentHttp {
    pub file_id: String,
    pub kind: String,
    pub name: String,
    pub mime_type: String,
    pub size: i64,
}
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminMessageBodyHttp {
    pub text: String,
    pub attachments: Vec<AdminMessageAttachmentHttp>,
    pub truncated: bool,
}
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminMessageAttachmentAccessHttp {
    pub file_id: String,
    pub url: String,
    pub expires_at: i64,
}

pub fn content_state(message: &Message) -> &'static str {
    if matches!(
        MessageStatus::try_from(message.status),
        Ok(MessageStatus::Recalled)
    ) {
        return "recalled";
    }
    if matches!(
        MessageStatus::try_from(message.status),
        Ok(MessageStatus::Deleted)
    ) || message.retention_state.as_ref().is_some_and(|state| {
        matches!(
            MessageRetentionLifecycle::try_from(state.lifecycle),
            Ok(MessageRetentionLifecycle::Expired | MessageRetentionLifecycle::Purged)
        ) || matches!(
            ContentVisibility::try_from(state.content_visibility),
            Ok(ContentVisibility::Hidden | ContentVisibility::Redacted | ContentVisibility::Purged)
        )
    }) {
        return "unavailable";
    }
    if message
        .content
        .as_ref()
        .and_then(|c| c.content.as_ref())
        .is_none()
    {
        return "unavailable";
    }
    "available"
}

fn attachment(
    id: &str,
    kind: &str,
    name: &str,
    mime: &str,
    size: i64,
) -> Option<AdminMessageAttachmentHttp> {
    if id.is_empty() || id.len() > 256 || id.chars().any(char::is_control) {
        return None;
    }
    Some(AdminMessageAttachmentHttp {
        file_id: id.into(),
        kind: kind.into(),
        name: name.chars().take(512).collect(),
        mime_type: mime.into(),
        size: size.max(0),
    })
}
fn image(image: &ImageInfo) -> Option<AdminMessageAttachmentHttp> {
    attachment(
        if image.image_id.is_empty() {
            &image.uuid
        } else {
            &image.image_id
        },
        "image",
        "",
        &image.mime_type,
        image.size,
    )
}
fn plain(content: &Content, depth: usize) -> String {
    if depth > 4 {
        return String::new();
    }
    match content {
        Content::Text(c) => c.text.clone(),
        Content::Image(c) => c.description.clone(),
        Content::ImageGroup(c) => c.description.clone(),
        Content::Video(c) => c.description.clone(),
        Content::Audio(c) => c.description.clone(),
        Content::File(c) => c.description.clone(),
        Content::RichText(c) => c.plain_text.clone(),
        Content::Quote(c) => {
            let current = c
                .current_content
                .as_ref()
                .and_then(|c| c.content.as_ref())
                .map(|c| plain(c, depth + 1))
                .unwrap_or_default();
            format!("{}\n{}", c.quoted_text_preview, current)
                .trim()
                .into()
        }
        Content::Forward(c) => c
            .items
            .iter()
            .take(20)
            .map(|item| item.plain_text.as_str())
            .collect::<Vec<_>>()
            .join("\n"),
        Content::System(c) => c.body.clone(),
        Content::Notification(c) => format!("{}\n{}", c.title, c.body).trim().into(),
        Content::AppCard(c) => format!("{}\n{}", c.title, c.summary).trim().into(),
        Content::Card(c) => format!("{}\n{}", c.title, c.subtitle).trim().into(),
        Content::LinkCard(c) => format!("{}\n{}", c.title, c.description).trim().into(),
        Content::Location(c) => format!(
            "{}\n{} ({}, {})",
            c.title, c.address, c.latitude, c.longitude
        )
        .trim()
        .into(),
        Content::Emoji(c) => {
            if c.description.is_empty() {
                c.emoji.clone()
            } else {
                c.description.clone()
            }
        }
        Content::Custom(c) => c.description.clone(),
        Content::Placeholder(c) => c.fallback_text.clone(),
        Content::Thread(c) => c.thread_title.clone(),
        Content::Sticker(c) => c.sticker_id.clone(),
    }
}
pub fn body(message: &Message, summary: bool) -> Option<AdminMessageBodyHttp> {
    if content_state(message) != "available" {
        return None;
    }
    let content = message.content.as_ref()?.content.as_ref()?;
    let mut attachments = Vec::new();
    match content {
        Content::Image(c) => attachments.extend(c.source.as_ref().and_then(image)),
        Content::ImageGroup(c) => attachments.extend(c.images.iter().take(20).filter_map(image)),
        Content::File(c) => attachments.extend(attachment(
            &c.file_id,
            "file",
            &c.file_name,
            &c.mime_type,
            c.file_size,
        )),
        Content::Video(c) => {
            if let Some(s) = &c.source {
                attachments.extend(attachment(
                    if c.video_id.is_empty() {
                        &s.uuid
                    } else {
                        &c.video_id
                    },
                    "video",
                    "",
                    &s.mime_type,
                    s.size,
                ));
            }
        }
        Content::Audio(c) => {
            if let Some(s) = &c.source {
                attachments.extend(attachment(
                    if c.audio_id.is_empty() {
                        &s.uuid
                    } else {
                        &c.audio_id
                    },
                    "audio",
                    "",
                    &s.mime_type,
                    s.size,
                ));
            }
        }
        _ => {}
    }
    let text = plain(content, 0);
    let truncated = summary && text.chars().count() > 4096;
    Some(AdminMessageBodyHttp {
        text: if truncated {
            text.chars().take(4096).collect()
        } else {
            text
        },
        attachments,
        truncated,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_proto::common::{FileContent, MessageContent, MessageRetentionState, TextContent};
    #[test]
    fn text_is_real_and_summaries_are_bounded() {
        let message = Message {
            status: 2,
            content: Some(MessageContent {
                content: Some(Content::Text(TextContent {
                    text: "界".repeat(5000),
                    ..Default::default()
                })),
            }),
            ..Default::default()
        };
        assert_eq!(body(&message, true).unwrap().text.chars().count(), 4096);
        assert_eq!(body(&message, false).unwrap().text.chars().count(), 5000);
    }
    #[test]
    fn hidden_content_and_untrusted_urls_are_not_exposed() {
        let mut message = Message {
            status: 2,
            content: Some(MessageContent {
                content: Some(Content::File(FileContent {
                    file_id: "registered".into(),
                    file_name: "report.pdf".into(),
                    url: "https://attacker.test/track".into(),
                    ..Default::default()
                })),
            }),
            ..Default::default()
        };
        let projected = body(&message, false).unwrap();
        assert_eq!(projected.attachments[0].file_id, "registered");
        assert!(
            !serde_json::to_string(&projected)
                .unwrap()
                .contains("attacker")
        );
        message.status = 5;
        assert!(body(&message, false).is_none());
        message.status = 2;
        message.retention_state = Some(MessageRetentionState {
            content_visibility: 3,
            ..Default::default()
        });
        assert!(body(&message, false).is_none());
        message.retention_state = Some(MessageRetentionState {
            lifecycle: MessageRetentionLifecycle::Expired as i32,
            ..Default::default()
        });
        assert!(body(&message, false).is_none());
    }
}
