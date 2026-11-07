use super::*;
use flare_proto::common::{Message, MessageStatus, message_content::Content};

impl MediaService {
    /// A bounded renewal of a private attachment already sent by its owner.
    /// No durable grant is created: Reader rechecks the original verified User
    /// or message-inspecting Staff identity on every request.
    pub async fn create_message_presigned_url(
        &self,
        ctx: &Context,
        file_id: &str,
        expires_in: i64,
        conversation_id: &str,
        message_seq: i64,
    ) -> Result<PresignedUrl> {
        let staff = ctx
            .actor()
            .is_some_and(|actor| actor.actor_type == flare_server_core::context::ActorType::Staff);
        if staff {
            flare_server_core::require_staff_permission(ctx, "core:messages:read").map_err(
                |_| {
                    flare_server_core::flare_err!(
                        ErrorCode::PermissionDenied,
                        "message inspection permission required"
                    )
                },
            )?;
        } else {
            require_owner(ctx)?;
        }
        if conversation_id.is_empty()
            || conversation_id.len() > 128
            || conversation_id.trim() != conversation_id
            || !(1..i64::MAX).contains(&message_seq)
        {
            return Err(flare_server_core::flare_err!(
                ErrorCode::InvalidParameter,
                "invalid message attachment reference"
            ));
        }
        let metadata = self.load_scoped_metadata(ctx, file_id).await?;
        // The User branch already required a verified IM identity and loaded
        // this application's metadata. Its presentation policy needs no owner,
        // read-grant, or conversation Reader check. Staff still uses the exact
        // permission-checked message inspection path below.
        if !staff {
            return self
                .presign_authorized_metadata(ctx, &metadata, expires_in)
                .await;
        }
        let reader = self.message_reader.as_ref().ok_or_else(|| {
            flare_server_core::flare_err!(
                ErrorCode::ServiceUnavailable,
                "message attachment reader unavailable"
            )
        })?;
        let message = reader
            .read_visible_message(ctx, conversation_id, message_seq)
            .await?;
        if !message.as_ref().is_some_and(|message| {
            owns_visible_attachment(message, &metadata, conversation_id, message_seq)
        }) {
            return Err(flare_server_core::flare_err!(
                ErrorCode::PermissionDenied,
                "message attachment is not readable"
            ));
        }
        let ttl = if expires_in > 0 {
            expires_in.min(60)
        } else {
            60
        };
        // Never fall back to an unsigned direct/CDN URL on this new recipient
        // path. Short object capabilities are renewed from authoritative reads.
        let repository = self
            .object_repo
            .as_ref()
            .filter(|repository| {
                metadata.storage_bucket().is_some() && repository.use_presigned_urls()
            })
            .ok_or_else(|| {
                flare_server_core::flare_err!(
                    ErrorCode::ServiceUnavailable,
                    "signed attachment storage unavailable"
                )
            })?;
        let url = repository
            .presign_object(metadata.storage_path().unwrap_or(file_id), ttl)
            .await?;
        if !is_absolute_http_url(&url) {
            return Err(flare_server_core::flare_err!(
                ErrorCode::ServiceUnavailable,
                "signed attachment URL unavailable"
            ));
        }
        self.usage
            .media_egress(require_tenant(ctx)?, usage_bytes(metadata.file_size));
        Ok(PresignedUrl {
            url,
            cdn_url: String::new(),
            expires_at: Utc::now() + Duration::seconds(ttl),
        })
    }
}

fn owns_visible_attachment(
    message: &Message,
    media: &MediaFileMetadata,
    conversation: &str,
    seq: i64,
) -> bool {
    if message.conversation_id != conversation
        || message.conversation_seq != seq as u64
        || media.owner_id.as_deref().filter(|id| !id.is_empty()) != Some(message.sender_id.as_str())
        || !matches!(
            MessageStatus::try_from(message.status),
            Ok(MessageStatus::Sent | MessageStatus::Persisted)
        )
        || message.retention_state.as_ref().is_some_and(|state| {
            matches!(
                flare_proto::common::MessageRetentionLifecycle::try_from(state.lifecycle),
                Ok(flare_proto::common::MessageRetentionLifecycle::Expired
                    | flare_proto::common::MessageRetentionLifecycle::Purged)
            ) || matches!(
                flare_proto::common::ContentVisibility::try_from(state.content_visibility),
                Ok(flare_proto::common::ContentVisibility::Hidden
                    | flare_proto::common::ContentVisibility::Redacted
                    | flare_proto::common::ContentVisibility::Purged)
            )
        })
    {
        return false;
    }
    let id = media.file_id.as_str();
    let image_matches =
        |image: &flare_proto::common::ImageInfo| image.image_id == id || image.uuid == id;
    match message
        .content
        .as_ref()
        .and_then(|content| content.content.as_ref())
    {
        Some(Content::File(file)) => file.file_id == id,
        Some(Content::Image(image)) => {
            image.source.as_ref().is_some_and(image_matches)
                || image.thumbnail.as_ref().is_some_and(image_matches)
        }
        Some(Content::ImageGroup(images)) => images.images.iter().any(image_matches),
        Some(Content::Video(video)) => {
            video.video_id == id
                || video
                    .source
                    .as_ref()
                    .is_some_and(|source| source.uuid == id)
                || video.cover.as_ref().is_some_and(image_matches)
        }
        Some(Content::Audio(audio)) => {
            audio.audio_id == id
                || audio
                    .source
                    .as_ref()
                    .is_some_and(|source| source.uuid == id)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::model::MediaFileMetadata;
    fn fixture() -> (Message, MediaFileMetadata) {
        let media = MediaFileMetadata {
            file_id: "file-1".into(),
            owner_id: Some("alice".into()),
            ..Default::default()
        };
        let message = Message {
            conversation_id: "chat-1".into(),
            conversation_seq: 7,
            sender_id: "alice".into(),
            status: MessageStatus::Sent as i32,
            content: Some(flare_proto::common::MessageContent {
                content: Some(Content::File(flare_proto::common::FileContent {
                    file_id: "file-1".into(),
                    ..Default::default()
                })),
            }),
            ..Default::default()
        };
        (message, media)
    }
    #[test]
    fn only_owner_sent_exact_typed_attachment_is_authority() {
        let (message, media) = fixture();
        assert!(owns_visible_attachment(&message, &media, "chat-1", 7));
        assert!(!owns_visible_attachment(&message, &media, "other-chat", 7));
        assert!(!owns_visible_attachment(&message, &media, "chat-1", 8));
        let mut forged = message.clone();
        forged.sender_id = "bob".into();
        assert!(!owns_visible_attachment(&forged, &media, "chat-1", 7));
        forged.sender_id = "alice".into();
        forged.status = MessageStatus::Recalled as i32;
        assert!(!owns_visible_attachment(&forged, &media, "chat-1", 7));
        forged.status = MessageStatus::Sent as i32;
        forged.content = None;
        assert!(!owns_visible_attachment(&forged, &media, "chat-1", 7));
        let mut other = media.clone();
        other.file_id = "another-file".into();
        assert!(!owns_visible_attachment(&message, &other, "chat-1", 7));
        let mut expired = message.clone();
        expired.retention_state = Some(flare_proto::common::MessageRetentionState {
            lifecycle: flare_proto::common::MessageRetentionLifecycle::Expired as i32,
            ..Default::default()
        });
        assert!(!owns_visible_attachment(&expired, &media, "chat-1", 7));
    }
}
