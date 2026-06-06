use std::sync::Arc;

use flare_im_core::Ctx;
use flare_im_core::utils::{TimelineMetadata, normalize_tenant_id};
use flare_server_core::Context;
use flare_server_core::error::Result;
use tracing::instrument;
use uuid::Uuid;

use crate::domain::PersistenceMode;
use crate::domain::model::{MessageProfile, MessageSubmission, notification_persistent};
use crate::domain::repository::{WalRepository, WalRepositoryItem};
use crate::domain::service::MessageDomainService;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WalReplayReport {
    pub scanned: usize,
    pub replayed: usize,
    pub failed: usize,
    pub skipped: usize,
}

pub struct WalReplayHandler {
    wal_repository: Arc<WalRepositoryItem>,
    message_domain_service: Arc<MessageDomainService>,
    default_tenant_id: String,
}

impl WalReplayHandler {
    pub fn new(
        wal_repository: Arc<WalRepositoryItem>,
        message_domain_service: Arc<MessageDomainService>,
        default_tenant_id: Option<String>,
    ) -> Self {
        Self {
            wal_repository,
            message_domain_service,
            default_tenant_id: default_tenant_id
                .map(|tenant_id| normalize_tenant_id(&tenant_id))
                .unwrap_or_else(|| "0".to_string()),
        }
    }

    #[instrument(skip(self), fields(limit))]
    pub async fn replay_once(&self, limit: usize) -> Result<WalReplayReport> {
        let pending = self.wal_repository.list_pending(limit).await?;
        let mut report = WalReplayReport {
            scanned: pending.len(),
            ..WalReplayReport::default()
        };

        for pending in pending {
            let mut message = pending.message;
            if message.server_id.trim().is_empty() {
                message.server_id = pending.message_id.clone();
            }

            let profile = MessageProfile::ensure(&mut message);
            let persistence_mode = replay_persistence_mode(&profile, &message);
            if persistence_mode.should_push_only(profile.is_temporary()) {
                self.wal_repository.remove(&pending.message_id).await?;
                report.skipped += 1;
                continue;
            }

            let tenant_id = if pending.tenant_id.trim().is_empty() {
                self.default_tenant_id.clone()
            } else {
                normalize_tenant_id(&pending.tenant_id)
            };
            let ctx = replay_ctx(&tenant_id);
            let submission = MessageSubmission {
                message,
                message_id: pending.message_id.clone(),
                timeline: TimelineMetadata::default(),
            };

            match self
                .message_domain_service
                .push_message(&ctx, &submission, &profile, persistence_mode)
                .await
            {
                Ok(()) => {
                    self.wal_repository.remove(&pending.message_id).await?;
                    report.replayed += 1;
                }
                Err(error) => {
                    report.failed += 1;
                    tracing::warn!(
                        message_id = %pending.message_id,
                        tenant_id = %tenant_id,
                        error = %error,
                        "Failed to replay WAL message; keeping entry for later retry"
                    );
                }
            }
        }

        Ok(report)
    }
}

fn replay_ctx(tenant_id: &str) -> Ctx {
    Arc::new(
        Context::with_request_id(format!("wal-replay-{}", Uuid::new_v4()))
            .with_tenant_id(tenant_id),
    )
}

fn replay_persistence_mode(
    profile: &MessageProfile,
    message: &flare_proto::common::Message,
) -> PersistenceMode {
    if profile.is_temporary() {
        PersistenceMode::ForcePushOnly
    } else if profile.is_notification() {
        match notification_persistent(message) {
            Some(false) => PersistenceMode::ForcePushOnly,
            _ => PersistenceMode::Auto,
        }
    } else {
        PersistenceMode::Auto
    }
}

#[cfg(test)]
mod tests {
    use super::replay_persistence_mode;
    use crate::domain::PersistenceMode;
    use crate::domain::model::MessageProfile;
    use flare_proto::common::message_content::Content;
    use flare_proto::common::{Message, MessageContent, NotificationContent, TextContent};

    fn notification(persistent: bool) -> (MessageProfile, Message) {
        let mut message = Message {
            content: Some(MessageContent {
                content: Some(Content::Notification(NotificationContent {
                    title: "title".to_string(),
                    body: "body".to_string(),
                    notification_type: "general".to_string(),
                    attributes: Default::default(),
                    target_user_ids: vec![],
                    target_role_id: String::new(),
                    notify_all: false,
                    persistent,
                    show_in_list: true,
                    show_badge: true,
                    play_sound: true,
                })),
            }),
            ..Message::default()
        };
        let profile = MessageProfile::ensure(&mut message);
        (profile, message)
    }

    #[test]
    fn replay_keeps_persistent_notifications_on_storage_path() {
        let (profile, message) = notification(true);
        assert_eq!(
            replay_persistence_mode(&profile, &message),
            PersistenceMode::Auto
        );

        let (profile, message) = notification(false);
        assert_eq!(
            replay_persistence_mode(&profile, &message),
            PersistenceMode::ForcePushOnly
        );
    }

    #[test]
    fn replay_keeps_normal_messages_on_storage_path() {
        let mut message = Message {
            content: Some(MessageContent {
                content: Some(Content::Text(TextContent {
                    text: "hello".to_string(),
                    mentions: vec![],
                })),
            }),
            ..Message::default()
        };
        let profile = MessageProfile::ensure(&mut message);
        assert_eq!(
            replay_persistence_mode(&profile, &message),
            PersistenceMode::Auto
        );
    }
}
