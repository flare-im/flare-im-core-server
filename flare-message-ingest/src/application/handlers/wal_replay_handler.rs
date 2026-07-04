use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use flare_im_contracts::Ctx;
use flare_im_contracts::utils::{TimelineMetadata, normalize_tenant_id};
use flare_server_core::Context;
use flare_server_core::error::Result;
use tracing::instrument;
use uuid::Uuid;

use crate::domain::PersistenceMode;
use crate::domain::model::{MessageProfile, MessageSubmission, notification_persistent};
use crate::domain::repository::WalRepository;
use crate::domain::service::MessageIngestService;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WalReplayReport {
    pub scanned: usize,
    pub replayed: usize,
    pub failed: usize,
    pub skipped: usize,
}

pub struct WalReplayHandler {
    wal_repository: Arc<dyn WalRepository>,
    publisher: Arc<dyn WalReplayPublisher>,
    default_tenant_id: String,
}

pub trait WalReplayPublisher: Send + Sync {
    fn push_wal_message<'a>(
        &'a self,
        ctx: &'a Ctx,
        submission: &'a MessageSubmission,
        profile: &'a MessageProfile,
        persistence_mode: PersistenceMode,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

impl WalReplayPublisher for MessageIngestService {
    fn push_wal_message<'a>(
        &'a self,
        ctx: &'a Ctx,
        submission: &'a MessageSubmission,
        profile: &'a MessageProfile,
        persistence_mode: PersistenceMode,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(self.push_message(ctx, submission, profile, persistence_mode))
    }
}

impl WalReplayHandler {
    pub fn new(
        wal_repository: Arc<dyn WalRepository>,
        publisher: Arc<dyn WalReplayPublisher>,
        default_tenant_id: Option<String>,
    ) -> Self {
        Self {
            wal_repository,
            publisher,
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
                .publisher
                .push_wal_message(&ctx, &submission, &profile, persistence_mode)
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
    use super::{WalReplayHandler, WalReplayPublisher, replay_persistence_mode};
    use crate::domain::PersistenceMode;
    use crate::domain::model::MessageProfile;
    use crate::domain::model::MessageSubmission;
    use crate::domain::repository::{WalPendingMessage, WalRepository};
    use flare_proto::common::message_content::Content;
    use flare_proto::common::{Message, MessageContent, NotificationContent, TextContent};
    use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
    use std::collections::HashMap;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct MemoryWalRepository {
        entries: Mutex<HashMap<String, WalPendingMessage>>,
        removed: Mutex<Vec<String>>,
    }

    impl MemoryWalRepository {
        fn with_entries(entries: Vec<WalPendingMessage>) -> Arc<Self> {
            Arc::new(Self {
                entries: Mutex::new(
                    entries
                        .into_iter()
                        .map(|entry| (entry.message_id.clone(), entry))
                        .collect(),
                ),
                removed: Mutex::new(Vec::new()),
            })
        }

        fn contains(&self, message_id: &str) -> bool {
            self.entries
                .lock()
                .expect("memory wal poisoned")
                .contains_key(message_id)
        }

        fn removed_ids(&self) -> Vec<String> {
            self.removed
                .lock()
                .expect("memory wal removed poisoned")
                .clone()
        }
    }

    impl WalRepository for MemoryWalRepository {
        fn append<'a>(
            &'a self,
            submission: &'a MessageSubmission,
            tenant_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async move {
                let message_id = submission.message_id.clone();
                self.entries.lock().expect("memory wal poisoned").insert(
                    message_id.clone(),
                    WalPendingMessage {
                        message_id,
                        tenant_id: tenant_id.to_string(),
                        message: submission.message.clone(),
                    },
                );
                Ok(())
            })
        }

        fn find_by_message_id<'a>(
            &'a self,
            message_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Option<Message>>> + Send + 'a>> {
            Box::pin(async move {
                Ok(self
                    .entries
                    .lock()
                    .expect("memory wal poisoned")
                    .get(message_id)
                    .map(|entry| entry.message.clone()))
            })
        }

        fn list_pending<'a>(
            &'a self,
            limit: usize,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<WalPendingMessage>>> + Send + 'a>> {
            Box::pin(async move {
                if limit == 0 {
                    return Ok(Vec::new());
                }
                let mut entries = self
                    .entries
                    .lock()
                    .expect("memory wal poisoned")
                    .values()
                    .cloned()
                    .collect::<Vec<_>>();
                entries.sort_by(|a, b| a.message_id.cmp(&b.message_id));
                entries.truncate(limit);
                Ok(entries)
            })
        }

        fn remove<'a>(
            &'a self,
            message_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async move {
                self.entries
                    .lock()
                    .expect("memory wal poisoned")
                    .remove(message_id);
                self.removed
                    .lock()
                    .expect("memory wal removed poisoned")
                    .push(message_id.to_string());
                Ok(())
            })
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct CapturedReplay {
        tenant_id: String,
        message_id: String,
        server_id: String,
        persistence_mode: PersistenceMode,
    }

    #[derive(Default)]
    struct FakeReplayPublisher {
        fail: bool,
        captured: Mutex<Vec<CapturedReplay>>,
    }

    impl FakeReplayPublisher {
        fn new(fail: bool) -> Arc<Self> {
            Arc::new(Self {
                fail,
                captured: Mutex::new(Vec::new()),
            })
        }

        fn captured(&self) -> Vec<CapturedReplay> {
            self.captured
                .lock()
                .expect("fake publisher poisoned")
                .clone()
        }
    }

    impl WalReplayPublisher for FakeReplayPublisher {
        fn push_wal_message<'a>(
            &'a self,
            ctx: &'a flare_im_contracts::Ctx,
            submission: &'a MessageSubmission,
            _profile: &'a MessageProfile,
            persistence_mode: PersistenceMode,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async move {
                self.captured
                    .lock()
                    .expect("fake publisher poisoned")
                    .push(CapturedReplay {
                        tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
                        message_id: submission.message_id.clone(),
                        server_id: submission.message.server_id.clone(),
                        persistence_mode,
                    });
                if self.fail {
                    return Err(
                        ErrorBuilder::new(ErrorCode::ServiceUnavailable, "replay failed")
                            .build_error(),
                    );
                }
                Ok(())
            })
        }
    }

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

    fn pending_text(message_id: &str, tenant_id: &str) -> WalPendingMessage {
        WalPendingMessage {
            message_id: message_id.to_string(),
            tenant_id: tenant_id.to_string(),
            message: Message {
                server_id: String::new(),
                conversation_id: "conv-1".to_string(),
                sender_id: "sender-1".to_string(),
                conversation_seq: 42,
                content: Some(MessageContent {
                    content: Some(Content::Text(TextContent {
                        text: "hello".to_string(),
                        mentions: vec![],
                    })),
                }),
                ..Message::default()
            },
        }
    }

    fn pending_notification(message_id: &str, persistent: bool) -> WalPendingMessage {
        let (_, message) = notification(persistent);
        WalPendingMessage {
            message_id: message_id.to_string(),
            tenant_id: "tenant-a".to_string(),
            message: Message {
                server_id: message_id.to_string(),
                conversation_id: "conv-1".to_string(),
                sender_id: "sender-1".to_string(),
                conversation_seq: 43,
                ..message
            },
        }
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

    #[tokio::test]
    async fn replay_success_removes_wal_entry_and_backfills_server_id() {
        let wal = MemoryWalRepository::with_entries(vec![pending_text("msg-1", "tenant-a")]);
        let publisher = FakeReplayPublisher::new(false);
        let handler = WalReplayHandler::new(wal.clone(), publisher.clone(), Some("0".to_string()));

        let report = handler.replay_once(10).await.unwrap();

        assert_eq!(
            report,
            super::WalReplayReport {
                scanned: 1,
                replayed: 1,
                failed: 0,
                skipped: 0,
            }
        );
        assert!(!wal.contains("msg-1"));
        assert_eq!(wal.removed_ids(), vec!["msg-1".to_string()]);
        assert_eq!(
            publisher.captured(),
            vec![CapturedReplay {
                tenant_id: "tenant-a".to_string(),
                message_id: "msg-1".to_string(),
                server_id: "msg-1".to_string(),
                persistence_mode: PersistenceMode::Auto,
            }]
        );
    }

    #[tokio::test]
    async fn replay_failure_keeps_wal_entry_for_later_retry() {
        let wal = MemoryWalRepository::with_entries(vec![pending_text("msg-1", "tenant-a")]);
        let publisher = FakeReplayPublisher::new(true);
        let handler = WalReplayHandler::new(wal.clone(), publisher.clone(), Some("0".to_string()));

        let report = handler.replay_once(10).await.unwrap();

        assert_eq!(
            report,
            super::WalReplayReport {
                scanned: 1,
                replayed: 0,
                failed: 1,
                skipped: 0,
            }
        );
        assert!(wal.contains("msg-1"));
        assert!(wal.removed_ids().is_empty());
        assert_eq!(publisher.captured().len(), 1);
    }

    #[tokio::test]
    async fn replay_skips_and_removes_push_only_entries() {
        let wal =
            MemoryWalRepository::with_entries(vec![pending_notification("msg-ephemeral", false)]);
        let publisher = FakeReplayPublisher::new(false);
        let handler = WalReplayHandler::new(wal.clone(), publisher.clone(), None);

        let report = handler.replay_once(10).await.unwrap();

        assert_eq!(
            report,
            super::WalReplayReport {
                scanned: 1,
                replayed: 0,
                failed: 0,
                skipped: 1,
            }
        );
        assert!(!wal.contains("msg-ephemeral"));
        assert_eq!(publisher.captured(), Vec::<CapturedReplay>::new());
    }

    #[tokio::test]
    async fn replay_limit_zero_does_not_claim_or_publish() {
        let wal = MemoryWalRepository::with_entries(vec![pending_text("msg-1", "tenant-a")]);
        let publisher = FakeReplayPublisher::new(false);
        let handler = WalReplayHandler::new(wal.clone(), publisher.clone(), None);

        let report = handler.replay_once(0).await.unwrap();

        assert_eq!(report, super::WalReplayReport::default());
        assert!(wal.contains("msg-1"));
        assert!(wal.removed_ids().is_empty());
        assert!(publisher.captured().is_empty());
    }
}
