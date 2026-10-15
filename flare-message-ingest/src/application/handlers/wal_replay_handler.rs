use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use flare_im_contracts::Ctx;
use flare_im_contracts::utils::{TimelineMetadata, normalize_optional_tenant_id};
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
    /// 没记租户、也没配置默认租户而被隔离的条目：不重放、不删除，不算失败（见
    /// [`WalReplayHandler::replay_once`]）。
    pub quarantined: usize,
}

/// 记住已报告过的隔离条目的上限（只影响日志去重：被挤出的条目下次被领取时再记一次）。
const QUARANTINE_REPORTED_CAPACITY: u64 = 10_000;

pub struct WalReplayHandler {
    write_verifier: Option<flare_im_service_kit::audit_effects::write_proof::ProofVerifier>,
    wal_repository: Arc<dyn WalRepository>,
    publisher: Arc<dyn WalReplayPublisher>,
    /// 显式配置的部署级默认租户（`MESSAGE_INGEST_DEFAULT_TENANT_ID`），只给没记租户的旧 WAL
    /// 条目用；没配置时这类条目不重放，不再按租户 "0" 发布。
    default_tenant_id: Option<String>,
    /// 本进程已报告过的隔离条目（消息 id）。条目每个领取租约到期都会被再领一次，只有第一次记
    /// ERROR，之后只计数。
    quarantine_reported: moka::future::Cache<String, ()>,
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
            write_verifier: None,
            wal_repository,
            publisher,
            default_tenant_id: normalize_optional_tenant_id(default_tenant_id.as_deref()),
            quarantine_reported: moka::future::Cache::new(QUARANTINE_REPORTED_CAPACITY),
        }
    }

    pub fn with_write_verifier(
        mut self,
        verifier: Option<flare_im_service_kit::audit_effects::write_proof::ProofVerifier>,
    ) -> Self {
        self.write_verifier = verifier;
        self
    }

    /// 重放一批待恢复的 WAL 条目。
    ///
    /// 没记租户、也没配置默认租户的条目被隔离：不重放（不按租户 "0" 发布）、不删除（留给运维
    /// 配置默认租户或人工处理，配置后重启即按默认租户重放），计入 `quarantined` 而不是 `failed`。
    /// 每个条目在本进程只记一次 ERROR（只含消息 id）；以前它每个领取租约都被再领一次、每次记
    /// ERROR 并计为失败。
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

            let Some(tenant_id) = normalize_optional_tenant_id(Some(&pending.tenant_id))
                .or_else(|| self.default_tenant_id.clone())
            else {
                report.quarantined += 1;
                let first_report = self
                    .quarantine_reported
                    .entry(pending.message_id.clone())
                    .or_insert(())
                    .await
                    .is_fresh();
                if first_report {
                    tracing::error!(
                        message_id = %pending.message_id,
                        "WAL entry has no tenant and no default tenant is configured; \
                         quarantining it (kept unpublished instead of replaying it as tenant 0, \
                         reported once)"
                    );
                } else {
                    tracing::debug!(
                        message_id = %pending.message_id,
                        "WAL entry without a tenant is still quarantined"
                    );
                }
                continue;
            };
            let mut ctx = replay_ctx(&tenant_id);
            if flare_im_service_kit::audit_effects::write_proof::requires_proof(&message)
                || pending.write_proof.is_some()
            {
                let recovered = self
                    .write_verifier
                    .as_ref()
                    .zip(pending.write_proof.as_deref())
                    .and_then(|(v, p)| v.recover(p, &tenant_id, &message).ok());
                let Some(recovered) = recovered else {
                    report.failed += 1;
                    continue;
                };
                ctx = Arc::new(ctx.insert_data(recovered));
            }
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
            write_proof: Option<&'a str>,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async move {
                let message_id = submission.message_id.clone();
                self.entries.lock().expect("memory wal poisoned").insert(
                    message_id.clone(),
                    WalPendingMessage {
                        write_proof: write_proof.map(str::to_owned),
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
            write_proof: None,
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
            write_proof: None,
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
                quarantined: 0,
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

    /// 没记租户的 WAL 条目：没配置默认租户时隔离——不重放、不删，不再按租户 "0" 发布（W11-T），
    /// 计入 `quarantined` 而不是 `failed`（W13-R2）；显式配置的默认租户（含 "0"）照常使用。
    #[tokio::test]
    async fn replay_without_tenant_uses_only_an_explicit_default() {
        for blank in ["", "  "] {
            let wal = MemoryWalRepository::with_entries(vec![pending_text("msg-1", blank)]);
            let publisher = FakeReplayPublisher::new(false);
            let handler = WalReplayHandler::new(wal.clone(), publisher.clone(), None);

            let report = handler.replay_once(10).await.unwrap();

            assert_eq!(
                report,
                super::WalReplayReport {
                    scanned: 1,
                    replayed: 0,
                    failed: 0,
                    skipped: 0,
                    quarantined: 1,
                }
            );
            assert!(wal.contains("msg-1"), "entry stays for an operator");
            assert!(
                publisher.captured().is_empty(),
                "nothing published as tenant 0"
            );
        }

        for default in ["tenant-d", "0"] {
            let wal = MemoryWalRepository::with_entries(vec![pending_text("msg-1", "")]);
            let publisher = FakeReplayPublisher::new(false);
            let handler =
                WalReplayHandler::new(wal.clone(), publisher.clone(), Some(default.to_string()));

            let report = handler.replay_once(10).await.unwrap();

            assert_eq!(report.replayed, 1);
            assert_eq!(publisher.captured()[0].tenant_id, default);
        }
    }

    /// 隔离的无租户条目每个领取租约都会被再领一次：每轮只计入 `quarantined`、不算失败，条目留在
    /// WAL、不发布，同批有租户的条目照常重放（W13-R2）。修复前每轮计为失败（ERROR 只记一次见
    /// `tests/wal_replay_quarantine.rs`，日志断言须独占一个测试二进制）。
    #[tokio::test]
    async fn tenantless_entries_are_quarantined_every_round_without_failing() {
        let wal = MemoryWalRepository::with_entries(vec![
            pending_text("msg-orphan-1", ""),
            pending_text("msg-orphan-2", " "),
        ]);
        let publisher = FakeReplayPublisher::new(false);
        let handler = WalReplayHandler::new(wal.clone(), publisher.clone(), None);

        for round in 1..=3 {
            if round == 2 {
                wal.append(
                    &MessageSubmission {
                        message: pending_text("msg-ok", "tenant-a").message,
                        message_id: "msg-ok".to_string(),
                        timeline: Default::default(),
                    },
                    "tenant-a",
                    None,
                )
                .await
                .unwrap();
            }
            let report = handler.replay_once(10).await.unwrap();
            assert_eq!(report.quarantined, 2, "round {round}: {report:?}");
            assert_eq!(report.failed, 0, "round {round}: {report:?}");
            assert_eq!(
                report.replayed,
                usize::from(round == 2),
                "round {round}: {report:?}"
            );
        }

        for orphan in ["msg-orphan-1", "msg-orphan-2"] {
            assert!(wal.contains(orphan), "{orphan} stays for an operator");
        }
        assert!(!wal.contains("msg-ok"));
        assert_eq!(
            publisher
                .captured()
                .iter()
                .map(|replay| replay.tenant_id.as_str())
                .collect::<Vec<_>>(),
            vec!["tenant-a"],
            "nothing published as tenant 0"
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
                quarantined: 0,
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
                quarantined: 0,
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

    #[tokio::test]
    async fn machine_wal_replay_keeps_original_proof_and_does_not_forge_missing_authority() {
        use ed25519_dalek::{
            SigningKey,
            pkcs8::{EncodePrivateKey, EncodePublicKey},
        };
        use flare_im_service_kit::{
            audit_effects::write_proof::{HEADER, ProofSigner, ProofVerifier, queue_headers},
            platform_send::{PLATFORM_API_KID, SYSTEM_MESSAGE_SCOPE},
        };
        use flare_server_core::{
            SignedActorType, SignedContextIdentity, SignedContextSigner, SignedContextVerifier,
        };
        struct ProofPublisher(Mutex<Vec<String>>);
        impl WalReplayPublisher for ProofPublisher {
            fn push_wal_message<'a>(
                &'a self,
                ctx: &'a flare_im_contracts::Ctx,
                submission: &'a MessageSubmission,
                _: &'a MessageProfile,
                _: PersistenceMode,
            ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
                Box::pin(async move {
                    let headers = queue_headers(ctx, &submission.message, None, HashMap::new())
                        .map_err(flare_server_core::error::FlareError::system)?;
                    self.0.lock().unwrap().push(headers[HEADER].clone());
                    Ok(())
                })
            }
        }
        let key = SigningKey::from_bytes(&[91; 32]);
        let pem = key.to_pkcs8_pem(Default::default()).unwrap();
        let public = key
            .verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap();
        let signer = SignedContextSigner::from_private_key(&pem)
            .unwrap()
            .with_kid(PLATFORM_API_KID)
            .unwrap();
        let mut ring = flare_server_core::auth::KeySet::empty();
        ring.insert(PLATFORM_API_KID, &public).unwrap();
        let verifier = SignedContextVerifier::from_key_set(ring);
        let token = signer
            .sign(
                &SignedContextIdentity::new(
                    SignedActorType::Service,
                    "service-a",
                    "tenant-a",
                    "original-request",
                )
                .with_credential_id("AAAAAAAAAAAA")
                .with_roles(vec![SYSTEM_MESSAGE_SCOPE.into()]),
            )
            .unwrap();
        let mut r = tonic::Request::new(());
        flare_server_core::set_signed_context_token(&mut r, &token).unwrap();
        let ctx =
            flare_server_core::grpc::middleware::signed_context::verify_signed_context_metadata(
                r.metadata(),
                &verifier,
            )
            .unwrap();
        let mut message = pending_text("signed-machine", "tenant-a").message;
        message.server_id = "signed-machine".into();
        message.sender_id = "service-a".into();
        message.content = Some(MessageContent {
            content: Some(Content::System(flare_proto::common::SystemContent {
                event_kind: "platform.notice".into(),
                body: "hello".into(),
                ..Default::default()
            })),
        });
        MessageProfile::ensure(&mut message);
        let proof = ProofSigner::from_pem(&pem)
            .unwrap()
            .stamp(&ctx, &message)
            .unwrap();
        let valid = WalPendingMessage {
            message_id: message.server_id.clone(),
            tenant_id: "tenant-a".into(),
            message: message.clone(),
            write_proof: Some(proof.clone()),
        };
        message.server_id = "missing-proof".into();
        let invalid = WalPendingMessage {
            message_id: message.server_id.clone(),
            tenant_id: "tenant-a".into(),
            message,
            write_proof: None,
        };
        let wal = MemoryWalRepository::with_entries(vec![valid, invalid]);
        let publisher = Arc::new(ProofPublisher(Mutex::new(vec![])));
        let handler = WalReplayHandler::new(wal.clone(), publisher.clone(), None)
            .with_write_verifier(Some(ProofVerifier::from_pem(&public).unwrap()));
        let report = handler.replay_once(10).await.unwrap();
        assert_eq!(report.replayed, 1);
        assert_eq!(report.failed, 1);
        assert_eq!(*publisher.0.lock().unwrap(), [proof]);
        assert!(wal.contains("missing-proof"));
        assert!(!wal.contains("signed-machine"));
    }
}
