//! Plugin card entry rules after the identity check (the PreSend hook's draft is checked again,
//! and a user cannot take the platform's card id), through
//! `MessageIngestHandler::handle_send_message` with a real hook pipeline, idempotency and publish:
//!
//! - a PreSend hook cannot turn a user's message into a plugin card (APP_CARD or Custom), nor
//!   move it to another conversation;
//! - a hook may refuse a platform card post but never rewrite it: what is published is the card
//!   the post context was signed for;
//! - platform card posts and user sends deduplicate in separate namespaces, and a card post
//!   replays only the card with its own signed digest.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
use flare_im_contracts::Ctx;
use flare_im_hooks::hooks::{HookDispatcher, HookRegistry};
use flare_im_hooks::{HookMetadata, HookSelector, MessageDraft, PreSendDecision, PreSendHook};
use flare_im_message_pipeline::MqPushRepository;
use flare_im_seq::SequenceAllocator;
use flare_im_service_kit::metrics::MessageOrchestratorMetrics;
use flare_im_service_kit::plugin_platform;
use flare_message_ingest::application::MessageIngestHandler;
use flare_message_ingest::application::commands::SendMessageCommand;
use flare_message_ingest::application::extension::{
    ExtensionOrchestrator, ExtensionPolicy, ExtensionRouting,
};
use flare_message_ingest::config::SessionCreationMode;
use flare_message_ingest::domain::model::{ConversationType, MessageDefaults};
use flare_message_ingest::domain::repository::{
    IdempotencyBegin, IdempotentRecord, IngestIdempotencyStore, RecipientRepository,
    WalRepositoryItem,
};
use flare_message_ingest::domain::service::{
    ConversationEnsureService, HookExecutionService, MessageIngestService,
    MessageIngestServiceOptions,
};
use flare_message_ingest::infrastructure::persistence::wal_repository_impl::NoopWalRepository;
use flare_proto::common::message_content::Content;
use flare_proto::common::{
    AppCardContent, CustomContent, Message, MessageContent, MessageSource, MessageType, MqEnvelope,
    TextContent, mq_envelope,
};
use flare_server_core::auth::KeySet;
use flare_server_core::error::{ErrorCode, Result};
use flare_server_core::grpc::middleware::signed_context::verify_signed_context_metadata;
use flare_server_core::mq::producer::{Producer, ProducerError, ProducerMessage};
use flare_server_core::{
    Context, SignedActorType, SignedContextIdentity, SignedContextSigner, SignedContextVerifier,
};
use prost::Message as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const GROUP: &str = "2-group-a";

#[derive(Default)]
struct MemoryIdempotency {
    slots: Mutex<HashMap<String, Option<IdempotentRecord>>>,
}

#[async_trait::async_trait]
impl IngestIdempotencyStore for MemoryIdempotency {
    async fn begin(&self, key: &str) -> Result<IdempotencyBegin> {
        let mut slots = self.slots.lock().unwrap();
        Ok(match slots.get(key) {
            None => {
                slots.insert(key.to_string(), None);
                IdempotencyBegin::Fresh
            }
            Some(None) => IdempotencyBegin::InFlight,
            Some(Some(record)) => IdempotencyBegin::Replay(record.clone()),
        })
    }

    async fn commit(&self, key: &str, record: &IdempotentRecord) -> Result<()> {
        self.slots
            .lock()
            .unwrap()
            .insert(key.to_string(), Some(record.clone()));
        Ok(())
    }

    async fn rollback(&self, key: &str) -> Result<()> {
        self.slots.lock().unwrap().remove(key);
        Ok(())
    }
}

/// Every published message, decoded from its main-queue envelope.
#[derive(Default)]
struct CapturingProducer {
    published: Mutex<Vec<Message>>,
}

impl CapturingProducer {
    fn published(&self) -> Vec<Message> {
        self.published.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl Producer for CapturingProducer {
    async fn send(
        &self,
        _ctx: &Ctx,
        _topic: &str,
        _key: Option<&str>,
        payload: Vec<u8>,
        _headers: Option<HashMap<String, String>>,
    ) -> std::result::Result<(), ProducerError> {
        if let Ok(MqEnvelope {
            payload: Some(mq_envelope::Payload::Message(message)),
            ..
        }) = MqEnvelope::decode(payload.as_slice())
        {
            self.published.lock().unwrap().push(message);
        }
        Ok(())
    }

    async fn send_batch(
        &self,
        _ctx: &Ctx,
        _messages: Vec<ProducerMessage>,
    ) -> std::result::Result<(), ProducerError> {
        Ok(())
    }

    fn name(&self) -> &str {
        "capturing-producer"
    }
}

struct NoRecipients;

type Recipients<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

impl RecipientRepository for NoRecipients {
    fn get_message_recipients<'a>(
        &'a self,
        _ctx: &'a Ctx,
        _conversation_id: &'a str,
        _conversation_type: ConversationType,
        _channel_id: Option<&'a str>,
        _sender_id: &'a str,
    ) -> Recipients<'a, Vec<String>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn get_event_recipients<'a>(
        &'a self,
        _ctx: &'a Ctx,
        _message_id: &'a str,
        _conversation_id: &'a str,
        _event_type: flare_proto::common::EventType,
    ) -> Recipients<'a, Vec<String>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn get_conversation_members<'a>(
        &'a self,
        _ctx: &'a Ctx,
        _conversation_id: &'a str,
    ) -> Recipients<'a, Vec<String>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn get_conversation_member_count<'a>(
        &'a self,
        _ctx: &'a Ctx,
        _conversation_id: &'a str,
    ) -> Recipients<'a, usize> {
        Box::pin(async { Ok(0) })
    }
}

/// Enough RESP for the sequence allocator: `EXEC` answers an increasing seq, the rest `+OK`.
async fn spawn_fake_redis() -> String {
    fn next_command(buf: &[u8]) -> Option<(usize, String)> {
        fn line(buf: &[u8], from: usize) -> Option<(&[u8], usize)> {
            let end = buf.get(from..)?.windows(2).position(|w| w == b"\r\n")? + from;
            Some((&buf[from..end], end + 2))
        }
        let (header, mut pos) = line(buf, 0)?;
        let count: usize = std::str::from_utf8(header.strip_prefix(b"*")?)
            .ok()?
            .parse()
            .ok()?;
        let mut name = String::new();
        for index in 0..count {
            let (len_line, next) = line(buf, pos)?;
            let len: usize = std::str::from_utf8(len_line.strip_prefix(b"$")?)
                .ok()?
                .parse()
                .ok()?;
            if buf.len() < next + len + 2 {
                return None;
            }
            if index == 0 {
                name = String::from_utf8_lossy(&buf[next..next + len]).to_ascii_uppercase();
            }
            pos = next + len + 2;
        }
        Some((pos, name))
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seq = Arc::new(AtomicUsize::new(0));
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let seq = Arc::clone(&seq);
            tokio::spawn(async move {
                let mut pending = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    match socket.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => pending.extend_from_slice(&chunk[..n]),
                    }
                    while let Some((consumed, name)) = next_command(&pending) {
                        pending.drain(..consumed);
                        let reply = match name.as_str() {
                            "INCR" => "+QUEUED\r\n".to_string(),
                            "EXEC" => {
                                format!("*1\r\n:{}\r\n", seq.fetch_add(1, Ordering::SeqCst) + 1)
                            }
                            "PING" => "+PONG\r\n".to_string(),
                            _ => "+OK\r\n".to_string(),
                        };
                        if socket.write_all(reply.as_bytes()).await.is_err() {
                            return;
                        }
                    }
                }
            });
        }
    });
    format!("redis://{addr}")
}

/// What the PreSend hook does to the draft.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Rewrite {
    Nothing,
    IntoAppCard,
    IntoCustomCard,
    MoveConversation,
    /// Edit the card payload and its conversation (a word filter, or a hostile endpoint).
    EditEverything,
    Refuse,
}

struct RewritingHook(Rewrite);

#[async_trait::async_trait]
impl PreSendHook for RewritingHook {
    async fn handle(&self, _ctx: &Ctx, draft: &mut MessageDraft) -> PreSendDecision {
        let card = |content: Content| MessageContent {
            content: Some(content),
        };
        match self.0 {
            Rewrite::Nothing => {}
            Rewrite::IntoAppCard => {
                draft.payload = card(Content::AppCard(AppCardContent {
                    app_id: "example.poll".into(),
                    card_type: plugin_platform::PLUGIN_CARD_TYPE.into(),
                    payload_schema: plugin_platform::PLUGIN_CARD_PAYLOAD_SCHEMA.into(),
                    payload: br#"{"pluginId":"example.poll"}"#.to_vec(),
                    ..Default::default()
                }))
                .encode_to_vec();
            }
            Rewrite::IntoCustomCard => {
                draft.payload = card(Content::Custom(CustomContent {
                    r#type: "Flare.Plugin.Message.V1".into(),
                    payload: br#"{"pluginId":"example.poll"}"#.to_vec(),
                    ..Default::default()
                }))
                .encode_to_vec();
            }
            Rewrite::MoveConversation => draft.conversation_id = Some("2-group-b".into()),
            Rewrite::EditEverything => {
                draft.payload = card(Content::AppCard(AppCardContent {
                    app_id: "example.poll".into(),
                    card_type: plugin_platform::PLUGIN_CARD_TYPE.into(),
                    payload_schema: plugin_platform::PLUGIN_CARD_PAYLOAD_SCHEMA.into(),
                    payload: br#"{"pluginId":"example.poll","anonymous":true}"#.to_vec(),
                    ..Default::default()
                }))
                .encode_to_vec();
                draft.conversation_id = Some("2-group-b".into());
            }
            Rewrite::Refuse => {
                return PreSendDecision::Reject {
                    error: flare_im_hooks::pre_send_rejection(
                        flare_server_core::error::ErrorBuilder::new(
                            ErrorCode::OperationFailed,
                            "muted",
                        ),
                    ),
                };
            }
        }
        PreSendDecision::Continue
    }
}

struct Harness {
    handler: MessageIngestHandler,
    store: Arc<MemoryIdempotency>,
    producer: Arc<CapturingProducer>,
}

async fn harness(rewrite: Rewrite) -> Harness {
    let redis = Arc::new(redis::Client::open(spawn_fake_redis().await).unwrap());
    let sequence_allocator = SequenceAllocator::new(redis, 100)
        .await
        .expect("fake redis handshake");
    let producer = Arc::new(CapturingProducer::default());
    let message_ingest_service = Arc::new(MessageIngestService::new(
        MqPushRepository::new(producer.clone()),
        Arc::new(NoRecipients),
        Arc::new(WalRepositoryItem::Noop(Arc::new(NoopWalRepository))),
        Arc::new(sequence_allocator),
        MessageDefaults {
            default_business_type: "im".to_string(),
            default_conversation_type: ConversationType::Group,
            default_sender_type: "user".to_string(),
            default_tenant_id: None,
        },
        MessageIngestServiceOptions::new(),
    ));
    let registry = HookRegistry::new();
    registry
        .register_pre_send(
            HookMetadata::default().with_name("tenant-webhook"),
            HookSelector::default(),
            Arc::new(RewritingHook(rewrite)),
        )
        .await;
    let extension_orchestrator = Arc::new(ExtensionOrchestrator::new(
        Arc::new(HookExecutionService::new(
            Arc::new(HookDispatcher::new(registry)),
            None,
        )),
        ExtensionPolicy::new(true),
        ExtensionRouting::new(Vec::new(), Vec::new()),
    ));
    let store = Arc::new(MemoryIdempotency::default());
    let handler = MessageIngestHandler::new(
        message_ingest_service,
        extension_orchestrator,
        Arc::new(ConversationEnsureService::new(
            None,
            SessionCreationMode::Async,
            None,
        )),
        Arc::new(MessageOrchestratorMetrics::new()),
    )
    .with_idempotency(store.clone());
    Harness {
        handler,
        store,
        producer,
    }
}

fn user_ctx() -> Ctx {
    Arc::new(
        Context::with_request_id("req-user")
            .with_tenant_id("tenant-a")
            .with_user_id("alice"),
    )
}

/// Capability's card post context for exactly `card`, verified like Ingest's send handler does.
fn card_post_ctx(card: &Message) -> Ctx {
    let key = SigningKey::from_bytes(&[71; 32]);
    let signer =
        SignedContextSigner::from_private_key(&key.to_pkcs8_pem(Default::default()).unwrap())
            .unwrap()
            .with_kid(plugin_platform::CARD_POST_KID)
            .unwrap();
    let mut keys = KeySet::empty();
    keys.insert(
        plugin_platform::CARD_POST_KID,
        &key.verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap(),
    )
    .unwrap();
    let token = signer
        .sign(
            &SignedContextIdentity::new(
                SignedActorType::User,
                &card.sender_id,
                "tenant-a",
                plugin_platform::card_post_request_id("tenant-a", card).unwrap(),
            )
            .with_roles(vec![plugin_platform::CARD_POST_ROLE.into()]),
        )
        .unwrap();
    let mut carrier = tonic::Request::new(());
    carrier
        .metadata_mut()
        .insert("x-flare-ctx", token.parse().unwrap());
    verify_signed_context_metadata(
        carrier.metadata(),
        &SignedContextVerifier::from_key_set(keys),
    )
    .unwrap()
}

fn text(client_msg_id: &str) -> Message {
    Message {
        conversation_id: GROUP.to_string(),
        conversation_type: flare_proto::common::ConversationType::Group as i32,
        sender_id: "alice".to_string(),
        client_msg_id: client_msg_id.to_string(),
        source: MessageSource::User as i32,
        message_type: MessageType::Text as i32,
        content: Some(MessageContent {
            content: Some(Content::Text(TextContent {
                text: "hello".to_string(),
                mentions: vec![],
            })),
        }),
        ..Default::default()
    }
}

/// The platform card Capability posts (the shape `verify_card_post` admits).
fn platform_card(client_msg_id: &str, payload: &[u8]) -> Message {
    Message {
        conversation_id: GROUP.to_string(),
        conversation_type: flare_proto::common::ConversationType::Group as i32,
        channel_id: GROUP.to_string(),
        sender_id: "alice".to_string(),
        client_msg_id: client_msg_id.to_string(),
        source: MessageSource::User as i32,
        message_type: MessageType::AppCard as i32,
        content: Some(MessageContent {
            content: Some(Content::AppCard(AppCardContent {
                app_id: "example.poll".into(),
                card_type: plugin_platform::PLUGIN_CARD_TYPE.into(),
                payload_schema: plugin_platform::PLUGIN_CARD_PAYLOAD_SCHEMA.into(),
                payload: payload.to_vec(),
                ..Default::default()
            })),
        }),
        ..Default::default()
    }
}

fn command(message: Message) -> SendMessageCommand {
    SendMessageCommand {
        conversation_id: message.conversation_id.clone(),
        message,
        burn_enabled: false,
        burn_after_read_seconds: None,
    }
}

fn reason(error: &flare_server_core::error::FlareError) -> String {
    error.reason().to_string()
}

#[tokio::test]
async fn a_hook_cannot_turn_a_user_message_into_a_plugin_card() {
    for rewrite in [Rewrite::IntoAppCard, Rewrite::IntoCustomCard] {
        let h = harness(rewrite).await;
        let error = h
            .handler
            .handle_send_message(&user_ctx(), command(text("cm-1")))
            .await
            .expect_err("a hook-made plugin card must not be sent");
        assert_eq!(error.code(), Some(ErrorCode::PermissionDenied));
        assert_eq!(reason(&error), plugin_platform::PLUGIN_CARD_PLATFORM_ONLY);
        assert!(h.producer.published().is_empty());
    }
    // The same pipeline without a rewrite sends the text.
    let h = harness(Rewrite::Nothing).await;
    h.handler
        .handle_send_message(&user_ctx(), command(text("cm-1")))
        .await
        .expect("plain text");
    assert_eq!(h.producer.published().len(), 1);
}

#[tokio::test]
async fn a_hook_cannot_move_a_message_to_another_conversation() {
    let h = harness(Rewrite::MoveConversation).await;
    let error = h
        .handler
        .handle_send_message(&user_ctx(), command(text("cm-1")))
        .await
        .expect_err("a hook must not reroute a message");
    assert_eq!(error.code(), Some(ErrorCode::PermissionDenied));
    assert!(h.producer.published().is_empty());
}

#[tokio::test]
async fn a_hook_may_refuse_a_platform_card_but_never_rewrite_it() {
    let card = platform_card("pcard-0001", br#"{"pluginId":"example.poll"}"#);
    let h = harness(Rewrite::EditEverything).await;
    let outcome = h
        .handler
        .handle_send_message(&card_post_ctx(&card), command(card.clone()))
        .await
        .expect("the signed card goes out");
    let published = h.producer.published();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].server_id, outcome.message_id);
    assert_eq!(published[0].conversation_id, GROUP);
    assert_eq!(published[0].content, card.content, "the signed card bytes");

    let h = harness(Rewrite::Refuse).await;
    assert!(
        h.handler
            .handle_send_message(&card_post_ctx(&card), command(card.clone()))
            .await
            .is_err()
    );
    assert!(h.producer.published().is_empty());
}

/// A user who used the derived card id first does not capture the card: the platform's
/// post deduplicates in its own namespace, so the real card is sent and its id is the one
/// Capability binds the entity to.
#[tokio::test]
async fn a_user_send_never_replays_as_the_platform_card() {
    let h = harness(Rewrite::Nothing).await;
    // The send handler refuses the reserved id from users; simulate a record that got there by
    // another route (an older Ingest, a replayed key) under the user namespace.
    let user = h
        .handler
        .handle_send_message(&user_ctx(), command(text("pcard-0002")))
        .await
        .expect("handler-level user send");
    let card = platform_card("pcard-0002", br#"{"pluginId":"example.poll"}"#);
    let posted = h
        .handler
        .handle_send_message(&card_post_ctx(&card), command(card.clone()))
        .await
        .expect("the platform card is sent");
    assert_ne!(posted.message_id, user.message_id);
    let published = h.producer.published();
    assert_eq!(published.len(), 2);
    assert_eq!(published[1].server_id, posted.message_id);
    assert_eq!(published[1].content, card.content);
    // A retry of the post replays the card, not the text.
    let replay = h
        .handler
        .handle_send_message(&card_post_ctx(&card), command(card.clone()))
        .await
        .expect("replay");
    assert_eq!(replay.message_id, posted.message_id);
    assert_eq!(h.producer.published().len(), 2);
    let keys: Vec<String> = h.store.slots.lock().unwrap().keys().cloned().collect();
    assert!(
        keys.iter()
            .any(|k| k.starts_with("idem-pcard:tenant-a:alice:")),
        "{keys:?}"
    );
    assert!(
        keys.iter().any(|k| k.starts_with("idem:tenant-a:alice:")),
        "{keys:?}"
    );
}

/// A card post replays only the card its own digest signed: another card under the same client
/// message id (a non-deterministic plugin result) is refused instead of answering the first card.
#[tokio::test]
async fn a_card_post_replays_only_its_own_card() {
    let h = harness(Rewrite::Nothing).await;
    let first = platform_card("pcard-0003", br#"{"pluginId":"example.poll","v":1}"#);
    let posted = h
        .handler
        .handle_send_message(&card_post_ctx(&first), command(first.clone()))
        .await
        .expect("first card");
    let other = platform_card("pcard-0003", br#"{"pluginId":"example.poll","v":2}"#);
    let error = h
        .handler
        .handle_send_message(&card_post_ctx(&other), command(other))
        .await
        .expect_err("another card under the same id");
    assert_eq!(error.code(), Some(ErrorCode::PermissionDenied));
    assert_eq!(h.producer.published().len(), 1);
    let replay = h
        .handler
        .handle_send_message(&card_post_ctx(&first), command(first))
        .await
        .expect("the same card replays");
    assert_eq!(replay.message_id, posted.message_id);
    assert_eq!(replay.durability, posted.durability);
}
