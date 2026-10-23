//! Who may do what to which message: edit, recall and every other message operation. Every
//! refusal here is a well-formed event that only fails on authorization (a malformed event would
//! make a refusal pass for the wrong reason): the same event from the right operator passes.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
use flare_proto::common::event::Payload;
use flare_proto::common::message_content::Content;
use flare_proto::common::{
    AppCardContent, ConversationUpdateEvent, CustomContent, CustomEvent, DeleteScope, DeleteType,
    Event, EventType, ImageContent, MarkEvent, Message, MessageContent, MessageDeleteEvent,
    MessageEditEvent, MessageRecallEvent, MessageRetentionExpiredEvent, PinEvent, ReactionEvent,
    ReadReceiptEvent, TextContent, UnmarkEvent, UnpinEvent,
};
use flare_server_core::auth::KeySet;
use flare_server_core::context::{ActorContext, ActorType};
use flare_server_core::error::ErrorCode;
use flare_server_core::{
    Context, SignedActorType, SignedContextIdentity, SignedContextSigner, SignedContextVerifier,
};

use super::*;

const GROUP: &str = "2-group-a";
const OTHER_GROUP: &str = "2-group-b";
/// The fixed clock of these tests.
const NOW: i64 = 1_800_000_000_000;

#[derive(Default)]
struct Messages {
    stored: Mutex<HashMap<String, Message>>,
    reads: Mutex<usize>,
    /// The ids of every batch lookup, one entry per lookup.
    batches: Mutex<Vec<Vec<String>>>,
}

impl Messages {
    fn put(&self, message: Message) {
        self.stored
            .lock()
            .unwrap()
            .insert(message.server_id.clone(), message);
    }

    fn reads(&self) -> usize {
        *self.reads.lock().unwrap()
    }

    fn batches(&self) -> Vec<Vec<String>> {
        self.batches.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl MessageTargetRepository for Messages {
    async fn message(
        &self,
        _ctx: &Ctx,
        tenant_id: &str,
        server_msg_id: &str,
    ) -> Result<Option<Message>> {
        assert_eq!(tenant_id, "tenant-a");
        *self.reads.lock().unwrap() += 1;
        Ok(self.stored.lock().unwrap().get(server_msg_id).cloned())
    }

    async fn conversations_of(
        &self,
        _ctx: &Ctx,
        tenant_id: &str,
        server_msg_ids: &[String],
    ) -> Result<HashMap<String, String>> {
        assert_eq!(tenant_id, "tenant-a");
        self.batches.lock().unwrap().push(server_msg_ids.to_vec());
        let stored = self.stored.lock().unwrap();
        Ok(server_msg_ids
            .iter()
            .filter_map(|id| {
                stored
                    .get(id)
                    .map(|message| (id.clone(), message.conversation_id.clone()))
            })
            .collect())
    }
}

/// In-seat members by (conversation, user); records every lookup; `down` fails them.
#[derive(Default)]
struct Members {
    seated: Mutex<Vec<(String, String)>>,
    asked: Mutex<Vec<(String, String, String)>>,
    /// How recent each lookup asked the answer to be.
    reads: Mutex<Vec<MembershipRead>>,
    down: Mutex<bool>,
}

impl Members {
    fn seat(&self, conversation: &str, user: &str) {
        self.seated
            .lock()
            .unwrap()
            .push((conversation.into(), user.into()));
    }

    fn asked(&self) -> Vec<(String, String, String)> {
        self.asked.lock().unwrap().clone()
    }

    fn reads(&self) -> Vec<MembershipRead> {
        self.reads.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl ConversationMembershipRepository for Members {
    async fn is_member(
        &self,
        _ctx: &Ctx,
        tenant_id: &str,
        conversation_id: &str,
        user_id: &str,
        read: MembershipRead,
    ) -> Result<bool> {
        self.asked
            .lock()
            .unwrap()
            .push((tenant_id.into(), conversation_id.into(), user_id.into()));
        self.reads.lock().unwrap().push(read);
        if *self.down.lock().unwrap() {
            return Err(ErrorBuilder::new(ErrorCode::ServiceUnavailable, "down").build_error());
        }
        Ok(self
            .seated
            .lock()
            .unwrap()
            .contains(&(conversation_id.to_owned(), user_id.to_owned())))
    }
}

fn text(text: &str) -> MessageContent {
    MessageContent {
        content: Some(Content::Text(TextContent {
            text: text.into(),
            mentions: vec![],
        })),
    }
}

fn plugin_app_card(card_type: &str) -> MessageContent {
    MessageContent {
        content: Some(Content::AppCard(AppCardContent {
            app_id: "example.poll".into(),
            card_type: card_type.into(),
            payload_schema: plugin_platform::PLUGIN_CARD_PAYLOAD_SCHEMA.into(),
            payload: br#"{"pluginId":"example.poll"}"#.to_vec(),
            ..Default::default()
        })),
    }
}

fn plugin_custom(kind: &str) -> MessageContent {
    MessageContent {
        content: Some(Content::Custom(CustomContent {
            r#type: kind.into(),
            payload: br#"{"pluginId":"example.poll"}"#.to_vec(),
            ..Default::default()
        })),
    }
}

fn stored(
    id: &str,
    sender: &str,
    conversation: &str,
    age_ms: i64,
    content: MessageContent,
) -> Message {
    Message {
        server_id: id.into(),
        conversation_id: conversation.into(),
        sender_id: sender.into(),
        created_at: NOW - age_ms,
        content: Some(content),
        ..Default::default()
    }
}

struct Fixture {
    policy: MessageOperationPolicy,
    messages: Arc<Messages>,
    members: Arc<Members>,
}

fn fixture() -> Fixture {
    let messages = Arc::new(Messages::default());
    messages.put(stored("m-alice", "alice", GROUP, 60_000, text("hi")));
    messages.put(stored(
        "m-old",
        "alice",
        GROUP,
        RECALL_WINDOW_MS + 1,
        text("old"),
    ));
    messages.put(stored(
        "m-elsewhere",
        "alice",
        OTHER_GROUP,
        60_000,
        text("there"),
    ));
    messages.put(stored(
        "m-card",
        "alice",
        GROUP,
        60_000,
        plugin_app_card(plugin_platform::PLUGIN_CARD_TYPE),
    ));
    messages.put(stored(
        "m-custom-card",
        "alice",
        GROUP,
        60_000,
        plugin_custom("Flare.Plugin.Message.V1"),
    ));
    messages.put(stored(
        "m-image",
        "alice",
        GROUP,
        60_000,
        MessageContent {
            content: Some(Content::Image(ImageContent::default())),
        },
    ));
    let members = Arc::new(Members::default());
    members.seat(GROUP, "alice");
    members.seat(GROUP, "mallory");
    members.seat(OTHER_GROUP, "alice");
    Fixture {
        policy: MessageOperationPolicy::new(Some(messages.clone()))
            .with_members(Some(members.clone()))
            .with_clock(|| NOW),
        messages,
        members,
    }
}

/// The legacy (unsigned) context the signaling route forwards for a connected user.
fn user(user: &str) -> Ctx {
    Arc::new(
        Context::with_request_id("req")
            .with_tenant_id("tenant-a")
            .with_user_id(user)
            .with_actor(ActorContext::new(user).with_type(ActorType::User)),
    )
}

/// Unsigned metadata claiming the system actor: what any internal process can send.
fn unsigned_system() -> Ctx {
    Arc::new(
        Context::with_request_id("req")
            .with_tenant_id("tenant-a")
            .with_actor(ActorContext::new("flare-scheduler").with_type(ActorType::System)),
    )
}

/// A context verified from a token signed under `kid` (`None`: the legacy Cell key).
fn signed(kid: Option<&str>, identity: SignedContextIdentity) -> Ctx {
    let key = SigningKey::from_bytes(&[23; 32]);
    let mut signer =
        SignedContextSigner::from_private_key(&key.to_pkcs8_pem(Default::default()).unwrap())
            .unwrap();
    let public = key
        .verifying_key()
        .to_public_key_pem(Default::default())
        .unwrap();
    let verifier = match kid {
        Some(kid) => {
            signer = signer.with_kid(kid).unwrap();
            let mut keys = KeySet::empty();
            keys.insert(kid, &public).unwrap();
            SignedContextVerifier::from_key_set(keys)
        }
        None => SignedContextVerifier::from_public_key(&public).unwrap(),
    };
    let token = signer.sign(&identity).unwrap();
    let mut carrier = tonic::Request::new(());
    carrier
        .metadata_mut()
        .insert("x-flare-ctx", token.parse().unwrap());
    flare_server_core::grpc::middleware::signed_context::verify_signed_context_metadata(
        carrier.metadata(),
        &verifier,
    )
    .unwrap()
}

fn signed_as(actor_type: SignedActorType, actor: &str) -> Ctx {
    signed(
        None,
        SignedContextIdentity::new(actor_type, actor, "tenant-a", "req"),
    )
}

fn event(payload: Payload, event_type: EventType) -> Event {
    Event {
        conversation_id: GROUP.into(),
        r#type: event_type as i32,
        event_id: "e-1".into(),
        created_at: NOW,
        payload: Some(payload),
        ..Default::default()
    }
}

fn recall(id: &str, reason: &str) -> Event {
    event(
        Payload::Recall(MessageRecallEvent {
            server_msg_id: id.into(),
            reason: reason.into(),
            time_limit_seconds: Some(i32::MAX),
            allow_admin_recall: Some(true),
        }),
        EventType::EventMessageRecall,
    )
}

fn edit(id: &str, content: MessageContent) -> Event {
    event(
        Payload::Edit(MessageEditEvent {
            server_msg_id: id.into(),
            new_content: Some(content),
            ..Default::default()
        }),
        EventType::EventMessageEdit,
    )
}

fn reason_of(error: &FlareError) -> String {
    error.reason().to_string()
}

async fn refused(policy: &MessageOperationPolicy, ctx: &Ctx, event: Event) -> FlareError {
    policy
        .authorize(ctx, "tenant-a", event)
        .await
        .expect_err("refused")
}

#[tokio::test]
async fn only_the_sender_recalls_within_the_window_and_client_limits_grant_nothing() {
    let f = fixture();
    // Another member, with the client's own "unlimited" window and admin flag.
    let error = refused(&f.policy, &user("mallory"), recall("m-alice", "")).await;
    assert_eq!(error.code(), Some(ErrorCode::PermissionDenied));
    assert_eq!(reason_of(&error), reason::NOT_SENDER);
    // The sender, past the server window.
    let error = refused(&f.policy, &user("alice"), recall("m-old", "")).await;
    assert_eq!(reason_of(&error), reason::WINDOW_EXPIRED);
    // The sender naming the message from another conversation.
    let error = refused(&f.policy, &user("alice"), recall("m-elsewhere", "")).await;
    assert_eq!(reason_of(&error), reason::CONVERSATION_MISMATCH);
    // The sender inside the window: accepted, the client's grants cleared.
    let accepted = f
        .policy
        .authorize(&user("alice"), "tenant-a", recall("m-alice", ""))
        .await
        .expect("own recall");
    let Some(Payload::Recall(recall_event)) = accepted.payload else {
        panic!("recall payload");
    };
    assert_eq!(recall_event.time_limit_seconds, None);
    assert_eq!(recall_event.allow_admin_recall, None);
    // The sender may recall their own plugin card (the plugin is told and refunds or cancels).
    f.policy
        .authorize(&user("alice"), "tenant-a", recall("m-card", ""))
        .await
        .expect("own card");
}

#[tokio::test]
async fn trusted_services_recall_with_a_reason_and_unsigned_system_metadata_is_not_trusted() {
    let f = fixture();
    for ctx in [
        signed_as(SignedActorType::System, "flare-scheduler"),
        signed_as(SignedActorType::Service, "flare-social-bridge"),
    ] {
        // Any member's message, past the sender window, with a reason.
        f.policy
            .authorize(&ctx, "tenant-a", recall("m-old", "group admin removed it"))
            .await
            .expect("trusted recall");
        let error = refused(&f.policy, &ctx, recall("m-old", " ")).await;
        assert_eq!(reason_of(&error), reason::REASON_REQUIRED);
        // Still bound to the conversation.
        let error = refused(&f.policy, &ctx, recall("m-elsewhere", "x")).await;
        assert_eq!(reason_of(&error), reason::CONVERSATION_MISMATCH);
    }
    // A Service the policy does not trust, unsigned System metadata, and a user claiming to be
    // trusted in metadata: none of them recalls another member's message.
    for ctx in [
        signed_as(SignedActorType::Service, "flare-media"),
        unsigned_system(),
    ] {
        let error = refused(&f.policy, &ctx, recall("m-alice", "x")).await;
        assert_eq!(error.code(), Some(ErrorCode::PermissionDenied));
    }
}

#[tokio::test]
async fn capability_contexts_never_operate_on_messages() {
    let f = fixture();
    // A card post token replayed at the Orchestrator, and the shared state publisher.
    let card_post = signed(
        Some(plugin_platform::CARD_POST_KID),
        SignedContextIdentity::new(SignedActorType::User, "alice", "tenant-a", "req")
            .with_roles(vec![plugin_platform::CARD_POST_ROLE.into()]),
    );
    let publisher = signed(
        Some(plugin_platform::CARD_POST_KID),
        SignedContextIdentity::new(
            SignedActorType::System,
            plugin_platform::STATE_PUBLISHER_ID,
            "tenant-a",
            "req",
        )
        .with_roles(vec![plugin_platform::STATE_PUBLISH_ROLE.into()]),
    );
    for ctx in [card_post, publisher] {
        let error = refused(&f.policy, &ctx, recall("m-alice", "x")).await;
        assert_eq!(reason_of(&error), reason::FORBIDDEN);
        let error = refused(&f.policy, &ctx, edit("m-alice", text("x"))).await;
        assert_eq!(reason_of(&error), reason::FORBIDDEN);
    }
    // The same recall from the sender passes.
    f.policy
        .authorize(&user("alice"), "tenant-a", recall("m-alice", ""))
        .await
        .expect("sender");
}

#[tokio::test]
async fn edits_are_the_senders_on_text_only_and_never_touch_a_plugin_card() {
    let f = fixture();
    let alice = user("alice");
    f.policy
        .authorize(&alice, "tenant-a", edit("m-alice", text("fixed")))
        .await
        .expect("own text edit");
    let error = refused(&f.policy, &user("mallory"), edit("m-alice", text("x"))).await;
    assert_eq!(reason_of(&error), reason::NOT_SENDER);
    let error = refused(&f.policy, &alice, edit("m-old", text("x"))).await;
    assert_eq!(reason_of(&error), reason::WINDOW_EXPIRED);
    let error = refused(&f.policy, &alice, edit("m-elsewhere", text("x"))).await;
    assert_eq!(reason_of(&error), reason::CONVERSATION_MISMATCH);
    // A plugin card is never edited, even by its sender (the creator) or a trusted service.
    for target in ["m-card", "m-custom-card"] {
        for ctx in [
            alice.clone(),
            signed_as(SignedActorType::Service, "flare-social-bridge"),
        ] {
            let error = refused(&f.policy, &ctx, edit(target, text("x"))).await;
            assert_eq!(
                reason_of(&error),
                reason::PLUGIN_CARD_NOT_EDITABLE,
                "{target}"
            );
        }
    }
    // No edit produces a plugin card, in any spelling of its type.
    for content in [
        plugin_app_card(plugin_platform::PLUGIN_CARD_TYPE),
        plugin_app_card(" FLARE.plugin.message.v1"),
        plugin_custom(plugin_platform::PLUGIN_CARD_TYPE),
        plugin_custom("flare.plugin.message.V1 "),
    ] {
        let error = refused(&f.policy, &alice, edit("m-alice", content)).await;
        assert_eq!(reason_of(&error), reason::PLUGIN_CARD_NOT_EDITABLE);
    }
    // Only text-like messages, into text-like content.
    let error = refused(&f.policy, &alice, edit("m-image", text("x"))).await;
    assert_eq!(reason_of(&error), reason::NOT_EDITABLE);
    let error = refused(
        &f.policy,
        &alice,
        edit(
            "m-alice",
            MessageContent {
                content: Some(Content::Image(ImageContent::default())),
            },
        ),
    )
    .await;
    assert_eq!(reason_of(&error), reason::NOT_EDITABLE);
}

fn delete(id: &str, kind: DeleteType, scope: Option<DeleteScope>, target: Option<&str>) -> Event {
    event(
        Payload::Delete(MessageDeleteEvent {
            server_msg_id: id.into(),
            delete_type: Some(kind as i32),
            scope: scope.map(|s| s as i32),
            target_user_id: target.map(str::to_owned),
            ..Default::default()
        }),
        EventType::EventMessageDelete,
    )
}

#[tokio::test]
async fn deletes_follow_their_scope() {
    let f = fixture();
    // A hard delete is a trusted service's.
    let error = refused(
        &f.policy,
        &user("alice"),
        delete("m-alice", DeleteType::Hard, None, None),
    )
    .await;
    assert_eq!(reason_of(&error), reason::FORBIDDEN);
    f.policy
        .authorize(
            &signed_as(SignedActorType::System, "flare-retention"),
            "tenant-a",
            delete("m-alice", DeleteType::Hard, None, None),
        )
        .await
        .expect("trusted hard delete");
    // A conversation-wide soft delete is the sender's, like a recall.
    let global = || {
        delete(
            "m-alice",
            DeleteType::Soft,
            Some(DeleteScope::ConversationGlobal),
            None,
        )
    };
    let error = refused(&f.policy, &user("mallory"), global()).await;
    assert_eq!(reason_of(&error), reason::NOT_SENDER);
    f.policy
        .authorize(&user("alice"), "tenant-a", global())
        .await
        .expect("sender global delete");
    // A private delete hides only the operator's own view.
    let error = refused(
        &f.policy,
        &user("mallory"),
        delete(
            "m-alice",
            DeleteType::Soft,
            Some(DeleteScope::UserPrivate),
            Some("alice"),
        ),
    )
    .await;
    assert_eq!(reason_of(&error), reason::IDENTITY_MISMATCH);
    let own = f
        .policy
        .authorize(
            &user("mallory"),
            "tenant-a",
            delete(
                "m-alice",
                DeleteType::Soft,
                Some(DeleteScope::UserPrivate),
                None,
            ),
        )
        .await
        .expect("own view");
    let Some(Payload::Delete(own)) = own.payload else {
        panic!("delete payload");
    };
    assert_eq!(own.target_user_id.as_deref(), Some("mallory"));
}

#[tokio::test]
async fn user_fields_name_the_operator_and_messages_their_conversation() {
    let f = fixture();
    let mallory = user("mallory");
    let named_alice = [
        event(
            Payload::Reaction(ReactionEvent {
                server_msg_id: "m-alice".into(),
                user_id: "alice".into(),
                emoji: "+1".into(),
                action: 0,
            }),
            EventType::EventReaction,
        ),
        event(
            Payload::Pin(PinEvent {
                server_msg_id: "m-alice".into(),
                pinned_by: "alice".into(),
                ..Default::default()
            }),
            EventType::EventPin,
        ),
        event(
            Payload::Unpin(UnpinEvent {
                server_msg_id: "m-alice".into(),
                unpinned_by: "alice".into(),
                ..Default::default()
            }),
            EventType::EventUnpin,
        ),
        event(
            Payload::Mark(MarkEvent {
                server_msg_id: "m-alice".into(),
                user_id: "alice".into(),
                ..Default::default()
            }),
            EventType::EventMark,
        ),
        event(
            Payload::Unmark(UnmarkEvent {
                server_msg_id: "m-alice".into(),
                user_id: "alice".into(),
                ..Default::default()
            }),
            EventType::EventUnmark,
        ),
        event(
            Payload::Read(ReadReceiptEvent {
                conversation_id: GROUP.into(),
                read_seq: 3,
                user_id: "alice".into(),
                ..Default::default()
            }),
            EventType::EventReadReceipt,
        ),
        event(
            Payload::Custom(CustomEvent {
                namespace: "biz".into(),
                name: "ping".into(),
                attributes: [("user_id".to_string(), "alice".to_string())].into(),
                ..Default::default()
            }),
            EventType::EventCustom,
        ),
    ];
    for named in named_alice {
        let event_type = named.r#type;
        let error = refused(&f.policy, &mallory, named.clone()).await;
        assert_eq!(reason_of(&error), reason::IDENTITY_MISMATCH, "{event_type}");
        f.policy
            .authorize(&user("alice"), "tenant-a", named)
            .await
            .unwrap_or_else(|e| panic!("{event_type}: {e}"));
    }
    // An empty user field becomes the operator.
    let filled = f
        .policy
        .authorize(
            &mallory,
            "tenant-a",
            event(
                Payload::Reaction(ReactionEvent {
                    server_msg_id: "m-alice".into(),
                    emoji: "+1".into(),
                    ..Default::default()
                }),
                EventType::EventReaction,
            ),
        )
        .await
        .expect("own reaction");
    assert!(matches!(filled.payload, Some(Payload::Reaction(ref r)) if r.user_id == "mallory"));
    // A reaction or pin on a message of another conversation.
    let error = refused(
        &f.policy,
        &mallory,
        event(
            Payload::Pin(PinEvent {
                server_msg_id: "m-elsewhere".into(),
                ..Default::default()
            }),
            EventType::EventPin,
        ),
    )
    .await;
    assert_eq!(reason_of(&error), reason::CONVERSATION_MISMATCH);
    // A receipt for another conversation than the event's.
    let error = refused(
        &f.policy,
        &mallory,
        event(
            Payload::Read(ReadReceiptEvent {
                conversation_id: OTHER_GROUP.into(),
                read_seq: 3,
                ..Default::default()
            }),
            EventType::EventReadReceipt,
        ),
    )
    .await;
    assert_eq!(reason_of(&error), reason::CONVERSATION_MISMATCH);
}

#[tokio::test]
async fn conversation_retention_and_message_events_are_trusted_services_only() {
    let f = fixture();
    let trusted_only = [
        event(
            Payload::Conversation(ConversationUpdateEvent {
                conversation_id: GROUP.into(),
                title: "pwned".into(),
                ..Default::default()
            }),
            EventType::EventConversationUpdate,
        ),
        event(
            Payload::RetentionExpired(MessageRetentionExpiredEvent {
                conversation_id: GROUP.into(),
                server_msg_id: "m-alice".into(),
                ..Default::default()
            }),
            EventType::EventMessageRetentionExpired,
        ),
        event(
            Payload::Message(stored("m-x", "alice", GROUP, 0, text("bypass ingest"))),
            EventType::EventMessage,
        ),
    ];
    for candidate in trusted_only {
        let event_type = candidate.r#type;
        let error = refused(&f.policy, &user("alice"), candidate.clone()).await;
        assert_eq!(reason_of(&error), reason::FORBIDDEN, "{event_type}");
        let error = refused(&f.policy, &unsigned_system(), candidate.clone()).await;
        assert_eq!(
            error.code(),
            Some(ErrorCode::PermissionDenied),
            "{event_type}"
        );
        f.policy
            .authorize(
                &signed_as(SignedActorType::System, "flare-scheduler"),
                "tenant-a",
                candidate,
            )
            .await
            .unwrap_or_else(|e| panic!("{event_type}: {e}"));
    }
}

#[tokio::test]
async fn an_unreadable_or_unstored_target_is_retryable_never_allowed() {
    let f = fixture();
    let error = refused(&f.policy, &user("alice"), recall("m-not-yet", "")).await;
    assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable));
    let blind = MessageOperationPolicy::new(None).with_clock(|| NOW);
    let error = refused(&blind, &user("alice"), recall("m-alice", "")).await;
    assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable));
    // A payload-less event is malformed, whoever sends it.
    let mut empty = recall("m-alice", "");
    empty.payload = None;
    let error = refused(&f.policy, &user("alice"), empty).await;
    assert_eq!(error.code(), Some(ErrorCode::InvalidParameter));
    // Checks that need no message read none.
    let before = f.messages.reads();
    f.policy
        .authorize(
            &user("alice"),
            "tenant-a",
            event(
                Payload::Read(ReadReceiptEvent {
                    conversation_id: GROUP.into(),
                    read_seq: 1,
                    ..Default::default()
                }),
                EventType::EventReadReceipt,
            ),
        )
        .await
        .expect("receipt");
    assert_eq!(f.messages.reads(), before);
}

fn reaction(id: &str) -> Event {
    event(
        Payload::Reaction(ReactionEvent {
            server_msg_id: id.into(),
            emoji: "+1".into(),
            ..Default::default()
        }),
        EventType::EventReaction,
    )
}

fn receipt(message_ids: &[&str]) -> Event {
    event(
        Payload::Read(ReadReceiptEvent {
            conversation_id: GROUP.into(),
            read_seq: 3,
            message_ids: message_ids.iter().map(|id| id.to_string()).collect(),
            ..Default::default()
        }),
        EventType::EventReadReceipt,
    )
}

/// Every event a user sends, each well formed and acceptable from a member.
fn member_events() -> Vec<Event> {
    vec![
        reaction("m-alice"),
        event(
            Payload::Pin(PinEvent {
                server_msg_id: "m-alice".into(),
                ..Default::default()
            }),
            EventType::EventPin,
        ),
        event(
            Payload::Unpin(UnpinEvent {
                server_msg_id: "m-alice".into(),
                ..Default::default()
            }),
            EventType::EventUnpin,
        ),
        event(
            Payload::Mark(MarkEvent {
                server_msg_id: "m-alice".into(),
                ..Default::default()
            }),
            EventType::EventMark,
        ),
        event(
            Payload::Unmark(UnmarkEvent {
                server_msg_id: "m-alice".into(),
                ..Default::default()
            }),
            EventType::EventUnmark,
        ),
        receipt(&[]),
        receipt(&["m-alice"]),
        event(
            Payload::Custom(CustomEvent {
                namespace: "biz".into(),
                name: "ping".into(),
                ..Default::default()
            }),
            EventType::EventCustom,
        ),
        delete(
            "m-alice",
            DeleteType::Soft,
            Some(DeleteScope::UserPrivate),
            None,
        ),
        recall("m-alice", ""),
        edit("m-alice", text("fixed")),
    ]
}

/// A user outside the conversation is refused every event before any message is read, by a
/// lookup of that one user; the same events from the member sender pass; a trusted service is not
/// looked up at all.
#[tokio::test]
async fn only_members_operate_and_only_the_operator_is_looked_up() {
    let f = fixture();
    // Eve is a member elsewhere, not here.
    f.members.seat(OTHER_GROUP, "eve");
    for forged in member_events() {
        let event_type = forged.r#type;
        let error = refused(&f.policy, &user("eve"), forged).await;
        assert_eq!(
            error.code(),
            Some(ErrorCode::PermissionDenied),
            "{event_type}"
        );
        assert_eq!(reason_of(&error), reason::NOT_MEMBER, "{event_type}");
    }
    assert_eq!(f.messages.reads(), 0, "a non-member costs no message read");
    assert!(f.messages.batches().is_empty(), "nor a batch lookup");
    let asked = f.members.asked();
    assert_eq!(asked.len(), member_events().len());
    assert!(
        asked
            .iter()
            .all(|key| *key == ("tenant-a".into(), GROUP.into(), "eve".into()))
    );
    // A signed user context outside the conversation is refused the same way.
    let error = refused(
        &f.policy,
        &signed_as(SignedActorType::User, "eve"),
        reaction("m-alice"),
    )
    .await;
    assert_eq!(reason_of(&error), reason::NOT_MEMBER);

    for accepted in member_events() {
        let event_type = accepted.r#type;
        f.policy
            .authorize(&user("alice"), "tenant-a", accepted)
            .await
            .unwrap_or_else(|e| panic!("{event_type}: {e}"));
    }

    let before = f.members.asked().len();
    for ctx in [
        signed_as(SignedActorType::System, "flare-scheduler"),
        signed_as(SignedActorType::Service, "flare-social-bridge"),
    ] {
        f.policy
            .authorize(&ctx, "tenant-a", recall("m-alice", "moderation"))
            .await
            .expect("trusted recall");
    }
    assert_eq!(
        f.members.asked().len(),
        before,
        "trusted services are not members"
    );
}

/// Read receipts and reactions may take a recent membership answer; every other event (edit,
/// recall, delete, pin, unpin, mark, unmark, custom, and kinds this code does not know) is checked
/// against the conversation service now, so a member just removed can do none of them.
#[tokio::test]
async fn only_receipts_and_reactions_take_a_recent_membership_answer() {
    let f = fixture();
    for accepted in member_events() {
        let event_type = accepted.r#type;
        let expected = if matches!(
            accepted.payload,
            Some(Payload::Read(_) | Payload::Reaction(_))
        ) {
            MembershipRead::Recent
        } else {
            MembershipRead::Current
        };
        f.policy
            .authorize(&user("alice"), "tenant-a", accepted)
            .await
            .unwrap_or_else(|e| panic!("{event_type}: {e}"));
        assert_eq!(f.members.reads().last(), Some(&expected), "{event_type}");
    }
    let reads = f.members.reads();
    assert_eq!(
        reads
            .iter()
            .filter(|read| **read == MembershipRead::Recent)
            .count(),
        3,
        "one reaction and two receipts"
    );
    assert_eq!(membership_read(None), MembershipRead::Current);
}

/// Membership that cannot be read refuses retryably and lets nothing through.
#[tokio::test]
async fn an_unreadable_membership_is_retryable_never_allowed() {
    let f = fixture();
    *f.members.down.lock().unwrap() = true;
    let error = refused(&f.policy, &user("alice"), reaction("m-alice")).await;
    assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable));
    let unwired = MessageOperationPolicy::new(Some(f.messages.clone())).with_clock(|| NOW);
    let error = refused(&unwired, &user("alice"), reaction("m-alice")).await;
    assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable));
    // Events no user may send are refused as before, without a lookup.
    let asked = f.members.asked().len();
    let error = refused(
        &f.policy,
        &user("alice"),
        event(
            Payload::Message(stored("m-x", "alice", GROUP, 0, text("bypass"))),
            EventType::EventMessage,
        ),
    )
    .await;
    assert_eq!(reason_of(&error), reason::FORBIDDEN);
    assert_eq!(f.members.asked().len(), asked);
}

/// Every message a read receipt names must be a stored message of the event's conversation,
/// whoever sends it; all of them in one batch lookup (each distinct id once), never one read per
/// message.
#[tokio::test]
async fn a_receipts_messages_must_belong_to_its_conversation() {
    let f = fixture();
    let alice = user("alice");
    f.policy
        .authorize(
            &alice,
            "tenant-a",
            receipt(&["m-alice", "m-card", "m-alice"]),
        )
        .await
        .expect("this conversation's messages");
    assert_eq!(f.messages.reads(), 0, "no single-message reads");
    assert_eq!(
        f.messages.batches(),
        [vec!["m-alice".to_string(), "m-card".to_string()]],
        "one lookup, duplicates once"
    );

    // A full page is still one lookup.
    let page: Vec<String> = (0..MAX_RECEIPT_MESSAGE_IDS)
        .map(|i| format!("m-page-{i}"))
        .collect();
    for id in &page {
        f.messages.put(stored(id, "bob", GROUP, NOW, text("hi")));
    }
    let page_ids: Vec<&str> = page.iter().map(String::as_str).collect();
    f.policy
        .authorize(&alice, "tenant-a", receipt(&page_ids))
        .await
        .expect("a full page");
    assert_eq!(f.messages.batches().len(), 2);
    assert_eq!(f.messages.batches()[1].len(), MAX_RECEIPT_MESSAGE_IDS);
    assert_eq!(f.messages.reads(), 0);

    let error = refused(&f.policy, &alice, receipt(&["m-alice", "m-elsewhere"])).await;
    assert_eq!(reason_of(&error), reason::CONVERSATION_MISMATCH);
    let trusted = signed_as(SignedActorType::Service, "flare-social-bridge");
    let mut for_alice = receipt(&["m-elsewhere"]);
    if let Some(Payload::Read(read)) = for_alice.payload.as_mut() {
        read.user_id = "alice".into();
    }
    let error = refused(&f.policy, &trusted, for_alice).await;
    assert_eq!(reason_of(&error), reason::CONVERSATION_MISMATCH);

    // Not stored yet: retryable, not allowed.
    let error = refused(&f.policy, &alice, receipt(&["m-not-yet"])).await;
    assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable));

    let too_many: Vec<String> = (0..=MAX_RECEIPT_MESSAGE_IDS)
        .map(|i| format!("m-{i}"))
        .collect();
    let too_many: Vec<&str> = too_many.iter().map(String::as_str).collect();
    let batches = f.messages.batches().len();
    let error = refused(&f.policy, &alice, receipt(&too_many)).await;
    assert_eq!(error.code(), Some(ErrorCode::InvalidParameter));
    assert_eq!(
        f.messages.batches().len(),
        batches,
        "an oversized receipt reads nothing"
    );
    assert_eq!(f.messages.reads(), 0);
}
