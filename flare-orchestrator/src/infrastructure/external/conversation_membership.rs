//! [`ConversationMembershipRepository`] over the conversation read service's member lookup:
//! `ListConversationParticipants` with `ext["member_ids"]` set to the one operator, a primary-key
//! lookup whose cost does not depend on the group's size (a 100,000-member group costs the same
//! as a chat). That key answers members who have not left, including those who hid the
//! conversation. It is read as the Orchestrator itself, a Cell-signed `Service` context for the
//! event's tenant, which the conversation service admits for member reads without the caller being
//! a member. Nothing the client sent names the identity of the read.
//!
//! [`CachedConversationMembership`] keeps "member" answers for [`MEMBER_CACHE_TTL`], so a member's
//! burst of read receipts and reactions costs one lookup. Only "member" is cached, so a member who
//! was just added is never refused from a stale answer. The cache answers only
//! [`MembershipRead::Recent`] reads (read receipts, reactions): a member removed from the group can
//! go on with those for at most the TTL, and with nothing else. A [`MembershipRead::Current`] read
//! (edit, recall, delete, pin, mark, custom events) always asks the conversation service, and its
//! answer replaces the cached one: once it says "not a member", the cached "member" is dropped and
//! the removed member's receipts and reactions are refused from then on too.
use std::sync::Arc;
use std::time::Duration;

use flare_grpc_proto::conversation::ListConversationParticipantsRequest;
use flare_grpc_proto::conversation::conversation_read_service_client::ConversationReadServiceClient;
use flare_im_contracts::Ctx;
use flare_server_core::SignedContextSigner;
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError, Result};
use tonic::transport::Channel;

use crate::domain::repository::{ConversationMembershipRepository, MembershipRead};

/// The conversation service's member lookup key (`flare-conversation`
/// `PARTICIPANT_MEMBERSHIP_EXT_KEY`).
const PARTICIPANT_MEMBERSHIP_EXT_KEY: &str = "member_ids";
/// Bound of one lookup.
const READ_TIMEOUT: Duration = Duration::from_secs(3);
/// How long a "member" answer is reused.
pub const MEMBER_CACHE_TTL: Duration = Duration::from_secs(30);
/// How many (tenant, conversation, user) answers are kept.
const MEMBER_CACHE_CAPACITY: u64 = 200_000;

pub struct ConversationReadMembership {
    channel: Channel,
    /// The Cell signer (`FLARE_INTERNAL_CTX_PRIVATE_KEY`); without it every lookup fails, so user
    /// events are refused (retryable), never allowed unchecked.
    signer: Option<SignedContextSigner>,
}

impl ConversationReadMembership {
    pub fn new(channel: Channel, signer: Option<SignedContextSigner>) -> Self {
        Self { channel, signer }
    }
}

fn unavailable(details: &str) -> FlareError {
    ErrorBuilder::new(
        ErrorCode::ServiceUnavailable,
        "conversation membership unavailable",
    )
    .details(details)
    .build_error()
}

#[async_trait::async_trait]
impl ConversationMembershipRepository for ConversationReadMembership {
    async fn is_member(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        conversation_id: &str,
        user_id: &str,
        _read: MembershipRead,
    ) -> Result<bool> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| unavailable("the orchestrator has no Cell signing key"))?;
        let token = flare_im_service_kit::signed_read::sign_service_read(
            signer,
            flare_im_contracts::service_names::ORCHESTRATOR,
            tenant_id,
            ctx.request_id(),
        )?;
        let mut request = tonic::Request::new(ListConversationParticipantsRequest {
            conversation_id: conversation_id.to_owned(),
            ext: [(
                PARTICIPANT_MEMBERSHIP_EXT_KEY.to_owned(),
                user_id.to_owned(),
            )]
            .into(),
            ..Default::default()
        });
        request.set_timeout(READ_TIMEOUT);
        flare_server_core::set_signed_context_token(&mut request, &token)
            .map_err(|_| unavailable("the read context could not be encoded"))?;
        let mut client = ConversationReadServiceClient::new(self.channel.clone());
        match tokio::time::timeout(READ_TIMEOUT, client.list_conversation_participants(request))
            .await
        {
            Ok(Ok(response)) => Ok(response
                .into_inner()
                .participants
                .iter()
                .any(|participant| participant.user_id == user_id)),
            // The lookup refuses an id it cannot look up (empty, or a list): nobody by that name.
            Ok(Err(status)) if status.code() == tonic::Code::InvalidArgument => Ok(false),
            Ok(Err(status)) => {
                tracing::warn!(code = ?status.code(), "conversation membership lookup failed");
                Err(unavailable(
                    "conversation service refused or failed the lookup",
                ))
            }
            Err(_) => Err(unavailable("conversation service did not answer in time")),
        }
    }
}

/// "Member" answers of `inner`, reused for [`MEMBER_CACHE_TTL`] (see the module docs).
pub struct CachedConversationMembership {
    inner: Arc<dyn ConversationMembershipRepository>,
    members: moka::future::Cache<(String, String, String), ()>,
}

impl CachedConversationMembership {
    pub fn new(inner: Arc<dyn ConversationMembershipRepository>) -> Self {
        Self::with_ttl(inner, MEMBER_CACHE_TTL)
    }

    pub fn with_ttl(inner: Arc<dyn ConversationMembershipRepository>, ttl: Duration) -> Self {
        Self {
            inner,
            members: moka::future::Cache::builder()
                .max_capacity(MEMBER_CACHE_CAPACITY)
                .time_to_live(ttl)
                .build(),
        }
    }
}

#[async_trait::async_trait]
impl ConversationMembershipRepository for CachedConversationMembership {
    async fn is_member(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        conversation_id: &str,
        user_id: &str,
        read: MembershipRead,
    ) -> Result<bool> {
        let key = (
            tenant_id.to_owned(),
            conversation_id.to_owned(),
            user_id.to_owned(),
        );
        if read == MembershipRead::Recent && self.members.get(&key).await.is_some() {
            return Ok(true);
        }
        let member = self
            .inner
            .is_member(ctx, tenant_id, conversation_id, user_id, read)
            .await?;
        if member {
            self.members.insert(key, ()).await;
        } else {
            self.members.invalidate(&key).await;
        }
        Ok(member)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Answers from a fixed member set, counting lookups; `down` fails them.
    #[derive(Default)]
    struct Lookups {
        members: Mutex<Vec<(String, String, String)>>,
        calls: Mutex<usize>,
        down: Mutex<bool>,
    }

    #[async_trait::async_trait]
    impl ConversationMembershipRepository for Lookups {
        async fn is_member(
            &self,
            _ctx: &Ctx,
            tenant_id: &str,
            conversation_id: &str,
            user_id: &str,
            _read: MembershipRead,
        ) -> Result<bool> {
            *self.calls.lock().unwrap() += 1;
            if *self.down.lock().unwrap() {
                return Err(unavailable("down"));
            }
            Ok(self.members.lock().unwrap().contains(&(
                tenant_id.to_owned(),
                conversation_id.to_owned(),
                user_id.to_owned(),
            )))
        }
    }

    fn ctx() -> Ctx {
        Arc::new(flare_server_core::Context::with_request_id("req").with_tenant_id("t"))
    }

    #[tokio::test]
    async fn members_are_cached_non_members_and_outages_are_not() {
        let lookups = Arc::new(Lookups::default());
        lookups
            .members
            .lock()
            .unwrap()
            .push(("t".into(), "g".into(), "alice".into()));
        let cached = CachedConversationMembership::new(lookups.clone());

        for _ in 0..3 {
            assert!(
                cached
                    .is_member(&ctx(), "t", "g", "alice", MembershipRead::Recent)
                    .await
                    .unwrap()
            );
        }
        assert_eq!(
            *lookups.calls.lock().unwrap(),
            1,
            "a member is looked up once"
        );

        // A non-member is asked every time, so being added takes effect at once.
        assert!(
            !cached
                .is_member(&ctx(), "t", "g", "bob", MembershipRead::Recent)
                .await
                .unwrap()
        );
        lookups
            .members
            .lock()
            .unwrap()
            .push(("t".into(), "g".into(), "bob".into()));
        assert!(
            cached
                .is_member(&ctx(), "t", "g", "bob", MembershipRead::Recent)
                .await
                .unwrap()
        );

        // Keys are per tenant and conversation.
        assert!(
            !cached
                .is_member(&ctx(), "t", "other", "alice", MembershipRead::Recent)
                .await
                .unwrap()
        );
        assert!(
            !cached
                .is_member(&ctx(), "t2", "g", "alice", MembershipRead::Recent)
                .await
                .unwrap()
        );

        // An outage surfaces (the caller answers retryable) and is not cached.
        *lookups.down.lock().unwrap() = true;
        let error = cached
            .is_member(&ctx(), "t", "g", "carol", MembershipRead::Recent)
            .await
            .unwrap_err();
        assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable));
        assert!(
            cached
                .is_member(&ctx(), "t", "g", "alice", MembershipRead::Recent)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn a_removed_member_is_refused_once_the_ttl_passes() {
        let lookups = Arc::new(Lookups::default());
        lookups
            .members
            .lock()
            .unwrap()
            .push(("t".into(), "g".into(), "alice".into()));
        let cached =
            CachedConversationMembership::with_ttl(lookups.clone(), Duration::from_millis(50));
        assert!(
            cached
                .is_member(&ctx(), "t", "g", "alice", MembershipRead::Recent)
                .await
                .unwrap()
        );
        lookups.members.lock().unwrap().clear();
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(
            !cached
                .is_member(&ctx(), "t", "g", "alice", MembershipRead::Recent)
                .await
                .unwrap()
        );
    }

    /// A `Current` read never takes the cached answer, and its "not a member" drops the cached
    /// one: once removed, a member's edit, recall or pin is refused at once, and from then on their
    /// read receipts and reactions too, without waiting for the TTL.
    #[tokio::test]
    async fn current_reads_skip_the_cache_and_a_refusal_clears_it() {
        let lookups = Arc::new(Lookups::default());
        lookups
            .members
            .lock()
            .unwrap()
            .push(("t".into(), "g".into(), "alice".into()));
        let cached = CachedConversationMembership::new(lookups.clone());
        let calls = || *lookups.calls.lock().unwrap();

        assert!(
            cached
                .is_member(&ctx(), "t", "g", "alice", MembershipRead::Recent)
                .await
                .unwrap()
        );
        assert_eq!(calls(), 1);
        // A member's current reads are asked every time, cached answer or not.
        for expected in [2, 3] {
            assert!(
                cached
                    .is_member(&ctx(), "t", "g", "alice", MembershipRead::Current)
                    .await
                    .unwrap()
            );
            assert_eq!(calls(), expected);
        }

        // Alice is removed. Within the TTL the cache would still let a receipt through ...
        lookups.members.lock().unwrap().clear();
        assert!(
            cached
                .is_member(&ctx(), "t", "g", "alice", MembershipRead::Recent)
                .await
                .unwrap()
        );
        assert_eq!(calls(), 3);
        // ... but an edit, recall or pin asks now and is refused,
        assert!(
            !cached
                .is_member(&ctx(), "t", "g", "alice", MembershipRead::Current)
                .await
                .unwrap()
        );
        assert_eq!(calls(), 4);
        // and that answer replaced the cached one: her receipts are refused from now on.
        assert!(
            !cached
                .is_member(&ctx(), "t", "g", "alice", MembershipRead::Recent)
                .await
                .unwrap()
        );
        assert_eq!(calls(), 5);
    }

    /// The policy over the real cache: a member removed from the group right after a reaction
    /// (their "member" still cached) cannot edit, recall or pin, and once refused their reactions
    /// stop too. Only a read receipt in between still rides the cached answer.
    #[tokio::test]
    async fn a_removed_member_cannot_edit_recall_or_pin_from_a_cached_answer() {
        use crate::domain::repository::MessageTargetRepository;
        use crate::domain::service::message_operation_policy::{MessageOperationPolicy, reason};
        use flare_proto::common::event::Payload;
        use flare_proto::common::message_content::Content;
        use flare_proto::common::{Event, EventType, Message, MessageContent, TextContent};

        const NOW: i64 = 1_800_000_000_000;
        struct Sent;
        #[async_trait::async_trait]
        impl MessageTargetRepository for Sent {
            async fn message(&self, _: &Ctx, _: &str, id: &str) -> Result<Option<Message>> {
                Ok((id == "m1").then(|| Message {
                    server_id: "m1".into(),
                    conversation_id: "g".into(),
                    sender_id: "alice".into(),
                    created_at: NOW - 1_000,
                    content: Some(MessageContent {
                        content: Some(Content::Text(TextContent {
                            text: "hi".into(),
                            mentions: vec![],
                        })),
                    }),
                    ..Default::default()
                }))
            }

            async fn conversations_of(
                &self,
                _: &Ctx,
                _: &str,
                ids: &[String],
            ) -> Result<std::collections::HashMap<String, String>> {
                Ok(ids
                    .iter()
                    .filter(|id| *id == "m1")
                    .map(|id| (id.clone(), "g".to_string()))
                    .collect())
            }
        }
        let lookups = Arc::new(Lookups::default());
        lookups
            .members
            .lock()
            .unwrap()
            .push(("t".into(), "g".into(), "alice".into()));
        let policy = MessageOperationPolicy::new(Some(Arc::new(Sent)))
            .with_members(Some(Arc::new(CachedConversationMembership::new(
                lookups.clone(),
            ))))
            .with_clock(|| NOW);
        let alice: Ctx = Arc::new(
            flare_server_core::Context::with_request_id("req")
                .with_tenant_id("t")
                .with_user_id("alice"),
        );
        let event = |r#type: EventType, payload: Payload| Event {
            conversation_id: "g".into(),
            r#type: r#type as i32,
            payload: Some(payload),
            ..Default::default()
        };
        let reaction = || {
            event(
                EventType::EventReaction,
                Payload::Reaction(flare_proto::common::ReactionEvent {
                    server_msg_id: "m1".into(),
                    emoji: "+1".into(),
                    ..Default::default()
                }),
            )
        };
        let receipt = event(
            EventType::EventReadReceipt,
            Payload::Read(flare_proto::common::ReadReceiptEvent {
                read_seq: 3,
                ..Default::default()
            }),
        );
        let writes = [
            event(
                EventType::EventMessageEdit,
                Payload::Edit(flare_proto::common::MessageEditEvent {
                    server_msg_id: "m1".into(),
                    new_content: Some(MessageContent {
                        content: Some(Content::Text(TextContent {
                            text: "abuse".into(),
                            mentions: vec![],
                        })),
                    }),
                    ..Default::default()
                }),
            ),
            event(
                EventType::EventMessageRecall,
                Payload::Recall(flare_proto::common::MessageRecallEvent {
                    server_msg_id: "m1".into(),
                    ..Default::default()
                }),
            ),
            event(
                EventType::EventPin,
                Payload::Pin(flare_proto::common::PinEvent {
                    server_msg_id: "m1".into(),
                    ..Default::default()
                }),
            ),
        ];
        // While a member, every one of these is allowed.
        policy.authorize(&alice, "t", reaction()).await.unwrap();
        for write in writes.clone() {
            policy.authorize(&alice, "t", write).await.unwrap();
        }

        lookups.members.lock().unwrap().clear();
        policy
            .authorize(&alice, "t", receipt)
            .await
            .expect("a receipt within the cache TTL");
        for write in writes {
            let error = policy.authorize(&alice, "t", write).await.unwrap_err();
            assert_eq!(error.reason(), reason::NOT_MEMBER);
        }
        let error = policy.authorize(&alice, "t", reaction()).await.unwrap_err();
        assert_eq!(error.reason(), reason::NOT_MEMBER);
    }

    /// Who asked (actor type, actor, tenant) and what (`conversation:ids`).
    type Asked = (flare_server_core::SignedActorType, String, String, String);

    /// A conversation read service behind the same signed-read boundary Core uses (signature
    /// required): records who asked for which ids and answers the lookup.
    #[derive(Clone, Default)]
    struct FakeLookup {
        seen: Arc<Mutex<Vec<Asked>>>,
    }

    #[tonic::async_trait]
    impl flare_grpc_proto::conversation::conversation_read_service_server::ConversationReadService
        for FakeLookup
    {
        async fn list_conversation_participants(
            &self,
            request: tonic::Request<ListConversationParticipantsRequest>,
        ) -> std::result::Result<
            tonic::Response<flare_grpc_proto::conversation::ListConversationParticipantsResponse>,
            tonic::Status,
        > {
            let claims = request
                .extensions()
                .get::<Ctx>()
                .and_then(|ctx| ctx.get_data::<flare_server_core::VerifiedContext>())
                .and_then(flare_server_core::VerifiedContext::claims)
                .expect("the boundary only admits verified calls")
                .clone();
            let req = request.into_inner();
            let ids = req
                .ext
                .get(PARTICIPANT_MEMBERSHIP_EXT_KEY)
                .cloned()
                .unwrap_or_default();
            let mut keys: Vec<&str> = req.ext.keys().map(String::as_str).collect();
            keys.sort_unstable();
            self.seen.lock().unwrap().push((
                claims.actor_type,
                claims.actor_id,
                claims.tenant_id,
                format!("{}:{}={ids}", req.conversation_id, keys.join(",")),
            ));
            match ids.as_str() {
                "broken" => Err(tonic::Status::unavailable("down")),
                "" => Err(tonic::Status::invalid_argument("member_ids required")),
                "alice" => Ok(tonic::Response::new(
                    flare_grpc_proto::conversation::ListConversationParticipantsResponse {
                        conversation_id: req.conversation_id,
                        participants: vec![flare_proto::common::ConversationParticipant {
                            user_id: "alice".into(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                )),
                _ => Ok(tonic::Response::new(Default::default())),
            }
        }

        async fn conversation_bootstrap(
            &self,
            _: tonic::Request<flare_grpc_proto::conversation::ConversationBootstrapRequest>,
        ) -> std::result::Result<
            tonic::Response<flare_grpc_proto::conversation::ConversationBootstrapResponse>,
            tonic::Status,
        > {
            Err(tonic::Status::unimplemented("not used"))
        }

        async fn list_conversations(
            &self,
            _: tonic::Request<flare_grpc_proto::conversation::ListConversationsRequest>,
        ) -> std::result::Result<
            tonic::Response<flare_grpc_proto::conversation::ListConversationsResponse>,
            tonic::Status,
        > {
            Err(tonic::Status::unimplemented("not used"))
        }

        async fn get_conversation_detail(
            &self,
            _: tonic::Request<flare_grpc_proto::conversation::GetConversationDetailRequest>,
        ) -> std::result::Result<
            tonic::Response<flare_grpc_proto::conversation::GetConversationDetailResponse>,
            tonic::Status,
        > {
            Err(tonic::Status::unimplemented("not used"))
        }
    }

    /// The lookup goes out as the Orchestrator's signed `Service` identity for the event's
    /// tenant, names only the operator, and reads the answer: member, not a member, an id the
    /// service refuses (nobody), an outage (retryable).
    #[tokio::test]
    async fn the_lookup_is_the_orchestrators_signed_read_of_one_user() {
        use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
        let key = ed25519_dalek::SigningKey::from_bytes(&[41; 32]);
        let signer =
            SignedContextSigner::from_private_key(&key.to_pkcs8_pem(Default::default()).unwrap())
                .unwrap();
        let verifier = flare_server_core::SignedContextVerifier::from_public_key(
            &key.verifying_key()
                .to_public_key_pem(Default::default())
                .unwrap(),
        )
        .unwrap();
        let fake = FakeLookup::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let service = flare_server_core::SignedFirstContextLayer::new(Some(verifier))
            .require_signed(true)
            .layer(
                flare_grpc_proto::conversation::conversation_read_service_server::ConversationReadServiceServer::new(
                    fake.clone(),
                ),
            );
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener)),
        );
        let channel = Channel::from_shared(format!("http://{address}"))
            .unwrap()
            .connect_lazy();
        let membership = ConversationReadMembership::new(channel, Some(signer));
        // The caller's own (user) context names nobody in the read.
        let caller: Ctx = Arc::new(
            flare_server_core::Context::with_request_id("req")
                .with_tenant_id("tenant-a")
                .with_user_id("mallory"),
        );

        assert!(
            membership
                .is_member(
                    &caller,
                    "tenant-a",
                    "2A-g",
                    "alice",
                    MembershipRead::Current
                )
                .await
                .unwrap()
        );
        assert!(
            !membership
                .is_member(&caller, "tenant-a", "2A-g", "bob", MembershipRead::Current)
                .await
                .unwrap()
        );
        assert!(
            !membership
                .is_member(&caller, "tenant-a", "2A-g", "", MembershipRead::Current)
                .await
                .unwrap()
        );
        let error = membership
            .is_member(
                &caller,
                "tenant-a",
                "2A-g",
                "broken",
                MembershipRead::Current,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable));

        let seen = fake.seen.lock().unwrap().clone();
        assert_eq!(
            seen.iter().map(|s| s.3.as_str()).collect::<Vec<_>>(),
            // Only the member key: the participant key would leave out who hid the conversation.
            [
                "2A-g:member_ids=alice",
                "2A-g:member_ids=bob",
                "2A-g:member_ids=",
                "2A-g:member_ids=broken"
            ]
        );
        for (actor_type, actor, tenant, _) in &seen {
            assert_eq!(*actor_type, flare_server_core::SignedActorType::Service);
            assert_eq!(actor, flare_im_contracts::service_names::ORCHESTRATOR);
            assert_eq!(tenant, "tenant-a");
        }
    }

    #[tokio::test]
    async fn without_a_signing_key_the_lookup_is_an_outage() {
        let channel = Channel::from_static("http://127.0.0.1:9").connect_lazy();
        let membership = ConversationReadMembership::new(channel, None);
        let error = membership
            .is_member(&ctx(), "t", "g", "alice", MembershipRead::Recent)
            .await
            .unwrap_err();
        assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable));
    }
}
