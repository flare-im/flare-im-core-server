use super::*;
use ed25519_dalek::{
    SigningKey,
    pkcs8::{EncodePrivateKey, EncodePublicKey},
};
use flare_grpc_proto::{
    capability::{
        GenericResponse,
        plugin_resource_backend_server::{PluginResourceBackend, PluginResourceBackendServer},
    },
    conversation::*,
};
use flare_proto::common::{ConversationDetail, ConversationParticipant};
use flare_server_core::SignedContextVerifier;
use serde_json::json;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tonic::{Request, Response, Status};

struct OwnerFixture {
    verifier: SignedContextVerifier,
    member: AtomicBool,
    mismatch: AtomicBool,
    social_hits: AtomicUsize,
    /// The conversation's members (not left), in Conversation's page order.
    roster: Vec<(String, String)>,
    /// Members who hid the conversation from their own list: Conversation leaves them
    /// out of its in-seat pages (no `members` scope) and keeps them in its member answers
    /// (the `members` scope, the `member_ids` lookup).
    hidden: Vec<String>,
    /// Conversation answers the lookup key; false is an older Conversation that pages
    /// every participant whatever the key says.
    lookup: AtomicBool,
    participant_calls: Mutex<Vec<ListConversationParticipantsRequest>>,
}
/// `u1` (the caller, owner), `u2`, `u3`, then 1 200 members with 128-byte ids.
fn roster() -> Vec<(String, String)> {
    let mut roster = vec![
        ("u1".to_string(), "owner".to_string()),
        ("u2".to_string(), "member".to_string()),
        ("u3".to_string(), "member".to_string()),
    ];
    roster.extend((0..1200).map(|i| (long_id(i), "member".to_string())));
    roster
}
fn long_id(i: usize) -> String {
    format!("m{i:0>127}")
}
impl OwnerFixture {
    fn actor<T>(&self, request: &Request<T>) {
        assert!(
            request.metadata().get("x-flare-plugin-call").is_none(),
            "never forward plugin tickets to public IM/Social"
        );
        let claims = self
            .verifier
            .verify(
                request
                    .metadata()
                    .get("x-flare-ctx")
                    .unwrap()
                    .to_str()
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(claims.actor_type, SignedActorType::User);
        assert_eq!(claims.actor_id, "u1");
        assert_eq!(claims.tenant_id, "201");
        assert_eq!(claims.request_id, "request-1");
        assert!(claims.roles.is_empty());
    }
}
#[tonic::async_trait]
impl conversation_read_service_server::ConversationReadService for OwnerFixture {
    async fn conversation_bootstrap(
        &self,
        _: Request<ConversationBootstrapRequest>,
    ) -> std::result::Result<Response<ConversationBootstrapResponse>, Status> {
        Err(Status::unimplemented("fixture"))
    }
    async fn list_conversations(
        &self,
        _: Request<ListConversationsRequest>,
    ) -> std::result::Result<Response<ListConversationsResponse>, Status> {
        Err(Status::unimplemented("fixture"))
    }
    async fn get_conversation_detail(
        &self,
        request: Request<GetConversationDetailRequest>,
    ) -> std::result::Result<Response<GetConversationDetailResponse>, Status> {
        self.actor(&request);
        assert_eq!(request.get_ref().conversation_id, "conversation-1");
        if !self.member.load(Ordering::SeqCst) {
            return Err(Status::permission_denied("removed member"));
        }
        Ok(Response::new(GetConversationDetailResponse {
            detail: Some(ConversationDetail {
                conversation_id: if self.mismatch.load(Ordering::SeqCst) {
                    "other"
                } else {
                    "conversation-1"
                }
                .into(),
                conversation_type: "group".into(),
                channel_id: "group-derived-from-im".into(),
                display_name: "Group".into(),
                member_count: 2,
                attributes: [("private".into(), "hidden".into())].into(),
                announcement: "private announcement".into(),
                ..Default::default()
            }),
            metadata: [("internal".into(), "hidden".into())].into(),
        }))
    }
    async fn list_conversation_participants(
        &self,
        request: Request<ListConversationParticipantsRequest>,
    ) -> std::result::Result<Response<ListConversationParticipantsResponse>, Status> {
        self.actor(&request);
        let body = request.into_inner();
        assert_eq!(body.conversation_id, "conversation-1");
        assert!(!body.include_removed);
        assert_eq!(
            body.ext.get("skip_meta").map(String::as_str),
            Some("1"),
            "plugin reads never need totals"
        );
        self.participant_calls.lock().unwrap().push(body.clone());
        let view = |(id, role): &(String, String)| ConversationParticipant {
            user_id: id.clone(),
            roles: vec![role.clone()],
            attributes: [("secret".into(), "hidden".into())].into(),
            ..Default::default()
        };
        let in_scope = |members: bool| {
            self.roster
                .iter()
                .filter(move |(id, _)| members || !self.hidden.contains(id))
        };
        if self.lookup.load(Ordering::SeqCst)
            && let Some(ids) = body.ext.get("member_ids")
        {
            let ids: Vec<&str> = ids.split(',').collect();
            return Ok(Response::new(ListConversationParticipantsResponse {
                conversation_id: "conversation-1".into(),
                participants: in_scope(true)
                    .filter(|(id, _)| ids.contains(&id.as_str()))
                    .map(view)
                    .collect(),
                ..Default::default()
            }));
        }
        // Conversation's paging: a number is an offset, `k:<n>` its own keyset cursor.
        let paged: Vec<&(String, String)> =
            in_scope(body.ext.get("members").map(String::as_str) == Some("1")).collect();
        let start: usize = match body.cursor.as_str() {
            "" => 0,
            cursor => cursor.strip_prefix("k:").unwrap_or(cursor).parse().unwrap(),
        };
        let end = (start + body.limit as usize).min(paged.len());
        let has_more = end < paged.len();
        Ok(Response::new(ListConversationParticipantsResponse {
            conversation_id: "conversation-1".into(),
            participants: paged[start..end].iter().copied().map(view).collect(),
            next_cursor: if has_more {
                format!("k:{end}")
            } else {
                String::new()
            },
            has_more,
            ..Default::default()
        }))
    }
}
#[tonic::async_trait]
impl PluginResourceBackend for OwnerFixture {
    async fn call(
        &self,
        request: Request<GenericRequest>,
    ) -> std::result::Result<Response<GenericResponse>, Status> {
        self.actor(&request);
        self.social_hits.fetch_add(1, Ordering::SeqCst);
        let request = request.into_inner();
        assert_eq!(request.operation, "social.group.member.read");
        assert!(request.metadata.is_empty());
        let lookup: GroupMemberLookup =
            serde_json::from_slice(&request.payload.unwrap().value).unwrap();
        assert_eq!(lookup.group_id, "group-derived-from-im");
        assert_eq!(lookup.user_id, "u2");
        Ok(Response::new(GenericResponse {
            ok: true,
            request_id: request.request_id,
            payload: Some(prost_types::Any {
                type_url: "json".into(),
                value: serde_json::to_vec(&GroupMemberView {
                    group_id: lookup.group_id,
                    user_id: if self.mismatch.load(Ordering::SeqCst) {
                        "different".into()
                    } else {
                        lookup.user_id
                    },
                    roles: vec![3],
                    muted: false,
                })
                .unwrap(),
            }),
            ..Default::default()
        }))
    }
}
struct Harness {
    owner: Arc<OwnerFixture>,
    adapter: GrpcResourceAccess,
    actor: ResourceActor,
    stop: tokio::sync::oneshot::Sender<()>,
    server: tokio::task::JoinHandle<std::result::Result<(), tonic::transport::Error>>,
}
impl Harness {
    async fn finish(self) {
        self.stop.send(()).unwrap();
        self.server.await.unwrap().unwrap();
    }
    fn participant_calls(&self) -> Vec<ListConversationParticipantsRequest> {
        std::mem::take(&mut *self.owner.participant_calls.lock().unwrap())
    }
}
async fn harness() -> Harness {
    let key = SigningKey::from_bytes(&[59; 32]);
    let signer =
        SignedContextSigner::from_private_key(&key.to_pkcs8_pem(Default::default()).unwrap())
            .unwrap();
    let owner = Arc::new(OwnerFixture {
        verifier: SignedContextVerifier::from_public_key(
            &key.verifying_key()
                .to_public_key_pem(Default::default())
                .unwrap(),
        )
        .unwrap(),
        member: AtomicBool::new(true),
        mismatch: AtomicBool::new(false),
        social_hits: AtomicUsize::new(0),
        roster: roster(),
        hidden: vec!["u3".to_string()],
        lookup: AtomicBool::new(true),
        participant_calls: Mutex::new(Vec::new()),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let authority = format!("http://{}", listener.local_addr().unwrap());
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(
                conversation_read_service_server::ConversationReadServiceServer::from_arc(
                    owner.clone(),
                ),
            )
            .add_service(PluginResourceBackendServer::from_arc(owner.clone()))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    let _ = stopped.await;
                },
            ),
    );
    let adapter = GrpcResourceAccess {
        conversation_authority: authority.clone(),
        social_group_authority: Some(authority),
        signer: Arc::new(signer),
    };
    let actor = ResourceActor {
        tenant_id: "201".into(),
        user_id: "u1".into(),
        conversation_id: "conversation-1".into(),
        request_id: "request-1".into(),
    };
    Harness {
        owner,
        adapter,
        actor,
        stop,
        server,
    }
}
#[tokio::test]
async fn resource_wire_uses_user_context_derived_group_and_whitelisted_views() {
    let harness = harness().await;
    let (owner, adapter, actor) = (&harness.owner, &harness.adapter, &harness.actor);
    let detail = adapter
        .read(actor, &ResourceRequest::ConversationRead {})
        .await
        .unwrap();
    assert_eq!(
        detail,
        json!({"conversationId":"conversation-1","conversationType":"group","displayName":"Group","memberCount":2})
    );
    let participants = adapter
        .read(
            actor,
            &ResourceRequest::ConversationParticipantsRead {
                cursor: "".into(),
                limit: 1,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        participants["participants"],
        json!([{"userId":"u1","roles":["owner"]}])
    );
    assert!(!participants.to_string().contains("hidden"));
    let request = ResourceRequest::SocialGroupMemberRead {
        user_id: "u2".into(),
    };
    let member = adapter.read(actor, &request).await.unwrap();
    assert_eq!(member["groupId"], "group-derived-from-im");
    assert_eq!(owner.social_hits.load(Ordering::SeqCst), 1);
    owner.member.store(false, Ordering::SeqCst);
    assert!(adapter.read(actor, &request).await.is_err());
    assert_eq!(
        owner.social_hits.load(Ordering::SeqCst),
        1,
        "IM owner denies before Social is reached"
    );
    owner.member.store(true, Ordering::SeqCst);
    owner.mismatch.store(true, Ordering::SeqCst);
    assert!(adapter.read(actor, &request).await.is_err());
    assert_eq!(
        owner.social_hits.load(Ordering::SeqCst),
        1,
        "mismatched IM identity denied before Social"
    );
    harness.finish().await;
}

fn page(value: &Value) -> ParticipantPage {
    serde_json::from_value(value.clone()).unwrap()
}
async fn participants(harness: &Harness, cursor: &str, limit: u32) -> Result<Value> {
    let request = ResourceRequest::ConversationParticipantsRead {
        cursor: cursor.into(),
        limit,
    };
    request.validate().expect("a request the bridge accepts");
    harness.adapter.read(&harness.actor, &request).await
}

/// Pages of up to 500 fit the plugin response bound, and the cursor a plugin
/// gets back is the decimal offset the protocol accepts, never Conversation's
/// keyset cursor.
#[tokio::test]
async fn participant_pages_take_500_and_hand_out_offset_cursors() {
    let harness = harness().await;
    let first = participants(&harness, "", MAX_PARTICIPANTS_PAGE)
        .await
        .unwrap();
    assert!(serde_json::to_vec(&first).unwrap().len() <= MAX_RESOURCE_BYTES);
    let first = page(&first);
    assert_eq!(first.participants.len(), 500);
    assert_eq!(first.participants[0].user_id, "u1");
    assert!(first.has_more);
    assert_eq!(first.next_cursor, "500");
    let calls = harness.participant_calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].cursor, "", "the first page sends no cursor");
    assert_eq!(calls[0].limit, 500);
    assert_eq!(calls[0].ext.get("members").map(String::as_str), Some("1"));

    let mut seen: Vec<String> = first
        .participants
        .iter()
        .map(|p| p.user_id.clone())
        .collect();
    let mut cursor = first.next_cursor;
    while !cursor.is_empty() {
        let next = page(&participants(&harness, &cursor, 500).await.unwrap());
        assert_eq!(
            harness.participant_calls()[0].cursor,
            cursor,
            "an offset reaches Conversation as is"
        );
        seen.extend(next.participants.iter().map(|p| p.user_id.clone()));
        assert_eq!(next.has_more, !next.next_cursor.is_empty());
        cursor = next.next_cursor;
    }
    assert_eq!(
        seen,
        harness
            .owner
            .roster
            .iter()
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>(),
        "every participant once, in order"
    );

    let small = page(&participants(&harness, "1", 2).await.unwrap());
    assert_eq!(
        small
            .participants
            .iter()
            .map(|p| p.user_id.as_str())
            .collect::<Vec<_>>(),
        ["u2", "u3"]
    );
    assert_eq!(small.next_cursor, "3");
    let last = page(&participants(&harness, "1202", 500).await.unwrap());
    assert_eq!(last.participants.len(), 1);
    assert!(!last.has_more);
    assert_eq!(last.next_cursor, "");
    harness.participant_calls();
    assert!(
        participants(&harness, &"9".repeat(40), 1).await.is_err(),
        "an offset past u64 is refused, not read as the first page"
    );
    assert!(harness.participant_calls().is_empty());
    harness.finish().await;
}

fn members_check(user_ids: &[&str]) -> ResourceRequest {
    let request = ResourceRequest::ConversationMembersCheck {
        user_ids: user_ids.iter().map(|id| id.to_string()).collect(),
    };
    request.validate().expect("a request the bridge accepts");
    request
}
async fn check(harness: &Harness, user_ids: &[&str]) -> MembersCheckView {
    let request = members_check(user_ids);
    let answer: MembersCheckView = serde_json::from_value(
        harness
            .adapter
            .read(&harness.actor, &request)
            .await
            .unwrap(),
    )
    .unwrap();
    let ResourceRequest::ConversationMembersCheck { user_ids } = &request else {
        unreachable!()
    };
    answer
        .validate_for(user_ids)
        .expect("the answer a plugin checks with validate_for");
    answer
}

/// The members check answers in request order from one lookup of the ids,
/// whatever the size of the conversation.
#[tokio::test]
async fn members_check_partitions_the_ids_with_one_lookup() {
    let harness = harness().await;
    let far = long_id(1100);
    let answer = check(&harness, &["ghost", "u2", &far, "u1", "left-user"]).await;
    assert_eq!(answer.members, ["u2", far.as_str(), "u1"]);
    assert_eq!(answer.non_members, ["ghost", "left-user"]);
    assert_eq!(
        serde_json::to_value(&answer).unwrap(),
        json!({"members": ["u2", far, "u1"], "nonMembers": ["ghost", "left-user"]})
    );
    let calls = harness.participant_calls();
    assert_eq!(calls.len(), 1, "one lookup, no paging");
    assert_eq!(
        calls[0].ext.get("member_ids").map(String::as_str),
        Some(format!("ghost,u2,{far},u1,left-user").as_str())
    );
    assert_eq!(calls[0].cursor, "");

    let none = check(&harness, &["ghost"]).await;
    assert!(none.members.is_empty());
    assert_eq!(none.non_members, ["ghost"]);

    // A removed caller is refused by the conversation detail before any lookup.
    harness.participant_calls();
    harness.owner.member.store(false, Ordering::SeqCst);
    assert!(
        harness
            .adapter
            .read(&harness.actor, &members_check(&["u2"]))
            .await
            .is_err()
    );
    assert!(harness.participant_calls().is_empty());
    harness.finish().await;
}

/// An older Conversation ignores the lookup key and pages every participant:
/// the check walks the pages until each id is found or the list ends, and
/// gives the same answer.
#[tokio::test]
async fn members_check_walks_pages_when_conversation_ignores_the_lookup() {
    let harness = harness().await;
    harness.owner.lookup.store(false, Ordering::SeqCst);
    let far = long_id(1100);
    let answer = check(&harness, &["ghost", "u2", &far, "u1"]).await;
    assert_eq!(answer.members, ["u2", far.as_str(), "u1"]);
    assert_eq!(answer.non_members, ["ghost"]);
    let calls = harness.participant_calls();
    assert_eq!(
        calls.iter().map(|c| c.cursor.as_str()).collect::<Vec<_>>(),
        ["", "k:500", "k:1000"],
        "a missing id reads to the end, through Conversation's own cursors"
    );

    let found = check(&harness, &["u3", "u1"]).await;
    assert_eq!(found.members, ["u3", "u1"]);
    assert_eq!(
        harness.participant_calls().len(),
        1,
        "stops once every id is found"
    );
    harness.finish().await;
}

/// A member who hid the conversation from their own list (`u3`) is still a member: the
/// members check a plugin runs before a member-only operation says so, and the
/// participant pages list them. Someone who left (`left-user`) is in neither. Both reads
/// ask Conversation for the member scope.
#[tokio::test]
async fn a_member_who_hid_the_conversation_is_still_a_member() {
    let harness = harness().await;
    let answer = check(&harness, &["u3", "left-user"]).await;
    assert_eq!(answer.members, ["u3"]);
    assert_eq!(answer.non_members, ["left-user"]);
    let calls = harness.participant_calls();
    assert_eq!(
        calls[0].ext.get("member_ids").map(String::as_str),
        Some("u3,left-user")
    );

    let first = page(&participants(&harness, "", 5).await.unwrap());
    assert_eq!(
        first
            .participants
            .iter()
            .map(|p| p.user_id.as_str())
            .take(3)
            .collect::<Vec<_>>(),
        ["u1", "u2", "u3"]
    );
    assert_eq!(
        harness.participant_calls()[0]
            .ext
            .get("members")
            .map(String::as_str),
        Some("1")
    );
    harness.finish().await;
}
