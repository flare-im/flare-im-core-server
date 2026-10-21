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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tonic::{Request, Response, Status};

struct OwnerFixture {
    verifier: SignedContextVerifier,
    member: AtomicBool,
    mismatch: AtomicBool,
    social_hits: AtomicUsize,
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
        assert_eq!(body.limit, 1);
        assert!(!body.include_removed);
        assert!(body.ext.is_empty());
        Ok(Response::new(ListConversationParticipantsResponse {
            conversation_id: "conversation-1".into(),
            participants: vec![ConversationParticipant {
                user_id: "u2".into(),
                roles: vec!["member".into()],
                attributes: [("secret".into(), "hidden".into())].into(),
                ..Default::default()
            }],
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
#[tokio::test]
async fn resource_wire_uses_user_context_derived_group_and_whitelisted_views() {
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
    let detail = adapter
        .read(&actor, &ResourceRequest::ConversationRead {})
        .await
        .unwrap();
    assert_eq!(
        detail,
        json!({"conversationId":"conversation-1","conversationType":"group","displayName":"Group","memberCount":2})
    );
    let participants = adapter
        .read(
            &actor,
            &ResourceRequest::ConversationParticipantsRead {
                cursor: "".into(),
                limit: 1,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        participants["participants"],
        json!([{"userId":"u2","roles":["member"]}])
    );
    assert!(!participants.to_string().contains("hidden"));
    let request = ResourceRequest::SocialGroupMemberRead {
        user_id: "u2".into(),
    };
    let member = adapter.read(&actor, &request).await.unwrap();
    assert_eq!(member["groupId"], "group-derived-from-im");
    assert_eq!(owner.social_hits.load(Ordering::SeqCst), 1);
    owner.member.store(false, Ordering::SeqCst);
    assert!(adapter.read(&actor, &request).await.is_err());
    assert_eq!(
        owner.social_hits.load(Ordering::SeqCst),
        1,
        "IM owner denies before Social is reached"
    );
    owner.member.store(true, Ordering::SeqCst);
    owner.mismatch.store(true, Ordering::SeqCst);
    assert!(adapter.read(&actor, &request).await.is_err());
    assert_eq!(
        owner.social_hits.load(Ordering::SeqCst),
        1,
        "mismatched IM identity denied before Social"
    );
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}
