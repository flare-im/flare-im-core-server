//! `ConversationAdminService`：管理员只读（admin-gateway 专用下游）。
//!
//! 只认 admin-gateway 原样转发的 Staff `x-flare-ctx`（网关 admin 分区按 `kid=gateway` 签，权限码
//! `core:conversations:read`），不依赖未签名的 `x-tenant-id` / actor 元数据；租户只取自签名，
//! 请求里没有 tenant_id 与 user_id。

use std::sync::Arc;

use flare_grpc_proto::conversation::conversation_admin_service_server::ConversationAdminService;
use flare_grpc_proto::conversation::{
    AdminGetConversationRequest, AdminGetConversationResponse, AdminListConversationsRequest,
    AdminListConversationsResponse, AdminListParticipantsRequest, AdminListParticipantsResponse,
};
use flare_im_service_kit::staff_context::{VerifiedStaff, require_staff};
use flare_proto::common::ConversationDetail;
use flare_server_core::SignedContextVerifier;
use flare_server_core::auth::core_admin::permission::CONVERSATIONS_READ;
use flare_server_core::error::grpc::IntoGrpc;
use tonic::{Request, Response, Status};

use super::shared::{domain_to_conversation_detail, participant_domain_to_proto};
use crate::application::ConversationAdminQueryHandler;
use crate::domain::model::AdminConversation;
use crate::domain::repository::ConversationAdminRepository;

pub struct ConversationAdminGrpcHandler<R: ConversationAdminRepository + 'static> {
    queries: Arc<ConversationAdminQueryHandler<R>>,
    /// Cell 公钥集，Staff 钉死 `kid=gateway`；没有配置时管理读一律 UNAUTHENTICATED。
    staff_verifier: Option<SignedContextVerifier>,
}

impl<R: ConversationAdminRepository + 'static> ConversationAdminGrpcHandler<R> {
    pub fn new(
        queries: Arc<ConversationAdminQueryHandler<R>>,
        staff_verifier: Option<SignedContextVerifier>,
    ) -> Self {
        Self {
            queries,
            staff_verifier,
        }
    }

    fn staff<T>(&self, request: &Request<T>) -> Result<VerifiedStaff, Status> {
        let verifier = self
            .staff_verifier
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("staff context verifier not configured"))?;
        require_staff(request, verifier, CONVERSATIONS_READ)
    }
}

impl<R: ConversationAdminRepository + 'static> Clone for ConversationAdminGrpcHandler<R> {
    fn clone(&self) -> Self {
        Self {
            queries: Arc::clone(&self.queries),
            staff_verifier: self.staff_verifier.clone(),
        }
    }
}

fn admin_detail(admin: AdminConversation) -> ConversationDetail {
    let member_count = i32::try_from(admin.member_count).unwrap_or(i32::MAX);
    ConversationDetail {
        member_count,
        ..domain_to_conversation_detail(admin.conversation)
    }
}

fn optional(value: &str) -> Option<&str> {
    (!value.is_empty()).then_some(value)
}

#[tonic::async_trait]
impl<R: ConversationAdminRepository + 'static> ConversationAdminService
    for ConversationAdminGrpcHandler<R>
{
    async fn admin_list_conversations(
        &self,
        request: Request<AdminListConversationsRequest>,
    ) -> Result<Response<AdminListConversationsResponse>, Status> {
        let staff = self.staff(&request)?;
        let req = request.into_inner();
        tracing::info!(actor_id = %staff.actor_id, tenant_id = %staff.tenant_id, request_id = %staff.request_id, "admin list_conversations");
        let page = self
            .queries
            .list_conversations(
                &staff.tenant_id,
                optional(&req.cursor),
                req.limit,
                req.include_deleted,
            )
            .await
            .into_grpc()?;
        Ok(Response::new(AdminListConversationsResponse {
            conversations: page.conversations.into_iter().map(admin_detail).collect(),
            next_cursor: page.next_cursor.unwrap_or_default(),
            has_more: page.has_more,
        }))
    }

    async fn admin_get_conversation(
        &self,
        request: Request<AdminGetConversationRequest>,
    ) -> Result<Response<AdminGetConversationResponse>, Status> {
        let staff = self.staff(&request)?;
        let req = request.into_inner();
        tracing::info!(actor_id = %staff.actor_id, tenant_id = %staff.tenant_id, request_id = %staff.request_id, conversation_id = %req.conversation_id, "admin get_conversation");
        let conversation = self
            .queries
            .get_conversation(&staff.tenant_id, &req.conversation_id)
            .await
            .into_grpc()?;
        Ok(Response::new(AdminGetConversationResponse {
            detail: Some(admin_detail(conversation)),
        }))
    }

    async fn admin_list_participants(
        &self,
        request: Request<AdminListParticipantsRequest>,
    ) -> Result<Response<AdminListParticipantsResponse>, Status> {
        let staff = self.staff(&request)?;
        let req = request.into_inner();
        tracing::info!(actor_id = %staff.actor_id, tenant_id = %staff.tenant_id, request_id = %staff.request_id, conversation_id = %req.conversation_id, "admin list_participants");
        let page = self
            .queries
            .list_participants(
                &staff.tenant_id,
                &req.conversation_id,
                optional(&req.cursor),
                req.limit,
                req.include_removed,
            )
            .await
            .into_grpc()?;
        Ok(Response::new(AdminListParticipantsResponse {
            conversation_id: page.conversation_id,
            participants: page
                .participants
                .into_iter()
                .map(participant_domain_to_proto)
                .collect(),
            next_cursor: page.next_cursor.unwrap_or_default(),
            has_more: page.has_more,
            member_count: page.member_count,
        }))
    }
}

#[cfg(test)]
mod tests {
    //! 缺上下文、错误 kid、平台断言、权限码不符、跨租户的负向用例，外加租户只取自签名的正向对照。

    use std::collections::HashMap;
    use std::sync::Mutex;

    use chrono::Utc;
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_im_service_kit::control_auth::{CORE_PROJECTION_AUDIENCE, PLATFORM_ISSUER};
    use flare_server_core::auth::{KeySet, kid};
    use flare_server_core::error::Result as FlareResult;
    use flare_server_core::{SignedActorType, SignedContextIdentity, SignedContextSigner};
    use tonic::Code;

    use super::*;
    use crate::domain::model::{
        AdminConversationPage, Conversation, ConversationLifecycleState, ConversationParticipant,
        ConversationParticipantsPage, ConversationType, ConversationVisibility,
    };

    /// (tenant, conversation) → 成员；记录每次调用收到的租户，证明租户只来自签名。
    #[derive(Default)]
    struct MemoryAdminRepo {
        conversations: HashMap<(String, String), Vec<String>>,
        tenants_seen: Mutex<Vec<String>>,
    }

    impl MemoryAdminRepo {
        fn with(mut self, tenant: &str, conversation: &str, members: &[&str]) -> Self {
            self.conversations.insert(
                (tenant.to_string(), conversation.to_string()),
                members.iter().map(|m| m.to_string()).collect(),
            );
            self
        }

        fn admin_conversation(
            tenant: &str,
            conversation: &str,
            members: usize,
        ) -> AdminConversation {
            AdminConversation {
                conversation: Conversation {
                    tenant_id: tenant.to_string(),
                    conversation_id: conversation.to_string(),
                    conversation_type: ConversationType::Group,
                    business_type: String::new(),
                    channel_id: String::new(),
                    display_name: Some(format!("name-{conversation}")),
                    attributes: HashMap::new(),
                    participants: Vec::new(),
                    visibility: ConversationVisibility::Private,
                    lifecycle_state: ConversationLifecycleState::Active,
                    policy: None,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                },
                member_count: members as i64,
            }
        }
    }

    #[async_trait::async_trait]
    impl ConversationAdminRepository for MemoryAdminRepo {
        async fn admin_list_conversations(
            &self,
            tenant_id: &str,
            _cursor: Option<&str>,
            _limit: i64,
            _include_deleted: bool,
        ) -> FlareResult<AdminConversationPage> {
            self.tenants_seen
                .lock()
                .unwrap()
                .push(tenant_id.to_string());
            let mut conversations: Vec<_> = self
                .conversations
                .iter()
                .filter(|((tenant, _), _)| tenant == tenant_id)
                .map(|((tenant, id), members)| Self::admin_conversation(tenant, id, members.len()))
                .collect();
            conversations.sort_by(|a, b| {
                a.conversation
                    .conversation_id
                    .cmp(&b.conversation.conversation_id)
            });
            Ok(AdminConversationPage {
                conversations,
                next_cursor: None,
                has_more: false,
            })
        }

        async fn admin_get_conversation(
            &self,
            tenant_id: &str,
            conversation_id: &str,
        ) -> FlareResult<Option<AdminConversation>> {
            self.tenants_seen
                .lock()
                .unwrap()
                .push(tenant_id.to_string());
            Ok(self
                .conversations
                .get(&(tenant_id.to_string(), conversation_id.to_string()))
                .map(|members| Self::admin_conversation(tenant_id, conversation_id, members.len())))
        }

        async fn admin_list_participants(
            &self,
            tenant_id: &str,
            conversation_id: &str,
            _cursor: Option<&str>,
            _limit: i32,
            _include_removed: bool,
        ) -> FlareResult<Option<ConversationParticipantsPage>> {
            self.tenants_seen
                .lock()
                .unwrap()
                .push(tenant_id.to_string());
            Ok(self
                .conversations
                .get(&(tenant_id.to_string(), conversation_id.to_string()))
                .map(|members| ConversationParticipantsPage {
                    conversation_id: conversation_id.to_string(),
                    participants: members
                        .iter()
                        .map(|user_id| ConversationParticipant {
                            user_id: user_id.clone(),
                            roles: Vec::new(),
                            muted: false,
                            pinned: false,
                            attributes: HashMap::new(),
                            visible_from_seq: 0,
                            mention_only: false,
                        })
                        .collect(),
                    next_cursor: None,
                    has_more: false,
                    participant_version: 0,
                    member_count: members.len() as i32,
                }))
        }
    }

    const GATEWAY_KEY: [u8; 32] = [61; 32];
    const HOOK_KEY: [u8; 32] = [62; 32];

    fn pem(seed: [u8; 32]) -> (String, String) {
        let key = SigningKey::from_bytes(&seed);
        (
            key.to_pkcs8_pem(Default::default()).unwrap().to_string(),
            key.verifying_key()
                .to_public_key_pem(Default::default())
                .unwrap(),
        )
    }

    fn handler(
        with_key: bool,
    ) -> (
        Arc<MemoryAdminRepo>,
        ConversationAdminGrpcHandler<MemoryAdminRepo>,
    ) {
        let repo = Arc::new(
            MemoryAdminRepo::default()
                .with("tenant-a", "g-a", &["alice", "bob"])
                .with("tenant-b", "g-b", &["mallory"]),
        );
        let verifier = with_key.then(|| {
            let mut keys = KeySet::empty();
            keys.insert(kid::GATEWAY, &pem(GATEWAY_KEY).1).unwrap();
            keys.insert(kid::HOOK, &pem(HOOK_KEY).1).unwrap();
            SignedContextVerifier::from_key_set(keys).require_staff_kid([kid::GATEWAY])
        });
        (
            Arc::clone(&repo),
            ConversationAdminGrpcHandler::new(
                Arc::new(ConversationAdminQueryHandler::new(repo)),
                verifier,
            ),
        )
    }

    fn signer(seed: [u8; 32], key_id: &str) -> SignedContextSigner {
        SignedContextSigner::from_private_key(&pem(seed).0)
            .unwrap()
            .with_kid(key_id)
            .unwrap()
    }

    fn staff_identity(tenant: &str, permission: &str) -> SignedContextIdentity {
        SignedContextIdentity::new(SignedActorType::Staff, "employee-1", tenant, "req-1")
            .with_staff_grant(Some("org-1".into()), Some(permission.into()), 2)
    }

    fn with_ctx<T>(payload: T, token: &str) -> Request<T> {
        let mut request = Request::new(payload);
        request
            .metadata_mut()
            .insert("x-flare-ctx", token.parse().unwrap());
        request
    }

    /// 网关 admin 分区签的 Staff 上下文，带会话读的权限码。
    fn admin<T>(payload: T, tenant: &str) -> Request<T> {
        let token = signer(GATEWAY_KEY, kid::GATEWAY)
            .sign(&staff_identity(tenant, CONVERSATIONS_READ))
            .unwrap();
        with_ctx(payload, &token)
    }

    fn code<T>(result: Result<T, Status>) -> Code {
        result.map(|_| ()).unwrap_err().code()
    }

    fn get(conversation_id: &str) -> AdminGetConversationRequest {
        AdminGetConversationRequest {
            conversation_id: conversation_id.to_string(),
        }
    }

    fn participants(conversation_id: &str) -> AdminListParticipantsRequest {
        AdminListParticipantsRequest {
            conversation_id: conversation_id.to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn admin_reads_require_the_forwarded_staff_context() {
        let (repo, conversations) = handler(true);
        assert_eq!(
            code(
                conversations
                    .admin_list_conversations(
                        Request::new(AdminListConversationsRequest::default())
                    )
                    .await
            ),
            Code::Unauthenticated
        );
        assert_eq!(
            code(
                conversations
                    .admin_get_conversation(Request::new(get("g-a")))
                    .await
            ),
            Code::Unauthenticated
        );
        assert_eq!(
            code(
                conversations
                    .admin_list_participants(Request::new(participants("g-a")))
                    .await
            ),
            Code::Unauthenticated
        );
        // 未签名的租户/actor 元数据与平台断言都不是身份。
        let mut forged = Request::new(get("g-a"));
        let (private, _) = pem([63; 32]);
        let assertion = flare_server_core::TokenService::from_ed25519_private_key(
            &private,
            PLATFORM_ISSUER,
            60,
        )
        .unwrap()
        .generate_with_ed25519("employee-1", Some("tenant-a"), "admin", "flare-core-admin")
        .unwrap();
        for (key, value) in [
            ("x-tenant-id", "tenant-a"),
            ("x-actor-type", "system"),
            ("x-actor-id", "flare-admin-gateway"),
            ("authorization", &format!("Bearer {assertion}")),
        ] {
            forged.metadata_mut().insert(key, value.parse().unwrap());
        }
        assert_eq!(
            code(conversations.admin_get_conversation(forged).await),
            Code::Unauthenticated
        );
        let control = flare_server_core::TokenService::from_ed25519_private_key(
            &private,
            PLATFORM_ISSUER,
            60,
        )
        .unwrap()
        .generate_with_ed25519(
            "employee-1",
            Some("tenant-a"),
            "control",
            CORE_PROJECTION_AUDIENCE,
        )
        .unwrap();
        let mut bearer = Request::new(get("g-a"));
        bearer.metadata_mut().insert(
            "authorization",
            format!("Bearer {control}").parse().unwrap(),
        );
        assert_eq!(
            code(conversations.admin_get_conversation(bearer).await),
            Code::Unauthenticated
        );
        // 没配 Cell 公钥集时 fail closed。
        let (_, unkeyed) = handler(false);
        assert_eq!(
            code(
                unkeyed
                    .admin_get_conversation(admin(get("g-a"), "tenant-a"))
                    .await
            ),
            Code::Unauthenticated
        );
        assert!(repo.tenants_seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn admin_reads_reject_wrong_kid_permission_and_non_staff_contexts() {
        let (repo, conversations) = handler(true);
        // hook 钥签的 Staff 上下文：签名有效，但 Staff 钉死 gateway。
        let forged = signer(HOOK_KEY, kid::HOOK)
            .sign(&staff_identity("tenant-a", CONVERSATIONS_READ))
            .unwrap();
        assert_eq!(
            code(
                conversations
                    .admin_list_conversations(with_ctx(
                        AdminListConversationsRequest::default(),
                        &forged
                    ))
                    .await
            ),
            Code::Unauthenticated
        );
        // 别的路由的权限码，或没有权限码。
        for permission in [Some("core:messages:read"), Some("core:users:read"), None] {
            let identity = SignedContextIdentity::new(
                SignedActorType::Staff,
                "employee-1",
                "tenant-a",
                "req-1",
            )
            .with_staff_grant(Some("org-1".into()), permission.map(Into::into), 2);
            let token = signer(GATEWAY_KEY, kid::GATEWAY).sign(&identity).unwrap();
            assert_eq!(
                code(
                    conversations
                        .admin_list_participants(with_ctx(participants("g-a"), &token))
                        .await
                ),
                Code::PermissionDenied,
                "{permission:?}"
            );
        }
        // 网关钥签的非 Staff 上下文。
        let service = signer(GATEWAY_KEY, kid::GATEWAY)
            .sign(&SignedContextIdentity::new(
                SignedActorType::Service,
                "flare-social-bridge",
                "tenant-a",
                "req-2",
            ))
            .unwrap();
        assert_eq!(
            code(
                conversations
                    .admin_get_conversation(with_ctx(get("g-a"), &service))
                    .await
            ),
            Code::PermissionDenied
        );
        // 没有应用的平台范围 Staff 上下文。
        let platform_scoped = signer(GATEWAY_KEY, kid::GATEWAY)
            .sign(
                &SignedContextIdentity::new(SignedActorType::Staff, "employee-1", "", "req-3")
                    .with_staff_grant(None, Some(CONVERSATIONS_READ.into()), 2),
            )
            .unwrap();
        assert_eq!(
            code(
                conversations
                    .admin_get_conversation(with_ctx(get("g-a"), &platform_scoped))
                    .await
            ),
            Code::PermissionDenied
        );
        assert!(repo.tenants_seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn admin_reads_are_scoped_to_the_signed_tenant() {
        let (repo, conversations) = handler(true);
        let page = conversations
            .admin_list_conversations(admin(AdminListConversationsRequest::default(), "tenant-a"))
            .await
            .unwrap()
            .into_inner();
        let ids: Vec<_> = page
            .conversations
            .iter()
            .map(|c| (c.conversation_id.as_str(), c.member_count))
            .collect();
        assert_eq!(ids, vec![("g-a", 2)]);

        let detail = conversations
            .admin_get_conversation(admin(get("g-a"), "tenant-a"))
            .await
            .unwrap()
            .into_inner()
            .detail
            .unwrap();
        assert_eq!(detail.member_count, 2);
        assert_eq!(detail.conversation_type, "group");

        // 跨租户：tenant-a 的员工读 tenant-b 的会话，与不存在同样是 NOT_FOUND。
        assert_eq!(
            code(
                conversations
                    .admin_get_conversation(admin(get("g-b"), "tenant-a"))
                    .await
            ),
            Code::NotFound
        );
        assert_eq!(
            code(
                conversations
                    .admin_list_participants(admin(participants("g-b"), "tenant-a"))
                    .await
            ),
            Code::NotFound
        );
        let members = conversations
            .admin_list_participants(admin(participants("g-b"), "tenant-b"))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(members.member_count, 1);
        assert_eq!(members.participants[0].user_id, "mallory");
        assert_eq!(
            *repo.tenants_seen.lock().unwrap(),
            vec!["tenant-a", "tenant-a", "tenant-a", "tenant-a", "tenant-b"]
        );

        assert_eq!(
            code(
                conversations
                    .admin_get_conversation(admin(get(""), "tenant-a"))
                    .await
            ),
            Code::InvalidArgument
        );
    }
}
