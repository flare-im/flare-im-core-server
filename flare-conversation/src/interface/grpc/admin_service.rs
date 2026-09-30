//! `ConversationAdminService`：管理员只读（admin-gateway 专用下游）。
//!
//! 只认 admin-gateway 原样转发的 admin 令牌（受众 `flare-core-admin`、员工、具体租户），
//! 不依赖未签名的 `x-tenant-id` / actor 元数据；租户只取自令牌，请求里没有 tenant_id 与 user_id。

use std::sync::Arc;

use flare_grpc_proto::conversation::conversation_admin_service_server::ConversationAdminService;
use flare_grpc_proto::conversation::{
    AdminGetConversationRequest, AdminGetConversationResponse, AdminListConversationsRequest,
    AdminListConversationsResponse, AdminListParticipantsRequest, AdminListParticipantsResponse,
};
use flare_im_service_kit::control_auth::{ConsoleControlVerifier, require_grpc_admin};
use flare_proto::common::ConversationDetail;
use flare_server_core::error::grpc::IntoGrpc;
use tonic::{Request, Response, Status};

use super::shared::{domain_to_conversation_detail, participant_domain_to_proto};
use crate::application::ConversationAdminQueryHandler;
use crate::domain::model::AdminConversation;
use crate::domain::repository::ConversationAdminRepository;

pub struct ConversationAdminGrpcHandler<R: ConversationAdminRepository + 'static> {
    queries: Arc<ConversationAdminQueryHandler<R>>,
    control_verifier: Option<ConsoleControlVerifier>,
}

impl<R: ConversationAdminRepository + 'static> ConversationAdminGrpcHandler<R> {
    pub fn new(
        queries: Arc<ConversationAdminQueryHandler<R>>,
        control_verifier: Option<ConsoleControlVerifier>,
    ) -> Self {
        Self {
            queries,
            control_verifier,
        }
    }
}

impl<R: ConversationAdminRepository + 'static> Clone for ConversationAdminGrpcHandler<R> {
    fn clone(&self) -> Self {
        Self {
            queries: Arc::clone(&self.queries),
            control_verifier: self.control_verifier.clone(),
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
        let actor = require_grpc_admin(&request, self.control_verifier.as_ref())?;
        let req = request.into_inner();
        tracing::info!(actor_id = %actor.subject, tenant_id = %actor.tenant_id, "admin list_conversations");
        let page = self
            .queries
            .list_conversations(
                &actor.tenant_id,
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
        let actor = require_grpc_admin(&request, self.control_verifier.as_ref())?;
        let req = request.into_inner();
        tracing::info!(actor_id = %actor.subject, tenant_id = %actor.tenant_id, conversation_id = %req.conversation_id, "admin get_conversation");
        let conversation = self
            .queries
            .get_conversation(&actor.tenant_id, &req.conversation_id)
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
        let actor = require_grpc_admin(&request, self.control_verifier.as_ref())?;
        let req = request.into_inner();
        tracing::info!(actor_id = %actor.subject, tenant_id = %actor.tenant_id, conversation_id = %req.conversation_id, "admin list_participants");
        let page = self
            .queries
            .list_participants(
                &actor.tenant_id,
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
    //! 缺令牌、错误受众、跨租户三类负向用例，外加租户只取自令牌的正向对照。

    use std::collections::HashMap;
    use std::sync::Mutex;

    use chrono::Utc;
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_im_service_kit::control_auth::{
        CONSOLE_ISSUER, CORE_ADMIN_AUDIENCE, CORE_CONTROL_AUDIENCE, PROJECTION_WORKER_SUBJECT,
        SCHEDULER_SUBJECT,
    };
    use flare_server_core::error::Result as FlareResult;
    use tonic::Code;

    use super::*;
    use crate::domain::model::{
        AdminConversationPage, Conversation, ConversationLifecycleState, ConversationParticipant,
        ConversationParticipantsPage, ConversationType, ConversationVisibility,
    };

    /// (tenant, conversation) → 成员；记录每次调用收到的租户，证明租户只来自令牌。
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

    const KEY: [u8; 32] = [61; 32];

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
            let public = SigningKey::from_bytes(&KEY)
                .verifying_key()
                .to_public_key_pem(Default::default())
                .unwrap();
            ConsoleControlVerifier::from_public_key(&public).unwrap()
        });
        (
            Arc::clone(&repo),
            ConversationAdminGrpcHandler::new(
                Arc::new(ConversationAdminQueryHandler::new(repo)),
                verifier,
            ),
        )
    }

    fn signed<T>(
        payload: T,
        subject: &str,
        tenant: &str,
        token_use: &str,
        aud: &str,
    ) -> Request<T> {
        let private = SigningKey::from_bytes(&KEY)
            .to_pkcs8_pem(Default::default())
            .unwrap();
        let token =
            flare_server_core::TokenService::from_ed25519_private_key(&private, CONSOLE_ISSUER, 60)
                .unwrap()
                .generate_with_ed25519(subject, Some(tenant), token_use, aud)
                .unwrap();
        let mut request = Request::new(payload);
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        request
    }

    fn admin<T>(payload: T, tenant: &str) -> Request<T> {
        signed(payload, "employee-1", tenant, "admin", CORE_ADMIN_AUDIENCE)
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
    async fn admin_reads_require_the_forwarded_admin_token() {
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
        // 未签名的租户/actor 元数据不是身份。
        let mut forged = Request::new(get("g-a"));
        for (key, value) in [
            ("x-tenant-id", "tenant-a"),
            ("x-actor-type", "system"),
            ("x-actor-id", "flare-admin-gateway"),
        ] {
            forged.metadata_mut().insert(key, value.parse().unwrap());
        }
        assert_eq!(
            code(conversations.admin_get_conversation(forged).await),
            Code::Unauthenticated
        );
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
    async fn admin_reads_reject_wrong_audience_and_non_employee_tokens() {
        let (repo, conversations) = handler(true);
        for (token_use, aud) in [
            ("control", CORE_CONTROL_AUDIENCE),
            ("admin", CORE_CONTROL_AUDIENCE),
            ("control", CORE_ADMIN_AUDIENCE),
        ] {
            assert_eq!(
                code(
                    conversations
                        .admin_list_conversations(signed(
                            AdminListConversationsRequest::default(),
                            "employee-1",
                            "tenant-a",
                            token_use,
                            aud,
                        ))
                        .await
                ),
                Code::Unauthenticated,
                "{token_use}/{aud}"
            );
        }
        for (subject, tenant) in [
            (PROJECTION_WORKER_SUBJECT, "tenant-a"),
            (SCHEDULER_SUBJECT, "tenant-a"),
            ("employee-1", "*"),
        ] {
            assert_eq!(
                code(
                    conversations
                        .admin_list_participants(signed(
                            participants("g-a"),
                            subject,
                            tenant,
                            "admin",
                            CORE_ADMIN_AUDIENCE,
                        ))
                        .await
                ),
                Code::PermissionDenied,
                "{subject}/{tenant}"
            );
        }
        assert!(repo.tenants_seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn admin_reads_are_scoped_to_the_token_tenant() {
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

        // 跨租户：tenant-a 的管理员读 tenant-b 的会话，与不存在同样是 NOT_FOUND。
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
