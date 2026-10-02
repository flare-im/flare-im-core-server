//! `ConversationManageService`：会话写命令与侧效应（游标、已读、在线等）

use flare_grpc_proto::conversation::conversation_manage_service_server::ConversationManageService;
use flare_grpc_proto::conversation::{
    CreateConversationRequest, CreateConversationResponse, DeleteConversationRequest,
    ForceConversationSyncRequest, ManageParticipantsRequest, ManageParticipantsResponse,
    MarkConversationAsReadRequest, PurgeConversationHistoryRequest,
    PurgeConversationHistoryResponse, SearchConversationsRequest, SearchConversationsResponse,
    UpdateConversationRequest, UpdateConversationResponse, UpdateConversationUserSettingsRequest,
    UpdateConversationUserSettingsResponse, UpdateCursorRequest, UpdatePresenceRequest,
};
use flare_im_contracts::utils::normalize_tenant_id;
use flare_proto::common::DeviceState as ProtoDeviceState;
use flare_server_core::auth::{SignedActorType, SignedContextVerifier};
use flare_server_core::context::Ctx;
use flare_server_core::error::grpc::IntoGrpc;
use flare_server_core::grpc::middleware::signed_context::{
    VerifiedContext, verify_signed_context_metadata,
};
use flare_server_core::utils::require_ctx_from_request;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

use crate::application::commands::{
    CreateConversationCommand, DeleteConversationCommand, ForceConversationSyncCommand,
    ManageParticipantsCommand, MarkConversationAsReadCommand, PurgeConversationHistoryCommand,
    UpdateConversationCommand, UpdateConversationUserSettingsCommand, UpdateCursorCommand,
    UpdatePresenceCommand,
};
use crate::application::queries::SearchConversationsQuery;
use crate::domain::model::{
    ConflictResolutionPolicy, ConversationLifecycleState, ConversationType, ConversationVisibility,
    DeviceState,
};
use flare_server_core::error::{ErrorBuilder, ErrorCode};

use super::ConversationGrpcHandler;
use super::shared::{
    domain_to_proto_conversation, participant_domain_to_proto, participant_proto_to_domain,
    proto_summary,
};

/// `PurgeConversationHistory` 唯一允许的调用方：flare-social-bridge 的服务身份。
pub(crate) const PURGE_HISTORY_SERVICE_ACTOR: &str = "flare-social-bridge";

/// `PurgeConversationHistory` 的逐 RPC 验签（REL-03 会话管理验签第 ② 步，flare-social
/// `docs/REL03-friendship-epoch.md`）。
///
/// 清除历史是破坏性操作，只有 Social 调用：验 `x-flare-ctx`（与 message ingest 同一把
/// `FLARE_INTERNAL_CTX_PUBLIC_KEY`），只收服务身份 `flare-social-bridge`，租户只取签名里的，调用方
/// 元数据里的 `x-tenant-id`、`x-user-id` 不起作用。没配验证器时 fail closed（UNAVAILABLE）。
/// 以前这个 RPC 与其余会话管理 RPC 一样只看调用方自报的元数据，只靠网络隔离保护。其余 RPC
/// 仍按元数据（第 ③④ 步）。
pub(crate) fn authorize_purge_history(
    metadata: &MetadataMap,
    verifier: Option<&SignedContextVerifier>,
) -> Result<Ctx, Status> {
    let verifier = verifier.ok_or_else(|| {
        Status::unavailable("conversation purge identity verifier not configured")
    })?;
    let ctx = verify_signed_context_metadata(metadata, verifier)?;
    let claims = ctx
        .get_data::<VerifiedContext>()
        .and_then(|verified| verified.claims())
        .ok_or_else(|| Status::unauthenticated("signed purge identity required"))?;
    if claims.actor_type != SignedActorType::Service
        || claims.actor_id != PURGE_HISTORY_SERVICE_ACTOR
    {
        return Err(Status::permission_denied(
            "actor not permitted to purge conversation history",
        ));
    }
    if claims.tenant_id.trim().is_empty()
        || claims.tenant_id != normalize_tenant_id(&claims.tenant_id)
    {
        return Err(Status::permission_denied("non-canonical tenant identity"));
    }
    Ok(ctx)
}

#[tonic::async_trait]
impl ConversationManageService for ConversationGrpcHandler {
    async fn create_conversation(
        &self,
        request: Request<CreateConversationRequest>,
    ) -> Result<Response<CreateConversationResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        let participants: Vec<_> = req
            .participants
            .into_iter()
            .map(participant_proto_to_domain)
            .collect();
        let visibility = ConversationVisibility::from_proto(req.visibility);
        let conv = self
            .command_handler
            .handle_create_conversation(
                &ctx,
                CreateConversationCommand {
                    conversation_type: ConversationType::from_proto(req.conversation_type),
                    business_type: req.business_type,
                    participants,
                    attributes: req.attributes,
                    visibility,
                    channel_id: req.channel_id,
                },
            )
            .await
            .into_grpc()?;
        Ok(Response::new(CreateConversationResponse {
            conversation: Some(domain_to_proto_conversation(conv)),
        }))
    }

    async fn update_conversation(
        &self,
        request: Request<UpdateConversationRequest>,
    ) -> Result<Response<UpdateConversationResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        let display_name = if req.display_name.is_empty() {
            None
        } else {
            Some(req.display_name)
        };
        let attributes = if req.attributes.is_empty() {
            None
        } else {
            Some(req.attributes)
        };
        let visibility = if req.visibility == 0 {
            None
        } else {
            Some(ConversationVisibility::from_proto(req.visibility))
        };
        let lifecycle_state = if req.lifecycle_state == 0 {
            None
        } else {
            Some(ConversationLifecycleState::from_proto(req.lifecycle_state))
        };
        let conv = self
            .command_handler
            .handle_update_conversation(
                &ctx,
                UpdateConversationCommand {
                    conversation_id: req.conversation_id,
                    display_name,
                    attributes,
                    visibility,
                    lifecycle_state,
                },
            )
            .await
            .into_grpc()?;
        Ok(Response::new(UpdateConversationResponse {
            conversation: Some(domain_to_proto_conversation(conv)),
        }))
    }

    async fn delete_conversation(
        &self,
        request: Request<DeleteConversationRequest>,
    ) -> Result<Response<()>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        self.command_handler
            .handle_delete_conversation(
                &ctx,
                DeleteConversationCommand {
                    conversation_id: req.conversation_id,
                    hard_delete: req.hard_delete,
                },
            )
            .await
            .into_grpc()?;
        Ok(Response::new(()))
    }

    async fn purge_conversation_history(
        &self,
        request: Request<PurgeConversationHistoryRequest>,
    ) -> Result<Response<PurgeConversationHistoryResponse>, Status> {
        let ctx = authorize_purge_history(request.metadata(), self.purge_verifier.as_ref())?;
        let req = request.into_inner();
        let purged = self
            .command_handler
            .handle_purge_conversation_history(
                &ctx,
                PurgeConversationHistoryCommand {
                    conversation_id: req.conversation_id,
                    before_ms: req.before_ms,
                    epoch_attribute: req.epoch_attribute,
                    before_epoch: req.before_epoch,
                },
            )
            .await
            .into_grpc()?;
        Ok(Response::new(PurgeConversationHistoryResponse {
            purged_messages: purged.purged_messages,
            max_purged_seq: purged.max_purged_seq,
        }))
    }

    async fn manage_participants(
        &self,
        request: Request<ManageParticipantsRequest>,
    ) -> Result<Response<ManageParticipantsResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        let to_add: Vec<_> = req
            .to_add
            .into_iter()
            .map(participant_proto_to_domain)
            .collect();
        let role_updates: Vec<(String, Vec<String>)> = req
            .role_updates
            .into_iter()
            .map(|u| (u.user_id, u.roles))
            .collect();
        let participants = self
            .command_handler
            .handle_manage_participants(
                &ctx,
                ManageParticipantsCommand {
                    conversation_id: req.conversation_id,
                    to_add,
                    to_remove: req.to_remove,
                    role_updates,
                },
            )
            .await
            .into_grpc()?;
        Ok(Response::new(ManageParticipantsResponse {
            participants: participants
                .into_iter()
                .map(participant_domain_to_proto)
                .collect(),
        }))
    }

    async fn search_conversations(
        &self,
        request: Request<SearchConversationsRequest>,
    ) -> Result<Response<SearchConversationsResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        let limit = req
            .pagination
            .as_ref()
            .map(|p| p.limit.max(1) as usize)
            .unwrap_or(20);
        let offset = req
            .pagination
            .as_ref()
            .and_then(|p| p.cursor.parse::<usize>().ok())
            .unwrap_or(0);
        let (summaries, total) = self
            .query_handler
            .handle_search_conversations(
                &ctx,
                SearchConversationsQuery {
                    filters: vec![],
                    sort: vec![],
                    limit,
                    offset,
                },
            )
            .await
            .into_grpc()?;
        let conversations: Vec<_> = summaries.into_iter().map(proto_summary).collect();
        Ok(Response::new(SearchConversationsResponse {
            conversations,
            pagination: Some(flare_proto::common::Pagination {
                cursor: offset.saturating_add(limit).to_string(),
                limit: limit as i32,
                has_more: offset + limit < total,
                previous_cursor: String::new(),
                total_size: total as i64,
            }),
        }))
    }

    async fn update_cursor(
        &self,
        request: Request<UpdateCursorRequest>,
    ) -> Result<Response<()>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        self.command_handler
            .handle_update_cursor(
                &ctx,
                UpdateCursorCommand {
                    conversation_id: req.conversation_id.clone(),
                    sync_seq: req.sync_seq,
                },
            )
            .await
            .into_grpc()?;

        Ok(Response::new(()))
    }

    async fn mark_conversation_as_read(
        &self,
        request: Request<MarkConversationAsReadRequest>,
    ) -> Result<Response<()>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        self.command_handler
            .handle_mark_conversation_as_read(
                &ctx,
                MarkConversationAsReadCommand {
                    conversation_id: req.conversation_id,
                    read_seq: req.read_seq,
                },
            )
            .await
            .into_grpc()?;
        Ok(Response::new(()))
    }

    async fn update_conversation_user_settings(
        &self,
        request: Request<UpdateConversationUserSettingsRequest>,
    ) -> Result<Response<UpdateConversationUserSettingsResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        let settings = self
            .command_handler
            .handle_update_conversation_user_settings(
                &ctx,
                UpdateConversationUserSettingsCommand {
                    conversation_id: req.conversation_id,
                    is_mention_only: req.is_mention_only,
                    is_pinned: req.is_pinned,
                    is_muted: req.is_muted,
                    is_archived: req.is_archived,
                    draft: req.draft,
                    base_settings_version: req.base_settings_version,
                },
            )
            .await
            .into_grpc()?;
        Ok(Response::new(UpdateConversationUserSettingsResponse {
            settings: Some(flare_proto::common::ConversationUserSettings {
                is_pinned: settings.is_pinned,
                is_muted: settings.is_muted,
                is_archived: settings.is_archived,
                mute_until: None,
                draft: settings.draft.unwrap_or_default(),
                settings_version: settings.settings_version,
                is_mention_only: settings.is_mention_only,
            }),
        }))
    }

    async fn update_presence(
        &self,
        request: Request<UpdatePresenceRequest>,
    ) -> Result<Response<()>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        let state = match ProtoDeviceState::try_from(req.state).ok() {
            Some(ProtoDeviceState::Unspecified) | None => DeviceState::Unspecified,
            Some(ProtoDeviceState::Online) => DeviceState::Online,
            Some(ProtoDeviceState::Offline) => DeviceState::Offline,
            Some(ProtoDeviceState::Conflict) => DeviceState::Conflict,
        };

        let resolution = ConflictResolutionPolicy::from_proto(req.resolution);
        let resolution = if resolution == ConflictResolutionPolicy::Unspecified {
            None
        } else {
            Some(resolution)
        };

        self.command_handler
            .handle_update_presence(
                &ctx,
                UpdatePresenceCommand {
                    device_id: req.device_id.clone(),
                    device_platform: if req.device_platform.is_empty() {
                        None
                    } else {
                        Some(req.device_platform)
                    },
                    state,
                    conflict_resolution: resolution,
                    notify_conflict: req.notify_conflict,
                    conflict_reason: if req.conflict_reason.is_empty() {
                        None
                    } else {
                        Some(req.conflict_reason)
                    },
                },
            )
            .await
            .into_grpc()?;

        Ok(Response::new(()))
    }

    async fn force_conversation_sync(
        &self,
        request: Request<ForceConversationSyncRequest>,
    ) -> Result<Response<()>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        let missing = self
            .command_handler
            .handle_force_conversation_sync(
                &ctx,
                ForceConversationSyncCommand {
                    conversation_ids: req.conversation_ids.clone(),
                    reason: if req.reason.is_empty() {
                        None
                    } else {
                        Some(req.reason)
                    },
                },
            )
            .await
            .into_grpc()?;

        if !missing.is_empty() {
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                format!("unknown conversations: {}", missing.join(",")),
            )
            .build_error()
            .into());
        }

        Ok(Response::new(()))
    }
}

#[cfg(test)]
mod purge_authorization_tests {
    //! `PurgeConversationHistory` 的验签与身份规则（REL-03 第 ② 步）。

    use super::*;
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_server_core::{SignedContextIdentity, SignedContextSigner};

    fn keys(seed: u8) -> (SignedContextSigner, SignedContextVerifier) {
        let key = SigningKey::from_bytes(&[seed; 32]);
        let private = key.to_pkcs8_pem(Default::default()).unwrap();
        let public = key
            .verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap();
        (
            SignedContextSigner::from_private_key(&private).unwrap(),
            SignedContextVerifier::from_public_key(&public).unwrap(),
        )
    }

    fn metadata(token: Option<&str>) -> MetadataMap {
        let mut metadata = MetadataMap::new();
        // 调用方自报的元数据不起作用。
        metadata.insert("x-tenant-id", "tenant-forged".parse().unwrap());
        metadata.insert("x-user-id", "mallory".parse().unwrap());
        if let Some(token) = token {
            metadata.insert("x-flare-ctx", token.parse().unwrap());
        }
        metadata
    }

    fn token(
        signer: &SignedContextSigner,
        actor_type: SignedActorType,
        actor: &str,
        tenant: &str,
    ) -> String {
        signer
            .sign(&SignedContextIdentity::new(
                actor_type,
                actor,
                tenant,
                "req-purge",
            ))
            .unwrap()
    }

    #[test]
    fn the_bridge_service_identity_is_accepted_with_the_signed_tenant() {
        let (signer, verifier) = keys(51);
        let bridge = token(
            &signer,
            SignedActorType::Service,
            PURGE_HISTORY_SERVICE_ACTOR,
            "tenant-a",
        );
        let ctx = authorize_purge_history(&metadata(Some(&bridge)), Some(&verifier))
            .expect("bridge identity");
        assert_eq!(ctx.tenant_id(), Some("tenant-a"));
        assert_eq!(ctx.user_id(), None);
    }

    #[test]
    fn a_missing_token_is_unauthenticated() {
        let (_, verifier) = keys(51);
        let status = authorize_purge_history(&metadata(None), Some(&verifier)).unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn a_token_from_another_key_is_unauthenticated() {
        let (_, verifier) = keys(51);
        let (other_signer, _) = keys(52);
        let forged = token(
            &other_signer,
            SignedActorType::Service,
            PURGE_HISTORY_SERVICE_ACTOR,
            "tenant-a",
        );
        let status =
            authorize_purge_history(&metadata(Some(&forged)), Some(&verifier)).unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn users_and_other_services_are_denied() {
        let (signer, verifier) = keys(51);
        for (actor_type, actor) in [
            (SignedActorType::User, "alice"),
            (SignedActorType::User, PURGE_HISTORY_SERVICE_ACTOR),
            (SignedActorType::Service, "flare-api-gateway"),
            (SignedActorType::System, PURGE_HISTORY_SERVICE_ACTOR),
            (SignedActorType::TenantAdmin, "admin-1"),
        ] {
            let other = token(&signer, actor_type, actor, "tenant-a");
            let status =
                authorize_purge_history(&metadata(Some(&other)), Some(&verifier)).unwrap_err();
            assert_eq!(
                status.code(),
                tonic::Code::PermissionDenied,
                "{actor_type:?} {actor}"
            );
        }
    }

    #[test]
    fn a_non_canonical_tenant_is_denied() {
        let (signer, verifier) = keys(51);
        let bridge = token(
            &signer,
            SignedActorType::Service,
            PURGE_HISTORY_SERVICE_ACTOR,
            "default",
        );
        let status =
            authorize_purge_history(&metadata(Some(&bridge)), Some(&verifier)).unwrap_err();
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
    }

    /// 没配验证器时 fail closed，带了合法令牌也一样。
    #[test]
    fn a_missing_verifier_fails_closed() {
        let (signer, _) = keys(51);
        let bridge = token(
            &signer,
            SignedActorType::Service,
            PURGE_HISTORY_SERVICE_ACTOR,
            "tenant-a",
        );
        let status = authorize_purge_history(&metadata(Some(&bridge)), None).unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }
}
