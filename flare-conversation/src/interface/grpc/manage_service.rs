//! `ConversationManageService`：会话写命令与侧效应（游标、已读、在线等）

use flare_grpc_proto::conversation::conversation_manage_service_server::ConversationManageService;
use flare_grpc_proto::conversation::{
    CreateConversationRequest, CreateConversationResponse, DeleteConversationRequest,
    ForceConversationSyncRequest, ManageParticipantsRequest, ManageParticipantsResponse,
    MarkConversationAsReadRequest, SearchConversationsRequest, SearchConversationsResponse,
    UpdateConversationRequest, UpdateConversationResponse, UpdateConversationUserSettingsRequest,
    UpdateConversationUserSettingsResponse, UpdateCursorRequest, UpdatePresenceRequest,
};
use flare_proto::common::DeviceState as ProtoDeviceState;
use flare_server_core::error::grpc::IntoGrpc;
use flare_server_core::utils::require_ctx_from_request;
use tonic::{Request, Response, Status};

use crate::application::commands::{
    CreateConversationCommand, DeleteConversationCommand, ForceConversationSyncCommand,
    ManageParticipantsCommand, MarkConversationAsReadCommand, UpdateConversationCommand,
    UpdateConversationUserSettingsCommand, UpdateCursorCommand, UpdatePresenceCommand,
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
