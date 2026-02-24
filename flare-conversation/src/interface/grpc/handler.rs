use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use flare_proto::common;
use flare_proto::common::DeviceState as ProtoDeviceState;
use flare_proto::common::ConversationSummary as ProtoConversationSummary;
use flare_proto::conversation::conversation_service_server::ConversationService;
use flare_proto::conversation::{
    BatchAcknowledgeRequest, BatchAcknowledgeResponse, CreateConversationRequest, CreateConversationResponse,
    DeleteConversationRequest, DeleteConversationResponse, DevicePresence as ProtoDevicePresence,
    ForceConversationSyncRequest, ForceConversationSyncResponse, ListConversationsRequest, ListConversationsResponse,
    ManageParticipantsRequest, ManageParticipantsResponse, SearchConversationsRequest,
    SearchConversationsResponse, ConversationBootstrapRequest, ConversationBootstrapResponse,
    ConversationPolicy as ProtoConversationPolicy, SyncMessagesRequest, SyncMessagesResponse,
    UpdateCursorRequest, UpdateCursorResponse, UpdatePresenceRequest, UpdatePresenceResponse,
    UpdateConversationRequest, UpdateConversationResponse,
};
use flare_server_core::context::Context;
use flare_server_core::error;
use flare_im_core::utils::context::require_context;
use prost_types::Timestamp;
use tonic::{Request, Response, Status};

use crate::application::commands::{
    BatchAcknowledgeCommand, CreateConversationCommand, DeleteConversationCommand, ForceConversationSyncCommand,
    ManageParticipantsCommand, UpdateCursorCommand, UpdatePresenceCommand, UpdateConversationCommand,
};
use crate::application::handlers::{ConversationCommandHandler, ConversationQueryHandler};
use crate::application::queries::{
    ListConversationsQuery, SearchConversationsQuery, ConversationBootstrapQuery, SyncMessagesQuery,
};
use crate::domain::model::{
    ConflictResolutionPolicy, DevicePresence, DeviceState, Conversation, ConversationFilter,
    ConversationLifecycleState, ConversationParticipant, ConversationPolicy, ConversationSort, ConversationSummary,
    ConversationVisibility, Thread, ThreadSortOrder,
};
use crate::domain::service::ThreadDomainService;

#[derive(Clone)]
pub struct ConversationGrpcHandler {
    command_handler: Arc<ConversationCommandHandler>,
    query_handler: Arc<ConversationQueryHandler>,
    thread_service: Option<Arc<ThreadDomainService>>,
}

impl ConversationGrpcHandler {
    pub fn new(
        command_handler: Arc<ConversationCommandHandler>,
        query_handler: Arc<ConversationQueryHandler>,
        thread_service: Option<Arc<ThreadDomainService>>,
    ) -> Self {
        Self {
            command_handler,
            query_handler,
            thread_service,
        }
    }
}

#[tonic::async_trait]
impl ConversationService for ConversationGrpcHandler {
    async fn conversation_bootstrap(
        &self,
        request: Request<ConversationBootstrapRequest>,
    ) -> Result<Response<ConversationBootstrapResponse>, Status> {
        let ctx = require_context(&request)?;
        let req = request.into_inner();
        let cursor_map = req.client_cursor_map;

        let include_recent = req.include_recent_messages;
        let recent_limit = if req.recent_message_limit > 0 {
            Some(req.recent_message_limit)
        } else {
            None
        };

        let bootstrap = self
            .query_handler
            .handle_conversation_bootstrap(
                &ctx,
                ConversationBootstrapQuery {
                    client_cursor: cursor_map.clone(),
                    include_recent,
                    recent_limit,
                },
            )
            .await
            .map_err(internal_error)?;

        let response = ConversationBootstrapResponse {
            conversations: bootstrap.summaries.into_iter().map(proto_summary).collect(),
            recent_messages: bootstrap.recent_messages,
            devices: bootstrap.devices.into_iter().map(proto_device).collect(),
            server_cursor_map: bootstrap.cursor_map,
            policy: Some(proto_policy(bootstrap.policy)),
            status: Some(error::ok_status()),
        };

        Ok(Response::new(response))
    }

    async fn list_conversations(
        &self,
        request: Request<ListConversationsRequest>,
    ) -> Result<Response<ListConversationsResponse>, Status> {
        let ctx = require_context(&request)?;
        let req = request.into_inner();
        let (summaries, next_cursor, has_more) = self
            .query_handler
            .handle_list_conversations(
                &ctx,
                ListConversationsQuery {
                    cursor: if req.cursor.is_empty() {
                        None
                    } else {
                        Some(req.cursor)
                    },
                    limit: if req.limit > 0 { req.limit } else { 20 },
                },
            )
            .await
            .map_err(internal_error)?;

        let response = ListConversationsResponse {
            conversations: summaries.into_iter().map(proto_summary).collect(),
            next_cursor: next_cursor.unwrap_or_default(),
            has_more,
            status: Some(error::ok_status()),
        };

        Ok(Response::new(response))
    }

    async fn sync_messages(
        &self,
        request: Request<SyncMessagesRequest>,
    ) -> Result<Response<SyncMessagesResponse>, Status> {
        let ctx = require_context(&request)?;
        let req = request.into_inner();

        let result = self
            .query_handler
            .handle_sync_messages(
                &ctx,
                SyncMessagesQuery {
                    conversation_id: req.conversation_id.clone(),
                    since_ts: req.since_ts,
                    cursor: if req.cursor.is_empty() {
                        None
                    } else {
                        Some(req.cursor)
                    },
                    limit: if req.limit > 0 { req.limit } else { 50 },
                },
            )
            .await
            .map_err(|err| {
                if err.to_string().contains("message provider not configured") {
                    failed_precondition(err)
                } else {
                    internal_error(err)
                }
            })?;

        let response = SyncMessagesResponse {
            messages: result.messages,
            next_cursor: result.next_cursor.unwrap_or_default(),
            server_cursor_ts: result.server_cursor_ts.unwrap_or_default(),
            status: Some(error::ok_status()),
        };

        Ok(Response::new(response))
    }

    async fn update_cursor(
        &self,
        request: Request<UpdateCursorRequest>,
    ) -> Result<Response<UpdateCursorResponse>, Status> {
        let ctx = require_context(&request)?;
        let req = request.into_inner();
        self.command_handler
            .handle_update_cursor(
                &ctx,
                UpdateCursorCommand {
                    conversation_id: req.conversation_id.clone(),
                    message_ts: req.message_ts,
                },
            )
            .await
            .map_err(internal_error)?;

        Ok(Response::new(UpdateCursorResponse {
            status: Some(error::ok_status()),
        }))
    }

    async fn update_presence(
        &self,
        request: Request<UpdatePresenceRequest>,
    ) -> Result<Response<UpdatePresenceResponse>, Status> {
        let ctx = require_context(&request)?;
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
            .map_err(internal_error)?;

        Ok(Response::new(UpdatePresenceResponse {
            status: Some(error::ok_status()),
        }))
    }

    async fn force_conversation_sync(
        &self,
        request: Request<ForceConversationSyncRequest>,
    ) -> Result<Response<ForceConversationSyncResponse>, Status> {
        let ctx = require_context(&request)?;
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
            .map_err(internal_error)?;

        if !missing.is_empty() {
            return Err(Status::failed_precondition(format!(
                "unknown conversations: {}",
                missing.join(",")
            )));
        }

        Ok(Response::new(ForceConversationSyncResponse {
            status: Some(error::ok_status()),
        }))
    }

    async fn create_conversation(
        &self,
        request: Request<CreateConversationRequest>,
    ) -> Result<Response<CreateConversationResponse>, Status> {
        let ctx = require_context(&request)?;
        let req = request.into_inner();

        let participants: Vec<ConversationParticipant> = req
            .participants
            .into_iter()
            .map(|p| ConversationParticipant {
                user_id: p.user_id,
                roles: p.roles,
                muted: p.muted,
                pinned: p.pinned,
                attributes: p.attributes,
            })
            .collect();

        let visibility = ConversationVisibility::from_proto(req.visibility);

        let conversation = self
            .command_handler
            .handle_create_conversation(
                &ctx,
                CreateConversationCommand {
                    conversation_type: req.conversation_type,
                    business_type: req.business_type,
                    participants,
                    attributes: req.attributes,
                    visibility,
                },
            )
            .await
            .map_err(internal_error)?;

        Ok(Response::new(CreateConversationResponse {
            conversation: Some(domain_to_proto_conversation(conversation)),
            status: Some(error::ok_status()),
        }))
    }

    async fn update_conversation(
        &self,
        request: Request<UpdateConversationRequest>,
    ) -> Result<Response<UpdateConversationResponse>, Status> {
        let ctx = require_context(&request)?;
        let req = request.into_inner();

        let display_name = if req.display_name.is_empty() {
            None
        } else {
            Some(req.display_name)
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

        let conversation = self
            .command_handler
            .handle_update_conversation(
                &ctx,
                UpdateConversationCommand {
                    conversation_id: req.conversation_id.clone(),
                    display_name,
                    attributes: if req.attributes.is_empty() {
                        None
                    } else {
                        Some(req.attributes)
                    },
                    visibility,
                    lifecycle_state,
                },
            )
            .await
            .map_err(internal_error)?;

        Ok(Response::new(UpdateConversationResponse {
            conversation: Some(domain_to_proto_conversation(conversation)),
            status: Some(error::ok_status()),
        }))
    }

    async fn delete_conversation(
        &self,
        request: Request<DeleteConversationRequest>,
    ) -> Result<Response<DeleteConversationResponse>, Status> {
        let ctx = require_context(&request)?;
        let req = request.into_inner();

        self.command_handler
            .handle_delete_conversation(
                &ctx,
                DeleteConversationCommand {
                    conversation_id: req.conversation_id.clone(),
                    hard_delete: req.hard_delete,
                },
            )
            .await
            .map_err(internal_error)?;

        Ok(Response::new(DeleteConversationResponse {
            status: Some(error::ok_status()),
        }))
    }

    async fn manage_participants(
        &self,
        request: Request<ManageParticipantsRequest>,
    ) -> Result<Response<ManageParticipantsResponse>, Status> {
        let ctx = require_context(&request)?;
        let req = request.into_inner();

        let to_add: Vec<ConversationParticipant> = req
            .to_add
            .into_iter()
            .map(|p| ConversationParticipant {
                user_id: p.user_id,
                roles: p.roles,
                muted: p.muted,
                pinned: p.pinned,
                attributes: p.attributes,
            })
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
                    conversation_id: req.conversation_id.clone(),
                    to_add,
                    to_remove: req.to_remove,
                    role_updates,
                },
            )
            .await
            .map_err(internal_error)?;

        Ok(Response::new(ManageParticipantsResponse {
            participants: participants
                .into_iter()
                .map(|p| flare_proto::conversation::ConversationParticipant {
                    user_id: p.user_id,
                    roles: p.roles,
                    muted: p.muted,
                    pinned: p.pinned,
                    attributes: p.attributes,
                })
                .collect(),
            status: Some(error::ok_status()),
        }))
    }

    async fn batch_acknowledge(
        &self,
        request: Request<BatchAcknowledgeRequest>,
    ) -> Result<Response<BatchAcknowledgeResponse>, Status> {
        let ctx = require_context(&request)?;
        let req = request.into_inner();

        let cursors: Vec<(String, i64)> = req
            .cursors
            .into_iter()
            .map(|c| (c.conversation_id, c.message_ts))
            .collect();

        self.command_handler
            .handle_batch_acknowledge(
                &ctx,
                BatchAcknowledgeCommand {
                    cursors,
                },
            )
            .await
            .map_err(internal_error)?;

        Ok(Response::new(BatchAcknowledgeResponse {
            status: Some(error::ok_status()),
        }))
    }

    async fn search_conversations(
        &self,
        request: Request<SearchConversationsRequest>,
    ) -> Result<Response<SearchConversationsResponse>, Status> {
        let ctx = require_context(&request)?;
        let req = request.into_inner();

        // 从protobuf FilterExpression转换为domain models
        let mut filters = Vec::new();
        for filter_expr in &req.filters {
            let filter = match filter_expr.field.as_str() {
                "conversation_type" => {
                    if !filter_expr.values.is_empty() {
                        Some(ConversationFilter {
                            conversation_type: Some(filter_expr.values[0].clone()),
                            business_type: None,
                            lifecycle_state: None,
                            visibility: None,
                            participant_user_id: None,
                        })
                    } else {
                        None
                    }
                }
                "business_type" => {
                    if !filter_expr.values.is_empty() {
                        Some(ConversationFilter {
                            conversation_type: None,
                            business_type: Some(filter_expr.values[0].clone()),
                            lifecycle_state: None,
                            visibility: None,
                            participant_user_id: None,
                        })
                    } else {
                        None
                    }
                }
                "lifecycle_state" => {
                    if !filter_expr.values.is_empty() {
                        let state_str = &filter_expr.values[0];
                        let state = match state_str.as_str() {
                            "active" => ConversationLifecycleState::Active,
                            "suspended" => ConversationLifecycleState::Suspended,
                            "archived" => ConversationLifecycleState::Archived,
                            "deleted" => ConversationLifecycleState::Deleted,
                            _ => ConversationLifecycleState::Unspecified,
                        };
                        Some(ConversationFilter {
                            conversation_type: None,
                            business_type: None,
                            lifecycle_state: Some(state),
                            visibility: None,
                            participant_user_id: None,
                        })
                    } else {
                        None
                    }
                }
                "visibility" => {
                    if !filter_expr.values.is_empty() {
                        let vis_str = &filter_expr.values[0];
                        let vis = match vis_str.as_str() {
                            "private" => ConversationVisibility::Private,
                            "tenant" => ConversationVisibility::Tenant,
                            "public" => ConversationVisibility::Public,
                            _ => ConversationVisibility::Unspecified,
                        };
                        Some(ConversationFilter {
                            conversation_type: None,
                            business_type: None,
                            lifecycle_state: None,
                            visibility: Some(vis),
                            participant_user_id: None,
                        })
                    } else {
                        None
                    }
                }
                "participant_user_id" => {
                    if !filter_expr.values.is_empty() {
                        Some(ConversationFilter {
                            conversation_type: None,
                            business_type: None,
                            lifecycle_state: None,
                            visibility: None,
                            participant_user_id: Some(filter_expr.values[0].clone()),
                        })
                    } else {
                        None
                    }
                }
                _ => None,
            };
            if let Some(f) = filter {
                filters.push(f);
            }
        }

        // 从protobuf SortExpression转换为domain models
        let sort: Vec<ConversationSort> = req
            .sort
            .into_iter()
            .map(|s| ConversationSort {
                field: s.field,
                ascending: s.direction == common::SortDirection::Asc as i32,
            })
            .collect();

        let limit = req
            .pagination
            .as_ref()
            .map(|p| p.limit as usize)
            .unwrap_or(20)
            .min(1000);
        let offset = 0; // 分页使用cursor，offset暂时为0

        let (summaries, total) = self
            .query_handler
            .handle_search_conversations(
                &ctx,
                SearchConversationsQuery {
                    filters,
                    sort,
                    limit,
                    offset,
                },
            )
            .await
            .map_err(internal_error)?;

        // 更新pagination信息
        let mut pagination = req.pagination.unwrap_or_default();
        pagination.total_size = total as i64;
        pagination.has_more = summaries.len() >= limit;

        Ok(Response::new(SearchConversationsResponse {
            conversations: summaries.into_iter().map(proto_summary).collect(),
            pagination: Some(pagination),
            status: Some(error::ok_status()),
        }))
    }

    async fn create_thread(
        &self,
        request: Request<flare_proto::conversation::CreateThreadRequest>,
    ) -> Result<Response<flare_proto::conversation::CreateThreadResponse>, Status> {
        let ctx = require_context(&request)?;
        let req = request.into_inner();
        let thread_service = self
            .thread_service
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("Thread service not configured"))?;

        let conversation_id = req.conversation_id.clone();
        let root_message_id = req.root_message_id.clone();
        let title = if req.title.is_empty() { None } else { Some(req.title.as_str()) };

        let created_thread = thread_service
            .create_thread(&ctx, &conversation_id, &root_message_id, title, ctx.user_id().unwrap_or(""))
            .await
            .map_err(internal_error)?;

        Ok(Response::new(flare_proto::conversation::CreateThreadResponse {
            thread: Some(thread_to_proto(created_thread)),
            status: Some(error::ok_status()),
        }))
    }

    async fn list_threads(
        &self,
        request: Request<flare_proto::conversation::ListThreadsRequest>,
    ) -> Result<Response<flare_proto::conversation::ListThreadsResponse>, Status> {
        // TODO: 实现列表功能，目前返回空列表
        Ok(Response::new(flare_proto::conversation::ListThreadsResponse {
            threads: vec![],
            total_count: 0,
            has_more: false,
            status: Some(error::ok_status()),
        }))
    }

    async fn get_thread(
        &self,
        request: Request<flare_proto::conversation::GetThreadRequest>,
    ) -> Result<Response<flare_proto::conversation::GetThreadResponse>, Status> {
        let ctx = require_context(&request)?;
        let req = request.into_inner();
        let thread_service = self
            .thread_service
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("Thread service not configured"))?;

        let thread = thread_service
            .get_thread(&ctx, &req.thread_id)
            .await
            .map_err(internal_error)?
            .ok_or_else(|| Status::not_found("Thread not found"))?;

        Ok(Response::new(flare_proto::conversation::GetThreadResponse {
            thread: Some(thread_to_proto(thread)),
            status: Some(error::ok_status()),
        }))
    }

    async fn update_thread(
        &self,
        request: Request<flare_proto::conversation::UpdateThreadRequest>,
    ) -> Result<Response<flare_proto::conversation::UpdateThreadResponse>, Status> {
        let ctx = require_context(&request)?;
        let req = request.into_inner();
        let thread_service = self
            .thread_service
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("Thread service not configured"))?;

        thread_service
            .update_thread(
                &ctx,
                &req.thread_id,
                req.title.as_deref(),
                req.is_pinned,
                req.is_locked,
                req.is_archived,
            )
            .await
            .map_err(internal_error)?;

        // 获取更新后的线程信息
        let updated_thread = thread_service
            .get_thread(&ctx, &req.thread_id)
            .await
            .map_err(internal_error)?
            .ok_or_else(|| Status::not_found("Thread not found after update"))?;

        Ok(Response::new(flare_proto::conversation::UpdateThreadResponse {
            thread: Some(thread_to_proto(updated_thread)),
            status: Some(error::ok_status()),
        }))
    }

    async fn delete_thread(
        &self,
        request: Request<flare_proto::conversation::DeleteThreadRequest>,
    ) -> Result<Response<flare_proto::conversation::DeleteThreadResponse>, Status> {
        let ctx = require_context(&request)?;
        let req = request.into_inner();
        let thread_service = self
            .thread_service
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("Thread service not configured"))?;

        thread_service
            .delete_thread(&ctx, &req.thread_id)
            .await
            .map_err(internal_error)?;

        Ok(Response::new(flare_proto::conversation::DeleteThreadResponse {
            success: true,
            status: Some(error::ok_status()),
        }))
    }

    async fn sync_conversations(
        &self,
        request: Request<flare_proto::common::SyncConversationsRequest>,
    ) -> Result<Response<flare_proto::common::SyncConversationsResponse>, Status> {
        let ctx = require_context(&request)?;
        let req = request.into_inner();

        let client_ms = req
            .client_conversation_cursor
            .as_ref()
            .map(|ts| ts.seconds as i64 * 1000 + (ts.nanos as i64) / 1_000_000)
            .unwrap_or(0);

        let limit = if req.limit > 0 { req.limit } else { 100 } as usize;

        let bootstrap = self
            .query_handler
            .handle_conversation_bootstrap(
                &ctx,
                ConversationBootstrapQuery {
                    client_cursor: HashMap::new(),
                    include_recent: false,
                    recent_limit: None,
                },
            )
            .await
            .map_err(internal_error)?;

        let mut candidates: Vec<_> = bootstrap
            .summaries
            .into_iter()
            .filter(|s| s.server_cursor_ts.unwrap_or(0) > client_ms)
            .collect();

        candidates.sort_by_key(|s| s.server_cursor_ts.unwrap_or(0));

        let has_more = candidates.len() > limit;
        candidates.truncate(limit);

        let patches: Vec<flare_proto::common::ConversationPatch> = candidates
            .into_iter()
            .map(|s| {
                let patched_at = s.server_cursor_ts.map(|ms| Timestamp {
                    seconds: ms / 1000,
                    nanos: ((ms % 1000) * 1_000_000) as i32,
                });
                flare_proto::common::ConversationPatch {
                    conversation_id: s.conversation_id.clone(),
                    patch_type: flare_proto::common::ConversationPatchType::ConversationPatchSummary as i32,
                    light: None,
                    summary: Some(proto_summary(s)),
                    patched_at,
                }
            })
            .collect();

        let server_cursor_ts = patches
            .iter()
            .filter_map(|p| p.patched_at.as_ref())
            .map(|ts| ts.seconds as i64 * 1000 + (ts.nanos as i64) / 1_000_000)
            .max()
            .unwrap_or_else(|| Utc::now().timestamp_millis());

        let server_conversation_cursor = Some(Timestamp {
            seconds: server_cursor_ts / 1000,
            nanos: ((server_cursor_ts % 1000) * 1_000_000) as i32,
        });

        Ok(Response::new(flare_proto::common::SyncConversationsResponse {
            patches,
            server_conversation_cursor,
            has_more,
            status: Some(error::ok_status()),
        }))
    }

    async fn get_all_conversations(
        &self,
        request: Request<flare_proto::common::ConversationSyncAllRequest>,
    ) -> Result<Response<flare_proto::common::ConversationSyncAllResponse>, Status> {
        let ctx = require_context(&request)?;
        let req = request.into_inner();

        let bootstrap = self
            .query_handler
            .handle_conversation_bootstrap(
                &ctx,
                ConversationBootstrapQuery {
                    client_cursor: HashMap::new(),
                    include_recent: false,
                    recent_limit: None,
                },
            )
            .await
            .map_err(internal_error)?;

        let conversations: Vec<ProtoConversationSummary> =
            bootstrap.summaries.into_iter().map(proto_summary).collect();

        let server_cursor_ts = conversations
            .iter()
            .filter_map(|s| s.updated_at.as_ref())
            .map(|ts| ts.seconds as i64 * 1000 + (ts.nanos as i64) / 1_000_000)
            .max()
            .unwrap_or_else(|| Utc::now().timestamp_millis());

        let server_conversation_cursor = Some(Timestamp {
            seconds: server_cursor_ts / 1000,
            nanos: ((server_cursor_ts % 1000) * 1_000_000) as i32,
        });

        Ok(Response::new(flare_proto::common::ConversationSyncAllResponse {
            conversations,
            server_conversation_cursor,
            server_max_seq: 0,
            metadata: Default::default(),
        }))
    }
}

fn proto_summary(summary: ConversationSummary) -> ProtoConversationSummary {
    let last_message_time = summary.last_message_time.and_then(timestamp_from_datetime);

    ProtoConversationSummary {
        conversation_id: summary.conversation_id,
        conversation_type: summary.conversation_type.unwrap_or_default(),
        business_type: summary.business_type.unwrap_or_default(),
        display_name: summary.display_name.unwrap_or_default(),
        avatar_url: String::new(),
        last_message: Some(flare_proto::common::MessagePreview {
            message_id: summary.last_message_id.unwrap_or_default(),
            sender_id: summary.last_sender_id.unwrap_or_default(),
            r#type: summary.last_message_type.unwrap_or_default(),
            text: String::new(),
            time: last_message_time,
        }),
        unread_count: summary.unread_count as u32,
        max_seq: 0,
        last_read_seq: 0,
        is_muted: false,
        is_pinned: false,
        updated_at: last_message_time,
        metadata: summary.metadata,
        labels: Vec::new(),
        is_muted_detail: false,
        mute_until: None,
        created_at: None,
    }
}

fn proto_device(device: DevicePresence) -> ProtoDevicePresence {
    let last_seen_at = device.last_seen_at.and_then(timestamp_from_datetime);

    ProtoDevicePresence {
        device_id: device.device_id,
        device_platform: device.device_platform.unwrap_or_default(),
        state: device.state.as_proto(),
        last_seen_at,
    }
}

fn proto_policy(policy: ConversationPolicy) -> ProtoConversationPolicy {
    ProtoConversationPolicy {
        conflict_resolution: policy.conflict_resolution.as_proto(),
        max_devices: policy.max_devices,
        allow_anonymous: policy.allow_anonymous,
        allow_history_sync: policy.allow_history_sync,
        metadata: policy.metadata,
    }
}

fn proto_common_policy(policy: ConversationPolicy) -> flare_proto::common::ConversationPolicy {
    flare_proto::common::ConversationPolicy {
        conflict_resolution: policy.conflict_resolution.as_proto(),
        max_devices: policy.max_devices,
        allow_anonymous: policy.allow_anonymous,
        allow_history_sync: policy.allow_history_sync,
        metadata: policy.metadata,
        allow_message_search: false,
        allow_file_transfer: true,
    }
}

fn timestamp_from_datetime(dt: DateTime<Utc>) -> Option<Timestamp> {
    Some(Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    })
}

fn internal_error(err: anyhow::Error) -> Status {
    Status::internal(err.to_string())
}

fn failed_precondition(err: anyhow::Error) -> Status {
    Status::failed_precondition(err.to_string())
}

fn thread_to_proto(thread: Thread) -> flare_proto::conversation::Thread {
    flare_proto::conversation::Thread {
        id: thread.id,
        conversation_id: thread.conversation_id,
        root_message_id: thread.root_message_id,
        title: thread.title.unwrap_or_default(),
        creator_id: thread.creator_id,
        reply_count: thread.reply_count,
        last_reply_at: thread.last_reply_at.and_then(timestamp_from_datetime),
        last_reply_id: thread.last_reply_id.unwrap_or_default(),
        last_reply_user_id: thread.last_reply_user_id.unwrap_or_default(),
        participant_count: thread.participant_count,
        is_pinned: thread.is_pinned,
        is_locked: thread.is_locked,
        is_archived: thread.is_archived,
        created_at: timestamp_from_datetime(thread.created_at),
        updated_at: timestamp_from_datetime(thread.updated_at),
        extra: thread.extra,
    }
}

fn domain_to_proto_conversation(conversation: Conversation) -> flare_proto::conversation::Conversation {
    flare_proto::conversation::Conversation {
        conversation_id: conversation.conversation_id,
        conversation_type: conversation.conversation_type,
        business_type: conversation.business_type,
        attributes: conversation.attributes,
        participants: conversation
            .participants
            .into_iter()
            .map(|p| flare_proto::common::ConversationParticipant {
                user_id: p.user_id,
                roles: p.roles,
                muted: p.muted,
                pinned: p.pinned,
                attributes: p.attributes,
                joined_at: None,
                nickname: String::new(),
            })
            .collect(),
        visibility: conversation.visibility.as_proto(),
        lifecycle_state: conversation.lifecycle_state.as_proto(),
        created_at: timestamp_from_datetime(conversation.created_at),
        updated_at: timestamp_from_datetime(conversation.updated_at),
        policy: conversation.policy.map(proto_common_policy),
    }
}
