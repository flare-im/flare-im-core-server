//! `ConversationReadService`：Bootstrap / 列表 / 详情（只读原子能力）

use flare_grpc_proto::conversation::conversation_read_service_server::ConversationReadService;
use flare_grpc_proto::conversation::{
    ConversationBootstrapRequest, ConversationBootstrapResponse, GetConversationDetailRequest,
    GetConversationDetailResponse, GetConversationMembersRequest, GetConversationMembersResponse,
    ListConversationsRequest, ListConversationsResponse,
};
use flare_server_core::error::grpc::IntoGrpc;
use flare_server_core::utils::require_ctx_from_request;
use tonic::{Request, Response, Status};

use crate::application::queries::{
    ConversationBootstrapQuery, GetConversationDetailQuery, ListConversationsQuery,
};

use super::ConversationGrpcHandler;
use super::shared::{domain_to_conversation_detail, proto_device, proto_policy, proto_summary};

#[tonic::async_trait]
impl ConversationReadService for ConversationGrpcHandler {
    async fn conversation_bootstrap(
        &self,
        request: Request<ConversationBootstrapRequest>,
    ) -> Result<Response<ConversationBootstrapResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
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
            .into_grpc()?;

        let response = ConversationBootstrapResponse {
            conversations: bootstrap.summaries.into_iter().map(proto_summary).collect(),
            recent_messages: bootstrap.recent_messages,
            devices: bootstrap.devices.into_iter().map(proto_device).collect(),
            server_cursor_map: bootstrap.cursor_map,
            policy: Some(proto_policy(bootstrap.policy)),
        };

        Ok(Response::new(response))
    }

    async fn list_conversations(
        &self,
        request: Request<ListConversationsRequest>,
    ) -> Result<Response<ListConversationsResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
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
            .into_grpc()?;

        let response = ListConversationsResponse {
            conversations: summaries.into_iter().map(proto_summary).collect(),
            next_cursor: next_cursor.unwrap_or_default(),
            has_more,
        };

        Ok(Response::new(response))
    }

    async fn get_conversation_detail(
        &self,
        request: Request<GetConversationDetailRequest>,
    ) -> Result<Response<GetConversationDetailResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let conv = self
            .query_handler
            .handle_get_conversation_detail(
                &ctx,
                GetConversationDetailQuery {
                    conversation_id: req.conversation_id,
                },
            )
            .await
            .into_grpc()?;

        let detail = domain_to_conversation_detail(conv);

        Ok(Response::new(GetConversationDetailResponse {
            detail: Some(detail),
            ..Default::default()
        }))
    }

    async fn get_conversation_members(
        &self,
        request: Request<GetConversationMembersRequest>,
    ) -> Result<Response<GetConversationMembersResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();

        let conv = self
            .query_handler
            .handle_get_conversation_detail(
                &ctx,
                GetConversationDetailQuery {
                    conversation_id: req.conversation_id,
                },
            )
            .await
            .into_grpc()?;

        let member_ids = conv.participants.into_iter().map(|p| p.user_id).collect();

        Ok(Response::new(GetConversationMembersResponse { member_ids }))
    }
}
