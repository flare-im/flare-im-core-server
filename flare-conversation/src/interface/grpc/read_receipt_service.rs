//! `MessageReadReceiptService`：群聊消息的已读摘要与已读名单（只读）。身份边界与
//! `ConversationReadService` 相同（见 [`super::signed_read_receipt_service`]）。

use flare_grpc_proto::conversation::message_read_receipt_service_server::MessageReadReceiptService;
use flare_grpc_proto::conversation::{
    GetMessageReadSummariesRequest, GetMessageReadSummariesResponse, ListMessageReadersRequest,
    ListMessageReadersResponse, MessageReadSummary as ProtoMessageReadSummary, MessageReaderState,
};
use flare_server_core::error::grpc::IntoGrpc;
use flare_server_core::utils::require_ctx_from_request;
use tonic::{Request, Response, Status};

use super::ConversationGrpcHandler;
use crate::application::queries::{GetMessageReadSummariesQuery, ListMessageReadersQuery};
use crate::domain::model::{ReadCounts, ReaderState};

fn summary_to_proto(server_msg_id: String, counts: ReadCounts) -> ProtoMessageReadSummary {
    ProtoMessageReadSummary {
        server_msg_id,
        read_count: counts.read_count,
        unread_count: counts.unread_count,
        total: counts.total,
    }
}

#[tonic::async_trait]
impl MessageReadReceiptService for ConversationGrpcHandler {
    async fn get_message_read_summaries(
        &self,
        request: Request<GetMessageReadSummariesRequest>,
    ) -> Result<Response<GetMessageReadSummariesResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        let conversation_id = req.conversation_id.trim().to_string();
        let answer = self
            .query_handler
            .handle_get_message_read_summaries(
                &ctx,
                GetMessageReadSummariesQuery {
                    conversation_id: conversation_id.clone(),
                    server_msg_ids: req.server_msg_ids,
                },
            )
            .await
            .into_grpc()?;
        Ok(Response::new(GetMessageReadSummariesResponse {
            conversation_id,
            available: answer.available,
            summaries: answer
                .summaries
                .into_iter()
                .map(|summary| summary_to_proto(summary.server_msg_id, summary.counts))
                .collect(),
        }))
    }

    async fn list_message_readers(
        &self,
        request: Request<ListMessageReadersRequest>,
    ) -> Result<Response<ListMessageReadersResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        let req = request.into_inner();
        let state = match MessageReaderState::try_from(req.state) {
            Ok(MessageReaderState::Read) => ReaderState::Read,
            Ok(MessageReaderState::Unread) => ReaderState::Unread,
            _ => {
                return Err(Status::invalid_argument(
                    "state must be MESSAGE_READER_STATE_READ or MESSAGE_READER_STATE_UNREAD",
                ));
            }
        };
        let conversation_id = req.conversation_id.trim().to_string();
        let server_msg_id = req.server_msg_id.trim().to_string();
        let page = self
            .query_handler
            .handle_list_message_readers(
                &ctx,
                ListMessageReadersQuery {
                    conversation_id: conversation_id.clone(),
                    server_msg_id: server_msg_id.clone(),
                    state,
                    cursor: req.cursor,
                    limit: req.limit,
                },
            )
            .await
            .into_grpc()?;
        Ok(Response::new(ListMessageReadersResponse {
            summary: page
                .counts
                .map(|counts| summary_to_proto(server_msg_id.clone(), counts)),
            conversation_id,
            server_msg_id,
            available: page.available,
            user_ids: page.user_ids,
            next_cursor: page.next_cursor,
            has_more: page.has_more,
        }))
    }
}
