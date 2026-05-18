//! 同步基础设施组合：IM 核心 gRPC 端口 + 可插拔群目录端口。

use std::sync::Arc;

use async_trait::async_trait;
use flare_grpc_proto::conversation::{
    ConversationBootstrapRequest, ConversationBootstrapResponse, GetConversationDetailRequest,
    GetConversationDetailResponse, ListConversationParticipantsRequest,
    ListConversationParticipantsResponse, UpdateCursorRequest,
};
use flare_im_core::Ctx;
use flare_proto::Message;
use flare_server_core::error::FlareError;
use prost_types::Timestamp;

use crate::application::ports::{
    ConversationEventReadPort, ConversationSyncPort, GroupDirectoryPage, GroupDirectorySyncPort,
    QueryEventsPage, StorageConversationMessageHead, StorageReadPort,
};
use crate::infrastructure::rpc::GrpcSyncAdapters;

/// 组合 [`GrpcSyncAdapters`] 与外部 [`GroupDirectorySyncPort`]。
pub struct SyncInfra {
    core: GrpcSyncAdapters,
    group_directory: Arc<dyn GroupDirectorySyncPort>,
}

impl SyncInfra {
    pub fn new(group_directory: Arc<dyn GroupDirectorySyncPort>) -> Self {
        Self {
            core: GrpcSyncAdapters,
            group_directory,
        }
    }
}

impl ConversationSyncPort for SyncInfra {
    async fn conversation_bootstrap(
        &self,
        ctx: &Ctx,
        req: ConversationBootstrapRequest,
    ) -> Result<ConversationBootstrapResponse, FlareError> {
        self.core.conversation_bootstrap(ctx, req).await
    }

    async fn update_read_cursor(
        &self,
        ctx: &Ctx,
        req: UpdateCursorRequest,
    ) -> Result<(), FlareError> {
        self.core.update_read_cursor(ctx, req).await
    }

    async fn conversation_detail(
        &self,
        ctx: &Ctx,
        req: GetConversationDetailRequest,
    ) -> Result<GetConversationDetailResponse, FlareError> {
        self.core.conversation_detail(ctx, req).await
    }

    async fn list_conversation_participants(
        &self,
        ctx: &Ctx,
        req: ListConversationParticipantsRequest,
    ) -> Result<ListConversationParticipantsResponse, FlareError> {
        self.core.list_conversation_participants(ctx, req).await
    }
}

impl StorageReadPort for SyncInfra {
    async fn query_messages_by_seq(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        after_seq: i64,
        before_seq: i64,
        limit: i32,
        user_id: &str,
    ) -> Result<(Vec<Message>, i64), FlareError> {
        self.core
            .query_messages_by_seq(
                ctx,
                conversation_id,
                after_seq,
                before_seq,
                limit,
                user_id,
            )
            .await
    }

    async fn get_conversation_message_head(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
    ) -> Result<StorageConversationMessageHead, FlareError> {
        self.core
            .get_conversation_message_head(ctx, conversation_id)
            .await
    }
}

impl ConversationEventReadPort for SyncInfra {
    async fn query_events_page(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        after_seq: i64,
        before_seq: i64,
        limit: i32,
        event_types: &[i32],
        include_deleted: bool,
    ) -> Result<QueryEventsPage, FlareError> {
        self.core
            .query_events_page(
                ctx,
                conversation_id,
                after_seq,
                before_seq,
                limit,
                event_types,
                include_deleted,
            )
            .await
    }
}

#[async_trait]
impl GroupDirectorySyncPort for SyncInfra {
    async fn sync_group_directory(
        &self,
        ctx: &Ctx,
        since_version: u64,
        since_updated_at: Option<Timestamp>,
        limit: i32,
    ) -> Result<GroupDirectoryPage, FlareError> {
        self.group_directory
            .sync_group_directory(ctx, since_version, since_updated_at, limit)
            .await
    }
}
