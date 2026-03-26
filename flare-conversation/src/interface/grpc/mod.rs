//! gRPC：`ConversationReadService`（只读原子）+ `ConversationManageService`（写与侧效应）。
//! 不提供聚合型「Sync*」RPC；跨域同步编排由 `flare.sync.v1.SyncService` 完成。

mod manage_service;
mod read_service;
mod shared;

use std::sync::Arc;

use crate::application::handlers::{ConversationCommandHandler, ConversationQueryHandler};

#[derive(Clone)]
pub struct ConversationGrpcHandler {
    pub(crate) command_handler: Arc<ConversationCommandHandler>,
    pub(crate) query_handler: Arc<ConversationQueryHandler>,
}

impl ConversationGrpcHandler {
    pub fn new(
        command_handler: Arc<ConversationCommandHandler>,
        query_handler: Arc<ConversationQueryHandler>,
    ) -> Self {
        Self {
            command_handler,
            query_handler,
        }
    }
}
