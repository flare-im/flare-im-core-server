//! gRPC：`ConversationReadService`（只读原子）+ `ConversationManageService`（写与侧效应）。
//! 不提供聚合型「Sync*」RPC；跨域同步编排由 `flare.sync.v1.SyncService` 完成。

mod admin_service;
mod manage_service;
mod read_service;
mod shared;

pub use admin_service::ConversationAdminGrpcHandler;

use std::sync::Arc;

use flare_server_core::auth::SignedContextVerifier;

use crate::application::handlers::{ConversationCommandHandler, ConversationQueryHandler};

#[derive(Clone)]
pub struct ConversationGrpcHandler {
    pub(crate) command_handler: Arc<ConversationCommandHandler>,
    pub(crate) query_handler: Arc<ConversationQueryHandler>,
    /// `PurgeConversationHistory` 的签名验证器（`FLARE_INTERNAL_CTX_PUBLIC_KEY`）。没有时该 RPC
    /// fail closed，见 [`manage_service::authorize_purge_history`]。
    pub(crate) purge_verifier: Option<SignedContextVerifier>,
}

impl ConversationGrpcHandler {
    pub fn new(
        command_handler: Arc<ConversationCommandHandler>,
        query_handler: Arc<ConversationQueryHandler>,
    ) -> Self {
        Self {
            command_handler,
            query_handler,
            purge_verifier: None,
        }
    }

    /// 设置 `PurgeConversationHistory` 的签名验证器。
    pub fn with_purge_verifier(mut self, verifier: Option<SignedContextVerifier>) -> Self {
        self.purge_verifier = verifier;
        self
    }
}
