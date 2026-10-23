//! gRPC：`ConversationReadService`（只读原子）+ `ConversationManageService`（写与侧效应）。
//! 不提供聚合型「Sync*」RPC；跨域同步编排由 `flare.sync.v1.SyncService` 完成。

mod admin_service;
mod manage_service;
mod read_receipt_service;
mod read_service;
mod shared;

pub use admin_service::ConversationAdminGrpcHandler;

use std::sync::Arc;

use flare_grpc_proto::conversation::conversation_read_service_server::ConversationReadServiceServer;
use flare_grpc_proto::conversation::message_read_receipt_service_server::MessageReadReceiptServiceServer;
use flare_server_core::auth::SignedContextVerifier;
use flare_server_core::{SignedFirstContextLayer, SignedFirstContextService};

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

/// 套上读接口身份边界的 `ConversationReadService`（见 `flare_im_service_kit::signed_read`）：
/// 带签名的 `x-flare-ctx` 决定租户与身份——网关代签的用户、租户管理员，或 Cell 内服务（服务按
/// 租户与会话读成员，不要求是成员）；与签名不一致的裸 `x-tenant-id`、`x-user-id` 一律拒绝。
/// 只有边界不要求签名时，未签名调用才按元数据取身份。
pub fn signed_read_service(
    handler: ConversationGrpcHandler,
    layer: &SignedFirstContextLayer,
) -> SignedFirstContextService<ConversationReadServiceServer<ConversationGrpcHandler>> {
    layer.layer(ConversationReadServiceServer::new(handler))
}

/// 套上读接口身份边界的 `MessageReadReceiptService`：只认网关代签的用户（已读摘要与名单只回答
/// 调用者自己发送的消息）。与 [`signed_read_service`] 共用验签器，但**一律要求签名**，不跟随
/// `FLARE_REQUIRE_SIGNED_READ_CTX`：这个服务没有需要兼容的未签名调用方，开关关着时也不能让
/// 网内进程带裸 `x-tenant-id`/`x-user-id` 读别人消息的已读名单。没有配置公钥时所有调用都被拒。
pub fn signed_read_receipt_service(
    handler: ConversationGrpcHandler,
    layer: &SignedFirstContextLayer,
) -> SignedFirstContextService<MessageReadReceiptServiceServer<ConversationGrpcHandler>> {
    layer
        .clone()
        .require_signed(true)
        .layer(MessageReadReceiptServiceServer::new(handler))
}
