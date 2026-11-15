//! gRPC：`ConversationReadService`（只读原子）+ `ConversationManageService`（写与侧效应）。
//! 不提供聚合型「Sync*」RPC；跨域同步编排由 `flare.sync.v1.SyncService` 完成。

mod admin_service;
mod internal_read_service;
mod manage_service;
mod notification_settings_service;
mod read_receipt_service;
mod read_service;
mod roster_policy;
mod shared;

pub use admin_service::ConversationAdminGrpcHandler;

use std::sync::Arc;

use flare_grpc_proto::conversation::conversation_internal_read_service_server::ConversationInternalReadServiceServer;
use flare_grpc_proto::conversation::conversation_read_service_server::ConversationReadServiceServer;
use flare_grpc_proto::conversation::message_read_receipt_service_server::MessageReadReceiptServiceServer;
use flare_grpc_proto::conversation::user_notification_settings_service_server::UserNotificationSettingsServiceServer;
use flare_server_core::auth::SignedContextVerifier;
use flare_server_core::{SignedActorType, SignedFirstContextLayer, SignedFirstContextService};

use crate::application::handlers::{ConversationCommandHandler, ConversationQueryHandler};

#[derive(Clone)]
pub struct ConversationGrpcHandler {
    pub(crate) command_handler: Arc<ConversationCommandHandler>,
    pub(crate) query_handler: Arc<ConversationQueryHandler>,
    /// `PurgeConversationHistory` 的签名验证器（`FLARE_INTERNAL_CTX_PUBLIC_KEY`）。没有时该 RPC
    /// fail closed，见 [`manage_service::authorize_purge_history`]。
    pub(crate) purge_verifier: Option<SignedContextVerifier>,
    pub(crate) roster_policy_endpoint: Option<String>,
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
            roster_policy_endpoint: None,
        }
    }

    /// 设置 `PurgeConversationHistory` 的签名验证器。
    pub fn with_purge_verifier(mut self, verifier: Option<SignedContextVerifier>) -> Self {
        self.purge_verifier = verifier;
        self
    }

    pub fn with_roster_policy_endpoint(mut self, endpoint: Option<String>) -> Self {
        self.roster_policy_endpoint = endpoint;
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

/// 套上身份边界的 `ConversationInternalReadService`：与 [`signed_read_service`] 共用验签器，但
/// **一律要求签名**，并且只放行 Cell 内服务。这个服务按请求体里的 `user_id` 读别人的数据，
/// 不能跟随 `FLARE_REQUIRE_SIGNED_READ_CTX`：开关关着时，网内进程带一个裸 `x-actor-type`
/// 就能读任何人有哪些未读会话。没有配置公钥时所有调用都被拒。
pub fn signed_internal_read_service(
    handler: ConversationGrpcHandler,
    layer: &SignedFirstContextLayer,
) -> SignedFirstContextService<ConversationInternalReadServiceServer<ConversationGrpcHandler>> {
    layer
        .clone()
        .require_signed(true)
        .allow_actor_types([SignedActorType::Service])
        .layer(ConversationInternalReadServiceServer::new(handler))
}

/// 套上身份边界的 `UserNotificationSettingsService`：与 [`signed_read_service`] 共用验签器，跟随
/// `FLARE_REQUIRE_SIGNED_READ_CTX`（同步编排转发网关代签的用户上下文，与会话设置同一条路），但
/// 只放行用户身份：读写的是调用者本人的设置，服务与管理员都没有「本人」。
pub fn signed_notification_settings_service(
    handler: ConversationGrpcHandler,
    layer: &SignedFirstContextLayer,
) -> SignedFirstContextService<UserNotificationSettingsServiceServer<ConversationGrpcHandler>> {
    layer
        .clone()
        .allow_actor_types([SignedActorType::User])
        .layer(UserNotificationSettingsServiceServer::new(handler))
}
