//! 命令处理器（编排层）- 轻量级，只负责编排领域服务

use std::sync::Arc;

use flare_grpc_proto::signaling::online::{
    HeartbeatResponse, LoginResponse, LogoutResponse, WatchPresenceRequest,
};
use flare_server_core::context::Context;
use flare_im_core::error::{ErrorCode, Result, map_infra_error};
use tracing::instrument;

use crate::application::commands::{HeartbeatCommand, LoginCommand, LogoutCommand};
use crate::domain::repository::{PresencePublisher, SubscriptionRepository};
use crate::domain::service::{OnlineStatusService, SubscriptionService};

/// 在线状态命令处理器（编排层）
pub struct OnlineCommandHandler<CR, SR, PP>
where
    CR: crate::domain::repository::ConversationRepository + Send + Sync,
    SR: SubscriptionRepository + Send + Sync,
    PP: PresencePublisher + Send + Sync,
{
    online_domain_service: Arc<OnlineStatusService<CR>>,
    subscription_domain_service: Arc<SubscriptionService<SR, PP>>,
}

impl<CR, SR, PP> OnlineCommandHandler<CR, SR, PP>
where
    CR: crate::domain::repository::ConversationRepository + Send + Sync,
    SR: SubscriptionRepository + Send + Sync,
    PP: PresencePublisher + Send + Sync,
{
    pub fn new(
        online_domain_service: Arc<OnlineStatusService<CR>>,
        subscription_domain_service: Arc<SubscriptionService<SR, PP>>,
    ) -> Self {
        Self {
            online_domain_service,
            subscription_domain_service,
        }
    }

    /// 处理登录命令
    #[instrument(skip(self, ctx), fields(user_id = %command.request.user_id, device_id = %command.request.device_id))]
    pub async fn handle_login(
        &self,
        ctx: &Context,
        command: LoginCommand,
    ) -> Result<LoginResponse> {
        self.online_domain_service
            .login(ctx, command.request)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::InternalError, "Failed to login"))
    }

    /// 处理登出命令
    #[instrument(skip(self, ctx), fields(user_id = %command.request.user_id, conversation_id = %command.request.conversation_id))]
    pub async fn handle_logout(
        &self,
        ctx: &Context,
        command: LogoutCommand,
    ) -> Result<LogoutResponse> {
        self.online_domain_service
            .logout(ctx, command.request)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::InternalError, "Failed to logout"))
    }

    /// 处理心跳命令
    #[instrument(skip(self, ctx), fields(conversation_id = %command.request.conversation_id, user_id = %command.request.user_id))]
    pub async fn handle_heartbeat(
        &self,
        ctx: &Context,
        command: HeartbeatCommand,
    ) -> Result<HeartbeatResponse> {
        self.online_domain_service
            .heartbeat(
                ctx,
                &command.request.conversation_id,
                &command.request.user_id,
                command.request.current_quality.as_ref(),
            )
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::InternalError, "Failed to heartbeat"))
    }

    /// 处理订阅用户状态命令
    #[instrument(skip(self), fields(user_id = %user_id))]
    pub async fn handle_subscribe_user_presence(
        &self,
        user_id: String,
    ) -> Result<Vec<flare_grpc_proto::signaling::online::UserPresenceEvent>> {
        self.subscription_domain_service
            .subscribe_user_presence(user_id)
            .await
    }

    /// 处理订阅在线状态命令
    #[instrument(skip(self))]
    pub async fn handle_watch_presence(
        &self,
        request: WatchPresenceRequest,
    ) -> Result<Vec<flare_grpc_proto::signaling::online::PresenceEvent>> {
        self.subscription_domain_service
            .watch_presence(request)
            .await
    }
}
