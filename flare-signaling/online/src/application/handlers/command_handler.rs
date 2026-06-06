//! 命令处理器（编排层）- 轻量级，只负责编排领域服务

use std::sync::Arc;

use flare_grpc_proto::signaling::online::{
    HeartbeatResponse, LoginRequest, LoginResponse, LogoutResponse, OnlineStatus, PresenceEvent,
    UserPresenceEvent, WatchPresenceRequest,
};
use flare_server_core::context::Context;
use flare_server_core::error::{ErrorCode, Result, map_infra_error};
use prost_types::Timestamp;
use tracing::{instrument, warn};

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
        let request = command.request;
        let response = self
            .online_domain_service
            .login(ctx, request.clone())
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::InternalError, "Failed to login"))?;

        self.publish_login_presence(request, &response).await;
        Ok(response)
    }

    /// 处理登出命令
    #[instrument(skip(self, ctx), fields(user_id = %command.request.user_id, conversation_id = %command.request.conversation_id))]
    pub async fn handle_logout(
        &self,
        ctx: &Context,
        command: LogoutCommand,
    ) -> Result<LogoutResponse> {
        let request = command.request;
        let response = self
            .online_domain_service
            .logout(ctx, request.clone())
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::InternalError, "Failed to logout"))?;

        self.publish_presence_snapshot(ctx, &request.user_id, "logout")
            .await;
        Ok(response)
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

    async fn publish_login_presence(&self, request: LoginRequest, response: &LoginResponse) {
        if !response.success || request.user_id.trim().is_empty() {
            return;
        }
        let now = chrono::Utc::now();
        let ts = Timestamp {
            seconds: now.timestamp(),
            nanos: now.timestamp_subsec_nanos() as i32,
        };
        let status = OnlineStatus {
            online: true,
            server_id: request.server_id.clone(),
            cluster_id: String::new(),
            last_seen: Some(ts),
            device_id: request.device_id.clone(),
            device_platform: request.device_platform.clone(),
            gateway_id: request
                .metadata
                .get("gateway_id")
                .cloned()
                .unwrap_or_default(),
        };
        let presence = PresenceEvent {
            user_id: request.user_id.clone(),
            status: Some(status),
            occurred_at: Some(ts),
            conflict_action: 0,
            reason: "login".to_string(),
        };
        if let Err(err) = self
            .subscription_domain_service
            .publish_presence_event(presence)
            .await
        {
            warn!(%err, user_id = %request.user_id, "publish login presence event failed");
        }
        let user_presence = UserPresenceEvent {
            user_id: request.user_id.clone(),
            is_online: true,
            device_id: request.device_id,
            timestamp: Some(ts),
        };
        if let Err(err) = self
            .subscription_domain_service
            .publish_user_presence_event(user_presence)
            .await
        {
            warn!(%err, user_id = %request.user_id, "publish login user presence event failed");
        }
    }

    async fn publish_presence_snapshot(&self, ctx: &Context, user_id: &str, reason: &str) {
        let user_id = user_id.trim();
        if user_id.is_empty() {
            return;
        }
        let now = chrono::Utc::now();
        let ts = Timestamp {
            seconds: now.timestamp(),
            nanos: now.timestamp_subsec_nanos() as i32,
        };
        let status = match self
            .online_domain_service
            .get_online_status(ctx, &[user_id.to_string()])
            .await
        {
            Ok(response) => response.statuses.get(user_id).cloned(),
            Err(err) => {
                warn!(%err, user_id = %user_id, "query presence snapshot after logout failed");
                None
            }
        }
        .unwrap_or_else(|| OnlineStatus {
            online: false,
            server_id: String::new(),
            cluster_id: String::new(),
            last_seen: Some(ts),
            device_id: String::new(),
            device_platform: String::new(),
            gateway_id: String::new(),
        });
        let presence = PresenceEvent {
            user_id: user_id.to_string(),
            status: Some(status.clone()),
            occurred_at: Some(ts),
            conflict_action: 0,
            reason: reason.to_string(),
        };
        if let Err(err) = self
            .subscription_domain_service
            .publish_presence_event(presence)
            .await
        {
            warn!(%err, user_id = %user_id, "publish presence snapshot event failed");
        }
        let user_presence = UserPresenceEvent {
            user_id: user_id.to_string(),
            is_online: status.online,
            device_id: status.device_id,
            timestamp: Some(ts),
        };
        if let Err(err) = self
            .subscription_domain_service
            .publish_user_presence_event(user_presence)
            .await
        {
            warn!(%err, user_id = %user_id, "publish user presence snapshot event failed");
        }
    }
}
