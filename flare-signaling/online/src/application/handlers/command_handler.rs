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
use crate::domain::repository::{PresencePublisher, SubscriptionRepository, kick_reason};
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
    // LoginRequest carries the access token and LoginCommand repeats the Context:
    // record only identifying fields, never the request/command as a whole.
    #[instrument(
        skip_all,
        fields(user_id = %command.request.user_id, device_id = %command.request.device_id)
    )]
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
            .map_err(login_error)?;

        self.publish_login_presence(ctx, request, &response).await;
        Ok(response)
    }

    /// 处理登出命令
    #[instrument(
        skip_all,
        fields(user_id = %command.request.user_id, conversation_id = %command.request.conversation_id)
    )]
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
    #[instrument(
        skip_all,
        fields(conversation_id = %command.request.conversation_id, user_id = %command.request.user_id)
    )]
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
        tenant_id: &str,
        user_id: String,
    ) -> Result<Vec<flare_grpc_proto::signaling::online::UserPresenceEvent>> {
        self.subscription_domain_service
            .subscribe_user_presence(tenant_id, user_id)
            .await
    }

    /// 处理订阅在线状态命令
    #[instrument(skip(self))]
    pub async fn handle_watch_presence(
        &self,
        tenant_id: &str,
        request: WatchPresenceRequest,
    ) -> Result<Vec<flare_grpc_proto::signaling::online::PresenceEvent>> {
        self.subscription_domain_service
            .watch_presence(tenant_id, request)
            .await
    }

    async fn publish_login_presence(
        &self,
        ctx: &Context,
        request: LoginRequest,
        response: &LoginResponse,
    ) {
        if !response.success || request.user_id.trim().is_empty() {
            return;
        }
        let Some(tenant_id) = ctx.tenant_id() else {
            return;
        };
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
            .publish_presence_event(tenant_id, presence)
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
            .publish_user_presence_event(tenant_id, user_presence)
            .await
        {
            warn!(%err, user_id = %request.user_id, "publish login user presence event failed");
        }
    }

    async fn publish_presence_snapshot(&self, ctx: &Context, user_id: &str, reason: &str) {
        let Some(tenant_id) = ctx.tenant_id() else {
            return;
        };
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
            .publish_presence_event(tenant_id, presence)
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
            .publish_user_presence_event(tenant_id, user_presence)
            .await
        {
            warn!(%err, user_id = %user_id, "publish user presence snapshot event failed");
        }
    }
}

/// 登录失败的对外错误。租户闸门的拒绝（`PermissionDenied` + `TENANT_UNAVAILABLE`）是确定的业务
/// 结论，原样交给网关，网关据此告诉客户端租户不可用；其余错误仍按内部错误上报，明细放进 details。
fn login_error(
    error: flare_server_core::error::FlareError,
) -> flare_server_core::error::FlareError {
    if error.code() == Some(ErrorCode::PermissionDenied)
        && error.reason() == kick_reason::TENANT_UNAVAILABLE
    {
        return error;
    }
    map_infra_error(error, ErrorCode::InternalError, "Failed to login")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use flare_grpc_proto::signaling::online::{DeviceConflictStrategy, LogoutRequest};
    use tracing_subscriber::fmt::format::FmtSpan;

    use super::*;
    use crate::domain::service::online_status_service::NoopConversationRepository;

    struct NoopSubscriptions;

    impl SubscriptionRepository for NoopSubscriptions {
        async fn add_subscription(&self, _: &str, _: String, _: String) -> Result<()> {
            Ok(())
        }
        async fn remove_subscription(&self, _: &Context, _: &[String]) -> Result<()> {
            Ok(())
        }
        async fn get_user_subscriptions(
            &self,
            _: &Context,
        ) -> Result<Vec<(String, HashMap<String, String>)>> {
            Ok(Vec::new())
        }
        async fn get_topic_subscribers(&self, _: &str, _: &str) -> Result<Vec<String>> {
            Ok(Vec::new())
        }
    }

    struct NoopPresence;

    impl PresencePublisher for NoopPresence {
        async fn publish_presence_event(&self, _: &str, _: PresenceEvent) -> Result<()> {
            Ok(())
        }
        async fn publish_user_presence_event(&self, _: &str, _: UserPresenceEvent) -> Result<()> {
            Ok(())
        }
    }

    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for CapturedLogs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn login_and_logout_spans_never_log_the_token_or_context() {
        const SECRET_PART: &str = "SIGNATURE-THAT-MUST-NOT-BE-LOGGED";
        let logs = CapturedLogs::default();
        let writer = logs.clone();
        // FmtSpan::NEW renders every span's recorded fields even without events.
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_span_events(FmtSpan::NEW)
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let handler = OnlineCommandHandler::new(
            Arc::new(OnlineStatusService::<_>::new(
                Arc::new(NoopConversationRepository),
                "gateway-test".to_string(),
            )),
            Arc::new(SubscriptionService::new(
                Arc::new(NoopSubscriptions),
                Arc::new(NoopPresence),
            )),
        );
        let ctx = Context::root()
            .with_tenant_id("tenant-a")
            .with_user_id("alice");
        let request = LoginRequest {
            user_id: "alice".to_string(),
            token: format!("eyJhbGciOi.payload.{SECRET_PART}"),
            device_id: "device-a".to_string(),
            device_platform: "web".to_string(),
            desired_conflict_strategy: DeviceConflictStrategy::Coexist as i32,
            metadata: HashMap::from([("connection_id".to_string(), "conn-a".to_string())]),
            ..Default::default()
        };
        let login = handler
            .handle_login(
                &ctx,
                LoginCommand {
                    request,
                    ctx: ctx.clone(),
                },
            )
            .await
            .expect("login");
        handler
            .handle_logout(
                &ctx,
                LogoutCommand {
                    request: LogoutRequest {
                        user_id: "alice".to_string(),
                        conversation_id: login.conversation_id.clone(),
                    },
                    ctx: ctx.clone(),
                },
            )
            .await
            .expect("logout");

        let output = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
        assert!(output.contains("handle_login{"), "{output}");
        assert!(output.contains("device_id=device-a"), "{output}");
        assert!(output.contains("handle_logout{"), "{output}");
        assert!(
            output.contains(&format!("conversation_id={}", login.conversation_id)),
            "{output}"
        );
        assert!(!output.contains(SECRET_PART), "{output}");
        assert!(!output.contains("LoginRequest"), "{output}");
        assert!(!output.contains("ContextInner"), "{output}");
    }

    /// 租户闸门的拒绝原样交给网关（gRPC PERMISSION_DENIED，reason `TENANT_UNAVAILABLE`）；
    /// 其余登录失败仍折叠成内部错误，不把领域错误码带出去。
    #[test]
    fn only_the_tenant_rejection_keeps_its_code_and_reason() {
        let tenant = login_error(flare_server_core::flare_err!(
            ErrorCode::PermissionDenied,
            kick_reason::TENANT_UNAVAILABLE
        ));
        assert_eq!(tenant.code(), Some(ErrorCode::PermissionDenied));
        assert_eq!(tenant.reason(), kick_reason::TENANT_UNAVAILABLE);
        let status = tonic::Status::from(tenant);
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert_eq!(status.message(), kick_reason::TENANT_UNAVAILABLE);

        for other in [
            flare_server_core::flare_err!(
                ErrorCode::PermissionDenied,
                "verified tenant is required"
            ),
            flare_server_core::flare_err!(ErrorCode::InvalidParameter, "device_id is required"),
        ] {
            let mapped = login_error(other);
            assert_eq!(mapped.code(), Some(ErrorCode::InternalError));
            assert_eq!(mapped.reason(), "Failed to login");
        }
    }

    /// 投影库读不到（如 PG 抖动）时登记失败，但交给网关的不是租户拒绝：gRPC 状态既不是
    /// PERMISSION_DENIED 也不带 `TENANT_UNAVAILABLE`，网关按「Online 不可用」处理，客户端照常重连，
    /// 而不是收到终止会话的 KICKED `TENANT_UNAVAILABLE`。
    #[tokio::test]
    async fn a_projection_outage_reaches_the_gateway_as_a_transient_failure() {
        use flare_im_contracts::domain::tenant::TenantRuntimeSnapshot;
        use flare_im_service_kit::tenant_runtime::{
            TenantAccessPolicy, TenantRuntimeCache, TenantRuntimeCacheOptions, TenantRuntimeSource,
        };

        struct UnreachableProjection;

        #[async_trait::async_trait]
        impl TenantRuntimeSource for UnreachableProjection {
            async fn load(
                &self,
                _tenant_id: &str,
            ) -> std::result::Result<Option<TenantRuntimeSnapshot>, String> {
                Err("connection refused".to_string())
            }
        }

        let handler = OnlineCommandHandler::new(
            Arc::new(
                OnlineStatusService::<_>::new(
                    Arc::new(NoopConversationRepository),
                    "gateway-test".to_string(),
                )
                .with_tenant_runtime(
                    TenantRuntimeCache::with_source(
                        Arc::new(UnreachableProjection),
                        TenantRuntimeCacheOptions::default(),
                    ),
                    TenantAccessPolicy::Lenient,
                    0,
                ),
            ),
            Arc::new(SubscriptionService::new(
                Arc::new(NoopSubscriptions),
                Arc::new(NoopPresence),
            )),
        );
        let ctx = Context::root()
            .with_tenant_id("tenant-a")
            .with_user_id("alice");
        let request = LoginRequest {
            user_id: "alice".to_string(),
            device_id: "device-a".to_string(),
            device_platform: "web".to_string(),
            desired_conflict_strategy: DeviceConflictStrategy::Coexist as i32,
            metadata: HashMap::from([("connection_id".to_string(), "conn-a".to_string())]),
            ..Default::default()
        };

        let error = handler
            .handle_login(
                &ctx,
                LoginCommand {
                    request,
                    ctx: ctx.clone(),
                },
            )
            .await
            .expect_err("login must fail closed while the projection is unreachable");

        let status = tonic::Status::from(error);
        assert_ne!(status.code(), tonic::Code::PermissionDenied, "{status:?}");
        assert_ne!(
            status.message(),
            kick_reason::TENANT_UNAVAILABLE,
            "{status:?}"
        );
    }
}
