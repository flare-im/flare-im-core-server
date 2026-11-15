//! `UserNotificationSettingsService`：调用者本人的用户级通知设置（读与部分更新）。
//!
//! 客户端经 IM 链路发 `Sync.user_notification_settings` / `Sync.get_user_notification_settings`，
//! 网关代签用户上下文，同步编排原样转发到这里。边界
//! （[`super::signed_notification_settings_service`]）只放行用户身份，处理器再核对一次：上下文里
//! 带了身份就必须是用户；读写的永远是上下文里的用户本人，请求不带 user_id 与 tenant_id。

use flare_grpc_proto::conversation::user_notification_settings_service_server::UserNotificationSettingsService;
use flare_grpc_proto::conversation::{
    GetUserNotificationSettingsRequest, GetUserNotificationSettingsResponse,
    UpdateUserNotificationSettingsRequest, UpdateUserNotificationSettingsResponse,
};
use flare_proto::common::{
    QuietHours as ProtoQuietHours, UserNotificationSettings as ProtoUserNotificationSettings,
};
use flare_server_core::context::{ActorType, Context};
use flare_server_core::error::grpc::IntoGrpc;
use flare_server_core::utils::require_ctx_from_request;
use tonic::{Request, Response, Status};

use super::ConversationGrpcHandler;
use crate::application::commands::UpdateUserNotificationSettingsCommand;
use crate::domain::model::{QuietHours, UserNotificationSettings};

/// 只有用户本人能读写自己的设置：服务、管理员、系统身份都没有「本人」。未签名的旧调用不带身份
/// 类型时按上下文里的用户处理（边界跟随 `FLARE_REQUIRE_SIGNED_READ_CTX`）。
fn require_user_caller(ctx: &Context) -> Result<(), Status> {
    let user = ctx
        .actor()
        .is_none_or(|actor| actor.actor_type == ActorType::User);
    if user {
        Ok(())
    } else {
        Err(Status::permission_denied(
            "only the user may read or change their own notification settings",
        ))
    }
}

/// 领域值 → 线上的 `UserNotificationSettings`（内部读服务也用它）。
pub(super) fn settings_to_proto(
    settings: &UserNotificationSettings,
) -> ProtoUserNotificationSettings {
    ProtoUserNotificationSettings {
        quiet_hours: settings.quiet_hours.map(|quiet| ProtoQuietHours {
            start_minute: quiet.start_minute,
            end_minute: quiet.end_minute,
            weekdays: quiet.weekdays,
            allow_mentions: quiet.allow_mentions,
        }),
        preview: settings.preview.wire(),
        multi_device: settings.multi_device.wire(),
        settings_version: settings.settings_version,
    }
}

/// 线上的勿扰时段 → 领域值（未校验，校验在构造补丁时做）。
fn quiet_hours_from_proto(quiet: ProtoQuietHours) -> QuietHours {
    QuietHours {
        start_minute: quiet.start_minute,
        end_minute: quiet.end_minute,
        weekdays: quiet.weekdays,
        allow_mentions: quiet.allow_mentions,
    }
}

#[tonic::async_trait]
impl UserNotificationSettingsService for ConversationGrpcHandler {
    async fn get_user_notification_settings(
        &self,
        request: Request<GetUserNotificationSettingsRequest>,
    ) -> Result<Response<GetUserNotificationSettingsResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        require_user_caller(&ctx)?;
        let settings = self
            .query_handler
            .handle_get_user_notification_settings(&ctx)
            .await
            .into_grpc()?;
        Ok(Response::new(GetUserNotificationSettingsResponse {
            settings: Some(settings_to_proto(&settings)),
        }))
    }

    async fn update_user_notification_settings(
        &self,
        request: Request<UpdateUserNotificationSettingsRequest>,
    ) -> Result<Response<UpdateUserNotificationSettingsResponse>, Status> {
        let ctx = require_ctx_from_request(&request)?;
        require_user_caller(&ctx)?;
        let req = request.into_inner();
        let settings = self
            .command_handler
            .handle_update_user_notification_settings(
                &ctx,
                UpdateUserNotificationSettingsCommand {
                    quiet_hours: req.quiet_hours.map(quiet_hours_from_proto),
                    clear_quiet_hours: req.clear_quiet_hours,
                    preview: req.preview,
                    multi_device: req.multi_device,
                    base_settings_version: req.base_settings_version,
                },
            )
            .await
            .into_grpc()?;
        Ok(Response::new(UpdateUserNotificationSettingsResponse {
            settings: Some(settings_to_proto(&settings)),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::model::{MultiDeviceAlertPolicy, NotificationPreview};
    use flare_server_core::context::ActorContext;

    fn with_actor(actor_type: ActorType) -> Context {
        Context::with_request_id("req-settings")
            .with_tenant_id("tenant-a")
            .with_user_id("alice")
            .with_actor(ActorContext::new("alice").with_type(actor_type))
    }

    /// 用户（含不带身份类型的旧调用）能读写自己的设置；服务、管理员、系统身份一律拒绝。
    #[test]
    fn only_the_user_reads_or_writes_their_own_settings() {
        assert!(require_user_caller(&with_actor(ActorType::User)).is_ok());
        let legacy = Context::with_request_id("req-settings")
            .with_tenant_id("tenant-a")
            .with_user_id("alice");
        assert!(require_user_caller(&legacy).is_ok());
        for other in [
            ActorType::Service,
            ActorType::TenantAdmin,
            ActorType::System,
        ] {
            assert_eq!(
                require_user_caller(&with_actor(other)).unwrap_err().code(),
                tonic::Code::PermissionDenied
            );
        }
    }

    #[test]
    fn settings_map_to_the_wire_shape() {
        let wire = settings_to_proto(&UserNotificationSettings {
            quiet_hours: Some(QuietHours {
                start_minute: 22 * 60,
                end_minute: 7 * 60,
                weekdays: 0b001_1111,
                allow_mentions: true,
            }),
            preview: NotificationPreview::SenderOnly,
            multi_device: MultiDeviceAlertPolicy::BadgeOnly,
            settings_version: 9,
        });
        let quiet = wire.quiet_hours.expect("quiet hours");
        assert_eq!(quiet.start_minute, 1320);
        assert_eq!(quiet.end_minute, 420);
        assert_eq!(quiet.weekdays, 0b001_1111);
        assert!(quiet.allow_mentions);
        assert_eq!(
            wire.preview,
            flare_proto::common::NotificationPreview::SenderOnly as i32
        );
        assert_eq!(
            wire.multi_device,
            flare_proto::common::MultiDeviceAlertPolicy::BadgeOnly as i32
        );
        assert_eq!(wire.settings_version, 9);

        let defaults = settings_to_proto(&UserNotificationSettings::default());
        assert!(defaults.quiet_hours.is_none());
        assert_eq!(defaults.preview, 0);
        assert_eq!(defaults.multi_device, 0);
        assert_eq!(defaults.settings_version, 0);
    }
}
