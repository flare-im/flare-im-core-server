//! 在线推送：查 Online 设备 → 按 `gateway_id` 分组 → [`GatewayRouter`] 直推 Access Gateway。

use std::sync::Arc;

use flare_im_core::error::{ErrorCode, Result, map_infra_error};
use flare_grpc_proto::access_gateway::{
    PushAckRequest, PushCustomRequest, PushEventRequest, PushMessageRequest,
    PushNotificationRequest,
};
use flare_grpc_proto::signaling::router::PushStrategy;
use flare_im_core::Ctx;

use flare_im_core::gateway::{GatewayRouter, GatewayRouterTrait};

use crate::domain::push_routing::{
    merge_push_ack_for_gateway, merge_push_custom_for_gateway, merge_push_event_for_gateway,
    merge_push_message_for_gateway, merge_push_notification_for_gateway,
    partition_targets_by_gateway, select_push_targets,
};
use crate::infrastructure::rpc::OnlineServiceClient;

pub struct GatewayPushExecutor {
    online: Arc<OnlineServiceClient>,
    gateway_router: Arc<GatewayRouter>,
}

impl GatewayPushExecutor {
    pub fn new(online: Arc<OnlineServiceClient>, gateway_router: Arc<GatewayRouter>) -> Self {
        Self {
            online,
            gateway_router,
        }
    }

    pub async fn push_message(
        &self,
        ctx: &Ctx,
        target_user_id: &str,
        strategy: PushStrategy,
        push: PushMessageRequest,
    ) -> Result<()> {
        let core = ctx.as_ref();
        let devices_resp = self.online.list_user_devices(core, target_user_id).await
            .map_err(|e| map_infra_error(e, ErrorCode::ServiceUnavailable, "Failed to list user devices"))?;
        let targets = select_push_targets(&devices_resp.devices, target_user_id, strategy)?;
        let by_gw = partition_targets_by_gateway(&targets);
        for (gid, ts) in by_gw {
            let push_g = merge_push_message_for_gateway(push.clone(), &ts);
            self.gateway_router.route_push_message(&gid, push_g).await
                .map_err(|e| map_infra_error(e, ErrorCode::MessageSendFailed, "Failed to route push message"))?;
        }
        Ok(())
    }

    pub async fn push_event(
        &self,
        ctx: &Ctx,
        target_user_id: &str,
        strategy: PushStrategy,
        push: PushEventRequest,
    ) -> Result<()> {
        let core = ctx.as_ref();
        let devices_resp = self.online.list_user_devices(core, target_user_id).await.map_err(|e| map_infra_error(e, ErrorCode::ServiceUnavailable, "Failed to list user devices"))?;
        let targets = select_push_targets(&devices_resp.devices, target_user_id, strategy)?;
        let by_gw = partition_targets_by_gateway(&targets);
        for (gid, ts) in by_gw {
            let push_g = merge_push_event_for_gateway(push.clone(), &ts);
            self.gateway_router.route_push_event(&gid, push_g).await.map_err(|e| map_infra_error(e, ErrorCode::MessageSendFailed, "Failed to route push event"))?;
        }
        Ok(())
    }

    pub async fn push_notification(
        &self,
        ctx: &Ctx,
        target_user_id: &str,
        strategy: PushStrategy,
        push: PushNotificationRequest,
    ) -> Result<()> {
        let core = ctx.as_ref();
        let devices_resp = self.online.list_user_devices(core, target_user_id).await.map_err(|e| map_infra_error(e, ErrorCode::ServiceUnavailable, "Failed to list user devices"))?;
        let targets = select_push_targets(&devices_resp.devices, target_user_id, strategy)?;
        let by_gw = partition_targets_by_gateway(&targets);
        for (gid, ts) in by_gw {
            let push_g = merge_push_notification_for_gateway(push.clone(), &ts);
            self.gateway_router
                .route_push_notification(&gid, push_g)
                .await?;
        }
        Ok(())
    }

    pub async fn push_ack(
        &self,
        ctx: &Ctx,
        target_user_id: &str,
        strategy: PushStrategy,
        push: PushAckRequest,
    ) -> Result<()> {
        let core = ctx.as_ref();
        let devices_resp = self.online.list_user_devices(core, target_user_id).await.map_err(|e| map_infra_error(e, ErrorCode::ServiceUnavailable, "Failed to list user devices"))?;
        let targets = select_push_targets(&devices_resp.devices, target_user_id, strategy)?;
        let by_gw = partition_targets_by_gateway(&targets);
        for (gid, ts) in by_gw {
            let push_g = merge_push_ack_for_gateway(push.clone(), &ts);
            self.gateway_router.route_push_ack(&gid, push_g).await.map_err(|e| map_infra_error(e, ErrorCode::MessageSendFailed, "Failed to route push ack"))?;
        }
        Ok(())
    }

    pub async fn push_custom(
        &self,
        ctx: &Ctx,
        target_user_id: &str,
        strategy: PushStrategy,
        push: PushCustomRequest,
    ) -> Result<()> {
        let core = ctx.as_ref();
        let devices_resp = self.online.list_user_devices(core, target_user_id).await.map_err(|e| map_infra_error(e, ErrorCode::ServiceUnavailable, "Failed to list user devices"))?;
        let targets = select_push_targets(&devices_resp.devices, target_user_id, strategy)?;
        let by_gw = partition_targets_by_gateway(&targets);
        for (gid, ts) in by_gw {
            let push_g = merge_push_custom_for_gateway(push.clone(), &ts);
            self.gateway_router.route_push_custom(&gid, push_g).await.map_err(|e| map_infra_error(e, ErrorCode::MessageSendFailed, "Failed to route push custom"))?;
        }
        Ok(())
    }
}
