//! 在线推送：查 Online 设备 → 按 `gateway_id` 分组 → [`GatewayRouter`] 直推 Access Gateway。

use std::sync::Arc;

use flare_grpc_proto::access_gateway::{
    PushAckRequest, PushCustomRequest, PushEventRequest, PushMessageRequest,
    PushNotificationRequest,
};
use flare_grpc_proto::signaling::router::PushStrategy;
use flare_im_contracts::Ctx;
use flare_proto::common::EventEnvelopeDeliveryMode;
use flare_server_core::error::{ErrorCode, FlareError, Result, map_infra_error};

use flare_im_service_kit::gateway::{GatewayRouter, GatewayRouterTrait};

use super::{ConversationPingDebouncer, PingDebounceDecision, PingDebounceKey};
use crate::domain::push_routing::{
    merge_push_ack_for_gateway, merge_push_custom_for_gateway, merge_push_event_for_gateway,
    merge_push_message_for_gateway, merge_push_notification_for_gateway,
    partition_targets_by_gateway, select_push_targets,
};
use crate::infrastructure::rpc::OnlineServiceClient;

pub struct GatewayPushExecutor {
    online: Arc<OnlineServiceClient>,
    gateway_router: Arc<GatewayRouter>,
    ping_debouncer: Option<Arc<ConversationPingDebouncer>>,
}

impl GatewayPushExecutor {
    pub fn new(online: Arc<OnlineServiceClient>, gateway_router: Arc<GatewayRouter>) -> Self {
        Self {
            online,
            gateway_router,
            ping_debouncer: None,
        }
    }

    pub fn with_ping_debouncer(mut self, debouncer: Arc<ConversationPingDebouncer>) -> Self {
        self.ping_debouncer = Some(debouncer);
        self
    }

    fn route_failure_code(error: &FlareError) -> ErrorCode {
        if matches!(error.code(), Some(ErrorCode::UserOffline)) {
            ErrorCode::UserOffline
        } else {
            ErrorCode::ServiceUnavailable
        }
    }

    fn map_route_failure(
        error: impl std::fmt::Display,
        code: ErrorCode,
        operation: &'static str,
    ) -> FlareError {
        map_infra_error(error, code, operation)
    }

    pub async fn push_message(
        &self,
        ctx: &Ctx,
        target_user_id: &str,
        strategy: PushStrategy,
        push: PushMessageRequest,
    ) -> Result<()> {
        let core = ctx.as_ref();
        let devices_resp = self
            .online
            .list_user_devices(core, target_user_id)
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::ServiceUnavailable,
                    "Failed to list user devices",
                )
            })?;
        let targets = select_push_targets(&devices_resp.devices, target_user_id, strategy)?;
        let by_gw = partition_targets_by_gateway(&targets);
        let mut success_count = 0usize;
        let mut failure_count = 0usize;
        let mut first_error = None::<(String, ErrorCode)>;
        for (gid, ts) in by_gw {
            let push_g = merge_push_message_for_gateway(push.clone(), &ts);
            match self.gateway_router.route_push_message(&gid, push_g).await {
                Ok(_) => success_count += 1,
                Err(error) => {
                    failure_count += 1;
                    let code = Self::route_failure_code(&error);
                    first_error.get_or_insert_with(|| (error.to_string(), code));
                    tracing::warn!(
                        target_user_id,
                        gateway_id = %gid,
                        error = %error,
                        "Skipping failed gateway route for push message"
                    );
                }
            }
        }
        if success_count == 0
            && let Some((error, code)) = first_error
        {
            return Err(Self::map_route_failure(
                flare_server_core::error::FlareError::system((error).to_string()),
                code,
                "Failed to route push message",
            ));
        }
        if failure_count > 0 {
            tracing::warn!(
                target_user_id,
                success_gateway_count = success_count,
                failed_gateway_count = failure_count,
                "Push message completed with stale or failed gateway routes"
            );
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
        if let Some(debouncer) = &self.ping_debouncer
            && let Some((conversation_id, max_conversation_seq)) = event_ping_watermark(&push)
        {
            let tenant_id = ctx.tenant_id().unwrap_or("0").to_string();
            let key = PingDebounceKey::new(&tenant_id, target_user_id, conversation_id.clone());
            let pending = pending_ping_request(&push, &conversation_id, max_conversation_seq);
            match debouncer.observe(key.clone(), pending).await {
                PingDebounceDecision::SendNow => {
                    return self
                        .push_event_now(ctx, target_user_id, strategy, push)
                        .await;
                }
                PingDebounceDecision::ScheduleAfter(delay) => {
                    self.spawn_debounced_push_event(
                        ctx.clone(),
                        target_user_id,
                        strategy,
                        key,
                        delay,
                    );
                    return Ok(());
                }
                PingDebounceDecision::Suppressed => return Ok(()),
            }
        }

        self.push_event_now(ctx, target_user_id, strategy, push)
            .await
    }

    fn spawn_debounced_push_event(
        &self,
        ctx: Ctx,
        target_user_id: &str,
        strategy: PushStrategy,
        key: PingDebounceKey,
        delay: std::time::Duration,
    ) {
        let Some(debouncer) = self.ping_debouncer.clone() else {
            return;
        };
        let online = self.online.clone();
        let gateway_router = self.gateway_router.clone();
        let target_user_id = target_user_id.to_string();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let Some(push) = debouncer.take_pending(&key).await else {
                return;
            };
            let executor = GatewayPushExecutor {
                online,
                gateway_router,
                ping_debouncer: None,
            };
            if let Err(error) = executor
                .push_event_now(&ctx, &target_user_id, strategy, push)
                .await
            {
                tracing::warn!(
                    target_user_id = %target_user_id,
                    error = %error,
                    "Debounced push event trailing ping failed"
                );
            }
        });
    }

    async fn push_event_now(
        &self,
        ctx: &Ctx,
        target_user_id: &str,
        strategy: PushStrategy,
        push: PushEventRequest,
    ) -> Result<()> {
        let core = ctx.as_ref();
        let devices_resp = self
            .online
            .list_user_devices(core, target_user_id)
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::ServiceUnavailable,
                    "Failed to list user devices",
                )
            })?;
        let targets = select_push_targets(&devices_resp.devices, target_user_id, strategy)?;
        let by_gw = partition_targets_by_gateway(&targets);
        let mut success_count = 0usize;
        let mut failure_count = 0usize;
        let mut first_error = None::<(String, ErrorCode)>;
        for (gid, ts) in by_gw {
            let push_g = merge_push_event_for_gateway(push.clone(), &ts);
            match self.gateway_router.route_push_event(&gid, push_g).await {
                Ok(_) => success_count += 1,
                Err(error) => {
                    failure_count += 1;
                    let code = Self::route_failure_code(&error);
                    first_error.get_or_insert_with(|| (error.to_string(), code));
                    tracing::warn!(
                        target_user_id,
                        gateway_id = %gid,
                        error = %error,
                        "Skipping failed gateway route for push event"
                    );
                }
            }
        }
        if success_count == 0
            && let Some((error, code)) = first_error
        {
            return Err(Self::map_route_failure(
                flare_server_core::error::FlareError::system((error).to_string()),
                code,
                "Failed to route push event",
            ));
        }
        if failure_count > 0 {
            tracing::warn!(
                target_user_id,
                success_gateway_count = success_count,
                failed_gateway_count = failure_count,
                "Push event completed with stale or failed gateway routes"
            );
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
        let devices_resp = self
            .online
            .list_user_devices(core, target_user_id)
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::ServiceUnavailable,
                    "Failed to list user devices",
                )
            })?;
        let targets = select_push_targets(&devices_resp.devices, target_user_id, strategy)?;
        let by_gw = partition_targets_by_gateway(&targets);
        let mut success_count = 0usize;
        let mut failure_count = 0usize;
        let mut first_error = None::<(String, ErrorCode)>;
        for (gid, ts) in by_gw {
            let push_g = merge_push_notification_for_gateway(push.clone(), &ts);
            match self
                .gateway_router
                .route_push_notification(&gid, push_g)
                .await
            {
                Ok(_) => success_count += 1,
                Err(error) => {
                    failure_count += 1;
                    let code = Self::route_failure_code(&error);
                    first_error.get_or_insert_with(|| (error.to_string(), code));
                    tracing::warn!(
                        target_user_id,
                        gateway_id = %gid,
                        error = %error,
                        "Skipping failed gateway route for push notification"
                    );
                }
            }
        }
        if success_count == 0
            && let Some((error, code)) = first_error
        {
            return Err(Self::map_route_failure(
                flare_server_core::error::FlareError::system((error).to_string()),
                code,
                "Failed to route push notification",
            ));
        }
        if failure_count > 0 {
            tracing::warn!(
                target_user_id,
                success_gateway_count = success_count,
                failed_gateway_count = failure_count,
                "Push notification completed with stale or failed gateway routes"
            );
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
        let devices_resp = self
            .online
            .list_user_devices(core, target_user_id)
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::ServiceUnavailable,
                    "Failed to list user devices",
                )
            })?;
        let targets = select_push_targets(&devices_resp.devices, target_user_id, strategy)?;
        let by_gw = partition_targets_by_gateway(&targets);
        let mut success_count = 0usize;
        let mut failure_count = 0usize;
        let mut first_error = None::<(String, ErrorCode)>;
        for (gid, ts) in by_gw {
            let push_g = merge_push_ack_for_gateway(push.clone(), &ts);
            match self.gateway_router.route_push_ack(&gid, push_g).await {
                Ok(_) => success_count += 1,
                Err(error) => {
                    failure_count += 1;
                    let code = Self::route_failure_code(&error);
                    first_error.get_or_insert_with(|| (error.to_string(), code));
                    tracing::warn!(
                        target_user_id,
                        gateway_id = %gid,
                        error = %error,
                        "Skipping failed gateway route for push ack"
                    );
                }
            }
        }
        if success_count == 0
            && let Some((error, code)) = first_error
        {
            return Err(Self::map_route_failure(
                flare_server_core::error::FlareError::system((error).to_string()),
                code,
                "Failed to route push ack",
            ));
        }
        if failure_count > 0 {
            tracing::warn!(
                target_user_id,
                success_gateway_count = success_count,
                failed_gateway_count = failure_count,
                "Push ack completed with stale or failed gateway routes"
            );
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
        let devices_resp = self
            .online
            .list_user_devices(core, target_user_id)
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::ServiceUnavailable,
                    "Failed to list user devices",
                )
            })?;
        let targets = select_push_targets(&devices_resp.devices, target_user_id, strategy)?;
        let by_gw = partition_targets_by_gateway(&targets);
        let mut success_count = 0usize;
        let mut failure_count = 0usize;
        let mut first_error = None::<(String, ErrorCode)>;
        for (gid, ts) in by_gw {
            let push_g = merge_push_custom_for_gateway(push.clone(), &ts);
            match self.gateway_router.route_push_custom(&gid, push_g).await {
                Ok(_) => success_count += 1,
                Err(error) => {
                    failure_count += 1;
                    let code = Self::route_failure_code(&error);
                    first_error.get_or_insert_with(|| (error.to_string(), code));
                    tracing::warn!(
                        target_user_id,
                        gateway_id = %gid,
                        error = %error,
                        "Skipping failed gateway route for push custom"
                    );
                }
            }
        }
        if success_count == 0
            && let Some((error, code)) = first_error
        {
            return Err(Self::map_route_failure(
                flare_server_core::error::FlareError::system((error).to_string()),
                code,
                "Failed to route push custom",
            ));
        }
        if failure_count > 0 {
            tracing::warn!(
                target_user_id,
                success_gateway_count = success_count,
                failed_gateway_count = failure_count,
                "Push custom completed with stale or failed gateway routes"
            );
        }
        Ok(())
    }
}

fn event_ping_watermark(push: &PushEventRequest) -> Option<(String, u64)> {
    let conversation_id = if push.conversation_id.trim().is_empty() {
        push.events.first()?.conversation_id.trim().to_string()
    } else {
        push.conversation_id.trim().to_string()
    };
    if conversation_id.is_empty() {
        return None;
    }

    let max_seq = push
        .events
        .iter()
        .map(|event| event.conversation_seq)
        .max()
        .unwrap_or(0)
        .max(push.max_conversation_seq);
    (max_seq > 0).then_some((conversation_id, max_seq))
}

fn pending_ping_request(
    push: &PushEventRequest,
    conversation_id: &str,
    max_conversation_seq: u64,
) -> PushEventRequest {
    PushEventRequest {
        user_ids: push.user_ids.clone(),
        events: vec![],
        options: push.options.clone(),
        conversation_id: conversation_id.to_string(),
        max_conversation_seq,
        delivery_mode: EventEnvelopeDeliveryMode::Ping as i32,
        inline_events_truncated: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gateway_unavailable_route_errors_are_retryable() {
        let error = flare_server_core::error::FlareError::system(
            "Gateway instance not found: gateway_id=gateway-a".to_string(),
        );
        let code = GatewayPushExecutor::route_failure_code(&error);

        assert_eq!(code, ErrorCode::ServiceUnavailable);
        assert!(code.is_retryable());
    }

    #[test]
    fn users_offline_route_errors_are_not_infra_retries() {
        let error = FlareError::user_offline("user-1");
        let code = GatewayPushExecutor::route_failure_code(&error);

        assert_eq!(code, ErrorCode::UserOffline);
        assert!(!code.is_retryable());
    }

    #[test]
    fn event_ping_watermark_uses_explicit_ping_fields() {
        let push = PushEventRequest {
            user_ids: vec!["u1".to_string()],
            events: vec![],
            options: None,
            conversation_id: "c1".to_string(),
            max_conversation_seq: 42,
            delivery_mode: EventEnvelopeDeliveryMode::Ping as i32,
            inline_events_truncated: true,
        };

        assert_eq!(event_ping_watermark(&push), Some(("c1".to_string(), 42)));
    }
}
