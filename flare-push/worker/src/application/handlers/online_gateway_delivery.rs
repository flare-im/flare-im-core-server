//! 在线推送：查 Online 设备 → 按 `gateway_id` 分组 → [`GatewayRouter`] 直推 Access Gateway。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use flare_grpc_proto::access_gateway::{
    ConversationEventsDelivery, ConversationMessagesDelivery, ConversationWatermarkPing,
    DeliverToConversationRequest, PushAckRequest, PushCustomRequest, PushEventRequest,
    PushMessageRequest, PushNotificationRequest,
    deliver_to_conversation_request::Payload as DeliverPayload,
};
use flare_grpc_proto::signaling::online::DeviceInfo;
use flare_grpc_proto::signaling::router::PushStrategy;
use flare_im_contracts::Ctx;
use flare_proto::common::{Event, Message};
use flare_server_core::error::{ErrorCode, FlareError, Result, map_infra_error};

use flare_im_service_kit::gateway::{GatewayRouter, GatewayRouterTrait};

use crate::domain::push_routing::{
    merge_push_ack_for_gateway, merge_push_custom_for_gateway, merge_push_notification_for_gateway,
    partition_targets_by_gateway, select_push_targets,
};
use crate::infrastructure::rpc::OnlineServiceClient;

const DEFAULT_DEVICE_ROUTE_CACHE_TTL: Duration = Duration::from_secs(5);
const DEVICE_ROUTE_CACHE_MAX_USERS: usize = 4096;
const DEVICE_ROUTE_CACHE_TTL_ENV: &str = "FLARE_PUSH_DEVICE_ROUTE_CACHE_TTL_MS";

pub struct GatewayPushExecutor {
    online: Arc<OnlineServiceClient>,
    gateway_router: Arc<GatewayRouter>,
    device_cache: tokio::sync::Mutex<HashMap<String, CachedDevices>>,
}

#[derive(Clone)]
struct CachedDevices {
    devices: Vec<DeviceInfo>,
    expires_at: Instant,
}

impl GatewayPushExecutor {
    pub fn new(online: Arc<OnlineServiceClient>, gateway_router: Arc<GatewayRouter>) -> Self {
        Self {
            online,
            gateway_router,
            device_cache: tokio::sync::Mutex::new(HashMap::new()),
        }
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

    async fn list_user_devices_cached(
        &self,
        core: &flare_server_core::context::Context,
        target_user_id: &str,
    ) -> Result<Vec<DeviceInfo>> {
        let now = Instant::now();
        let cache_ttl = device_route_cache_ttl();
        if cache_ttl.is_zero() {
            return self.list_user_devices(core, target_user_id).await;
        }
        if let Some(cached) = self.device_cache.lock().await.get(target_user_id).cloned()
            && cached.expires_at > now
        {
            return Ok(cached.devices);
        }

        let devices = self.list_user_devices(core, target_user_id).await?;

        let mut cache = self.device_cache.lock().await;
        if cache.len() >= DEVICE_ROUTE_CACHE_MAX_USERS {
            cache.retain(|_, cached| cached.expires_at > now);
            if cache.len() >= DEVICE_ROUTE_CACHE_MAX_USERS
                && let Some(key) = cache.keys().next().cloned()
            {
                cache.remove(&key);
            }
        }
        cache.insert(
            target_user_id.to_string(),
            CachedDevices {
                devices: devices.clone(),
                expires_at: now + cache_ttl,
            },
        );

        Ok(devices)
    }

    async fn list_user_devices(
        &self,
        core: &flare_server_core::context::Context,
        target_user_id: &str,
    ) -> Result<Vec<DeviceInfo>> {
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
        Ok(devices_resp.devices)
    }

    /// 统一读扩散：**不再解析收件人/按用户分网关**。按 conversation_id 分组（同批可能含多会话），
    /// 每会话经 [`GatewayRouter::broadcast_deliver_to_conversation`] 广播到所有网关节点，各节点用
    /// 本地会话订阅表过滤投递（O(在线/节点)，与群人数无关）。`target_user_id`/`strategy` 在读扩散下不再需要。
    pub async fn push_message(
        &self,
        _ctx: &Ctx,
        _target_user_id: &str,
        _strategy: PushStrategy,
        push: PushMessageRequest,
    ) -> Result<()> {
        if push.messages.is_empty() {
            return Err(flare_server_core::error::ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "PushMessage: messages is empty",
            )
            .build_error());
        }
        // 按会话分组（每条 NATS entry 仅在一个组、仅调一次 push_message → 不会重复投递）。
        let mut by_conversation: HashMap<String, Vec<Message>> = HashMap::new();
        for message in push.messages {
            by_conversation
                .entry(message.conversation_id.clone())
                .or_default()
                .push(message);
        }
        for (conversation_id, messages) in by_conversation {
            if conversation_id.trim().is_empty() {
                tracing::warn!("PushMessage: skip message batch with empty conversation_id");
                continue;
            }
            let request = DeliverToConversationRequest {
                conversation_id,
                options: push.options.clone(),
                payload: Some(DeliverPayload::Messages(ConversationMessagesDelivery {
                    messages,
                })),
            };
            self.gateway_router
                .broadcast_deliver_to_conversation(request)
                .await?;
        }
        Ok(())
    }

    /// 统一读扩散：**不再解析收件人/按用户分网关**。按 conversation_id 分组事件，
    /// 每会话经 [`GatewayRouter::broadcast_deliver_to_conversation`] 广播 `EventEnvelope` 到所有网关节点，
    /// 各节点用本地会话订阅表过滤投递（O(在线/节点)，与群人数无关）。群消息主路径（消息经 EventEnvelope 下行）。
    /// `target_user_id`/`strategy` 在读扩散下不再需要。
    pub async fn push_event(
        &self,
        _ctx: &Ctx,
        _target_user_id: &str,
        _strategy: PushStrategy,
        push: PushEventRequest,
    ) -> Result<()> {
        // 纯 ping（events 空）：按 request.conversation_id 广播一次水位 ping。
        if push.events.is_empty() {
            let conversation_id = push.conversation_id.trim().to_string();
            if conversation_id.is_empty() || push.max_conversation_seq == 0 {
                return Err(flare_server_core::error::ErrorBuilder::new(
                    ErrorCode::InvalidParameter,
                    "PushEvent: events is empty and ping fields are incomplete",
                )
                .build_error());
            }
            let request = DeliverToConversationRequest {
                conversation_id,
                options: push.options.clone(),
                payload: Some(DeliverPayload::Ping(ConversationWatermarkPing {
                    max_conversation_seq: push.max_conversation_seq,
                    delivery_mode: push.delivery_mode,
                    inline_events_truncated: push.inline_events_truncated,
                })),
            };
            self.gateway_router
                .broadcast_deliver_to_conversation(request)
                .await?;
            return Ok(());
        }

        // 有 events：按 conversation_id 分组（同批可能含多会话），每会话广播一次。
        // max_conversation_seq 留 0 由网关从 events 的 conversation_seq 推断（避免跨会话水位串扰）。
        let mut by_conversation: HashMap<String, Vec<Event>> = HashMap::new();
        for event in push.events {
            by_conversation
                .entry(event.conversation_id.clone())
                .or_default()
                .push(event);
        }
        for (conversation_id, events) in by_conversation {
            if conversation_id.trim().is_empty() {
                tracing::warn!("PushEvent: skip event batch with empty conversation_id");
                continue;
            }
            let request = DeliverToConversationRequest {
                conversation_id,
                options: push.options.clone(),
                payload: Some(DeliverPayload::Events(ConversationEventsDelivery {
                    events,
                    delivery_mode: push.delivery_mode,
                    inline_events_truncated: push.inline_events_truncated,
                })),
            };
            self.gateway_router
                .broadcast_deliver_to_conversation(request)
                .await?;
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
        let devices = self.list_user_devices_cached(core, target_user_id).await?;
        let targets = select_push_targets(&devices, target_user_id, strategy)?;
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
        let devices = self.list_user_devices_cached(core, target_user_id).await?;
        let targets = select_push_targets(&devices, target_user_id, strategy)?;
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
        let devices = self.list_user_devices_cached(core, target_user_id).await?;
        let targets = select_push_targets(&devices, target_user_id, strategy)?;
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

fn device_route_cache_ttl() -> Duration {
    std::env::var(DEVICE_ROUTE_CACHE_TTL_ENV)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_DEVICE_ROUTE_CACHE_TTL)
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
}
