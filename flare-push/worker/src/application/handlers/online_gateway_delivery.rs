//! 在线推送：查 Online 设备 → 按 `gateway_id` 分组 → [`GatewayRouter`] 直推 Access Gateway。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use flare_grpc_proto::access_gateway::{
    ConversationEventsDelivery, ConversationMessagesDelivery, ConversationWatermarkPing,
    DeliverToConversationRequest, PushAckRequest, PushCapabilityRequest, PushCustomRequest,
    PushEventRequest, PushMessageRequest, PushNotificationRequest,
    deliver_to_conversation_request::Payload as DeliverPayload,
};
use flare_grpc_proto::signaling::online::DeviceInfo;
use flare_grpc_proto::signaling::router::PushStrategy;
use flare_im_contracts::Ctx;
use flare_proto::common::{Event, Message};
use flare_server_core::error::{ErrorCode, FlareError, Result, map_infra_error};

use flare_im_service_kit::gateway::{GatewayRouter, GatewayRouterTrait};

use crate::domain::model::GatewayPushTarget;
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
        let tenant_id = core
            .tenant_id()
            .ok_or_else(|| FlareError::system("push device lookup requires tenant"))?;
        if !flare_im_contracts::domain::tenant::is_valid_tenant_id(tenant_id)
            || flare_im_contracts::utils::normalize_tenant_id(tenant_id) != tenant_id
        {
            return Err(FlareError::system("invalid push device lookup tenant"));
        }
        let cache_key = format!("{tenant_id}:{target_user_id}");
        let now = Instant::now();
        let cache_ttl = device_route_cache_ttl();
        if cache_ttl.is_zero() {
            return self.list_user_devices(core, target_user_id).await;
        }
        if let Some(cached) = self.device_cache.lock().await.get(&cache_key).cloned()
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
            cache_key,
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
        ctx: &Ctx,
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
                .broadcast_deliver_to_conversation(ctx, request)
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
        ctx: &Ctx,
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
                .broadcast_deliver_to_conversation(ctx, request)
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
                .broadcast_deliver_to_conversation(ctx, request)
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
        let targets = retain_routable_targets(&self.gateway_router, targets).await;
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
        let targets = retain_routable_targets(&self.gateway_router, targets).await;
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

    /// A plugin realtime packet: broadcast to every gateway node, each of which writes it to the
    /// listed users' own connections. At most once: past its expiry it is dropped, and a failed
    /// node is skipped rather than retried (a redelivered packet would arrive twice).
    pub async fn push_capability(&self, ctx: &Ctx, push: PushCapabilityRequest) -> Result<()> {
        if capability_expired(&push, chrono::Utc::now().timestamp_millis()) {
            tracing::debug!(
                user_count = push.user_ids.len(),
                "Dropping expired capability packet"
            );
            return Ok(());
        }
        if let Err(error) = self
            .gateway_router
            .broadcast_push_capability(ctx, push)
            .await
        {
            tracing::warn!(%error, "Capability packet not delivered (dropped)");
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
        let targets = retain_routable_targets(&self.gateway_router, targets).await;
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

/// 去掉所在网关租约缺失（已宕机）的设备：它们按离线处理，不往死网关投递，也不为一台不存在的
/// 网关 Nack 重投；离线通道由 push-server 依据 Online 在线状态决定（Online 清扫器会在网关宕机后
/// 移除这些会话）。全部设备都在死网关上时与「没有在线设备」同一结果。
async fn retain_routable_targets(
    router: &GatewayRouter,
    targets: Vec<GatewayPushTarget>,
) -> Vec<GatewayPushTarget> {
    let mut verdicts: HashMap<String, bool> = HashMap::new();
    let mut routable = Vec::with_capacity(targets.len());
    for target in targets {
        let alive = match verdicts.get(&target.gateway_id) {
            Some(alive) => *alive,
            None => {
                let alive = router.is_gateway_routable(&target.gateway_id).await;
                verdicts.insert(target.gateway_id.clone(), alive);
                alive
            }
        };
        if alive {
            routable.push(target);
        } else {
            tracing::debug!(
                user_id = %target.user_id,
                device_id = %target.device_id,
                gateway_id = %target.gateway_id,
                "device on a gateway without lease treated as offline"
            );
        }
    }
    routable
}

/// `options.expire_at_ms > 0` and already reached.
fn capability_expired(push: &PushCapabilityRequest, now_ms: i64) -> bool {
    push.options
        .as_ref()
        .is_some_and(|options| options.expire_at_ms > 0 && now_ms >= options.expire_at_ms)
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
    fn capability_packets_expire_at_their_deadline_only() {
        let push = |expire_at_ms| PushCapabilityRequest {
            options: Some(flare_grpc_proto::access_gateway::PushOptions {
                expire_at_ms,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(!capability_expired(&push(0), 10));
        assert!(!capability_expired(&push(11), 10));
        assert!(capability_expired(&push(10), 10));
        assert!(!capability_expired(&PushCapabilityRequest::default(), 10));
    }

    #[test]
    fn gateway_unavailable_route_errors_are_retryable() {
        let error = flare_server_core::error::FlareError::system(
            "Gateway instance not found: gateway_id=gateway-a".to_string(),
        );
        let code = GatewayPushExecutor::route_failure_code(&error);

        assert_eq!(code, ErrorCode::ServiceUnavailable);
        assert!(code.is_retryable());
    }

    struct DeadGateways(&'static [&'static str]);

    #[async_trait::async_trait]
    impl flare_im_service_kit::gateway::lease::GatewayLeaseSource for DeadGateways {
        async fn lookup(&self, gateway_id: &str) -> redis::RedisResult<(bool, bool)> {
            Ok((true, !self.0.contains(&gateway_id)))
        }
    }

    /// 死网关（租约缺失）上的设备不再投递；没挂租约探针时一律照常投递。
    #[tokio::test]
    async fn devices_on_gateways_without_lease_are_dropped_from_online_routing() {
        let target = |device: &str, gateway: &str| GatewayPushTarget {
            user_id: "u1".into(),
            device_id: device.into(),
            gateway_id: gateway.into(),
        };
        let targets = vec![
            target("phone", "gw-dead"),
            target("pc", "gw-live"),
            target("pad", "gw-dead"),
        ];

        let router = GatewayRouter::new(Default::default());
        let kept = retain_routable_targets(&router, targets.clone()).await;
        assert_eq!(kept.len(), 3, "no probe: every device stays routable");

        router.attach_lease_probe(Arc::new(
            flare_im_service_kit::gateway::lease::GatewayLeaseProbe::new(
                Arc::new(DeadGateways(&["gw-dead"])),
                flare_im_service_kit::gateway::lease::LEASE_PROBE_CACHE_TTL,
            ),
        ));
        let kept = retain_routable_targets(&router, targets).await;
        let devices: Vec<&str> = kept.iter().map(|t| t.device_id.as_str()).collect();
        assert_eq!(devices, vec!["pc"]);
    }

    #[test]
    fn users_offline_route_errors_are_not_infra_retries() {
        let error = FlareError::user_offline("user-1");
        let code = GatewayPushExecutor::route_failure_code(&error);

        assert_eq!(code, ErrorCode::UserOffline);
        assert!(!code.is_retryable());
    }
}
