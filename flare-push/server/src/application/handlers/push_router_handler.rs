use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use flare_grpc_proto::access_gateway;
use flare_proto::common::EventEnvelopeDeliveryMode;
use flare_proto::common::{
    Ack, AckPayload, CustomData, CustomPayload, NotificationKind, NotificationMessage,
    NotificationPayload, NotificationPriority, PushAck, PushEnvelope, PushTaskEnvelope,
    PushTaskPayloadKind, SystemPayload, ack, notification_message, push_envelope,
};
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result, map_infra_error};
use prost::Message as _;
use tokio::sync::Mutex as AsyncMutex;

use crate::infrastructure::mq::publisher::PushServerMqPublisher;
use crate::infrastructure::online::online_status_service::OnlineStatusService;

#[async_trait]
pub trait ConversationOnlineIndexReader: Send + Sync {
    async fn online_user_ids(
        &self,
        ctx: &flare_server_core::context::Ctx,
        conversation_id: &str,
    ) -> Result<Vec<String>>;
}

#[async_trait]
pub trait OnlineStatusReader: Send + Sync {
    async fn online_statuses(
        &self,
        ctx: &flare_server_core::context::Ctx,
        user_ids: &[String],
    ) -> Result<HashMap<String, bool>>;

    fn default_tenant_id(&self) -> &str;
}

#[async_trait]
impl OnlineStatusReader for OnlineStatusService {
    async fn online_statuses(
        &self,
        ctx: &flare_server_core::context::Ctx,
        user_ids: &[String],
    ) -> Result<HashMap<String, bool>> {
        OnlineStatusService::online_statuses(self, ctx, user_ids).await
    }

    fn default_tenant_id(&self) -> &str {
        OnlineStatusService::default_tenant_id(self)
    }
}

#[async_trait]
impl ConversationOnlineIndexReader for OnlineStatusService {
    async fn online_user_ids(
        &self,
        ctx: &flare_server_core::context::Ctx,
        conversation_id: &str,
    ) -> Result<Vec<String>> {
        OnlineStatusService::conversation_online_user_ids(self, ctx, conversation_id).await
    }
}

#[async_trait]
pub trait PushTaskPublisher: Send + Sync {
    async fn publish_online_task(
        &self,
        ctx: &flare_server_core::context::Ctx,
        key: Option<&str>,
        payload: Vec<u8>,
    ) -> Result<()>;

    async fn publish_offline_task(
        &self,
        ctx: &flare_server_core::context::Ctx,
        key: Option<&str>,
        payload: Vec<u8>,
    ) -> Result<()>;
}

#[async_trait]
impl PushTaskPublisher for PushServerMqPublisher {
    async fn publish_online_task(
        &self,
        ctx: &flare_server_core::context::Ctx,
        key: Option<&str>,
        payload: Vec<u8>,
    ) -> Result<()> {
        PushServerMqPublisher::publish_online_task(self, ctx, key, payload).await
    }

    async fn publish_offline_task(
        &self,
        ctx: &flare_server_core::context::Ctx,
        key: Option<&str>,
        payload: Vec<u8>,
    ) -> Result<()> {
        PushServerMqPublisher::publish_offline_task(self, ctx, key, payload).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ConversationPingCoalesceKey {
    tenant_id: String,
    conversation_id: String,
}

impl ConversationPingCoalesceKey {
    fn new(tenant_id: impl Into<String>, conversation_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            conversation_id: conversation_id.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConversationPingCoalesceDecision {
    SendNow,
    ScheduleAfter(Duration),
    Suppressed,
}

struct ConversationPingCoalescer {
    window: Duration,
    state: AsyncMutex<HashMap<ConversationPingCoalesceKey, ConversationPingCoalesceEntry>>,
}

struct ConversationPingCoalesceEntry {
    last_sent: Instant,
    scheduled: bool,
    pending: Option<access_gateway::PushEventRequest>,
}

impl ConversationPingCoalescer {
    fn new(window: Duration) -> Self {
        Self {
            window,
            state: AsyncMutex::new(HashMap::new()),
        }
    }

    async fn observe(
        &self,
        key: ConversationPingCoalesceKey,
        pending: access_gateway::PushEventRequest,
    ) -> ConversationPingCoalesceDecision {
        if self.window.is_zero() {
            return ConversationPingCoalesceDecision::SendNow;
        }

        let now = Instant::now();
        let mut state = self.state.lock().await;
        let Some(entry) = state.get_mut(&key) else {
            state.insert(
                key,
                ConversationPingCoalesceEntry {
                    last_sent: now,
                    scheduled: false,
                    pending: None,
                },
            );
            return ConversationPingCoalesceDecision::SendNow;
        };

        let elapsed = now.duration_since(entry.last_sent);
        if elapsed >= self.window {
            entry.last_sent = now;
            entry.scheduled = false;
            entry.pending = None;
            return ConversationPingCoalesceDecision::SendNow;
        }

        entry.pending = Some(match entry.pending.take() {
            Some(existing) => merge_pending_conversation_ping(existing, pending),
            None => pending,
        });
        if entry.scheduled {
            ConversationPingCoalesceDecision::Suppressed
        } else {
            entry.scheduled = true;
            ConversationPingCoalesceDecision::ScheduleAfter(self.window - elapsed)
        }
    }

    async fn take_pending(
        &self,
        key: &ConversationPingCoalesceKey,
    ) -> Option<access_gateway::PushEventRequest> {
        let mut state = self.state.lock().await;
        let entry = state.get_mut(key)?;
        entry.scheduled = false;
        let pending = entry.pending.take();
        if pending.is_some() {
            entry.last_sent = Instant::now();
        }
        pending
    }
}

fn pending_conversation_ping_request(
    req: &access_gateway::PushEventRequest,
    conversation_id: &str,
    max_conversation_seq: u64,
) -> access_gateway::PushEventRequest {
    let mut pending = req.clone();
    pending.user_ids.clear();
    pending.events.clear();
    pending.conversation_id = conversation_id.to_string();
    pending.max_conversation_seq = max_conversation_seq;
    pending.delivery_mode = EventEnvelopeDeliveryMode::Ping as i32;
    pending.inline_events_truncated = true;
    pending
}

fn merge_pending_conversation_ping(
    mut existing: access_gateway::PushEventRequest,
    incoming: access_gateway::PushEventRequest,
) -> access_gateway::PushEventRequest {
    existing.max_conversation_seq = existing
        .max_conversation_seq
        .max(incoming.max_conversation_seq);
    existing.options = incoming.options.or(existing.options);
    existing.user_ids.clear();
    existing.events.clear();
    existing.delivery_mode = EventEnvelopeDeliveryMode::Ping as i32;
    existing.inline_events_truncated = true;
    existing
}

#[derive(Clone)]
pub struct PushRouterHandler {
    online_status: Arc<dyn OnlineStatusReader>,
    conversation_online_index: Arc<dyn ConversationOnlineIndexReader>,
    publisher: Arc<dyn PushTaskPublisher>,
    conversation_ping_coalescer: Option<Arc<ConversationPingCoalescer>>,
}

struct PushTaskTemplate<'a> {
    message_id: &'a str,
    conversation_id: &'a str,
    tenant_id: &'a str,
    priority: i32,
    expire_at: Option<i64>,
    push_payload: &'a [u8],
    headers: &'a HashMap<String, String>,
    payload_kind: i32,
}

impl PushRouterHandler {
    pub fn new(
        online_status: Arc<dyn OnlineStatusReader>,
        conversation_online_index: Arc<dyn ConversationOnlineIndexReader>,
        publisher: Arc<dyn PushTaskPublisher>,
    ) -> Self {
        Self {
            online_status,
            conversation_online_index,
            publisher,
            conversation_ping_coalescer: None,
        }
    }

    pub fn with_conversation_ping_coalesce_window(mut self, window: Duration) -> Self {
        if !window.is_zero() {
            self.conversation_ping_coalescer =
                Some(Arc::new(ConversationPingCoalescer::new(window)));
        }
        self
    }

    fn gateway_options(
        options: &Option<flare_proto::common::PushOptions>,
        device_ids: &[String],
    ) -> Option<access_gateway::PushOptions> {
        if options.is_none() && device_ids.is_empty() {
            return None;
        }

        let common = options.as_ref();
        Some(access_gateway::PushOptions {
            require_ack: common.map(|o| o.require_ack).unwrap_or(false),
            priority: common.map(|o| o.priority).unwrap_or(5),
            expire_at_ms: common.and_then(|o| o.expire_at).unwrap_or_default(),
            offline_push: false,
            device_ids: device_ids.to_vec(),
            platforms: Vec::new(),
            delivery_mode: 0,
            allow_duplicate: false,
            enforce_order: false,
            shard_key: String::new(),
        })
    }

    fn ack_from_payload(envelope_id: &str, payload: AckPayload) -> Ack {
        let mut metadata = HashMap::new();
        metadata.insert("conversation_id".to_string(), payload.conversation_id);
        metadata.insert("ack_type".to_string(), payload.ack_type);

        Ack {
            ack_id: Some(envelope_id.to_string()),
            ack_at: Some(payload.ack_at),
            payload: Some(ack::Payload::Push(PushAck {
                window_id: payload.message_id,
                ack_seq: payload.seq,
                ack_id: Some(envelope_id.to_string()),
                ack_at: Some(payload.ack_at),
                device_id: None,
                attributes: metadata,
            })),
        }
    }

    fn notification_from_payload(payload: NotificationPayload) -> NotificationMessage {
        let mut attributes = payload.attributes;
        if !payload.icon.is_empty() {
            attributes.insert("icon".to_string(), payload.icon);
        }
        if !payload.sound.is_empty() {
            attributes.insert("sound".to_string(), payload.sound);
        }
        if !payload.click_action.is_empty() {
            attributes.insert("click_action".to_string(), payload.click_action);
        }

        NotificationMessage {
            kind: NotificationKind::Custom as i32,
            notification_id: payload.notification_id,
            title: payload.title,
            body: payload.body,
            priority: NotificationPriority::Normal as i32,
            expire_at: None,
            created_at: payload.created_at,
            attributes,
            action: None,
            payload: None,
        }
    }

    fn notification_from_system(payload: SystemPayload) -> NotificationMessage {
        NotificationMessage {
            kind: NotificationKind::System as i32,
            notification_id: format!("system-{}", uuid::Uuid::new_v4()),
            title: payload.title,
            body: payload.content,
            priority: NotificationPriority::Normal as i32,
            expire_at: None,
            created_at: payload.created_at,
            attributes: payload.attributes,
            action: None,
            payload: Some(notification_message::Payload::System(Default::default())),
        }
    }

    fn custom_from_payload(payload: CustomPayload) -> CustomData {
        CustomData {
            r#type: payload.custom_type,
            payload: payload.custom_data,
            attributes: payload.attributes,
        }
    }

    fn push_task_payload(
        envelope: &PushEnvelope,
        target_user_ids: Vec<String>,
    ) -> Result<(PushTaskPayloadKind, Vec<u8>, String)> {
        let options = Self::gateway_options(&envelope.options, &envelope.target_device_ids);
        let payload = envelope.payload.clone().ok_or_else(|| {
            ErrorBuilder::new(ErrorCode::InvalidParameter, "push envelope payload missing")
                .build_error()
        })?;

        match payload {
            push_envelope::Payload::Ack(ack) => {
                let req = access_gateway::PushAckRequest {
                    user_ids: target_user_ids,
                    ack: Some(Self::ack_from_payload(&envelope.envelope_id, ack)),
                    options,
                };
                Ok((
                    PushTaskPayloadKind::Ack,
                    req.encode_to_vec(),
                    envelope.envelope_id.clone(),
                ))
            }
            push_envelope::Payload::Notification(notification) => {
                let message_id = if notification.notification_id.is_empty() {
                    envelope.envelope_id.clone()
                } else {
                    notification.notification_id.clone()
                };
                let req = access_gateway::PushNotificationRequest {
                    user_ids: target_user_ids,
                    notification: Some(Self::notification_from_payload(notification)),
                    options,
                };
                Ok((
                    PushTaskPayloadKind::Notification,
                    req.encode_to_vec(),
                    message_id,
                ))
            }
            push_envelope::Payload::Custom(custom) => {
                let message_id = if custom.custom_type.is_empty() {
                    envelope.envelope_id.clone()
                } else {
                    format!("{}:{}", envelope.envelope_id, custom.custom_type)
                };
                let req = access_gateway::PushCustomRequest {
                    user_ids: target_user_ids,
                    custom_data: Some(Self::custom_from_payload(custom)),
                    options,
                };
                Ok((PushTaskPayloadKind::Custom, req.encode_to_vec(), message_id))
            }
            push_envelope::Payload::System(system) => {
                let req = access_gateway::PushNotificationRequest {
                    user_ids: target_user_ids,
                    notification: Some(Self::notification_from_system(system)),
                    options,
                };
                Ok((
                    PushTaskPayloadKind::Notification,
                    req.encode_to_vec(),
                    envelope.envelope_id.clone(),
                ))
            }
        }
    }

    async fn load_online_statuses(
        &self,
        ctx: &flare_server_core::context::Ctx,
        user_ids: &[String],
    ) -> Result<HashMap<String, bool>> {
        self.online_status
            .online_statuses(ctx, user_ids)
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::ServiceUnavailable,
                    "Failed to query online status",
                )
            })
    }

    async fn publish_targeted_tasks(
        &self,
        ctx: &flare_server_core::context::Ctx,
        user_ids: &[String],
        online_statuses: &HashMap<String, bool>,
        template: PushTaskTemplate<'_>,
        online_only: bool,
    ) -> Result<()> {
        for user_id in user_ids {
            let is_online = online_statuses.get(user_id).copied().unwrap_or(false);
            if online_only && !is_online {
                tracing::trace!(
                    user_id = %user_id,
                    conversation_id = %template.conversation_id,
                    "Skipping offline user for pure conversation ping"
                );
                continue;
            }

            let task = PushTaskEnvelope {
                user_id: user_id.clone(),
                message_id: template.message_id.to_string(),
                conversation_id: template.conversation_id.to_string(),
                tenant_id: template.tenant_id.to_string(),
                priority: template.priority,
                expire_at: template.expire_at,
                push_payload: template.push_payload.to_vec(),
                headers: template.headers.clone(),
                payload_kind: template.payload_kind,
            };

            let payload = task.encode_to_vec();
            if is_online {
                self.publisher
                    .publish_online_task(ctx, Some(user_id.as_str()), payload)
                    .await
                    .map_err(|e| {
                        map_infra_error(
                            e,
                            ErrorCode::ServiceUnavailable,
                            "Failed to publish online push task",
                        )
                    })?;
            } else if !online_only {
                self.publisher
                    .publish_offline_task(ctx, Some(user_id.as_str()), payload)
                    .await
                    .map_err(|e| {
                        map_infra_error(
                            e,
                            ErrorCode::ServiceUnavailable,
                            "Failed to publish offline push task",
                        )
                    })?;
            }
        }

        Ok(())
    }

    fn pure_conversation_ping(req: &access_gateway::PushEventRequest) -> Option<(String, u64)> {
        if !req.events.is_empty() {
            return None;
        }
        if req.conversation_id.trim().is_empty() || req.max_conversation_seq == 0 {
            return None;
        }
        let mode = EventEnvelopeDeliveryMode::try_from(req.delivery_mode).ok()?;
        matches!(mode, EventEnvelopeDeliveryMode::Ping)
            .then(|| (req.conversation_id.clone(), req.max_conversation_seq))
    }

    fn event_conversation_id(req: &access_gateway::PushEventRequest) -> String {
        if !req.conversation_id.trim().is_empty() {
            return req.conversation_id.clone();
        }
        req.events
            .first()
            .map(|event| event.conversation_id.clone())
            .unwrap_or_default()
    }

    fn event_message_id(req: &access_gateway::PushEventRequest) -> String {
        if let Some(event) = req.events.first() {
            return if event.event_id.is_empty() {
                format!("event-{}", uuid::Uuid::new_v4())
            } else {
                event.event_id.clone()
            };
        }
        if let Some((conversation_id, max_seq)) = Self::pure_conversation_ping(req) {
            return format!("conversation-ping:{conversation_id}:{max_seq}");
        }
        format!("event-{}", uuid::Uuid::new_v4())
    }

    async fn publish_event_to_user_batch(
        &self,
        ctx: &flare_server_core::context::Ctx,
        req: &access_gateway::PushEventRequest,
        user_ids: &[String],
        online_only: bool,
    ) -> Result<()> {
        if user_ids.is_empty() {
            return Ok(());
        }

        let priority = req.options.as_ref().map(|o| o.priority).unwrap_or(5);
        let expire_at = req
            .options
            .as_ref()
            .map(|o| o.expire_at_ms)
            .filter(|expire_at| *expire_at > 0);
        let conversation_id = Self::event_conversation_id(req);
        let message_id = Self::event_message_id(req);

        let mut push_req = req.clone();
        push_req.user_ids = user_ids.to_vec();
        let push_payload = push_req.encode_to_vec();
        let metadata = HashMap::new();
        let tenant_id = self.online_status.default_tenant_id().to_string();
        let online_statuses = self.load_online_statuses(ctx, user_ids).await?;
        self.publish_targeted_tasks(
            ctx,
            user_ids,
            &online_statuses,
            PushTaskTemplate {
                message_id: &message_id,
                conversation_id: &conversation_id,
                tenant_id: &tenant_id,
                priority,
                expire_at,
                push_payload: &push_payload,
                headers: &metadata,
                payload_kind: PushTaskPayloadKind::Event as i32,
            },
            online_only,
        )
        .await
    }

    async fn publish_event_to_online_index_batch(
        &self,
        ctx: &flare_server_core::context::Ctx,
        req: &access_gateway::PushEventRequest,
        user_ids: &[String],
    ) -> Result<()> {
        if user_ids.is_empty() {
            return Ok(());
        }

        let priority = req.options.as_ref().map(|o| o.priority).unwrap_or(5);
        let expire_at = req
            .options
            .as_ref()
            .map(|o| o.expire_at_ms)
            .filter(|expire_at| *expire_at > 0);
        let conversation_id = Self::event_conversation_id(req);
        let message_id = Self::event_message_id(req);

        let mut push_req = req.clone();
        push_req.user_ids = user_ids.to_vec();
        let push_payload = push_req.encode_to_vec();
        let metadata = HashMap::new();
        let tenant_id = self.online_status.default_tenant_id().to_string();
        let online_statuses = user_ids
            .iter()
            .map(|user_id| (user_id.clone(), true))
            .collect::<HashMap<_, _>>();
        self.publish_targeted_tasks(
            ctx,
            user_ids,
            &online_statuses,
            PushTaskTemplate {
                message_id: &message_id,
                conversation_id: &conversation_id,
                tenant_id: &tenant_id,
                priority,
                expire_at,
                push_payload: &push_payload,
                headers: &metadata,
                payload_kind: PushTaskPayloadKind::Event as i32,
            },
            true,
        )
        .await
    }

    async fn handle_conversation_ping_without_recipients(
        &self,
        ctx: &flare_server_core::context::Ctx,
        req: access_gateway::PushEventRequest,
        conversation_id: String,
    ) -> Result<()> {
        let online_user_ids = self
            .conversation_online_index
            .online_user_ids(ctx, &conversation_id)
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::ServiceUnavailable,
                    "Failed to query conversation online index",
                )
            })?;
        if online_user_ids.is_empty() {
            tracing::trace!(
                conversation_id = %conversation_id,
                "Recipient-less conversation ping has no online users"
            );
            return Ok(());
        }
        self.publish_event_to_online_index_batch(ctx, &req, &online_user_ids)
            .await?;

        tracing::trace!(
            conversation_id = %conversation_id,
            online_user_count = online_user_ids.len(),
            "Resolved recipient-less conversation ping through online index"
        );
        Ok(())
    }

    async fn handle_coalesced_conversation_ping_without_recipients(
        &self,
        ctx: &flare_server_core::context::Ctx,
        req: access_gateway::PushEventRequest,
        conversation_id: String,
        max_conversation_seq: u64,
    ) -> Result<()> {
        let Some(coalescer) = &self.conversation_ping_coalescer else {
            return self
                .handle_conversation_ping_without_recipients(ctx, req, conversation_id)
                .await;
        };

        let tenant_id = ctx
            .tenant_id()
            .unwrap_or_else(|| self.online_status.default_tenant_id())
            .to_string();
        let key = ConversationPingCoalesceKey::new(tenant_id, conversation_id.clone());
        let pending =
            pending_conversation_ping_request(&req, &conversation_id, max_conversation_seq);

        match coalescer.observe(key.clone(), pending).await {
            ConversationPingCoalesceDecision::SendNow => {
                self.handle_conversation_ping_without_recipients(ctx, req, conversation_id)
                    .await
            }
            ConversationPingCoalesceDecision::ScheduleAfter(delay) => {
                self.spawn_coalesced_conversation_ping(ctx.clone(), coalescer.clone(), key, delay);
                Ok(())
            }
            ConversationPingCoalesceDecision::Suppressed => {
                tracing::trace!(
                    conversation_id = %conversation_id,
                    max_conversation_seq,
                    "Suppressed duplicate recipient-less conversation ping inside coalesce window"
                );
                Ok(())
            }
        }
    }

    fn spawn_coalesced_conversation_ping(
        &self,
        ctx: flare_server_core::context::Ctx,
        coalescer: Arc<ConversationPingCoalescer>,
        key: ConversationPingCoalesceKey,
        delay: Duration,
    ) {
        let handler = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let Some(pending) = coalescer.take_pending(&key).await else {
                return;
            };
            let Some((conversation_id, max_conversation_seq)) =
                PushRouterHandler::pure_conversation_ping(&pending)
            else {
                tracing::warn!(
                    tenant_id = %key.tenant_id,
                    conversation_id = %key.conversation_id,
                    "Coalesced conversation ping lost pure-ping contract"
                );
                return;
            };

            if let Err(error) = handler
                .handle_conversation_ping_without_recipients(&ctx, pending, conversation_id.clone())
                .await
            {
                tracing::warn!(
                    tenant_id = %key.tenant_id,
                    conversation_id = %conversation_id,
                    max_conversation_seq,
                    error = %error,
                    "Failed to dispatch trailing coalesced conversation ping"
                );
            }
        });
    }

    pub async fn handle_message(
        &self,
        ctx: &flare_server_core::context::Ctx,
        req: access_gateway::PushMessageRequest,
    ) -> Result<()> {
        if req.user_ids.is_empty() {
            return Ok(());
        }

        let message = req.messages.first().cloned().unwrap_or_default();
        let conversation_id = message.conversation_id.clone();
        let message_id = message.server_id.clone();
        let priority = 5;
        let expire_at = None;

        let push_payload = req.encode_to_vec();
        let metadata = HashMap::new();
        let tenant_id = self.online_status.default_tenant_id().to_string();
        let online_statuses = self.load_online_statuses(ctx, &req.user_ids).await?;
        self.publish_targeted_tasks(
            ctx,
            &req.user_ids,
            &online_statuses,
            PushTaskTemplate {
                message_id: &message_id,
                conversation_id: &conversation_id,
                tenant_id: &tenant_id,
                priority,
                expire_at,
                push_payload: &push_payload,
                headers: &metadata,
                payload_kind: PushTaskPayloadKind::Message as i32,
            },
            false,
        )
        .await?;

        Ok(())
    }

    pub async fn handle_event(
        &self,
        ctx: &flare_server_core::context::Ctx,
        req: access_gateway::PushEventRequest,
    ) -> Result<()> {
        if req.user_ids.is_empty() {
            if let Some((conversation_id, max_conversation_seq)) =
                Self::pure_conversation_ping(&req)
            {
                return self
                    .handle_coalesced_conversation_ping_without_recipients(
                        ctx,
                        req,
                        conversation_id,
                        max_conversation_seq,
                    )
                    .await;
            }
            return Ok(());
        }

        let online_only = Self::pure_conversation_ping(&req).is_some();
        self.publish_event_to_user_batch(ctx, &req, &req.user_ids, online_only)
            .await
    }

    /// 处理统一推送信封
    ///
    /// ## 设计
    /// - 统一处理 ACK、通知、CustomData、系统消息
    /// - 支持全量推送、用户列表推送、设备列表推送
    pub async fn handle_push_envelope(
        &self,
        ctx: &flare_server_core::context::Ctx,
        envelope: PushEnvelope,
    ) -> Result<()> {
        // 提取推送选项
        let priority = envelope.options.as_ref().map(|o| o.priority).unwrap_or(5);
        let expire_at = envelope.options.as_ref().and_then(|o| o.expire_at);

        // 根据目标类型处理
        let target_user_ids = match flare_proto::PushTargetType::try_from(envelope.target_type) {
            Ok(flare_proto::PushTargetType::All) => return unsupported_push_target("all"),
            Ok(flare_proto::PushTargetType::Users) => envelope.target_user_ids.clone(),
            Ok(flare_proto::PushTargetType::Devices) => return unsupported_push_target("devices"),
            Ok(flare_proto::PushTargetType::Unspecified) | Err(_) => {
                return unsupported_push_target("unspecified");
            }
        };

        if target_user_ids.is_empty() {
            return Ok(());
        }

        let (payload_kind, push_payload, message_id) =
            Self::push_task_payload(&envelope, target_user_ids.clone())?;
        let conversation_id = String::new();

        let online_statuses = self.load_online_statuses(ctx, &target_user_ids).await?;
        self.publish_targeted_tasks(
            ctx,
            &target_user_ids,
            &online_statuses,
            PushTaskTemplate {
                message_id: &message_id,
                conversation_id: &conversation_id,
                tenant_id: &envelope.tenant_id,
                priority,
                expire_at,
                push_payload: &push_payload,
                headers: &envelope.headers,
                payload_kind: payload_kind as i32,
            },
            false,
        )
        .await?;

        Ok(())
    }
}

fn unsupported_push_target<T>(target: &str) -> Result<T> {
    Err(ErrorBuilder::new(
        ErrorCode::InvalidParameter,
        format!("push target type `{target}` is not supported"),
    )
    .build_error())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_proto::PushTaskEnvelope;
    use flare_proto::common::EventEnvelopeDeliveryMode;
    use flare_server_core::Context;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    struct MockOnlineStatusReader {
        statuses: HashMap<String, bool>,
        tenant_id: String,
    }

    #[async_trait]
    impl OnlineStatusReader for MockOnlineStatusReader {
        async fn online_statuses(
            &self,
            _ctx: &flare_server_core::context::Ctx,
            user_ids: &[String],
        ) -> Result<HashMap<String, bool>> {
            Ok(user_ids
                .iter()
                .map(|user_id| {
                    (
                        user_id.clone(),
                        self.statuses.get(user_id).copied().unwrap_or(false),
                    )
                })
                .collect())
        }

        fn default_tenant_id(&self) -> &str {
            &self.tenant_id
        }
    }

    #[derive(Default)]
    struct MockPushTaskPublisher {
        online: Mutex<Vec<(Option<String>, PushTaskEnvelope)>>,
        offline: Mutex<Vec<(Option<String>, PushTaskEnvelope)>>,
    }

    #[async_trait]
    impl PushTaskPublisher for MockPushTaskPublisher {
        async fn publish_online_task(
            &self,
            _ctx: &flare_server_core::context::Ctx,
            key: Option<&str>,
            payload: Vec<u8>,
        ) -> Result<()> {
            let task = PushTaskEnvelope::decode(payload.as_slice())
                .expect("online task payload must decode");
            self.online
                .lock()
                .expect("online task lock poisoned")
                .push((key.map(ToString::to_string), task));
            Ok(())
        }

        async fn publish_offline_task(
            &self,
            _ctx: &flare_server_core::context::Ctx,
            key: Option<&str>,
            payload: Vec<u8>,
        ) -> Result<()> {
            let task = PushTaskEnvelope::decode(payload.as_slice())
                .expect("offline task payload must decode");
            self.offline
                .lock()
                .expect("offline task lock poisoned")
                .push((key.map(ToString::to_string), task));
            Ok(())
        }
    }

    struct MockConversationOnlineIndexReader {
        user_ids: Vec<String>,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl ConversationOnlineIndexReader for MockConversationOnlineIndexReader {
        async fn online_user_ids(
            &self,
            _ctx: &flare_server_core::context::Ctx,
            conversation_id: &str,
        ) -> Result<Vec<String>> {
            assert_eq!(conversation_id, "conversation-large");
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.user_ids.clone())
        }
    }

    #[tokio::test]
    async fn recipientless_conversation_ping_uses_online_index() {
        let online = Arc::new(MockOnlineStatusReader {
            statuses: HashMap::new(),
            tenant_id: "tenant-a".to_string(),
        });
        let publisher = Arc::new(MockPushTaskPublisher::default());
        let online_index = Arc::new(MockConversationOnlineIndexReader {
            user_ids: vec!["online-a".to_string(), "online-c".to_string()],
            calls: Arc::new(AtomicUsize::new(0)),
        });
        let handler = PushRouterHandler::new(online, online_index.clone(), publisher.clone());
        let ctx: flare_server_core::context::Ctx = Arc::new(
            Context::with_request_id("req-recipientless-ping")
                .with_trace_id("trace-recipientless-ping")
                .with_tenant_id("tenant-a"),
        );

        handler
            .handle_event(
                &ctx,
                access_gateway::PushEventRequest {
                    user_ids: vec![],
                    events: vec![],
                    options: None,
                    conversation_id: "conversation-large".to_string(),
                    max_conversation_seq: 42,
                    delivery_mode: EventEnvelopeDeliveryMode::Ping as i32,
                    inline_events_truncated: true,
                },
            )
            .await
            .expect("recipient-less ping should route");
        assert_eq!(online_index.calls.load(Ordering::SeqCst), 1);

        let online_tasks = publisher
            .online
            .lock()
            .expect("online task lock poisoned")
            .clone();
        let offline_tasks = publisher
            .offline
            .lock()
            .expect("offline task lock poisoned")
            .clone();
        assert_eq!(offline_tasks.len(), 0, "pure ping must skip offline tasks");
        assert_eq!(
            online_tasks
                .iter()
                .map(|(key, _)| key.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("online-a"), Some("online-c")]
        );

        for (_key, task) in online_tasks {
            assert_eq!(task.conversation_id, "conversation-large");
            assert_eq!(task.tenant_id, "tenant-a");
            let push = access_gateway::PushEventRequest::decode(task.push_payload.as_slice())
                .expect("push event request should decode");
            assert!(push.events.is_empty());
            assert_eq!(push.conversation_id, "conversation-large");
            assert_eq!(push.max_conversation_seq, 42);
            assert_eq!(push.delivery_mode, EventEnvelopeDeliveryMode::Ping as i32);
            assert!(push.inline_events_truncated);
        }
    }

    #[tokio::test]
    async fn recipientless_conversation_ping_coalesces_high_volume_watermarks_before_online_index_read()
     {
        let online = Arc::new(MockOnlineStatusReader {
            statuses: HashMap::new(),
            tenant_id: "tenant-a".to_string(),
        });
        let publisher = Arc::new(MockPushTaskPublisher::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let online_user_ids = vec![
            "member-000000".to_string(),
            "member-049999".to_string(),
            "member-099999".to_string(),
        ];
        let handler = PushRouterHandler::new(
            online,
            Arc::new(MockConversationOnlineIndexReader {
                user_ids: online_user_ids.clone(),
                calls: calls.clone(),
            }),
            publisher.clone(),
        )
        .with_conversation_ping_coalesce_window(Duration::from_millis(500));
        let ctx: flare_server_core::context::Ctx = Arc::new(
            Context::with_request_id("req-coalesced-ping")
                .with_trace_id("trace-coalesced-ping")
                .with_tenant_id("tenant-a"),
        );

        handler
            .handle_event(
                &ctx,
                access_gateway::PushEventRequest {
                    user_ids: vec![],
                    events: vec![],
                    options: None,
                    conversation_id: "conversation-large".to_string(),
                    max_conversation_seq: 41,
                    delivery_mode: EventEnvelopeDeliveryMode::Ping as i32,
                    inline_events_truncated: true,
                },
            )
            .await
            .expect("first ping should route immediately");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            publisher
                .online
                .lock()
                .expect("online task lock poisoned")
                .iter()
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>(),
            online_user_ids
                .iter()
                .map(|user_id| Some(user_id.clone()))
                .collect::<Vec<_>>(),
            "first ping should publish only users returned by the online index"
        );

        handler
            .handle_event(
                &ctx,
                access_gateway::PushEventRequest {
                    user_ids: vec![],
                    events: vec![],
                    options: None,
                    conversation_id: "conversation-large".to_string(),
                    max_conversation_seq: 45,
                    delivery_mode: EventEnvelopeDeliveryMode::Ping as i32,
                    inline_events_truncated: true,
                },
            )
            .await
            .expect("second ping should be coalesced");

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "coalesced high-volume ping must not reread the online index immediately"
        );
        assert_eq!(
            publisher
                .online
                .lock()
                .expect("online task lock poisoned")
                .len(),
            3,
            "second ping should only update pending watermark inside coalesce window"
        );

        let key = ConversationPingCoalesceKey::new("tenant-a", "conversation-large");
        let pending = handler
            .conversation_ping_coalescer
            .as_ref()
            .expect("coalescer")
            .take_pending(&key)
            .await
            .expect("trailing ping should be pending");
        assert!(pending.events.is_empty());
        assert_eq!(pending.conversation_id, "conversation-large");
        assert_eq!(pending.max_conversation_seq, 45);
        assert_eq!(
            pending.delivery_mode,
            EventEnvelopeDeliveryMode::Ping as i32
        );
        assert!(pending.inline_events_truncated);
    }

    #[tokio::test]
    async fn unsupported_push_envelope_targets_fail_fast_without_publishing_tasks() {
        for target_type in [
            flare_proto::PushTargetType::All,
            flare_proto::PushTargetType::Devices,
            flare_proto::PushTargetType::Unspecified,
        ] {
            let online = Arc::new(MockOnlineStatusReader {
                statuses: HashMap::new(),
                tenant_id: "tenant-a".to_string(),
            });
            let publisher = Arc::new(MockPushTaskPublisher::default());
            let handler = PushRouterHandler::new(
                online,
                Arc::new(MockConversationOnlineIndexReader {
                    user_ids: Vec::new(),
                    calls: Arc::new(AtomicUsize::new(0)),
                }),
                publisher.clone(),
            );
            let ctx: flare_server_core::context::Ctx = Arc::new(
                Context::with_request_id("req-unsupported-push-target")
                    .with_trace_id("trace-unsupported-push-target")
                    .with_tenant_id("tenant-a"),
            );

            let err = handler
                .handle_push_envelope(
                    &ctx,
                    PushEnvelope {
                        envelope_id: format!("envelope-{target_type:?}"),
                        tenant_id: "tenant-a".to_string(),
                        trace_id: "trace-unsupported-push-target".to_string(),
                        created_at: 1,
                        target_type: target_type as i32,
                        target_user_ids: Vec::new(),
                        target_device_ids: vec!["device-a".to_string()],
                        payload_kind: 0,
                        options: None,
                        payload: None,
                        headers: HashMap::new(),
                    },
                )
                .await
                .expect_err("unsupported push target must fail");
            assert!(
                err.to_string().contains("push target type"),
                "unexpected error: {err}"
            );
            assert!(
                publisher
                    .online
                    .lock()
                    .expect("online task lock poisoned")
                    .is_empty()
            );
            assert!(
                publisher
                    .offline
                    .lock()
                    .expect("offline task lock poisoned")
                    .is_empty()
            );
        }
    }
}
