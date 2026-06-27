//! 将 Push 请求写入 MQ。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use flare_grpc_proto::push::{
    PushCustomRequest, PushMessageRequest, PushNotificationRequest, PushOptions as GrpcPushOptions,
};
use flare_im_contracts::Ctx;
use flare_im_contracts::event::types::types;
use flare_proto::common::{
    CustomPayload, NotificationPayload, PushEnvelope, PushOptions, PushPayloadKind, PushTargetType,
    PushTaskEnvelope, push_envelope,
};
use flare_server_core::context::keys;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use flare_server_core::eventbus::EventEnvelope;
use flare_server_core::eventbus::EventPublisher;
use flare_server_core::eventbus::MqEventBus;
use flare_server_core::mq::kafka::KafkaProducerBuilder;
use flare_server_core::mq::nats::NatsProducerBuilder;
use flare_server_core::mq::producer::Producer;
use prost::Message as _;
use tracing::instrument;
use uuid::Uuid;

use crate::config::PushProxyConfig;

const DEFAULT_TENANT_ID: &str = "0";
const MAX_PUSH_ENVELOPE_SIZE: usize = 10 * 1024 * 1024;

/// Push Proxy 使用的 MQ 发布器。
pub struct PushProxyMqPublisher {
    config: Arc<PushProxyConfig>,
    producer: Arc<dyn Producer>,
    event_publisher: Arc<MqEventBus>,
}

impl PushProxyMqPublisher {
    pub async fn new(config: Arc<PushProxyConfig>) -> Result<Self> {
        let producer: Arc<dyn Producer> = match config.mq_backend.as_str() {
            "kafka" => Arc::new(KafkaProducerBuilder::new().build(config.as_ref()).map_err(
                |e| {
                    flare_server_core::error::FlareError::system(format!(
                        "failed to build kafka producer: {}",
                        e
                    ))
                },
            )?),
            "nats" => Arc::new(
                NatsProducerBuilder::new()
                    .build(config.as_ref())
                    .await
                    .map_err(|e| {
                        flare_server_core::error::FlareError::system(format!(
                            "failed to build jetstream producer: {}",
                            e
                        ))
                    })?,
            ),
            other => {
                return Err(flare_server_core::error::FlareError::system(format!(
                    "unsupported mq backend: {other}"
                )));
            }
        };
        let event_publisher = MqEventBus::new(Arc::clone(&producer));

        Ok(Self {
            config,
            producer,
            event_publisher,
        })
    }

    /// 将 PushMessageRequest 写入 push-request topic（设计文档：PushProxy 只负责入队缓冲）
    #[instrument(skip(self, ctx, req), fields(user_count = req.user_ids.len()))]
    pub async fn publish_push_message(&self, ctx: &Ctx, req: &PushMessageRequest) -> Result<()> {
        let key = req.user_ids.first().map(String::as_str);
        let envelope = EventEnvelope::new(
            types::MESSAGE,
            key.unwrap_or_default(),
            0,
            req.encode_to_vec(),
        );
        self.event_publisher
            .publish(ctx, &self.config.push_request_topic, &envelope)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("event publish failed: {}", e))
            })?;
        Ok(())
    }

    /// 将 PushNotificationRequest 转为统一 PushEnvelope，写入 push-envelope topic。
    #[instrument(skip(self, ctx, req), fields(user_count = req.user_ids.len()))]
    pub async fn publish_push_notification(
        &self,
        ctx: &Ctx,
        req: &PushNotificationRequest,
    ) -> Result<()> {
        let envelope = Self::notification_envelope_from_request(ctx, req)?;
        self.publish_push_envelope(ctx, &envelope).await
    }

    /// 将 PushCustomRequest 转为统一 PushEnvelope，写入 push-envelope topic。
    #[instrument(skip(self, ctx, req), fields(user_count = req.user_ids.len()))]
    pub async fn publish_push_custom(&self, ctx: &Ctx, req: &PushCustomRequest) -> Result<()> {
        let envelope = Self::custom_envelope_from_request(ctx, req)?;
        self.publish_push_envelope(ctx, &envelope).await
    }

    /// 发布 PushTaskEnvelope 到在线/离线推送 topic
    #[instrument(skip(self, ctx, task))]
    pub async fn publish_push_task(
        &self,
        ctx: &Ctx,
        task: &PushTaskEnvelope,
        is_online: bool,
    ) -> Result<()> {
        let topic = if is_online {
            &self.config.push_online_topic
        } else {
            &self.config.push_offline_topic
        };

        let envelope = EventEnvelope::new(types::SYSTEM, &task.user_id, 0, task.encode_to_vec())
            .with_source("flare-push-proxy");

        self.event_publisher
            .publish(ctx, topic, &envelope)
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("event publish failed: {}", e))
            })
    }

    async fn publish_push_envelope(&self, ctx: &Ctx, envelope: &PushEnvelope) -> Result<()> {
        let payload = envelope.encode_to_vec();
        if payload.len() > MAX_PUSH_ENVELOPE_SIZE {
            return Err(
                ErrorBuilder::new(ErrorCode::InvalidParameter, "push envelope too large")
                    .param("size", payload.len().to_string())
                    .param("max_size", MAX_PUSH_ENVELOPE_SIZE.to_string())
                    .build_error(),
            );
        }

        self.producer
            .send(
                ctx,
                &self.config.push_envelope_topic,
                Some(envelope.envelope_id.as_str()),
                payload,
                Some(envelope.headers.clone()),
            )
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("mq publish failed: {}", e))
            })
    }

    fn notification_envelope_from_request(
        ctx: &Ctx,
        req: &PushNotificationRequest,
    ) -> Result<PushEnvelope> {
        let notification = req.notification.as_ref().ok_or_else(|| {
            ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "push notification payload missing",
            )
            .build_error()
        })?;
        let envelope_id = Uuid::new_v4().to_string();
        let created_at = current_time_ms();
        let attributes = notification_attributes(req);

        let payload = NotificationPayload {
            notification_id: req
                .metadata
                .get("notification_id")
                .filter(|id| !id.trim().is_empty())
                .cloned()
                .unwrap_or_else(|| envelope_id.clone()),
            title: notification.title.clone(),
            body: notification.body.clone(),
            icon: notification.icon.clone(),
            sound: notification.sound.clone(),
            click_action: notification.click_action.clone(),
            attributes,
            created_at,
        };

        Ok(base_envelope(
            ctx,
            envelope_id,
            created_at,
            req.user_ids.clone(),
            PushPayloadKind::Notification,
            req.options.as_ref(),
            Some(push_envelope::Payload::Notification(payload)),
        ))
    }

    fn custom_envelope_from_request(ctx: &Ctx, req: &PushCustomRequest) -> Result<PushEnvelope> {
        let custom_data = req.custom_data.as_ref().ok_or_else(|| {
            ErrorBuilder::new(ErrorCode::InvalidParameter, "push custom payload missing")
                .build_error()
        })?;
        let envelope_id = Uuid::new_v4().to_string();
        let created_at = current_time_ms();

        let payload = CustomPayload {
            custom_type: custom_data.r#type.clone(),
            custom_data: custom_data.payload.clone(),
            attributes: custom_attributes(req),
            created_at,
        };

        Ok(base_envelope(
            ctx,
            envelope_id,
            created_at,
            req.user_ids.clone(),
            PushPayloadKind::Custom,
            req.options.as_ref(),
            Some(push_envelope::Payload::Custom(payload)),
        ))
    }
}

fn base_envelope(
    ctx: &Ctx,
    envelope_id: String,
    created_at: i64,
    target_user_ids: Vec<String>,
    payload_kind: PushPayloadKind,
    options: Option<&GrpcPushOptions>,
    payload: Option<push_envelope::Payload>,
) -> PushEnvelope {
    let tenant_id = ctx
        .tenant_id()
        .filter(|tenant_id| !tenant_id.trim().is_empty())
        .unwrap_or(DEFAULT_TENANT_ID)
        .to_string();
    let trace_id = if ctx.trace_id().trim().is_empty() {
        ctx.request_id().to_string()
    } else {
        ctx.trace_id().to_string()
    };
    let headers = envelope_headers(ctx, &tenant_id, &envelope_id, created_at);

    PushEnvelope {
        envelope_id,
        tenant_id,
        trace_id,
        created_at,
        target_type: PushTargetType::Users as i32,
        target_user_ids,
        target_device_ids: Vec::new(),
        payload_kind: payload_kind as i32,
        options: push_options(options),
        payload,
        headers,
    }
}

fn push_options(options: Option<&GrpcPushOptions>) -> Option<PushOptions> {
    options.map(|options| PushOptions {
        priority: options.priority,
        expire_at: None,
        require_ack: false,
        retry_count: 0,
        retry_delay_ms: 0,
        attributes: HashMap::new(),
    })
}

fn envelope_headers(
    ctx: &Ctx,
    tenant_id: &str,
    envelope_id: &str,
    produced_at: i64,
) -> HashMap<String, String> {
    let mut headers = flare_server_core::utils::ctx_to_map(ctx);
    headers
        .entry(keys::TENANT_ID.to_string())
        .or_insert_with(|| tenant_id.to_string());
    headers
        .entry("x-envelope-id".to_string())
        .or_insert_with(|| envelope_id.to_string());
    headers
        .entry("x-produced-at-ms".to_string())
        .or_insert_with(|| produced_at.to_string());
    headers
}

fn notification_attributes(req: &PushNotificationRequest) -> HashMap<String, String> {
    let mut attributes = req
        .options
        .as_ref()
        .map(|options| options.metadata.clone())
        .unwrap_or_default();

    if let Some(notification) = req.notification.as_ref() {
        attributes.extend(notification.data.clone());
        attributes.extend(notification.metadata.clone());
    }
    attributes.extend(req.metadata.clone());
    attributes
}

fn custom_attributes(req: &PushCustomRequest) -> HashMap<String, String> {
    let mut attributes = req
        .options
        .as_ref()
        .map(|options| options.metadata.clone())
        .unwrap_or_default();

    if let Some(custom_data) = req.custom_data.as_ref() {
        attributes.extend(custom_data.attributes.clone());
    }
    attributes.extend(req.metadata.clone());
    attributes
}

fn current_time_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_grpc_proto::push::{Notification, PushOptions as GrpcPushOptions};
    use flare_proto::common::CustomData;
    use flare_server_core::Context;

    #[test]
    fn notification_request_maps_to_raw_push_envelope() {
        let ctx = Arc::new(
            Context::with_request_id("req-1")
                .with_trace_id("trace-1")
                .with_tenant_id("tenant-1"),
        );
        let req = PushNotificationRequest {
            user_ids: vec!["user-1".to_string(), "user-2".to_string()],
            notification: Some(Notification {
                title: "System".to_string(),
                body: "Maintenance".to_string(),
                data: HashMap::from([("source".to_string(), "ops".to_string())]),
                metadata: HashMap::from([("level".to_string(), "info".to_string())]),
                click_action: "app://notice".to_string(),
                icon: "icon.png".to_string(),
                sound: "default".to_string(),
            }),
            options: Some(GrpcPushOptions {
                require_online: false,
                persist_if_offline: true,
                priority: 7,
                metadata: HashMap::from([("option".to_string(), "value".to_string())]),
                channel: String::new(),
                mute_when_quiet: false,
            }),
            metadata: HashMap::from([("notification_id".to_string(), "notif-1".to_string())]),
        };

        let envelope =
            PushProxyMqPublisher::notification_envelope_from_request(&ctx, &req).unwrap();

        assert_eq!(envelope.tenant_id, "tenant-1");
        assert_eq!(envelope.trace_id, "trace-1");
        assert_eq!(envelope.target_type, PushTargetType::Users as i32);
        assert_eq!(envelope.target_user_ids, req.user_ids);
        assert_eq!(envelope.payload_kind, PushPayloadKind::Notification as i32);
        assert_eq!(envelope.options.as_ref().unwrap().priority, 7);
        assert_eq!(
            envelope.headers.get(keys::REQUEST_ID).map(String::as_str),
            Some("req-1")
        );

        let push_envelope::Payload::Notification(payload) = envelope.payload.unwrap() else {
            panic!("expected notification payload");
        };
        assert_eq!(payload.notification_id, "notif-1");
        assert_eq!(payload.title, "System");
        assert_eq!(
            payload.attributes.get("source").map(String::as_str),
            Some("ops")
        );
        assert_eq!(
            payload.attributes.get("level").map(String::as_str),
            Some("info")
        );
        assert_eq!(
            payload.attributes.get("option").map(String::as_str),
            Some("value")
        );
    }

    #[test]
    fn custom_request_maps_to_raw_push_envelope() {
        let ctx = Arc::new(Context::with_request_id("req-2").with_tenant_id("tenant-2"));
        let req = PushCustomRequest {
            user_ids: vec!["user-3".to_string()],
            custom_data: Some(CustomData {
                r#type: "badge".to_string(),
                payload: vec![1, 2, 3],
                attributes: HashMap::from([("custom".to_string(), "yes".to_string())]),
            }),
            options: None,
            metadata: HashMap::from([("request".to_string(), "meta".to_string())]),
        };

        let envelope = PushProxyMqPublisher::custom_envelope_from_request(&ctx, &req).unwrap();

        assert_eq!(envelope.tenant_id, "tenant-2");
        assert_eq!(envelope.trace_id, "req-2");
        assert_eq!(envelope.payload_kind, PushPayloadKind::Custom as i32);
        assert!(envelope.options.is_none());

        let push_envelope::Payload::Custom(payload) = envelope.payload.unwrap() else {
            panic!("expected custom payload");
        };
        assert_eq!(payload.custom_type, "badge");
        assert_eq!(payload.custom_data, vec![1, 2, 3]);
        assert_eq!(
            payload.attributes.get("custom").map(String::as_str),
            Some("yes")
        );
        assert_eq!(
            payload.attributes.get("request").map(String::as_str),
            Some("meta")
        );
    }

    #[test]
    fn missing_notification_payload_is_rejected() {
        let ctx = Arc::new(Context::with_request_id("req-3"));
        let req = PushNotificationRequest {
            user_ids: vec!["user-1".to_string()],
            notification: None,
            options: None,
            metadata: HashMap::new(),
        };

        assert!(PushProxyMqPublisher::notification_envelope_from_request(&ctx, &req).is_err());
    }
}
