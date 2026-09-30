//! 将 Push 请求写入 MQ。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use flare_grpc_proto::push::{
    PushCustomRequest, PushMessageRequest, PushNotificationRequest, PushOptions as GrpcPushOptions,
};
use flare_im_contracts::Ctx;
use flare_im_contracts::event::mq_envelope_for_main_queue_message_with_headers;
use flare_proto::common::{
    CustomPayload, NotificationPayload, PushEnvelope, PushOptions, PushPayloadKind, PushTargetType,
    push_envelope,
};
use flare_server_core::context::keys;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use flare_server_core::mq::kafka::KafkaProducerBuilder;
use flare_server_core::mq::nats::NatsProducerBuilder;
use flare_server_core::mq::producer::Producer;
use prost::Message as _;
use tracing::instrument;
use uuid::Uuid;

use crate::config::PushProxyConfig;

const MAX_PUSH_ENVELOPE_SIZE: usize = 10 * 1024 * 1024;

/// 一条待写入 `push_request_topic` 的 MQ 记录。
struct PushMessageRecord {
    key: String,
    payload: Vec<u8>,
    headers: HashMap<String, String>,
}

/// Push Proxy 使用的 MQ 发布器。
pub struct PushProxyMqPublisher {
    config: Arc<PushProxyConfig>,
    producer: Arc<dyn Producer>,
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
        Ok(Self::with_producer(config, producer))
    }

    /// 使用已构建的 Producer 创建发布器（Broker 选择由调用方决定）。
    pub fn with_producer(config: Arc<PushProxyConfig>, producer: Arc<dyn Producer>) -> Self {
        Self { config, producer }
    }

    /// 将 PushMessageRequest 写入 push-request topic（设计文档：PushProxy 只负责入队缓冲）。
    ///
    /// 线格式与 message pipeline 的 push-only 记录一致：protobuf `MqEnvelope`
    /// （`payload_kind = MESSAGE`、`push_only = true`），Push Server 的消息消费端按此解码。
    /// `MqEnvelope` 没有推送选项字段，请求中的 `options` / `metadata` 不进入 MQ 记录。
    #[instrument(skip(self, ctx, req), fields(user_count = req.user_ids.len()))]
    pub async fn publish_push_message(&self, ctx: &Ctx, req: &PushMessageRequest) -> Result<()> {
        let record = push_message_record(ctx, req)?;
        self.producer
            .send(
                ctx,
                &self.config.push_request_topic,
                Some(record.key.as_str()),
                record.payload,
                Some(record.headers),
            )
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!("mq publish failed: {}", e))
            })
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

    async fn publish_push_envelope(&self, ctx: &Ctx, envelope: &PushEnvelope) -> Result<()> {
        let payload = envelope.encode_to_vec();
        ensure_payload_size(payload.len(), "push envelope too large")?;

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

        base_envelope(
            ctx,
            envelope_id,
            created_at,
            req.user_ids.clone(),
            PushPayloadKind::Notification,
            req.options.as_ref(),
            Some(push_envelope::Payload::Notification(payload)),
        )
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

        base_envelope(
            ctx,
            envelope_id,
            created_at,
            req.user_ids.clone(),
            PushPayloadKind::Custom,
            req.options.as_ref(),
            Some(push_envelope::Payload::Custom(payload)),
        )
    }
}

/// 构造 `push_request_topic` 上的一条记录。
///
/// 分区键取会话 ID（与 pipeline 的 push-only 记录一致，同会话推送保持顺序）；记录头与信封头
/// 相同，消费端据记录头重建租户与追踪上下文。
fn push_message_record(ctx: &Ctx, req: &PushMessageRequest) -> Result<PushMessageRecord> {
    let tenant_id = required_push_tenant(ctx)?;
    let message = req.message.as_ref().ok_or_else(|| {
        ErrorBuilder::new(ErrorCode::InvalidParameter, "push message payload missing").build_error()
    })?;
    let Some(first_user_id) = req.user_ids.first() else {
        return Err(ErrorBuilder::new(
            ErrorCode::InvalidParameter,
            "push message recipients missing",
        )
        .build_error());
    };

    let mut headers = flare_server_core::utils::ctx_to_map(ctx);
    headers.insert(keys::TENANT_ID.to_string(), tenant_id);
    let mut envelope = mq_envelope_for_main_queue_message_with_headers(
        message,
        req.user_ids.clone(),
        headers,
        false,
    );
    envelope.push_only = true;
    let record_headers = [
        ("push-only", "true".to_string()),
        ("x-envelope-id", envelope.envelope_id.clone()),
        ("x-produced-at-ms", envelope.produced_at.to_string()),
        ("x-conversation-id", envelope.conversation_id.clone()),
        ("x-conversation-seq", envelope.seq.to_string()),
    ];
    for (name, value) in record_headers {
        envelope.headers.insert(name.to_string(), value);
    }

    let payload = envelope.encode_to_vec();
    ensure_payload_size(payload.len(), "push message too large")?;
    let key = if envelope.conversation_id.trim().is_empty() {
        first_user_id.clone()
    } else {
        envelope.conversation_id.clone()
    };
    Ok(PushMessageRecord {
        key,
        payload,
        headers: envelope.headers,
    })
}

fn ensure_payload_size(size: usize, reason: &str) -> Result<()> {
    if size > MAX_PUSH_ENVELOPE_SIZE {
        return Err(ErrorBuilder::new(ErrorCode::InvalidParameter, reason)
            .param("size", size.to_string())
            .param("max_size", MAX_PUSH_ENVELOPE_SIZE.to_string())
            .build_error());
    }
    Ok(())
}

fn base_envelope(
    ctx: &Ctx,
    envelope_id: String,
    created_at: i64,
    target_user_ids: Vec<String>,
    payload_kind: PushPayloadKind,
    options: Option<&GrpcPushOptions>,
    payload: Option<push_envelope::Payload>,
) -> Result<PushEnvelope> {
    let tenant_id = required_push_tenant(ctx)?;
    let trace_id = if ctx.trace_id().trim().is_empty() {
        ctx.request_id().to_string()
    } else {
        ctx.trace_id().to_string()
    };
    let headers = envelope_headers(ctx, &tenant_id, &envelope_id, created_at);

    Ok(PushEnvelope {
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
    })
}

fn required_push_tenant(ctx: &Ctx) -> Result<String> {
    ctx.tenant_id()
        .filter(|tenant_id| {
            flare_im_contracts::domain::tenant::is_valid_tenant_id(tenant_id)
                && flare_im_contracts::utils::normalize_tenant_id(tenant_id) == *tenant_id
        })
        .map(ToString::to_string)
        .ok_or_else(|| {
            ErrorBuilder::new(
                ErrorCode::PermissionDenied,
                "canonical tenant_id required for Push publication",
            )
            .build_error()
        })
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
    use flare_proto::common::{CustomData, Message, MqEnvelope, MqPayloadKind, mq_envelope};
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

    fn push_message_request(message: Option<Message>) -> PushMessageRequest {
        PushMessageRequest {
            user_ids: vec!["user-1".to_string(), "user-2".to_string()],
            message,
            options: None,
            metadata: HashMap::new(),
        }
    }

    #[test]
    fn push_message_record_is_push_only_mq_envelope() {
        let ctx = Arc::new(
            Context::with_request_id("req-4")
                .with_trace_id("trace-4")
                .with_tenant_id("tenant-4"),
        );
        let message = Message {
            server_id: "srv-4".to_string(),
            conversation_id: "conv-4".to_string(),
            conversation_seq: 9,
            ..Message::default()
        };
        let req = push_message_request(Some(message.clone()));

        let record = push_message_record(&ctx, &req).unwrap();
        let envelope = MqEnvelope::decode(record.payload.as_slice()).unwrap();

        assert_eq!(record.key, "conv-4");
        assert_eq!(envelope.payload_kind, MqPayloadKind::Message as i32);
        assert_eq!(
            envelope.payload,
            Some(mq_envelope::Payload::Message(message))
        );
        assert_eq!(envelope.recipient_user_ids, req.user_ids);
        assert_eq!(envelope.conversation_id, "conv-4");
        assert_eq!(envelope.seq, 9);
        assert!(envelope.push_only);
        assert!(!envelope.large_conversation);
        assert!(!envelope.envelope_id.is_empty());
        assert_eq!(envelope.headers, record.headers);
        for (name, value) in [
            (keys::TENANT_ID, "tenant-4"),
            (keys::TRACE_ID, "trace-4"),
            (keys::REQUEST_ID, "req-4"),
            ("push-only", "true"),
            ("x-envelope-id", envelope.envelope_id.as_str()),
            ("x-conversation-id", "conv-4"),
            ("x-conversation-seq", "9"),
        ] {
            assert_eq!(record.headers.get(name).map(String::as_str), Some(value));
        }
    }

    #[test]
    fn push_message_record_without_conversation_keys_on_first_recipient() {
        let ctx = Arc::new(Context::with_request_id("req-5").with_tenant_id("tenant-5"));
        let req = push_message_request(Some(Message::default()));

        assert_eq!(push_message_record(&ctx, &req).unwrap().key, "user-1");
    }

    #[test]
    fn push_message_record_requires_message_and_recipients() {
        let ctx = Arc::new(Context::with_request_id("req-6").with_tenant_id("tenant-6"));
        assert!(push_message_record(&ctx, &push_message_request(None)).is_err());

        let mut no_recipients = push_message_request(Some(Message::default()));
        no_recipients.user_ids.clear();
        assert!(push_message_record(&ctx, &no_recipients).is_err());
    }

    #[test]
    fn push_publication_never_invents_a_default_tenant() {
        let missing = Arc::new(Context::with_request_id("req-missing"));
        let noncanonical =
            Arc::new(Context::with_request_id("req-alias").with_tenant_id(" default "));
        for ctx in [&missing, &noncanonical] {
            assert!(required_push_tenant(ctx).is_err());
            assert!(
                push_message_record(ctx, &push_message_request(Some(Message::default()))).is_err()
            );
            assert!(
                base_envelope(
                    ctx,
                    "envelope-1".to_string(),
                    1,
                    vec!["user-1".to_string()],
                    PushPayloadKind::Notification,
                    None,
                    None,
                )
                .is_err()
            );
        }
        let explicit = Arc::new(Context::with_request_id("req-explicit").with_tenant_id("0"));
        assert_eq!(required_push_tenant(&explicit).unwrap(), "0");
    }
}
