//! Kafka 事件消费者
//!
//! - ReadReceipt：消费 TOPIC_MESSAGE_EVENTS（与 Orchestrator `publish_domain_event` 对齐）
//! - ConversationEnsure：消费 TOPIC_CONVERSATION_ENSURE（与 Orchestrator `publish_conversation_ensure` 对齐）
//!
//! 注意：当前主链路投递为 **protobuf MqEnvelope**（`payload_kind=Event`）；
//! 历史链路可能仍有 JSON EventEnvelope 或 raw Event，本文件需兼容三种格式。

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{ErrorBuilder, ErrorCode, Result, map_infra_error};
use flare_proto::common::{Event, EventType, MqEnvelope, MqPayloadKind, mq_envelope};
use flare_proto::common::event::Payload;
use flare_server_core::context::Context;
use flare_server_core::eventbus::EventEnvelope;
use flare_server_core::kafka::{build_kafka_consumer, subscribe_and_wait_for_assignment};
use prost::Message as _;
use rdkafka::Message as RdkafkaMessage;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::BorrowedMessage;
use tracing::{debug, error, info, warn};

use flare_im_core::constants::groups::CONVERSATION_READ_RECEIPT_GROUP_DEFAULT;
use flare_im_core::constants::topics::{TOPIC_CONVERSATION_ENSURE, TOPIC_MESSAGE_EVENTS};
use flare_im_core::event::{
    EVENT_TYPE_OPERATION_CONVERSATION_ENSURE, EVENT_TYPE_OPERATION_READ_RECEIPT,
};

use crate::config::ConversationConfig;
use crate::domain::model::{ConversationParticipant, ConversationType, ConversationVisibility};
use crate::domain::service::DefaultConversationDomainService;

/// Kafka 消费者配置（实现 KafkaConsumerConfig）
struct ReadReceiptConsumerConfig {
    bootstrap: String,
    group: String,
    #[allow(dead_code)]
    topic: String,
}

impl flare_server_core::mq::kafka::config::KafkaConsumerConfig for ReadReceiptConsumerConfig {
    fn kafka_bootstrap(&self) -> &str {
        &self.bootstrap
    }
    fn consumer_group(&self) -> &str {
        &self.group
    }
    fn fetch_min_bytes(&self) -> usize {
        1
    }
    fn fetch_max_wait_ms(&self) -> u64 {
        500
    }
    fn enable_auto_commit(&self) -> bool {
        false
    }
    fn session_timeout_ms(&self) -> u64 {
        10000
    }
    fn auto_offset_reset(&self) -> &str {
        "latest"
    }
    fn fetch_message_max_bytes(&self) -> usize {
        1024 * 1024
    }
    fn max_partition_fetch_bytes(&self) -> usize {
        1024 * 1024
    }
    fn metadata_max_age_ms(&self) -> u64 {
        300000
    }
}

/// ReadReceipt 事件消费者：仅处理 EVENT_READ_RECEIPT，调用 mark_as_read 更新未读数
pub struct ReadReceiptEventConsumer {
    consumer: StreamConsumer,
    domain_service: Arc<DefaultConversationDomainService>,
    topic: String,
}

impl ReadReceiptEventConsumer {
    pub async fn new(
        config: &ConversationConfig,
        domain_service: Arc<DefaultConversationDomainService>,
    ) -> Result<Self> {
        let bootstrap = config
            .kafka_bootstrap
            .as_deref()
            .unwrap_or("127.0.0.1:29092");
        let topic = config
            .kafka_events_topic
            .as_deref()
            .or_else(|| config.kafka_operation_topic.as_deref())
            .unwrap_or(TOPIC_MESSAGE_EVENTS);
        let group = config
            .kafka_group
            .as_deref()
            .unwrap_or(CONVERSATION_READ_RECEIPT_GROUP_DEFAULT);

        let consumer_config = ReadReceiptConsumerConfig {
            bootstrap: bootstrap.to_string(),
            group: group.to_string(),
            topic: topic.to_string(),
        };

        let consumer = build_kafka_consumer(&consumer_config)
            .map_err(|e| map_infra_error(e, ErrorCode::ConfigurationError, "build kafka consumer"))?;

        let topic_slice: &[&str] = &[topic];
        subscribe_and_wait_for_assignment(&consumer, topic_slice)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::NetworkError, "subscribe kafka"))?;

        info!(
            topic = %topic,
            group = %group,
            "ReadReceipt event consumer subscribed"
        );

        Ok(Self {
            consumer,
            domain_service,
            topic: topic.to_string(),
        })
    }

    /// 消费循环：只处理 payload 为 Read 的事件
    pub async fn run(&self) -> Result<()> {
        info!(topic = %self.topic, "ReadReceipt consumer loop started");

        loop {
            match tokio::time::timeout(std::time::Duration::from_secs(5), self.consumer.recv())
                .await
            {
                Ok(Ok(message)) => {
                    if let Err(e) = self.process_message(&message).await {
                        error!(error = %e, "Process ReadReceipt event failed");
                    }
                    if let Err(e) = self.consumer.commit_message(&message, CommitMode::Async) {
                        warn!(error = %e, "Commit offset failed");
                    }
                }
                Ok(Err(e)) => {
                    error!(error = ?e, "Kafka recv error");
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                Err(_) => {}
            }
        }
    }

    async fn process_message(&self, message: &BorrowedMessage<'_>) -> Result<()> {
        let raw = RdkafkaMessage::payload(message).ok_or_else(|| {
            ErrorBuilder::new(ErrorCode::MessageFormatError, "empty kafka payload").build_error()
        })?;
        let Some(event) = decode_read_receipt_event(raw)? else {
            return Ok(());
        };

        let Some(Payload::Read(read)) = event.payload.as_ref() else {
            return Ok(());
        };

        let conversation_id = event.conversation_id.as_str();
        if conversation_id.is_empty() {
            debug!("ReadReceipt with empty conversation_id, skip");
            return Ok(());
        }

        let tenant_id = "0";
        let user_id = read.user_id.as_str();
        if user_id.is_empty() {
            debug!("ReadReceipt with empty user_id, skip");
            return Ok(());
        }

        let read_seq = read.read_seq as i64;

        let ctx = Context::root()
            .with_tenant_id(tenant_id)
            .with_user_id(user_id);

        self.domain_service
            .mark_as_read(&ctx, conversation_id, read_seq)
            .await?;

        debug!(
            conversation_id = %conversation_id,
            user_id = %user_id,
            read_seq = read_seq,
            "Unread updated from ReadReceipt"
        );
        Ok(())
    }
}

fn decode_read_receipt_event(raw: &[u8]) -> Result<Option<Event>> {
    // 优先：当前链路是 protobuf MqEnvelope（topic=flare.im.message.events）
    if let Ok(mq) = MqEnvelope::decode(raw) {
        if mq.payload_kind != MqPayloadKind::Event as i32 {
            return Ok(None);
        }
        if let Some(mq_envelope::Payload::Event(event)) = mq.payload {
            return Ok(matches_read_receipt_event(&event).then_some(event));
        }
        return Ok(None);
    }

    // 兼容：旧链路 JSON EventEnvelope
    if let Ok(envelope) = serde_json::from_slice::<EventEnvelope>(raw) {
        if envelope.event_type != EVENT_TYPE_OPERATION_READ_RECEIPT {
            return Ok(None);
        }
        // EventEnvelope.payload 可能是 MqEnvelope(proto)，也可能是 Event(proto)
        if let Ok(mq) = MqEnvelope::decode(&*envelope.payload) {
            if mq.payload_kind == MqPayloadKind::Event as i32 {
                if let Some(mq_envelope::Payload::Event(event)) = mq.payload {
                    return Ok(matches_read_receipt_event(&event).then_some(event));
                }
            }
            return Ok(None);
        }
        if let Ok(event) = Event::decode(&*envelope.payload) {
            return Ok(matches_read_receipt_event(&event).then_some(event));
        }
        return Ok(None);
    }

    // 兼容：某些链路可能直接把 Event(proto) 作为 Kafka payload 投递，而非 EventEnvelope(JSON)。
    if let Ok(event) = Event::decode(raw) {
        return Ok(matches_read_receipt_event(&event).then_some(event));
    }

    // 该 topic 可能混入非 ReadReceipt 业务消息；无法识别时直接跳过，避免误报错误刷屏。
    debug!("Skip kafka payload: unsupported format for ReadReceipt consumer");
    Ok(None)
}

fn matches_read_receipt_event(event: &Event) -> bool {
    event.r#type == EventType::EventReadReceipt as i32 || matches!(event.payload, Some(Payload::Read(_)))
}

// -----------------------------------------------------------------------------
// ConversationEnsure 消费者（Orchestrator 异步会话创建）
// -----------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct ConversationEnsurePayload {
    conversation_type: i32,
    business_type: String,
    participants: Vec<String>,
    /// 非单聊时与消息 channel_id 一致；单聊应为空（由读路径组装对端）
    #[serde(default)]
    channel_id: String,
}

struct ConversationEnsureConsumerConfig {
    bootstrap: String,
    group: String,
    #[allow(dead_code)]
    topic: String,
}

impl flare_server_core::mq::kafka::config::KafkaConsumerConfig
    for ConversationEnsureConsumerConfig
{
    fn kafka_bootstrap(&self) -> &str {
        &self.bootstrap
    }
    fn consumer_group(&self) -> &str {
        &self.group
    }
    fn fetch_min_bytes(&self) -> usize {
        1
    }
    fn fetch_max_wait_ms(&self) -> u64 {
        500
    }
    fn enable_auto_commit(&self) -> bool {
        false
    }
    fn session_timeout_ms(&self) -> u64 {
        10000
    }
    fn auto_offset_reset(&self) -> &str {
        "latest"
    }
    fn fetch_message_max_bytes(&self) -> usize {
        1024 * 1024
    }
    fn max_partition_fetch_bytes(&self) -> usize {
        1024 * 1024
    }
    fn metadata_max_age_ms(&self) -> u64 {
        300000
    }
}

/// 会话确保事件消费者：消费 conversation.ensure 事件，幂等创建会话
pub struct ConversationEnsureEventConsumer {
    consumer: StreamConsumer,
    domain_service: Arc<DefaultConversationDomainService>,
    topic: String,
}

impl ConversationEnsureEventConsumer {
    pub async fn new(
        config: &ConversationConfig,
        domain_service: Arc<DefaultConversationDomainService>,
    ) -> Result<Self> {
        let bootstrap = config.kafka_bootstrap.as_deref().ok_or_else(|| {
            ErrorBuilder::new(
                ErrorCode::ConfigurationError,
                "kafka_bootstrap required for ConversationEnsure consumer",
            )
            .build_error()
        })?;
        let topic = config
            .kafka_ensure_topic
            .as_deref()
            .unwrap_or(TOPIC_CONVERSATION_ENSURE);
        let group = config
            .kafka_ensure_group
            .as_deref()
            .unwrap_or("conversation-ensure");

        let consumer_config = ConversationEnsureConsumerConfig {
            bootstrap: bootstrap.to_string(),
            group: group.to_string(),
            topic: topic.to_string(),
        };

        let consumer = build_kafka_consumer(&consumer_config).map_err(|e| {
            map_infra_error(e, ErrorCode::ConfigurationError, "build kafka consumer for ensure")
        })?;

        let topic_slice: &[&str] = &[topic];
        subscribe_and_wait_for_assignment(&consumer, topic_slice)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::NetworkError, "subscribe kafka ensure"))?;

        info!(
            topic = %topic,
            group = %group,
            "ConversationEnsure event consumer subscribed"
        );

        Ok(Self {
            consumer,
            domain_service,
            topic: topic.to_string(),
        })
    }

    pub async fn run(&self) -> Result<()> {
        info!(topic = %self.topic, "ConversationEnsure consumer loop started");

        loop {
            match tokio::time::timeout(std::time::Duration::from_secs(5), self.consumer.recv())
                .await
            {
                Ok(Ok(message)) => {
                    if let Err(e) = self.process_message(&message).await {
                        error!(error = %e, "Process ConversationEnsure event failed");
                    }
                    if let Err(e) = self.consumer.commit_message(&message, CommitMode::Async) {
                        warn!(error = %e, "Commit offset failed");
                    }
                }
                Ok(Err(e)) => {
                    error!(error = ?e, "Kafka recv error");
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                Err(_) => {}
            }
        }
    }

    async fn process_message(&self, message: &BorrowedMessage<'_>) -> Result<()> {
        let raw = RdkafkaMessage::payload(message).ok_or_else(|| {
            ErrorBuilder::new(ErrorCode::MessageFormatError, "empty kafka payload").build_error()
        })?;
        let envelope: EventEnvelope = serde_json::from_slice(raw).map_err(|e| {
            map_infra_error(e, ErrorCode::DeserializationError, "parse EventEnvelope JSON")
        })?;

        if envelope.event_type != EVENT_TYPE_OPERATION_CONVERSATION_ENSURE {
            return Ok(());
        }

        let ensure_payload: ConversationEnsurePayload = serde_json::from_slice(&envelope.payload)
            .map_err(|e| map_infra_error(e, ErrorCode::DeserializationError, "parse ensure"))?;

        let conversation_id = envelope.partition_key.as_str();
        if conversation_id.is_empty() {
            debug!("ConversationEnsure with empty partition_key, skip");
            return Ok(());
        }

        let tenant_id = "0";

        let participants: Vec<ConversationParticipant> = ensure_payload
            .participants
            .into_iter()
            .map(|user_id| ConversationParticipant {
                user_id,
                roles: vec![],
                muted: false,
                pinned: false,
                attributes: HashMap::new(),
            })
            .collect();

        let mut attributes = HashMap::new();
        attributes.insert("conversation_id".to_string(), conversation_id.to_string());

        let ctx = Context::root().with_tenant_id(tenant_id);

        self.domain_service
            .create_conversation(
                &ctx,
                ConversationType::from_proto(ensure_payload.conversation_type),
                ensure_payload.business_type,
                participants,
                attributes,
                ConversationVisibility::Private,
                ensure_payload.channel_id,
            )
            .await?;

        debug!(
            conversation_id = %conversation_id,
            "Conversation ensured from event (async)"
        );
        Ok(())
    }
}
