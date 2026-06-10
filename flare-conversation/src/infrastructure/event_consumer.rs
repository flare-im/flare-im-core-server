//! JetStream 事件消费者
//!
//! - ReadReceipt：消费 TOPIC_MESSAGE_EVENTS（与 Orchestrator `publish_domain_event` 对齐）
//! - ConversationEnsure：消费 TOPIC_CONVERSATION_ENSURE（与 Orchestrator `publish_conversation_ensure` 对齐）
//!
//! 注意：当前主链路投递为 **protobuf MqEnvelope**（`payload_kind=Event`）；
//! 迁移后仅接受 protobuf MqEnvelope 与当前 JSON EventEnvelope 事件信封。

use std::collections::HashMap;
use std::sync::Arc;

use flare_proto::common::event::Payload;
use flare_proto::common::{Event, EventType, MqEnvelope, MqPayloadKind, mq_envelope};
use flare_server_core::context::Context;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result, map_infra_error};
use flare_server_core::eventbus::EventEnvelope;
use flare_server_core::mq::consumer::MessageFetcher;
use flare_server_core::mq::kafka::{KafkaConsumerConfig, KafkaMessageFetcher, KafkaProducerConfig};
use flare_server_core::mq::nats::{NatsConsumerConfig, NatsMessageFetcher, NatsStreamSpec};
use prost::Message as _;
use tracing::{debug, error, info, warn};

use flare_im_core::constants::groups::CONVERSATION_READ_RECEIPT_GROUP_DEFAULT;
use flare_im_core::constants::topics::{TOPIC_CONVERSATION_ENSURE, TOPIC_MESSAGE_EVENTS};
use flare_im_core::event::{
    EVENT_TYPE_OPERATION_CONVERSATION_ENSURE, EVENT_TYPE_OPERATION_READ_RECEIPT,
};

use crate::config::ConversationConfig;
use crate::domain::model::{ConversationParticipant, ConversationType, ConversationVisibility};
use crate::domain::service::DefaultConversationDomainService;

/// JetStream 消费者配置（实现 NatsConsumerConfig）
struct ReadReceiptConsumerConfig {
    url: String,
    group: String,
    #[allow(dead_code)]
    subject: String,
    stream_specs: Vec<NatsStreamSpec>,
    kafka_brokers: Vec<String>,
    kafka_client_id: String,
}

impl NatsConsumerConfig for ReadReceiptConsumerConfig {
    fn nats_url(&self) -> &str {
        &self.url
    }
    fn consumer_group(&self) -> &str {
        &self.group
    }
    fn enable_manual_ack(&self) -> bool {
        true
    }
    fn batch_size(&self) -> usize {
        64
    }
    fn batch_timeout_ms(&self) -> u64 {
        500
    }
    fn enable_durable(&self) -> bool {
        true
    }
    fn stream_specs(&self) -> Vec<NatsStreamSpec> {
        self.stream_specs.clone()
    }
}

impl KafkaProducerConfig for ReadReceiptConsumerConfig {
    fn kafka_brokers(&self) -> Vec<String> {
        self.kafka_brokers.clone()
    }

    fn kafka_client_id(&self) -> &str {
        &self.kafka_client_id
    }
}

impl KafkaConsumerConfig for ReadReceiptConsumerConfig {
    fn kafka_consumer_group(&self) -> &str {
        &self.group
    }
}

/// ReadReceipt 事件消费者：仅处理 EVENT_READ_RECEIPT，调用 mark_as_read 更新未读数
pub struct ReadReceiptEventConsumer {
    fetcher: tokio::sync::Mutex<Box<dyn MessageFetcher + Send>>,
    domain_service: Arc<DefaultConversationDomainService>,
    subject: String,
}

impl ReadReceiptEventConsumer {
    pub async fn new(
        config: &ConversationConfig,
        domain_service: Arc<DefaultConversationDomainService>,
    ) -> Result<Self> {
        let url = config
            .jetstream_url
            .as_deref()
            .unwrap_or("nats://127.0.0.1:24222");
        let subject = config
            .jetstream_events_subject
            .as_deref()
            .or(config.jetstream_operation_subject.as_deref())
            .unwrap_or(TOPIC_MESSAGE_EVENTS);
        let group = config
            .jetstream_group
            .as_deref()
            .unwrap_or(CONVERSATION_READ_RECEIPT_GROUP_DEFAULT);

        let consumer_config = ReadReceiptConsumerConfig {
            url: url.to_string(),
            group: group.to_string(),
            subject: subject.to_string(),
            stream_specs: config.jetstream_stream_specs.clone(),
            kafka_brokers: config.kafka_brokers.clone(),
            kafka_client_id: config.kafka_client_id.clone(),
        };

        let fetcher: Box<dyn MessageFetcher + Send> = match config.mq_backend.as_str() {
            "kafka" => Box::new(
                KafkaMessageFetcher::new(&consumer_config, vec![subject.to_string()]).map_err(
                    |e| map_infra_error(e, ErrorCode::ConfigurationError, "build Kafka consumer"),
                )?,
            ),
            "nats" | "jetstream" => Box::new(
                NatsMessageFetcher::new(&consumer_config, vec![subject.to_string()])
                    .await
                    .map_err(|e| {
                        map_infra_error(
                            e,
                            ErrorCode::ConfigurationError,
                            "build JetStream consumer",
                        )
                    })?,
            ),
            other => {
                return Err(ErrorBuilder::new(
                    ErrorCode::ConfigurationError,
                    format!("unsupported mq backend: {other}"),
                )
                .build_error());
            }
        };

        info!(
            subject = %subject,
            group = %group,
            "ReadReceipt event consumer subscribed"
        );

        Ok(Self {
            fetcher: tokio::sync::Mutex::new(fetcher),
            domain_service,
            subject: subject.to_string(),
        })
    }

    /// 消费循环：只处理 payload 为 Read 的事件
    pub async fn run(&self) -> Result<()> {
        info!(subject = %self.subject, "ReadReceipt consumer loop started");

        loop {
            let next = {
                let mut fetcher = self.fetcher.lock().await;
                tokio::time::timeout(std::time::Duration::from_secs(5), fetcher.fetch()).await
            };
            match next {
                Ok(Ok(Some(message))) => {
                    let ack = message.ack_handle.clone();
                    if let Err(e) = self.process_payload(&message.payload).await {
                        error!(error = %e, "Process ReadReceipt event failed");
                        if let Some(ack) = ack
                            && let Err(err) = ack.nack().await
                        {
                            warn!(error = %err, "JetStream nack failed");
                        }
                    } else if let Some(ack) = ack
                        && let Err(err) = ack.ack().await
                    {
                        warn!(error = %err, "JetStream ack failed");
                    }
                }
                Ok(Ok(None)) | Err(_) => {}
                Ok(Err(e)) => {
                    error!(error = ?e, "JetStream recv error");
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            }
        }
    }

    async fn process_payload(&self, raw: &[u8]) -> Result<()> {
        let Some(event) = decode_message_event(raw)? else {
            return Ok(());
        };

        match event.payload.as_ref() {
            Some(Payload::Read(read)) => {
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

                let Some(read_seq) = read_seq_for_conversation_cursor(read.read_seq) else {
                    debug!(
                        conversation_id = %conversation_id,
                        user_id = %user_id,
                        message_id_count = read.message_ids.len(),
                        "ReadReceipt without positive read_seq does not advance conversation read cursor"
                    );
                    return Ok(());
                };

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
            }
            Some(Payload::Message(msg)) => {
                let conversation_id = if event.conversation_id.is_empty() {
                    msg.conversation_id.as_str()
                } else {
                    event.conversation_id.as_str()
                };
                if conversation_id.is_empty() {
                    debug!("Message event with empty conversation_id, skip");
                    return Ok(());
                }
                let sender_id = msg.sender_id.as_str();
                if sender_id.is_empty() {
                    debug!("Message event with empty sender_id, skip");
                    return Ok(());
                }
                let seq = msg.conversation_seq as i64;
                if seq <= 0 {
                    debug!(conversation_id = %conversation_id, seq, "Message event with invalid seq, skip");
                    return Ok(());
                }

                let ctx = Context::root().with_tenant_id("0");
                self.domain_service
                    .apply_message_event(&ctx, conversation_id, sender_id, seq, msg.status)
                    .await?;
                debug!(
                    conversation_id = %conversation_id,
                    sender_id = %sender_id,
                    seq,
                    status = msg.status,
                    "Unread increment applied from message event"
                );
            }
            _ => {}
        }
        Ok(())
    }
}

fn read_seq_for_conversation_cursor(read_seq: u64) -> Option<i64> {
    if read_seq == 0 {
        None
    } else {
        Some(i64::try_from(read_seq).unwrap_or(i64::MAX))
    }
}

fn decode_message_event(raw: &[u8]) -> Result<Option<Event>> {
    // 优先：当前链路是 protobuf MqEnvelope（topic=flare.im.message.events）
    if let Ok(mq) = MqEnvelope::decode(raw) {
        if mq.payload_kind != MqPayloadKind::Event as i32 {
            return Ok(None);
        }
        if let Some(mq_envelope::Payload::Event(event)) = mq.payload {
            return Ok(matches_supported_event(&event).then_some(event));
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
            if mq.payload_kind == MqPayloadKind::Event as i32
                && let Some(mq_envelope::Payload::Event(event)) = mq.payload
            {
                return Ok(matches_supported_event(&event).then_some(event));
            }
            return Ok(None);
        }
        if let Ok(event) = Event::decode(&*envelope.payload) {
            return Ok(matches_supported_event(&event).then_some(event));
        }
        return Ok(None);
    }

    // 兼容：某些链路可能直接把 Event(proto) 作为 JetStream payload 投递，而非 EventEnvelope(JSON)。
    if let Ok(event) = Event::decode(raw) {
        return Ok(matches_supported_event(&event).then_some(event));
    }

    // 该 topic 可能混入非 ReadReceipt 业务消息；无法识别时直接跳过，避免误报错误刷屏。
    debug!("Skip jetstream payload: unsupported format for conversation event consumer");
    Ok(None)
}

fn matches_supported_event(event: &Event) -> bool {
    event.r#type == EventType::EventReadReceipt as i32
        || event.r#type == EventType::EventMessage as i32
        || matches!(
            event.payload,
            Some(Payload::Read(_)) | Some(Payload::Message(_))
        )
}

// -----------------------------------------------------------------------------
// ConversationEnsure 消费者（Orchestrator 异步会话创建）
// -----------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct ConversationEnsurePayload {
    #[serde(default)]
    tenant_id: String,
    conversation_type: i32,
    business_type: String,
    participants: Vec<String>,
    /// 非单聊时与消息 channel_id 一致；单聊应为空（由读路径组装对端）
    #[serde(default)]
    channel_id: String,
}

struct ConversationEnsureConsumerConfig {
    url: String,
    group: String,
    #[allow(dead_code)]
    subject: String,
    stream_specs: Vec<NatsStreamSpec>,
    kafka_brokers: Vec<String>,
    kafka_client_id: String,
}

impl NatsConsumerConfig for ConversationEnsureConsumerConfig {
    fn nats_url(&self) -> &str {
        &self.url
    }
    fn consumer_group(&self) -> &str {
        &self.group
    }
    fn enable_manual_ack(&self) -> bool {
        true
    }
    fn batch_size(&self) -> usize {
        64
    }
    fn batch_timeout_ms(&self) -> u64 {
        500
    }
    fn enable_durable(&self) -> bool {
        true
    }
    fn stream_specs(&self) -> Vec<NatsStreamSpec> {
        self.stream_specs.clone()
    }
}

impl KafkaProducerConfig for ConversationEnsureConsumerConfig {
    fn kafka_brokers(&self) -> Vec<String> {
        self.kafka_brokers.clone()
    }

    fn kafka_client_id(&self) -> &str {
        &self.kafka_client_id
    }
}

impl KafkaConsumerConfig for ConversationEnsureConsumerConfig {
    fn kafka_consumer_group(&self) -> &str {
        &self.group
    }
}

/// 会话确保事件消费者：消费 conversation.ensure 事件，幂等创建会话
pub struct ConversationEnsureEventConsumer {
    fetcher: tokio::sync::Mutex<Box<dyn MessageFetcher + Send>>,
    domain_service: Arc<DefaultConversationDomainService>,
    subject: String,
}

impl ConversationEnsureEventConsumer {
    pub async fn new(
        config: &ConversationConfig,
        domain_service: Arc<DefaultConversationDomainService>,
    ) -> Result<Self> {
        let url = config
            .jetstream_url
            .as_deref()
            .unwrap_or("nats://127.0.0.1:24222");
        let subject = config
            .jetstream_ensure_subject
            .as_deref()
            .unwrap_or(TOPIC_CONVERSATION_ENSURE);
        let group = config
            .jetstream_ensure_group
            .as_deref()
            .unwrap_or("conversation-ensure");

        let consumer_config = ConversationEnsureConsumerConfig {
            url: url.to_string(),
            group: group.to_string(),
            subject: subject.to_string(),
            stream_specs: config.jetstream_stream_specs.clone(),
            kafka_brokers: config.kafka_brokers.clone(),
            kafka_client_id: config.kafka_client_id.clone(),
        };

        let fetcher: Box<dyn MessageFetcher + Send> = match config.mq_backend.as_str() {
            "kafka" => Box::new(
                KafkaMessageFetcher::new(&consumer_config, vec![subject.to_string()]).map_err(
                    |e| {
                        map_infra_error(
                            e,
                            ErrorCode::ConfigurationError,
                            "build Kafka consumer for ensure",
                        )
                    },
                )?,
            ),
            "nats" | "jetstream" => Box::new(
                NatsMessageFetcher::new(&consumer_config, vec![subject.to_string()])
                    .await
                    .map_err(|e| {
                        map_infra_error(
                            e,
                            ErrorCode::ConfigurationError,
                            "build JetStream consumer for ensure",
                        )
                    })?,
            ),
            other => {
                return Err(ErrorBuilder::new(
                    ErrorCode::ConfigurationError,
                    format!("unsupported mq backend: {other}"),
                )
                .build_error());
            }
        };

        info!(
            subject = %subject,
            group = %group,
            "ConversationEnsure event consumer subscribed"
        );

        Ok(Self {
            fetcher: tokio::sync::Mutex::new(fetcher),
            domain_service,
            subject: subject.to_string(),
        })
    }

    pub async fn run(&self) -> Result<()> {
        info!(subject = %self.subject, "ConversationEnsure consumer loop started");

        loop {
            let next = {
                let mut fetcher = self.fetcher.lock().await;
                tokio::time::timeout(std::time::Duration::from_secs(5), fetcher.fetch()).await
            };
            match next {
                Ok(Ok(Some(message))) => {
                    let ack = message.ack_handle.clone();
                    if let Err(e) = self.process_payload(&message.payload).await {
                        error!(error = %e, "Process ConversationEnsure event failed");
                        if let Some(ack) = ack
                            && let Err(err) = ack.nack().await
                        {
                            warn!(error = %err, "JetStream nack failed");
                        }
                    } else if let Some(ack) = ack
                        && let Err(err) = ack.ack().await
                    {
                        warn!(error = %err, "JetStream ack failed");
                    }
                }
                Ok(Ok(None)) | Err(_) => {}
                Ok(Err(e)) => {
                    error!(error = ?e, "JetStream recv error");
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            }
        }
    }

    async fn process_payload(&self, raw: &[u8]) -> Result<()> {
        let envelope: EventEnvelope = serde_json::from_slice(raw).map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::DeserializationError,
                "parse EventEnvelope JSON",
            )
        })?;

        if envelope.event_type != EVENT_TYPE_OPERATION_CONVERSATION_ENSURE {
            return Ok(());
        }

        let ensure_payload: ConversationEnsurePayload =
            serde_json::from_slice(&envelope.payload)
                .map_err(|e| map_infra_error(e, ErrorCode::DeserializationError, "parse ensure"))?;

        let conversation_id = envelope.partition_key.as_str();
        if conversation_id.is_empty() {
            debug!("ConversationEnsure with empty partition_key, skip");
            return Ok(());
        }

        let tenant_id = if ensure_payload.tenant_id.trim().is_empty() {
            "0"
        } else {
            ensure_payload.tenant_id.trim()
        };

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

#[cfg(test)]
mod tests {
    use super::read_seq_for_conversation_cursor;

    #[test]
    fn zero_read_seq_does_not_advance_conversation_cursor() {
        assert_eq!(read_seq_for_conversation_cursor(0), None);
    }

    #[test]
    fn positive_read_seq_advances_conversation_cursor() {
        assert_eq!(read_seq_for_conversation_cursor(42), Some(42));
    }
}
