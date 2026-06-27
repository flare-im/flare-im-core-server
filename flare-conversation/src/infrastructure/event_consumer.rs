//! JetStream 事件消费者
//!
//! - ReadReceipt：消费 TOPIC_MESSAGE_EVENTS（与 Orchestrator `publish_domain_event` 对齐）
//! - ConversationEnsure：消费 TOPIC_CONVERSATION_ENSURE（与 Orchestrator `publish_conversation_ensure` 对齐）
//!
//! 注意：事件 topic 仅接受 **protobuf MqEnvelope**（`payload_kind=Event`）。

use std::collections::HashMap;
use std::sync::Arc;

use flare_proto::common::event::Payload;
use flare_proto::common::{Event, EventType, MqEnvelope, MqPayloadKind, mq_envelope};
use flare_server_core::context::Context;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result, map_infra_error};
use flare_server_core::mq::consumer::MessageFetcher;
use flare_server_core::mq::kafka::{KafkaConsumerConfig, KafkaMessageFetcher, KafkaProducerConfig};
use flare_server_core::mq::nats::{NatsConsumerConfig, NatsMessageFetcher, NatsStreamSpec};
use prost::Message as _;
use tracing::{debug, error, info, warn};

use flare_im_contracts::constants::groups::CONVERSATION_READ_RECEIPT_GROUP_DEFAULT;
use flare_im_contracts::constants::topics::{TOPIC_CONVERSATION_ENSURE, TOPIC_MESSAGE_EVENTS};

use crate::config::ConversationConfig;
use crate::domain::model::{ConversationParticipant, ConversationType, ConversationVisibility};
use crate::domain::service::DefaultConversationDomainService;

const CONVERSATION_ENSURE_NAMESPACE: &str = "flare.core";
const CONVERSATION_ENSURE_EVENT_NAME: &str = "conversation.ensure";
const CONVERSATION_ENSURE_EVENT_VERSION: &str = "1";

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
            "nats" => Box::new(
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
    let mq = MqEnvelope::decode(raw)
        .map_err(|e| map_infra_error(e, ErrorCode::DeserializationError, "decode MqEnvelope"))?;

    if mq.payload_kind != MqPayloadKind::Event as i32 {
        return Ok(None);
    }
    if let Some(mq_envelope::Payload::Event(event)) = mq.payload {
        return Ok(matches_supported_event(&event).then_some(event));
    }

    Ok(None)
}

fn decode_conversation_ensure(raw: &[u8]) -> Result<Option<(String, ConversationEnsurePayload)>> {
    let mq = MqEnvelope::decode(raw).map_err(|e| {
        map_infra_error(
            e,
            ErrorCode::DeserializationError,
            "decode ConversationEnsure MqEnvelope",
        )
    })?;

    if mq.payload_kind != MqPayloadKind::Event as i32 {
        return Ok(None);
    }

    let Some(mq_envelope::Payload::Event(event)) = mq.payload else {
        return Ok(None);
    };
    if event.r#type != EventType::EventCustom as i32 {
        return Ok(None);
    }

    let conversation_id = if event.conversation_id.trim().is_empty() {
        mq.conversation_id
    } else {
        event.conversation_id.clone()
    };

    let Some(Payload::Custom(custom)) = event.payload else {
        return Ok(None);
    };
    if custom.namespace != CONVERSATION_ENSURE_NAMESPACE
        || custom.name != CONVERSATION_ENSURE_EVENT_NAME
        || custom.version != CONVERSATION_ENSURE_EVENT_VERSION
    {
        return Ok(None);
    }

    let ensure_payload: ConversationEnsurePayload = serde_json::from_slice(&custom.payload)
        .map_err(|e| map_infra_error(e, ErrorCode::DeserializationError, "parse ensure"))?;

    Ok(Some((conversation_id, ensure_payload)))
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

#[derive(serde::Deserialize, serde::Serialize)]
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
            "nats" => Box::new(
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
        let Some((conversation_id, ensure_payload)) = decode_conversation_ensure(raw)? else {
            return Ok(());
        };

        if conversation_id.is_empty() {
            debug!("ConversationEnsure with empty conversation_id, skip");
            return Ok(());
        }

        let tenant_id = if ensure_payload.tenant_id.trim().is_empty() {
            "0".to_string()
        } else {
            ensure_payload.tenant_id.trim().to_string()
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
        attributes.insert("conversation_id".to_string(), conversation_id.clone());

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
    use std::collections::HashMap;

    use flare_proto::common::{
        CustomEvent, Event, EventType, MqEnvelope, MqPayloadKind, ReadReceiptEvent, event,
        mq_envelope,
    };
    use prost::Message as _;

    use super::{
        CONVERSATION_ENSURE_EVENT_NAME, CONVERSATION_ENSURE_EVENT_VERSION,
        CONVERSATION_ENSURE_NAMESPACE, ConversationEnsurePayload, decode_conversation_ensure,
        decode_message_event, read_seq_for_conversation_cursor,
    };

    #[test]
    fn zero_read_seq_does_not_advance_conversation_cursor() {
        assert_eq!(read_seq_for_conversation_cursor(0), None);
    }

    #[test]
    fn positive_read_seq_advances_conversation_cursor() {
        assert_eq!(read_seq_for_conversation_cursor(42), Some(42));
    }

    #[test]
    fn decode_message_event_accepts_mq_envelope_event() {
        let raw = encode_event_envelope(read_receipt_event());

        let event = decode_message_event(&raw)
            .expect("decode")
            .expect("supported event");

        assert_eq!(event.conversation_id, "conv-1");
        assert_eq!(event.r#type, EventType::EventReadReceipt as i32);
    }

    #[test]
    fn decode_message_event_does_not_accept_direct_event_payload() {
        let raw = read_receipt_event().encode_to_vec();

        let accepted = decode_message_event(&raw).ok().flatten();

        assert!(accepted.is_none());
    }

    #[test]
    fn decode_conversation_ensure_accepts_custom_event_envelope() {
        let payload = ConversationEnsurePayload {
            tenant_id: "tenant-a".to_string(),
            conversation_type: 2,
            business_type: "team".to_string(),
            participants: vec!["u1".to_string(), "u2".to_string()],
            channel_id: "channel-1".to_string(),
        };
        let payload_bytes = serde_json::to_vec(&payload).expect("payload");
        let event = Event {
            conversation_id: "conv-ensure".to_string(),
            conversation_seq: 0,
            r#type: EventType::EventCustom as i32,
            created_at: 100,
            event_id: "event-1".to_string(),
            request_id: Some("request-1".to_string()),
            payload: Some(event::Payload::Custom(CustomEvent {
                namespace: CONVERSATION_ENSURE_NAMESPACE.to_string(),
                name: CONVERSATION_ENSURE_EVENT_NAME.to_string(),
                version: CONVERSATION_ENSURE_EVENT_VERSION.to_string(),
                payload: payload_bytes,
                attributes: HashMap::new(),
            })),
        };
        let raw = encode_event_envelope(event);

        let (conversation_id, decoded) = decode_conversation_ensure(&raw)
            .expect("decode")
            .expect("ensure event");

        assert_eq!(conversation_id, "conv-ensure");
        assert_eq!(decoded.tenant_id, "tenant-a");
        assert_eq!(decoded.conversation_type, 2);
        assert_eq!(decoded.business_type, "team");
        assert_eq!(decoded.participants, vec!["u1", "u2"]);
        assert_eq!(decoded.channel_id, "channel-1");
    }

    fn read_receipt_event() -> Event {
        Event {
            conversation_id: "conv-1".to_string(),
            conversation_seq: 42,
            r#type: EventType::EventReadReceipt as i32,
            created_at: 100,
            event_id: "event-read-1".to_string(),
            request_id: None,
            payload: Some(event::Payload::Read(ReadReceiptEvent {
                conversation_id: "conv-1".to_string(),
                read_seq: 42,
                user_id: "u1".to_string(),
                message_ids: Vec::new(),
                read_at: None,
            })),
        }
    }

    fn encode_event_envelope(event: Event) -> Vec<u8> {
        let conversation_id = event.conversation_id.clone();
        let seq = event.conversation_seq;
        MqEnvelope {
            envelope_id: "envelope-1".to_string(),
            recipient_user_ids: Vec::new(),
            conversation_id,
            seq,
            produced_at: 100,
            payload_kind: MqPayloadKind::Event as i32,
            headers: HashMap::new(),
            push_only: false,
            persistence_only: false,
            large_conversation: false,
            payload: Some(mq_envelope::Payload::Event(event)),
        }
        .encode_to_vec()
    }
}
