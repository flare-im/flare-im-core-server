//! Kafka 事件消费者
//!
//! - ReadReceipt：优先消费 TOPIC_MESSAGE_EVENTS（与 Orchestrator 单事件流对齐），仅处理 operation.read_receipt，驱动未读数更新
//! - ConversationEnsure：消费 TOPIC_CONVERSATION_ENSURE，Orchestrator 异步会话创建时幂等创建会话
//!
//! 与 event_bus/topic_envelope 对齐：消息体为 TopicEventEnvelope（proto），内嵌 event 与 tenant_id。

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use flare_proto::common::event::Payload;
use flare_proto::common::TopicEventEnvelope;
use flare_server_core::context::Context;
use flare_server_core::kafka::{build_kafka_consumer, subscribe_and_wait_for_assignment};
use prost::Message as _;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::BorrowedMessage;
use rdkafka::Message as RdkafkaMessage;
use tracing::{debug, error, info, warn};

use flare_im_core::abstractions::topics::{
    EVENT_TYPE_CONVERSATION_ENSURE, EVENT_TYPE_OPERATION_READ_RECEIPT,
};
use flare_im_core::constants::groups::CONVERSATION_READ_RECEIPT_GROUP_DEFAULT;
use flare_im_core::constants::topics::{TOPIC_CONVERSATION_ENSURE, TOPIC_MESSAGE_EVENTS};

use crate::config::ConversationConfig;
use crate::domain::model::{ConversationParticipant, ConversationVisibility};
use crate::domain::service::{ConversationDomainService, DefaultConversationDomainService};

/// Kafka 消费者配置（实现 KafkaConsumerConfig）
struct ReadReceiptConsumerConfig {
    bootstrap: String,
    group: String,
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
            .map_err(|e| anyhow::anyhow!("build kafka consumer: {}", e))?;

        let topic_slice: &[&str] = &[topic];
        subscribe_and_wait_for_assignment(&consumer, topic_slice)
            .await
            .map_err(|e| anyhow::anyhow!("subscribe kafka: {}", e))?;

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
            match tokio::time::timeout(
                std::time::Duration::from_secs(5),
                self.consumer.recv(),
            )
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
        let payload = RdkafkaMessage::payload(message).ok_or_else(|| anyhow::anyhow!("empty payload"))?;
        let envelope = TopicEventEnvelope::decode(payload)?;
        if envelope.event_type != EVENT_TYPE_OPERATION_READ_RECEIPT {
            return Ok(());
        }
        let event = match envelope.event {
            Some(e) => e,
            None => return Ok(()),
        };

        let Some(Payload::Read(read)) = &event.payload else {
            return Ok(());
        };

        let conversation_id = event.conversation_id.as_str();
        if conversation_id.is_empty() {
            debug!("ReadReceipt with empty conversation_id, skip");
            return Ok(());
        }

        let tenant_id = if envelope.tenant_id.is_empty() {
            "0"
        } else {
            envelope.tenant_id.as_str()
        };
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

// -----------------------------------------------------------------------------
// ConversationEnsure 消费者（Orchestrator 异步会话创建）
// -----------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct ConversationEnsurePayload {
    conversation_type: String,
    business_type: String,
    participants: Vec<String>,
}

struct ConversationEnsureConsumerConfig {
    bootstrap: String,
    group: String,
    topic: String,
}

impl flare_server_core::mq::kafka::config::KafkaConsumerConfig for ConversationEnsureConsumerConfig {
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
        let bootstrap = config
            .kafka_bootstrap
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("kafka_bootstrap required for ConversationEnsure consumer"))?;
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

        let consumer = build_kafka_consumer(&consumer_config)
            .map_err(|e| anyhow::anyhow!("build kafka consumer for ensure: {}", e))?;

        let topic_slice: &[&str] = &[topic];
        subscribe_and_wait_for_assignment(&consumer, topic_slice)
            .await
            .map_err(|e| anyhow::anyhow!("subscribe kafka ensure topic: {}", e))?;

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
            match tokio::time::timeout(
                std::time::Duration::from_secs(5),
                self.consumer.recv(),
            )
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
        let payload = RdkafkaMessage::payload(message)
            .ok_or_else(|| anyhow::anyhow!("empty payload"))?;
        let envelope = TopicEventEnvelope::decode(payload)?;

        if envelope.event_type != EVENT_TYPE_CONVERSATION_ENSURE {
            return Ok(());
        }

        let event = match &envelope.event {
            Some(e) => e,
            None => return Ok(()),
        };

        let Some(Payload::Custom(custom)) = &event.payload else {
            return Ok(());
        };

        let ensure_payload: ConversationEnsurePayload =
            serde_json::from_slice(&custom.payload).map_err(|e| anyhow::anyhow!("parse ensure payload: {}", e))?;

        let conversation_id = envelope.conversation_id.as_str();
        if conversation_id.is_empty() {
            debug!("ConversationEnsure with empty conversation_id, skip");
            return Ok(());
        }

        let tenant_id = if envelope.tenant_id.is_empty() {
            "0"
        } else {
            envelope.tenant_id.as_str()
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
                ensure_payload.conversation_type,
                ensure_payload.business_type,
                participants,
                attributes,
                ConversationVisibility::Private,
            )
            .await?;

        debug!(
            conversation_id = %conversation_id,
            "Conversation ensured from event (async)"
        );
        Ok(())
    }
}
