//! Push Proxy → Push Server 在 `flare.im.push.messages` 上的线格式契约。
//!
//! Proxy 真实发布路径写出的字节必须能被 Server 消息消费端的解码函数还原出全部字段；
//! 旧的 `EventBusEnvelope(PushMessageRequest)` 编码会被消费端拒绝（进 DLQ）。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use flare_grpc_proto::push::{PushMessageRequest, PushOptions};
use flare_im_contracts::Ctx;
use flare_im_contracts::constants::topics::TOPIC_PUSH_MESSAGES;
use flare_im_contracts::event::types::types;
use flare_proto::common::message_content::Content;
use flare_proto::common::{
    ConversationType, Message, MessageContent, MessageSource, MessageType, MqPayloadKind,
    TextContent,
};
use flare_push_proxy::config::PushProxyConfig;
use flare_push_proxy::infrastructure::PushProxyMqPublisher;
use flare_push_server::interface::messaging::message_consumer::decode_push_message_record;
use flare_server_core::Context;
use flare_server_core::eventbus::{EventEnvelope, EventPublisher, MqEventBus};
use flare_server_core::mq::context::mq_headers_to_ctx;
use flare_server_core::mq::producer::{Producer, ProducerError, ProducerMessage};
use prost::Message as _;

struct PublishedRecord {
    topic: String,
    key: Option<String>,
    payload: Vec<u8>,
    headers: HashMap<String, String>,
}

/// 记录 Proxy 交给 Broker 的原始记录（topic、key、字节、头）。
#[derive(Default)]
struct RecordingProducer {
    records: Mutex<Vec<PublishedRecord>>,
}

impl RecordingProducer {
    fn take(&self) -> Vec<PublishedRecord> {
        std::mem::take(&mut *self.records.lock().unwrap())
    }
}

#[async_trait]
impl Producer for RecordingProducer {
    async fn send(
        &self,
        _ctx: &Ctx,
        topic: &str,
        key: Option<&str>,
        payload: Vec<u8>,
        headers: Option<HashMap<String, String>>,
    ) -> Result<(), ProducerError> {
        self.records.lock().unwrap().push(PublishedRecord {
            topic: topic.to_string(),
            key: key.map(str::to_string),
            payload,
            headers: headers.unwrap_or_default(),
        });
        Ok(())
    }

    async fn send_batch(
        &self,
        ctx: &Ctx,
        messages: Vec<ProducerMessage>,
    ) -> Result<(), ProducerError> {
        for message in messages {
            self.send(
                ctx,
                &message.topic,
                message.key.as_deref(),
                message.payload,
                Some(message.headers),
            )
            .await?;
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "recording-producer"
    }
}

fn proxy_config() -> PushProxyConfig {
    PushProxyConfig {
        mq_backend: "nats".to_string(),
        jetstream_url: String::new(),
        push_request_topic: TOPIC_PUSH_MESSAGES.to_string(),
        push_envelope_topic: String::new(),
        push_online_topic: String::new(),
        push_offline_topic: String::new(),
        jetstream_timeout_ms: 0,
        jetstream_retries: 0,
        jetstream_retry_backoff_ms: 0,
        jetstream_stream_specs: Vec::new(),
        kafka_brokers: Vec::new(),
        kafka_client_id: String::new(),
        kafka_options: HashMap::new(),
        redis_url: String::new(),
        redis_key_prefix: String::new(),
        device_token_redis_url: String::new(),
        device_token_key_prefix: String::new(),
    }
}

fn golden_ctx() -> Ctx {
    Arc::new(
        Context::with_request_id("req-golden")
            .with_trace_id("trace-golden")
            .with_tenant_id("tenant-golden"),
    )
}

fn golden_message() -> Message {
    Message {
        server_id: "srv-golden".to_string(),
        conversation_id: "conv-golden".to_string(),
        client_msg_id: "client-golden".to_string(),
        sender_id: "user-sender".to_string(),
        source: MessageSource::User as i32,
        conversation_seq: 42,
        created_at: 1_760_000_000_123,
        conversation_type: ConversationType::Group as i32,
        message_type: MessageType::Text as i32,
        message_seq: Some(7),
        channel_id: "group-golden".to_string(),
        sender_name: "Sender".to_string(),
        thread_id: Some("thread-golden".to_string()),
        content: Some(MessageContent {
            content: Some(Content::Text(TextContent {
                text: "hello golden".to_string(),
                mentions: Vec::new(),
            })),
        }),
        attributes: HashMap::from([("attr".to_string(), "value".to_string())]),
        extensions: HashMap::from([("biz.golden".to_string(), vec![1, 2, 3])]),
        ..Message::default()
    }
}

fn golden_request() -> PushMessageRequest {
    PushMessageRequest {
        user_ids: vec!["user-a".to_string(), "user-b".to_string()],
        message: Some(golden_message()),
        options: Some(PushOptions {
            priority: 5,
            ..PushOptions::default()
        }),
        metadata: HashMap::new(),
    }
}

#[tokio::test]
async fn proxy_push_message_bytes_decode_in_push_server() {
    let producer = Arc::new(RecordingProducer::default());
    let publisher = PushProxyMqPublisher::with_producer(Arc::new(proxy_config()), producer.clone());
    let req = golden_request();

    publisher
        .publish_push_message(&golden_ctx(), &req)
        .await
        .expect("proxy publishes push message");

    let records = producer.take();
    assert_eq!(records.len(), 1);
    let record = &records[0];
    assert_eq!(record.topic, TOPIC_PUSH_MESSAGES);

    // Server 消息消费端（PushMessageHandler 使用的同一解码函数）还原字段。
    let decoded =
        decode_push_message_record(&record.payload).expect("push server decodes the proxy record");
    // 分区键与 pipeline 的 push-only 记录一致：同会话推送有序。
    assert_eq!(record.key.as_deref(), Some("conv-golden"));
    assert_eq!(decoded.request.user_ids, req.user_ids);
    assert_eq!(decoded.request.messages, vec![golden_message()]);
    assert!(decoded.request.options.is_none());

    let envelope = &decoded.envelope;
    assert_eq!(envelope.payload_kind, MqPayloadKind::Message as i32);
    assert_eq!(envelope.recipient_user_ids, req.user_ids);
    assert_eq!(envelope.conversation_id, "conv-golden");
    assert_eq!(envelope.seq, 42);
    assert!(envelope.push_only);
    assert!(!envelope.persistence_only);
    assert!(!envelope.large_conversation);
    assert!(!envelope.envelope_id.is_empty());
    assert!(envelope.produced_at > 0);
    assert_eq!(envelope.headers, record.headers);
    let produced_at = envelope.produced_at.to_string();
    for (name, value) in [
        ("x-tenant-id", "tenant-golden"),
        ("x-trace-id", "trace-golden"),
        ("x-request-id", "req-golden"),
        ("push-only", "true"),
        ("x-envelope-id", envelope.envelope_id.as_str()),
        ("x-produced-at-ms", produced_at.as_str()),
        ("x-conversation-id", "conv-golden"),
        ("x-conversation-seq", "42"),
    ] {
        assert_eq!(
            record.headers.get(name).map(String::as_str),
            Some(value),
            "record header {name}"
        );
    }

    // 消费端从记录头重建上下文，Push Server 以该租户为记录签发服务身份。
    let consumer_ctx = mq_headers_to_ctx(&record.headers);
    assert_eq!(consumer_ctx.tenant_id(), Some("tenant-golden"));
    assert_eq!(consumer_ctx.trace_id(), "trace-golden");
    assert_eq!(consumer_ctx.request_id(), "req-golden");
}

#[tokio::test]
async fn legacy_event_bus_encoding_is_rejected_by_push_server() {
    let producer = Arc::new(RecordingProducer::default());
    let legacy_bus = MqEventBus::new(producer.clone());
    let req = golden_request();
    let legacy = EventEnvelope::new(types::MESSAGE, "user-a", 0, req.encode_to_vec());

    legacy_bus
        .publish(&golden_ctx(), TOPIC_PUSH_MESSAGES, &legacy)
        .await
        .expect("legacy event bus publish");

    let records = producer.take();
    assert_eq!(records.len(), 1);
    assert!(decode_push_message_record(&records[0].payload).is_err());
}
