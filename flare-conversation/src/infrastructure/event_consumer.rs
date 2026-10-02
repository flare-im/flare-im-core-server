//! JetStream 事件消费者
//!
//! - ReadReceipt：消费 TOPIC_MESSAGE_EVENTS（与 Orchestrator `publish_domain_event` 对齐）
//! - ConversationEnsure：消费 TOPIC_CONVERSATION_ENSURE（与 Orchestrator `publish_conversation_ensure` 对齐）
//!
//! 注意：事件 topic 仅接受 **protobuf MqEnvelope**（`payload_kind=Event`）。

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use flare_proto::common::event::Payload;
use flare_proto::common::{Event, EventType, MqEnvelope, MqPayloadKind, mq_envelope};
use flare_server_core::context::Context;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result, map_infra_error};
use flare_server_core::mq::consumer::{
    ConsumerConfig, DeadLetterPublisher, FailureAction, FailureContext, FailureTopic,
    Message as MqMessage, MessageAck, MessageFetcher, ProducerDeadLetterPublisher, RetryPolicy,
};
use flare_server_core::mq::kafka::{KafkaConsumerConfig, KafkaMessageFetcher, KafkaProducerConfig};
use flare_server_core::mq::nats::config::dlq_stream_spec;
use flare_server_core::mq::nats::{
    NatsConsumerConfig, NatsMessageFetcher, NatsProducer, NatsProducerConfig, NatsStreamSpec,
};
use prost::Message as _;
use tracing::{debug, error, info, warn};

use flare_im_contracts::constants::groups::CONVERSATION_READ_RECEIPT_GROUP_DEFAULT;
use flare_im_contracts::constants::topics::{
    TOPIC_CONVERSATION_ENSURE, TOPIC_MESSAGE_CREATED, TOPIC_MESSAGE_EVENTS,
};

use crate::config::ConversationConfig;
use crate::domain::model::{ConversationParticipant, ConversationType, ConversationVisibility};
use crate::domain::service::DefaultConversationDomainService;

/// 一次取消息最多等多久；超时后回到循环顶部重新检查停机信号。
const FETCH_WAIT: Duration = Duration::from_secs(5);
/// 取消息出错后的退避。
const FETCH_ERROR_BACKOFF: Duration = Duration::from_millis(500);

/// 消费循环：取一条消息交给 `handle`，直到 `shutdown` 完成。
///
/// 停机信号在等消息、出错退避时立即生效；已经取到的消息先处理完（含 ack/nack）再退出，
/// 不在处理半途中打断。等消息时被打断的那次拉取没有交给处理，broker 按 ack 超时重投。
/// 此前两个消费者的循环都不看停机信号，每次停机都要等运行时 5 s 的任务停机超时后被 abort
/// （W6-S2 F6）。
pub(crate) async fn consume_until<F, Fut>(
    consumer: &str,
    fetcher: &tokio::sync::Mutex<Box<dyn MessageFetcher + Send>>,
    shutdown: impl Future<Output = ()>,
    mut handle: F,
) where
    F: FnMut(MqMessage) -> Fut,
    Fut: Future<Output = ()>,
{
    tokio::pin!(shutdown);
    loop {
        let next = tokio::select! {
            biased;
            _ = &mut shutdown => break,
            next = async {
                let mut fetcher = fetcher.lock().await;
                tokio::time::timeout(FETCH_WAIT, fetcher.fetch()).await
            } => next,
        };
        match next {
            Ok(Ok(Some(message))) => handle(message).await,
            Ok(Ok(None)) | Err(_) => {}
            Ok(Err(e)) => {
                error!(consumer, error = ?e, "JetStream recv error");
                tokio::select! {
                    biased;
                    _ = &mut shutdown => break,
                    _ = tokio::time::sleep(FETCH_ERROR_BACKOFF) => {}
                }
            }
        }
    }
    info!(consumer, "consumer loop stopped on shutdown");
}

/// 已读回执消费者的死信 subject：落在死信流 `flare.im.dlq.>` 内，不与任何消费者订阅的 subject 重叠。
const READ_RECEIPT_DLQ_SUBJECT: &str = "flare.im.dlq.conversation-read-receipt";
/// 会话确保消费者的死信 subject。
const CONVERSATION_ENSURE_DLQ_SUBJECT: &str = "flare.im.dlq.conversation-ensure";

/// 两个消费者的重投预算：与 push-server、编排、落库相同（service-kit `consumer_retry`：12 次
/// 重试，1 s 起翻倍、封顶 15 s，等待合计 135 s），首投加全部重试落在 JetStream 默认的
/// `max_deliver`（16）之内。
fn consumer_retry_config() -> ConsumerConfig {
    flare_im_service_kit::consumer_retry::with_downstream_outage_retry(ConsumerConfig::default())
}

/// 处理失败后的处置（见 [`settle_message`]）。
pub(crate) struct FailureSettlement {
    policy: RetryPolicy,
    dead_letter: Option<Arc<dyn DeadLetterPublisher>>,
}

impl FailureSettlement {
    fn new(dead_letter: Option<Arc<dyn DeadLetterPublisher>>) -> Self {
        Self {
            policy: RetryPolicy::from(&consumer_retry_config()),
            dead_letter,
        }
    }
}

/// 死信生产者的连接参数：地址沿用消费者的 NATS 地址，只确保死信流（`FLARE_DLQ`），不依赖服务
/// 自身的拓扑是否包含它（与 server-core `build_nats_consumer_tasks_with_dlq` 相同）。超时与重试
/// 取 `config/base.toml` 里 JetStream 档案的值。
struct DeadLetterProducerConfig {
    url: String,
}

impl NatsProducerConfig for DeadLetterProducerConfig {
    fn nats_url(&self) -> &str {
        &self.url
    }
    fn timeout_ms(&self) -> u64 {
        5_000
    }
    fn retries(&self) -> u32 {
        8
    }
    fn retry_backoff_ms(&self) -> u64 {
        25
    }
    fn stream_specs(&self) -> Vec<NatsStreamSpec> {
        vec![dlq_stream_spec()]
    }
}

/// NATS 后端的死信发布器。建不起来时消费者照常运行并记 ERROR：重试耗尽的消息退回 nack，
/// JetStream 达到 `max_deliver` 后停投（以前的行为）。Kafka 后端没有死信发布器，见 [`settle_message`]。
async fn dead_letter_publisher(
    mq_backend: &str,
    url: &str,
    subject: &'static str,
) -> Option<Arc<dyn DeadLetterPublisher>> {
    if mq_backend != "nats" {
        return None;
    }
    let config = DeadLetterProducerConfig {
        url: url.to_string(),
    };
    match NatsProducer::new(&config).await {
        Ok(producer) => Some(Arc::new(ProducerDeadLetterPublisher::new(
            Arc::new(producer),
            FailureTopic::fixed(subject),
        ))),
        Err(e) => {
            error!(
                dlq_subject = subject,
                error = %e,
                "Conversation DLQ producer unavailable; exhausted events stay unsettled"
            );
            None
        }
    }
}

/// 处理一条消息并结算。
///
/// - 成功：ack；这是一次重投（`retry_count > 0`）时记一行 INFO，说明此前失败的事件已经生效。
/// - 失败：按 [`consumer_retry_config`] 的退避延迟 NAK，由 broker 重投；重试耗尽，或载荷解不开
///   （重投也不会好）时投死信流再 term。死信投递失败时按退避再 NAK，不丢消息。
///
/// 以前失败时立即 nack：下游（Postgres、Redis）重启期间 16 次重投在同一瞬间用完，JetStream 随即
/// 停投，事件既没有生效也不在 DLQ 里。
///
/// Kafka 后端的确认句柄不支持重投（`nack` 不提交也不重投，后面的偏移一提交就跳过它），这里只记
/// ERROR，行为不变。
async fn settle_message<Fut>(
    kind: &str,
    message: MqMessage,
    failure: &FailureSettlement,
    process: impl FnOnce(Vec<u8>) -> Fut,
) where
    Fut: Future<Output = Result<()>>,
{
    let retry_count = message.context.retry_count;
    let outcome = process(message.payload.clone()).await;
    let Some(ack) = message.ack_handle.clone() else {
        if let Err(e) = outcome {
            error!(error = %e, "Process {kind} event failed");
        }
        return;
    };
    let error = match outcome {
        Ok(()) => {
            if retry_count > 0 {
                info!(
                    kind,
                    retry_count, "{kind} event processed after broker redelivery"
                );
            }
            if let Err(err) = ack.ack().await {
                warn!(error = %err, "JetStream ack failed");
            }
            return;
        }
        Err(error) => error,
    };

    if !ack.supports_native_retry() {
        error!(
            kind,
            error = %error,
            "Process {kind} event failed; this backend cannot redeliver it and a later commit skips it"
        );
        if let Err(err) = ack.nack().await {
            debug!(error = %err, "nack without native redelivery");
        }
        return;
    }

    let undecodable = error.code() == Some(ErrorCode::DeserializationError);
    let action = if undecodable {
        failure.policy.action_for_dead_letter()
    } else {
        failure.policy.action_for_nack(retry_count)
    };
    match action {
        FailureAction::Retry => {
            let delay = failure.policy.retry_delay(retry_count);
            warn!(
                kind,
                retry_count,
                retry_in_ms = delay.map(|d| d.as_millis() as u64),
                error = %error,
                "Process {kind} event failed, redelivering after backoff"
            );
            redeliver(ack.as_ref(), delay).await;
        }
        FailureAction::DeadLetter => {
            let reason = if undecodable {
                "undecodable conversation event"
            } else {
                "conversation event retries exhausted"
            };
            dead_letter(
                kind,
                &message,
                ack.as_ref(),
                failure,
                FailureContext::new(reason, Some(error.to_string()), retry_count),
            )
            .await;
        }
    }
}

/// 交给 broker 在 `delay` 之后重投；没有延迟时立即重投。
async fn redeliver(ack: &dyn MessageAck, delay: Option<Duration>) {
    let result = match delay {
        Some(delay) if !delay.is_zero() => ack.nack_with_delay(delay).await,
        _ => ack.nack().await,
    };
    if let Err(err) = result {
        warn!(error = %err, "JetStream nack failed");
    }
}

/// 投死信流后 term；没有死信发布器或投递失败时按退避 NAK，消息留在原流里。
async fn dead_letter(
    kind: &str,
    message: &MqMessage,
    ack: &dyn MessageAck,
    failure: &FailureSettlement,
    context: FailureContext,
) {
    let retry_count = context.retry_count;
    let Some(publisher) = failure.dead_letter.as_ref() else {
        error!(
            kind,
            retry_count,
            reason = %context.reason,
            "Process {kind} event failed and no DLQ is configured; nacking"
        );
        redeliver(ack, failure.policy.retry_delay(retry_count)).await;
        return;
    };
    let reason = context.reason.clone();
    match publisher.publish_dead_letter(message, context).await {
        Ok(()) => {
            error!(
                kind,
                retry_count,
                reason = %reason,
                topic = %message.context.topic,
                "{kind} event moved to DLQ"
            );
            if let Err(err) = ack.term().await {
                warn!(error = %err, "JetStream term failed");
            }
        }
        Err(err) => {
            error!(
                kind,
                retry_count,
                error = %err,
                "{kind} event DLQ publish failed; redelivering"
            );
            redeliver(ack, failure.policy.retry_delay(retry_count)).await;
        }
    }
}

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
    fn deliver_from_new(&self) -> bool {
        // 这个消费者订阅的 subject 从一条变成两条，durable 名字里含 subject 列表，
        // 于是会新建一个 durable。流里留着几十万条历史，DeliverAll 会把它们全部重放，
        // 而未读自增不是幂等的——会把未读数写坏。这里只关心新增量。
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
    failure: FailureSettlement,
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

        // 会话摘要（last_message_seq / 未读）要靠**消息**事件推进，而消息走的是
        // TOPIC_MESSAGE_CREATED；这里原本只订阅 TOPIC_MESSAGE_EVENTS，
        // 后者只承载已读/撤回/编辑这类"事件记录"，从来不带 Message 载荷。
        // 少订阅这一条的后果是 conversations.last_message_seq 永远为 NULL，
        // 客户端会话列表的预览永远慢一拍（线上实测）。
        //
        // 用"多订阅一条已有 subject"而不是"多发一份消息事件"：JetStream 里多个消费组
        // 共享同一份已存储的消息，不增加任何发布量。
        let subjects = vec![subject.to_string(), TOPIC_MESSAGE_CREATED.to_string()];

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
                KafkaMessageFetcher::new(&consumer_config, subjects.clone()).map_err(|e| {
                    map_infra_error(e, ErrorCode::ConfigurationError, "build Kafka consumer")
                })?,
            ),
            "nats" => Box::new(
                NatsMessageFetcher::new(&consumer_config, subjects.clone())
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
            subjects = ?subjects,
            group = %group,
            "Conversation event consumer subscribed"
        );
        let dead_letter =
            dead_letter_publisher(config.mq_backend.as_str(), url, READ_RECEIPT_DLQ_SUBJECT).await;

        Ok(Self {
            fetcher: tokio::sync::Mutex::new(fetcher),
            domain_service,
            subject: subject.to_string(),
            failure: FailureSettlement::new(dead_letter),
        })
    }

    /// 消费循环：只处理 payload 为 Read 的事件，`shutdown` 完成即退出（见 [`consume_until`]）。
    pub async fn run_until(&self, shutdown: impl Future<Output = ()>) -> Result<()> {
        info!(subject = %self.subject, "ReadReceipt consumer loop started");
        consume_until("read-receipt", &self.fetcher, shutdown, |message| {
            settle_message(
                "ReadReceipt",
                message,
                &self.failure,
                |payload| async move { self.process_payload(&payload).await },
            )
        })
        .await;
        Ok(())
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

    match mq.payload {
        // TOPIC_MESSAGE_EVENTS 上的"事件记录"（已读 / 撤回 / 编辑）。
        Some(mq_envelope::Payload::Event(event))
            if mq.payload_kind == MqPayloadKind::Event as i32 =>
        {
            Ok(matches_supported_event(&event).then_some(event))
        }
        // TOPIC_MESSAGE_CREATED 上的消息本体：信封直接装 Message，不套 Event。
        // 包成 Event 交给同一个处理分支，避免把处理逻辑写两遍。
        Some(mq_envelope::Payload::Message(message)) => Ok(Some(Event {
            conversation_id: message.conversation_id.clone(),
            conversation_seq: message.conversation_seq,
            payload: Some(Payload::Message(message)),
            ..Default::default()
        })),
        _ => Ok(None),
    }
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
    failure: FailureSettlement,
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
        let dead_letter = dead_letter_publisher(
            config.mq_backend.as_str(),
            url,
            CONVERSATION_ENSURE_DLQ_SUBJECT,
        )
        .await;

        Ok(Self {
            fetcher: tokio::sync::Mutex::new(fetcher),
            domain_service,
            subject: subject.to_string(),
            failure: FailureSettlement::new(dead_letter),
        })
    }

    /// 消费循环，`shutdown` 完成即退出（见 [`consume_until`]）。
    pub async fn run_until(&self, shutdown: impl Future<Output = ()>) -> Result<()> {
        info!(subject = %self.subject, "ConversationEnsure consumer loop started");
        consume_until("conversation-ensure", &self.fetcher, shutdown, |message| {
            settle_message(
                "ConversationEnsure",
                message,
                &self.failure,
                |payload| async move { self.process_payload(&payload).await },
            )
        })
        .await;
        Ok(())
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
                // 建会话时的初始成员无历史下限：他们本来就在场，没有「入群前」可言。
                visible_from_seq: 0,
                mention_only: false,
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
mod shutdown_tests {
    //! 消费循环必须响应停机信号（W6-S2 F6）。

    use std::sync::Mutex as StdMutex;
    use std::time::Instant;

    use flare_server_core::mq::consumer::{ConsumerError, ContentType, MessageContext};

    use super::*;

    /// 先交出 `queued` 里的消息，之后的拉取永远等不到消息（空闲的 JetStream 拉取）。
    struct ScriptedFetcher {
        queued: Vec<MqMessage>,
    }

    #[async_trait::async_trait]
    impl MessageFetcher for ScriptedFetcher {
        async fn fetch(&mut self) -> std::result::Result<Option<MqMessage>, ConsumerError> {
            match self.queued.pop() {
                Some(message) => Ok(Some(message)),
                None => std::future::pending().await,
            }
        }
    }

    fn fetcher(queued: Vec<MqMessage>) -> tokio::sync::Mutex<Box<dyn MessageFetcher + Send>> {
        tokio::sync::Mutex::new(Box::new(ScriptedFetcher { queued }))
    }

    fn message() -> MqMessage {
        MqMessage::new(
            b"payload".to_vec(),
            ContentType::Protobuf,
            MessageContext::new(
                Arc::new(Context::with_request_id("req-1")),
                TOPIC_MESSAGE_EVENTS.to_string(),
            ),
        )
    }

    #[tokio::test]
    async fn an_idle_loop_stops_as_soon_as_shutdown_fires() {
        let fetcher = fetcher(Vec::new());
        let (stop, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let started = Instant::now();
        let stopper = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = stop.send(());
        });
        tokio::time::timeout(
            Duration::from_secs(3),
            consume_until(
                "test",
                &fetcher,
                async {
                    let _ = stop_rx.await;
                },
                |_message| async {},
            ),
        )
        .await
        .expect("consumer loop ignored the shutdown signal");
        stopper.await.unwrap();
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_millis(500),
            "stopped after {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn a_message_in_hand_is_finished_before_the_loop_stops() {
        let fetcher = fetcher(vec![message()]);
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let handled = Arc::new(StdMutex::new(Vec::new()));
        let wait = Arc::clone(&shutdown);
        tokio::time::timeout(
            Duration::from_secs(3),
            consume_until(
                "test",
                &fetcher,
                async move { wait.notified().await },
                |message| {
                    let shutdown = Arc::clone(&shutdown);
                    let handled = Arc::clone(&handled);
                    async move {
                        // 停机信号在处理途中到达。
                        shutdown.notify_one();
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        handled.lock().unwrap().push(message.payload);
                    }
                },
            ),
        )
        .await
        .expect("consumer loop ignored the shutdown signal");
        assert_eq!(*handled.lock().unwrap(), vec![b"payload".to_vec()]);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use flare_proto::common::{
        CustomEvent, Event, EventType, Message, MqEnvelope, MqPayloadKind, ReadReceiptEvent, event,
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

    /// 消息本体在 TOPIC_MESSAGE_CREATED 上是**直接装 Message 的信封**，不套 Event。
    /// 解码器一度只认 Event 信封，于是 conversations.last_message_seq 永远为 NULL：
    /// 客户端会话列表的预览永远慢一拍，而链路上没有任何报错（跳过分支是 debug! 级）。
    #[test]
    fn decode_message_event_accepts_bare_message_envelope() {
        let message = Message {
            conversation_id: "conv-1".to_string(),
            conversation_seq: 42,
            sender_id: "u-1".to_string(),
            ..Default::default()
        };
        let mq = MqEnvelope {
            payload_kind: MqPayloadKind::Message as i32,
            payload: Some(mq_envelope::Payload::Message(message)),
            ..Default::default()
        };

        let event = decode_message_event(&mq.encode_to_vec())
            .expect("decode")
            .expect("消息信封必须能解出事件，否则会话摘要永远不更新");

        assert_eq!(event.conversation_id, "conv-1");
        assert_eq!(event.conversation_seq, 42);
        let Some(event::Payload::Message(m)) = event.payload else {
            panic!("payload 必须是 Message");
        };
        assert_eq!(m.conversation_seq, 42);
        assert_eq!(m.sender_id, "u-1");
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

#[cfg(test)]
mod settle_tests {
    //! 处理失败的结算：退避重投、耗尽或解不开时进 DLQ（W9 ②，与 W8-C 的消费者一致）。

    use std::io::Write;
    use std::sync::Mutex as StdMutex;

    use flare_server_core::mq::consumer::{ConsumerError, ContentType, MessageContext};

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Settled {
        Ack,
        Nack,
        NackAfter(Duration),
        Term,
    }

    #[derive(Default)]
    struct RecordingAck(StdMutex<Vec<Settled>>);

    #[async_trait::async_trait]
    impl MessageAck for RecordingAck {
        async fn ack(&self) -> std::result::Result<(), ConsumerError> {
            self.0.lock().unwrap().push(Settled::Ack);
            Ok(())
        }
        async fn nack(&self) -> std::result::Result<(), ConsumerError> {
            self.0.lock().unwrap().push(Settled::Nack);
            Ok(())
        }
        async fn term(&self) -> std::result::Result<(), ConsumerError> {
            self.0.lock().unwrap().push(Settled::Term);
            Ok(())
        }
        async fn nack_with_delay(&self, delay: Duration) -> std::result::Result<(), ConsumerError> {
            self.0.lock().unwrap().push(Settled::NackAfter(delay));
            Ok(())
        }
    }

    /// 记下投递的死信（原 topic、失败原因、重试次数）；`fail` 时投递失败。
    #[derive(Default)]
    struct RecordingDeadLetters {
        fail: bool,
        published: StdMutex<Vec<(String, String, u32)>>,
    }

    #[async_trait::async_trait]
    impl DeadLetterPublisher for RecordingDeadLetters {
        async fn publish_dead_letter(
            &self,
            message: &MqMessage,
            failure: FailureContext,
        ) -> std::result::Result<(), ConsumerError> {
            if self.fail {
                return Err(ConsumerError::DeadLetter("dlq stream unavailable".into()));
            }
            self.published.lock().unwrap().push((
                message.context.topic.clone(),
                failure.reason,
                failure.retry_count,
            ));
            Ok(())
        }
    }

    fn delivery(retry_count: u32) -> (MqMessage, Arc<RecordingAck>) {
        let ack = Arc::new(RecordingAck::default());
        let mut context = MessageContext::new(
            Arc::new(Context::with_request_id("req-settle")),
            TOPIC_MESSAGE_EVENTS.to_string(),
        );
        context.retry_count = retry_count;
        let message = MqMessage::new(b"payload".to_vec(), ContentType::Protobuf, context)
            .with_ack_handle(ack.clone());
        (message, ack)
    }

    fn settlement(dlq: Arc<RecordingDeadLetters>) -> FailureSettlement {
        FailureSettlement::new(Some(dlq))
    }

    async fn settle(
        retry_count: u32,
        failure: &FailureSettlement,
        outcome: fn() -> Result<()>,
    ) -> Vec<Settled> {
        let (message, ack) = delivery(retry_count);
        settle_message(
            "Test",
            message,
            failure,
            |_payload| async move { outcome() },
        )
        .await;
        ack.0.lock().unwrap().clone()
    }

    fn database_down() -> Result<()> {
        Err(ErrorBuilder::new(ErrorCode::DatabaseError, "connection refused").build_error())
    }

    fn undecodable() -> Result<()> {
        Err(ErrorBuilder::new(ErrorCode::DeserializationError, "decode MqEnvelope").build_error())
    }

    fn ok() -> Result<()> {
        Ok(())
    }

    /// 下游暂时失败：按 1 s 起翻倍、封顶 15 s 的延迟 NAK，不立即重投、不进 DLQ。
    #[tokio::test]
    async fn a_failed_event_is_redelivered_after_backoff_not_immediately() {
        let dlq = Arc::new(RecordingDeadLetters::default());
        let failure = settlement(dlq.clone());
        for (retry_count, delay) in [(0, 1), (1, 2), (3, 8), (4, 15), (11, 15)] {
            assert_eq!(
                settle(retry_count, &failure, database_down).await,
                vec![Settled::NackAfter(Duration::from_secs(delay))],
                "retry_count {retry_count}"
            );
        }
        assert!(dlq.published.lock().unwrap().is_empty());
    }

    /// 12 次重试用完（第 13 次投递仍失败）：投死信流再 term，不再留在原流里等 JetStream 停投。
    #[tokio::test]
    async fn exhausted_retries_go_to_the_dlq_then_term() {
        let dlq = Arc::new(RecordingDeadLetters::default());
        let failure = settlement(dlq.clone());
        assert_eq!(
            settle(12, &failure, database_down).await,
            vec![Settled::Term]
        );
        assert_eq!(
            *dlq.published.lock().unwrap(),
            vec![(
                TOPIC_MESSAGE_EVENTS.to_string(),
                "conversation event retries exhausted".to_string(),
                12
            )]
        );
    }

    /// 载荷解不开，重投也不会好：首投失败就进 DLQ。
    #[tokio::test]
    async fn an_undecodable_event_goes_straight_to_the_dlq() {
        let dlq = Arc::new(RecordingDeadLetters::default());
        let failure = settlement(dlq.clone());
        assert_eq!(settle(0, &failure, undecodable).await, vec![Settled::Term]);
        assert_eq!(dlq.published.lock().unwrap().len(), 1);
        assert_eq!(
            dlq.published.lock().unwrap()[0].1,
            "undecodable conversation event"
        );
    }

    /// 死信投递失败：按退避 NAK，消息留在原流里，不 term（否则就丢了）。
    #[tokio::test]
    async fn a_failed_dlq_publish_keeps_the_event_in_the_stream() {
        let dlq = Arc::new(RecordingDeadLetters {
            fail: true,
            ..Default::default()
        });
        let failure = settlement(dlq);
        assert_eq!(
            settle(12, &failure, database_down).await,
            vec![Settled::NackAfter(Duration::from_secs(15))]
        );
    }

    #[derive(Clone, Default)]
    struct Captured(Arc<StdMutex<Vec<u8>>>);

    impl Write for Captured {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buffer);
            Ok(buffer.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// 成功即 ack；重投后才成功的事件记一行 INFO（以前只看得出它没进 DLQ）。
    #[tokio::test]
    async fn success_acks_and_reports_a_redelivered_success() {
        let captured = Captured::default();
        let writer = captured.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let failure = settlement(Arc::new(RecordingDeadLetters::default()));
        assert_eq!(settle(0, &failure, ok).await, vec![Settled::Ack]);
        let first = String::from_utf8_lossy(&captured.0.lock().unwrap()).into_owned();
        assert!(!first.contains("after broker redelivery"), "{first}");

        assert_eq!(settle(3, &failure, ok).await, vec![Settled::Ack]);
        let logs = String::from_utf8_lossy(&captured.0.lock().unwrap()).into_owned();
        assert!(
            logs.contains("Test event processed after broker redelivery"),
            "{logs}"
        );
        assert!(logs.contains("retry_count=3"), "{logs}");
    }

    /// 门禁：两个消费者的重试预算覆盖下游停机与重启窗口，首投加全部重试落在各自 JetStream
    /// 消费者的 `max_deliver` 之内；死信 subject 在死信流内、不被任何消费者重新订阅。
    #[test]
    fn retry_budget_spans_a_restart_window_inside_max_deliver() {
        let read_receipt = ReadReceiptConsumerConfig {
            url: String::new(),
            group: String::new(),
            subject: String::new(),
            stream_specs: Vec::new(),
            kafka_brokers: Vec::new(),
            kafka_client_id: String::new(),
        };
        let ensure = ConversationEnsureConsumerConfig {
            url: String::new(),
            group: String::new(),
            subject: String::new(),
            stream_specs: Vec::new(),
            kafka_brokers: Vec::new(),
            kafka_client_id: String::new(),
        };
        for max_deliver in [read_receipt.max_deliver(), ensure.max_deliver()] {
            let window = flare_im_service_kit::consumer_retry::check_downstream_outage_retry(
                &consumer_retry_config(),
                max_deliver,
            )
            .unwrap_or_else(|error| panic!("conversation retry budget: {error}"));
            assert_eq!(window, Duration::from_secs(135));
        }
        assert!(consumer_retry_config().enable_dlq);

        let dlq_stream = dlq_stream_spec();
        for subject in [READ_RECEIPT_DLQ_SUBJECT, CONVERSATION_ENSURE_DLQ_SUBJECT] {
            assert!(
                dlq_stream.subjects.iter().any(|pattern| {
                    flare_server_core::mq::nats::subject_matches(pattern, subject)
                }),
                "{subject} must land in the DLQ stream"
            );
            for consumed in [
                TOPIC_MESSAGE_EVENTS,
                TOPIC_MESSAGE_CREATED,
                TOPIC_CONVERSATION_ENSURE,
            ] {
                assert_ne!(subject, consumed);
            }
        }
    }
}
