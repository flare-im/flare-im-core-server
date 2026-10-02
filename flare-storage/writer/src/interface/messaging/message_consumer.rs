//! 消息创建消费者 - 处理 TOPIC_MESSAGE_CREATED 中的 MqEnvelope 消息
//!
//! ## 核心职责
//! 1. 消费 TOPIC_MESSAGE_CREATED 中的 MqEnvelope 消息
//! 2. 反序列化 MqEnvelope 并验证 payload_kind 为 MESSAGE
//! 3. 调用 MessagePersistenceCommandHandler 处理消息持久化
//!
//! ## 设计原则
//! - Interface 层：负责 MQ 消息的接收和反序列化
//! - 上下文重建：从 MQ headers 中提取追踪信息
//! - 委托给 Application 层：调用 MessagePersistenceCommandHandler 处理业务

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use flare_im_contracts::utils::normalize_optional_tenant_id;
use flare_im_contracts::{Ctx, context_from_mq_metadata};
use flare_proto::common::{MqEnvelope, MqPayloadKind, mq_envelope};
use flare_server_core::mq::consumer::{ConsumerError, Message, MessageHandler, MessageResult};
use tracing::instrument;

use crate::application::commands::ProcessStoreMessageCommand;
use crate::application::handlers::MessagePersistenceCommandHandler;
use crate::domain::model::TenantContext;

// 类型别名，简化泛型参数
type IdempotencyRepo =
    crate::infrastructure::persistence::repository::redis_idempotency::RedisIdempotencyRepository;
type HotCacheRepo =
    crate::infrastructure::persistence::repository::redis_cache::RedisHotCacheRepository;
type ArchiveRepo =
    crate::infrastructure::persistence::repository::postgres_store::PostgresMessageStore;
type EventStreamRepo =
    crate::infrastructure::persistence::repository::event_stream::PostgresEventStreamStore;
type WalCleanupRepo =
    crate::infrastructure::persistence::repository::redis_wal_cleanup::RedisWalCleanupRepository;
type AckPub = crate::infrastructure::messaging::ack_publisher::MqAckPublisher;

type MessagePersistenceHandler = MessagePersistenceCommandHandler<
    IdempotencyRepo,
    HotCacheRepo,
    ArchiveRepo,
    EventStreamRepo,
    WalCleanupRepo,
    AckPub,
>;

/// 消息创建消费者处理器
///
/// 处理 `TOPIC_MESSAGE_CREATED` 中的 MqEnvelope 消息，负责消息持久化
pub struct MessageCreatedHandler {
    /// 消息持久化处理器（应用层）
    persistence_handler: Arc<MessagePersistenceHandler>,
}

struct DecodedStoreMessage {
    ctx: Ctx,
    /// 落库租户：生产方按已验证上下文写进消息头的 `x-tenant-id`，与 `command.tenant` 一致。
    tenant_id: String,
    command: ProcessStoreMessageCommand,
    envelope_id: String,
    conversation_id: String,
}

/// 消息头里没有租户的消息不落库，首投即进死信（不按租户 `"0"` 写）。
const MESSAGE_WITHOUT_TENANT: &str = "stored message without a tenant header";

impl MessageCreatedHandler {
    /// 创建新的消息创建消费者处理器
    ///
    /// # 参数
    /// - `persistence_handler`: 消息持久化处理器
    ///
    /// # 返回
    /// - `Self`: 消息创建消费者处理器实例
    pub fn new(persistence_handler: Arc<MessagePersistenceHandler>) -> Self {
        Self {
            persistence_handler,
        }
    }

    fn decode_store_message(
        message: &Message,
    ) -> Result<Option<DecodedStoreMessage>, ConsumerError> {
        let envelope = message.decode_protobuf::<MqEnvelope>().map_err(|e| {
            tracing::error!(
                error = %e,
                topic = %message.context.topic,
                "Failed to deserialize MqEnvelope"
            );
            ConsumerError::Deserialization(format!("Failed to deserialize MqEnvelope: {}", e))
        })?;

        tracing::trace!(
            envelope_id = %envelope.envelope_id,
            conversation_id = %envelope.conversation_id,
            payload_kind = ?envelope.payload_kind,
            seq = envelope.seq,
            "Processing MqEnvelope from TOPIC_MESSAGE_CREATED"
        );

        if envelope.payload_kind != MqPayloadKind::Message as i32 {
            tracing::warn!(
                envelope_id = %envelope.envelope_id,
                payload_kind = ?envelope.payload_kind,
                "Unexpected payload_kind, expected MESSAGE, skipping"
            );
            return Ok(None);
        }

        let proto_message = match &envelope.payload {
            Some(mq_envelope::Payload::Message(m)) => m,
            _ => {
                tracing::error!(
                    envelope_id = %envelope.envelope_id,
                    "Message payload is missing or not Message variant"
                );
                return Err(ConsumerError::Deserialization(
                    "Message payload is missing or not Message variant".to_string(),
                ));
            }
        };

        let mut merged_headers = message.context.headers.clone();
        for (key, value) in &envelope.headers {
            merged_headers.insert(key.clone(), value.clone());
        }
        let ctx = context_from_mq_metadata(&merged_headers);

        // 租户只认消息头：生产方（ingest、orchestrator）按已验证的上下文写 `x-tenant-id`。
        // 消息体 attributes 里的租户字段由发送方或 PreSend Hook 提供，不作数。
        let Some(tenant_id) = normalize_optional_tenant_id(ctx.tenant_id()) else {
            tracing::error!(
                envelope_id = %envelope.envelope_id,
                conversation_id = %envelope.conversation_id,
                reason = MESSAGE_WITHOUT_TENANT,
                "Rejecting stored message without a tenant header"
            );
            return Err(ConsumerError::DeadLetter(
                MESSAGE_WITHOUT_TENANT.to_string(),
            ));
        };

        let mut command = crate::convert::message_command_from_proto(proto_message.clone());
        for (key, value) in merged_headers {
            command.metadata.insert(key, value);
        }
        command.tenant = Some(TenantContext {
            tenant_id: tenant_id.clone(),
            user_id: ctx.user_id().map(ToString::to_string),
        });

        Ok(Some(DecodedStoreMessage {
            ctx,
            tenant_id,
            command,
            envelope_id: envelope.envelope_id,
            conversation_id: envelope.conversation_id,
        }))
    }

    fn batch_group_key(decoded: &DecodedStoreMessage) -> String {
        decoded.tenant_id.clone()
    }
}

#[async_trait]
impl MessageHandler for MessageCreatedHandler {
    /// 处理 MQ 消息
    ///
    /// # 处理流程
    /// 1. 反序列化 MqEnvelope
    /// 2. 验证 payload_kind 为 MESSAGE
    /// 3. 从 payload 提取 Message
    /// 4. 从 headers 中重建上下文
    /// 5. 调用 MessagePersistenceCommandHandler.handle()
    /// 6. 返回处理结果
    ///
    /// # 参数
    /// - `message`: MQ 消息
    ///
    /// # 返回
    /// - `Ok(MessageResult::Ack)`: 处理成功
    /// - `Err(ConsumerError)`: 处理失败
    #[instrument(skip(self, message), fields(
        topic = %message.context.topic,
        partition = message.context.partition,
        offset = message.context.offset,
    ))]
    async fn handle(&self, message: Message) -> Result<MessageResult, ConsumerError> {
        let Some(decoded) = Self::decode_store_message(&message)? else {
            return Ok(MessageResult::Ack);
        };

        // 6. 调用 Application 层
        match self
            .persistence_handler
            .handle(&decoded.ctx, decoded.command)
            .await
        {
            Ok(Some(result)) => {
                tracing::trace!(
                    topic = %message.context.topic,
                    partition = message.context.partition,
                    offset = message.context.offset,
                    message_id = %result.message_id,
                    conversation_id = %result.conversation_id,
                    deduplicated = result.deduplicated,
                    elapsed_ms = message.context.elapsed_ms(),
                    "Successfully processed MqEnvelope"
                );
                Ok(MessageResult::Ack)
            }
            Ok(None) => {
                tracing::trace!(
                    topic = %message.context.topic,
                    partition = message.context.partition,
                    offset = message.context.offset,
                    elapsed_ms = message.context.elapsed_ms(),
                    "Message processed but no result returned (operation message)"
                );
                Ok(MessageResult::Ack)
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    topic = %message.context.topic,
                    partition = message.context.partition,
                    offset = message.context.offset,
                    "Failed to process MqEnvelope"
                );
                Err(ConsumerError::Handler(format!(
                    "MessagePersistenceCommandHandler error: {}",
                    e
                )))
            }
        }
    }

    #[instrument(skip(self, messages), fields(batch_size = messages.len()))]
    async fn handle_batch(
        &self,
        messages: Vec<Message>,
    ) -> Result<Vec<MessageResult>, ConsumerError> {
        if messages.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = vec![MessageResult::Ack; messages.len()];
        let mut groups: BTreeMap<String, (Ctx, Vec<ProcessStoreMessageCommand>)> = BTreeMap::new();
        let mut decoded_count = 0usize;

        for (index, message) in messages.iter().enumerate() {
            // 解不开或缺租户的消息逐条进死信，不连累同批其余消息。
            let decoded = match Self::decode_store_message(message) {
                Ok(Some(decoded)) => decoded,
                Ok(None) => {
                    results[index] = MessageResult::Ack;
                    continue;
                }
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        topic = %message.context.topic,
                        partition = message.context.partition,
                        offset = message.context.offset,
                        "Dead-lettering undecodable stored message in batch"
                    );
                    results[index] = MessageResult::DeadLetter;
                    continue;
                }
            };

            tracing::trace!(
                envelope_id = %decoded.envelope_id,
                conversation_id = %decoded.conversation_id,
                "Decoded message for batch persistence"
            );

            decoded_count += 1;
            let key = Self::batch_group_key(&decoded);
            let entry = groups
                .entry(key)
                .or_insert_with(|| (decoded.ctx.clone(), Vec::new()));
            entry.1.push(decoded.command);
        }

        for (tenant_id, (ctx, commands)) in groups {
            let batch_size = commands.len();
            self.persistence_handler
                .handle_batch(&ctx, commands)
                .await
                .map_err(|e| {
                    tracing::error!(
                        error = %e,
                        tenant_id = %tenant_id,
                        batch_size,
                        "Failed to process message persistence batch"
                    );
                    ConsumerError::Handler(format!(
                        "MessagePersistenceCommandHandler batch error: {}",
                        e
                    ))
                })?;
        }

        tracing::trace!(
            decoded_count,
            result_count = results.len(),
            "Successfully processed message persistence batch"
        );
        Ok(results)
    }

    /// 获取处理器名称
    fn name(&self) -> &str {
        "storage-message-created-handler"
    }

    fn supports_batch(&self) -> bool {
        true
    }
}

/// 消息创建消费者工厂
///
/// 提供创建消息创建处理器的便捷方法
pub struct MessageCreatedConsumerFactory;

impl MessageCreatedConsumerFactory {
    /// 创建消息创建处理器
    ///
    /// # 参数
    /// - `persistence_handler`: 消息持久化处理器
    ///
    /// # 返回
    /// - `Arc<dyn MessageHandler>`: 消息创建处理器实例
    pub fn create_handler(
        persistence_handler: Arc<MessagePersistenceHandler>,
    ) -> Arc<dyn MessageHandler> {
        Arc::new(MessageCreatedHandler::new(persistence_handler))
    }

    /// 获取订阅的主题
    ///
    /// # 返回
    /// - `&'static str`: 主题名称
    pub fn topic() -> &'static str {
        flare_im_contracts::constants::topics::TOPIC_MESSAGE_CREATED
    }

    /// 获取消费者组名称
    ///
    /// # 返回
    /// - `&'static str`: 消费者组名称
    pub fn consumer_group() -> &'static str {
        flare_im_contracts::constants::groups::STORAGE_GROUP_DEFAULT
    }
}

#[cfg(test)]
mod tests {
    //! 落库租户只取生产方按已验证上下文写的消息头（W11-T）。
    //!
    //! 消息体 `attributes` 是发送方提交、PreSend Hook 可整体改写的数据：Message Ingest 以前只
    //! 校验其中的 `x-tenant-id`，Writer 却优先取 `x-tenant-id`/`tenant_id` 作落库租户，事件流、
    //! 写入台账与批量分组都会跟着它走。这里按生产方的头部编码（`context_to_mq_metadata`，与
    //! `MqPushRepository` 相同）构造 TOPIC_MESSAGE_CREATED 上的信封，走消费者解码与领域服务的
    //! 准备、落库全程。
    use super::*;
    use crate::domain::events::AckEvent;
    use crate::domain::model::{Event, Message as DomainMessage};
    use crate::domain::repository::{
        AckPublisher, ArchiveStoreRepository, EventStreamRepository, HotCacheRepository,
        MessageIdempotencyRepository, WalCleanupRepository,
    };
    use crate::domain::service::MessagePersistenceDomainService;
    use flare_im_contracts::context_to_mq_metadata;
    use flare_im_contracts::event::mq_envelope_for_main_queue_message_with_headers;
    use flare_im_contracts::utils::Context;
    use flare_server_core::error::Result as FlareResult;
    use flare_server_core::mq::consumer::{ContentType, MessageContext};
    use prost::Message as _;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Recorded {
        archive: Vec<(Option<String>, Option<String>)>,
        events: Vec<String>,
    }

    #[derive(Clone, Default)]
    struct Recorder(Arc<Mutex<Recorded>>);

    impl Recorder {
        fn archive(&self) -> Vec<(Option<String>, Option<String>)> {
            self.0.lock().expect("recorder").archive.clone()
        }

        fn events(&self) -> Vec<String> {
            self.0.lock().expect("recorder").events.clone()
        }
    }

    impl ArchiveStoreRepository for Recorder {
        async fn store_archive(&self, ctx: &Ctx, message: &DomainMessage) -> FlareResult<()> {
            self.0.lock().expect("recorder").archive.push((
                ctx.tenant_id().map(ToString::to_string),
                message.extra.get("tenant_id").cloned(),
            ));
            Ok(())
        }
    }

    impl EventStreamRepository for Recorder {
        async fn append_event_to_stream(&self, _ctx: &Ctx, event: &Event) -> FlareResult<()> {
            self.0
                .lock()
                .expect("recorder")
                .events
                .push(event.tenant_id.clone());
            Ok(())
        }

        async fn event_exists(
            &self,
            _ctx: &Ctx,
            _tenant_id: &str,
            _conversation_id: &str,
            _seq: i64,
        ) -> FlareResult<bool> {
            Ok(false)
        }
    }

    struct Noop;

    impl MessageIdempotencyRepository for Noop {
        async fn is_new(&self, _ctx: &Ctx, _message_id: &str) -> FlareResult<bool> {
            Ok(true)
        }
    }

    impl HotCacheRepository for Noop {
        async fn store_hot(&self, _ctx: &Ctx, _message: &DomainMessage) -> FlareResult<()> {
            Ok(())
        }
    }

    impl WalCleanupRepository for Noop {
        async fn remove(&self, _ctx: &Ctx, _message_id: &str) -> FlareResult<()> {
            Ok(())
        }
    }

    impl AckPublisher for Noop {
        async fn publish(&self, _ctx: &Ctx, _event: AckEvent<'_>) -> FlareResult<()> {
            Ok(())
        }
    }

    type RecordingService =
        MessagePersistenceDomainService<Noop, Noop, Recorder, Recorder, Noop, Noop>;

    fn recording_service(recorder: &Recorder) -> RecordingService {
        MessagePersistenceDomainService::new(
            None,
            None,
            Some(Arc::new(recorder.clone())),
            Some(Arc::new(recorder.clone())),
            None,
            Some(Arc::new(Noop)),
        )
    }

    /// TOPIC_MESSAGE_CREATED 上的一条消息：头部按 `header_tenant` 的上下文编码（`None` 即生产方
    /// 上下文没有租户），消息体带发送方提交的 `attributes`。
    fn created_message(header_tenant: Option<&str>, attributes: &[(&str, &str)]) -> Message {
        let mut sender = Context::with_request_id("req-w11t").with_user_id("alice");
        if let Some(tenant_id) = header_tenant {
            sender = sender.with_tenant_id(tenant_id);
        }
        let headers = context_to_mq_metadata(&Arc::new(sender));
        let message = flare_proto::common::Message {
            server_id: "msg-w11t".to_string(),
            conversation_id: "single:alice:bob".to_string(),
            sender_id: "alice".to_string(),
            conversation_seq: 7,
            attributes: attributes
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
            ..Default::default()
        };
        let envelope = mq_envelope_for_main_queue_message_with_headers(
            &message,
            Vec::new(),
            headers.clone(),
            false,
        );
        let mut context = MessageContext::new(
            Arc::new(Context::with_request_id("transport")),
            MessageCreatedConsumerFactory::topic().to_string(),
        );
        context.headers = headers;
        Message::new(envelope.encode_to_vec(), ContentType::Protobuf, context)
    }

    async fn stored_tenants(message: &Message) -> (Recorder, String) {
        let decoded = MessageCreatedHandler::decode_store_message(message)
            .expect("decodes")
            .expect("is a message");
        let group = MessageCreatedHandler::batch_group_key(&decoded);
        let recorder = Recorder::default();
        let service = recording_service(&recorder);
        let prepared = service.prepare_message(decoded.command).expect("prepares");
        service
            .ensure_consistency(&decoded.ctx, prepared)
            .await
            .expect("persists");
        (recorder, group)
    }

    #[tokio::test]
    async fn attribute_tenant_id_does_not_choose_the_storage_tenant() {
        let message = created_message(Some("tenant-a"), &[("tenant_id", "tenant-b")]);
        let (recorder, group) = stored_tenants(&message).await;

        assert_eq!(
            recorder.events(),
            vec!["tenant-a".to_string()],
            "event stream row (what Sync reads) carries the header tenant"
        );
        assert_eq!(
            recorder.archive(),
            vec![(Some("tenant-a".to_string()), Some("tenant-a".to_string()))],
            "archive row and its stored attributes carry the header tenant"
        );
        assert_eq!(group, "tenant-a", "batch group follows the signed header");
    }

    #[tokio::test]
    async fn attribute_x_tenant_id_does_not_choose_the_storage_tenant() {
        let message = created_message(Some("tenant-a"), &[("x-tenant-id", "tenant-b")]);
        let (recorder, group) = stored_tenants(&message).await;

        assert_eq!(recorder.events(), vec!["tenant-a".to_string()]);
        assert_eq!(
            recorder.archive(),
            vec![(Some("tenant-a".to_string()), Some("tenant-a".to_string()))]
        );
        assert_eq!(group, "tenant-a");
    }

    #[tokio::test]
    async fn explicit_tenant_zero_is_still_a_normal_tenant() {
        let message = created_message(Some("0"), &[]);
        let (recorder, group) = stored_tenants(&message).await;

        assert_eq!(group, "0");
        assert_eq!(recorder.events(), vec!["0".to_string()]);
        assert_eq!(
            recorder.archive(),
            vec![(Some("0".to_string()), Some("0".to_string()))]
        );
    }

    #[test]
    fn message_without_header_tenant_is_dead_lettered_not_stored_as_zero() {
        for attributes in [vec![], vec![("tenant_id", "tenant-b")]] {
            let message = created_message(None, &attributes);
            let error = match MessageCreatedHandler::decode_store_message(&message) {
                Err(error) => error,
                Ok(_) => panic!("a message without a header tenant was accepted ({attributes:?})"),
            };
            assert!(
                matches!(error, ConsumerError::DeadLetter(_)),
                "unexpected error {error:?}"
            );
            assert!(
                !error.is_retryable(),
                "must go to the DLQ on first delivery"
            );
        }
    }

    fn persistence_handler_without_backends() -> Arc<MessagePersistenceHandler> {
        let service = MessagePersistenceDomainService::<
            IdempotencyRepo,
            HotCacheRepo,
            ArchiveRepo,
            EventStreamRepo,
            WalCleanupRepo,
            AckPub,
        >::new(None, None, None, None, None, None);
        Arc::new(MessagePersistenceCommandHandler::new(
            Arc::new(service),
            Arc::new(flare_im_service_kit::metrics::StorageWriterMetrics::new()),
        ))
    }

    #[tokio::test]
    async fn batch_dead_letters_the_message_without_tenant_instead_of_failing_the_batch() {
        let handler = MessageCreatedHandler::new(persistence_handler_without_backends());
        let results = handler
            .handle_batch(vec![
                created_message(None, &[("tenant_id", "tenant-b")]),
                created_message(None, &[]),
            ])
            .await
            .expect("per-message rejection, not a batch error");
        assert_eq!(
            results,
            vec![MessageResult::DeadLetter, MessageResult::DeadLetter]
        );

        let single = handler
            .handle(created_message(None, &[]))
            .await
            .expect_err("single delivery is rejected");
        assert!(matches!(single, ConsumerError::DeadLetter(_)));
    }
}
