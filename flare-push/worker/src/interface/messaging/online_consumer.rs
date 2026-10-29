//! 在线推送消费者 - 处理 TOPIC_PUSH_ONLINE 中的 PushTaskEnvelope 消息
//!
//! ## 核心职责
//! 1. 消费 TOPIC_PUSH_ONLINE 中的 PushTaskEnvelope 消息
//! 2. 根据 payload_kind 路由到对应的推送逻辑
//! 3. 失败时发送到 DLQ
//!
//! ## 设计原则
//! - Interface 层：负责 MQ 消息的接收和反序列化
//! - 上下文重建：从 MQ headers 中提取追踪信息
//! - 委托给 Application 层：调用 GatewayPushExecutor 处理推送

use std::collections::{HashMap, HashSet, hash_map::Entry};
use std::sync::Arc;

use flare_grpc_proto::access_gateway::{
    PushAckRequest, PushCapabilityRequest, PushCustomRequest, PushEventRequest, PushMessageRequest,
    PushNotificationRequest, PushOptions,
};
use flare_grpc_proto::signaling::router::PushStrategy;
use flare_proto::common::{PushTaskEnvelope, PushTaskPayloadKind};
use flare_server_core::mq::consumer::{ConsumerError, Message, MessageHandler, MessageResult};
use flare_server_core::{ErrorCode, FlareError, flare_err};
use futures::{StreamExt, stream};
use prost::Message as _;
use tracing::instrument;

use crate::application::GatewayPushExecutor;
use crate::infrastructure::mq::dlq_publisher::DlqPublisher;

const MESSAGE_GROUP_FANOUT_CONCURRENCY: usize = 64;
const ACK_FANOUT_CONCURRENCY: usize = 64;

/// 在线推送消费者处理器
pub struct OnlinePushHandler {
    gateway_push: Arc<GatewayPushExecutor>,
    dlq: Arc<DlqPublisher>,
}

struct BatchEntry {
    index: usize,
    message: Message,
    envelope: PushTaskEnvelope,
}

#[derive(Hash, Eq, PartialEq)]
struct MessageBatchKey {
    tenant_id: String,
    user_ids: Vec<String>,
    options: Vec<u8>,
}

struct MessageBatchGroup {
    request: PushMessageRequest,
    entry_indices: Vec<usize>,
}

impl OnlinePushHandler {
    pub fn new(gateway_push: Arc<GatewayPushExecutor>, dlq: Arc<DlqPublisher>) -> Self {
        Self { gateway_push, dlq }
    }

    fn decode_task_envelope(message: &Message) -> Result<PushTaskEnvelope, ConsumerError> {
        PushTaskEnvelope::decode(message.payload.as_slice())
            .map_err(|e| ConsumerError::Deserialization(format!("PushTaskEnvelope: {}", e)))
    }

    fn payload_kind(envelope: &PushTaskEnvelope) -> PushTaskPayloadKind {
        PushTaskPayloadKind::try_from(envelope.payload_kind)
            .unwrap_or(PushTaskPayloadKind::Unspecified)
    }

    fn decode_push_message_request(
        envelope: &PushTaskEnvelope,
    ) -> Result<PushMessageRequest, FlareError> {
        PushMessageRequest::decode(envelope.push_payload.as_slice()).map_err(|e| {
            flare_err!(
                ErrorCode::InvalidParameter,
                format!("decode PushMessageRequest: {}", e)
            )
        })
    }

    fn push_options_key(options: &Option<PushOptions>) -> Vec<u8> {
        options
            .as_ref()
            .map(|options| options.encode_to_vec())
            .unwrap_or_default()
    }

    fn build_message_groups(
        entries: &[BatchEntry],
    ) -> (HashMap<MessageBatchKey, MessageBatchGroup>, HashSet<usize>) {
        let mut groups = HashMap::<MessageBatchKey, MessageBatchGroup>::new();
        let mut grouped_indices = HashSet::<usize>::new();

        for entry in entries {
            if Self::payload_kind(&entry.envelope) != PushTaskPayloadKind::Message {
                continue;
            }
            // The first entry's context is used for the whole group at dispatch.
            // Never combine same-user tasks from different tenants, even when
            // their request options and target IDs are otherwise identical.
            let tenant_id = entry.envelope.tenant_id.as_str();
            if tenant_id.is_empty() || tenant_id.trim() != tenant_id {
                continue;
            }
            let Ok(mut request) = Self::decode_push_message_request(&entry.envelope) else {
                continue;
            };
            let user_ids = if entry.envelope.user_id.trim().is_empty() {
                request
                    .user_ids
                    .iter()
                    .filter(|user_id| !user_id.trim().is_empty())
                    .cloned()
                    .collect::<Vec<_>>()
            } else {
                vec![entry.envelope.user_id.clone()]
            };
            if user_ids.is_empty() {
                continue;
            }
            let options = Self::push_options_key(&request.options);
            let messages = std::mem::take(&mut request.messages);
            request.user_ids = user_ids.clone();
            request.messages = messages;
            let key = MessageBatchKey {
                tenant_id: tenant_id.to_string(),
                user_ids,
                options,
            };

            match groups.entry(key) {
                Entry::Occupied(mut occupied) => {
                    let group = occupied.get_mut();
                    group.request.messages.extend(request.messages);
                    group.entry_indices.push(entry.index);
                }
                Entry::Vacant(vacant) => {
                    vacant.insert(MessageBatchGroup {
                        request,
                        entry_indices: vec![entry.index],
                    });
                }
            }
            grouped_indices.insert(entry.index);
        }

        (groups, grouped_indices)
    }

    fn ack_entry_positions(entries: &[BatchEntry], grouped_indices: &HashSet<usize>) -> Vec<usize> {
        entries
            .iter()
            .enumerate()
            .filter_map(|(position, entry)| {
                (!grouped_indices.contains(&entry.index)
                    && Self::payload_kind(&entry.envelope) == PushTaskPayloadKind::Ack)
                    .then_some(position)
            })
            .collect()
    }

    async fn route_by_payload_kind(
        &self,
        ctx: &flare_server_core::context::Ctx,
        envelope: &PushTaskEnvelope,
        user_id: &str,
        strategy: PushStrategy,
    ) -> Result<(), FlareError> {
        match Self::payload_kind(envelope) {
            PushTaskPayloadKind::Message => {
                let req = Self::decode_push_message_request(envelope)?;
                self.gateway_push
                    .push_message(ctx, user_id, strategy, req)
                    .await
            }
            PushTaskPayloadKind::Event => {
                let req =
                    PushEventRequest::decode(envelope.push_payload.as_slice()).map_err(|e| {
                        flare_err!(
                            ErrorCode::InvalidParameter,
                            format!("decode PushEventRequest: {}", e)
                        )
                    })?;
                self.gateway_push
                    .push_event(ctx, user_id, strategy, req)
                    .await
            }
            PushTaskPayloadKind::Notification => {
                let req = PushNotificationRequest::decode(envelope.push_payload.as_slice())
                    .map_err(|e| {
                        flare_err!(
                            ErrorCode::InvalidParameter,
                            format!("decode PushNotificationRequest: {}", e)
                        )
                    })?;
                self.gateway_push
                    .push_notification(ctx, user_id, strategy, req)
                    .await
            }
            PushTaskPayloadKind::Ack => {
                let req =
                    PushAckRequest::decode(envelope.push_payload.as_slice()).map_err(|e| {
                        flare_err!(
                            ErrorCode::InvalidParameter,
                            format!("decode PushAckRequest: {}", e)
                        )
                    })?;
                self.gateway_push
                    .push_ack(ctx, user_id, strategy, req)
                    .await
            }
            PushTaskPayloadKind::Custom => {
                let req =
                    PushCustomRequest::decode(envelope.push_payload.as_slice()).map_err(|e| {
                        flare_err!(
                            ErrorCode::InvalidParameter,
                            format!("decode PushCustomRequest: {}", e)
                        )
                    })?;
                self.gateway_push
                    .push_custom(ctx, user_id, strategy, req)
                    .await
            }
            PushTaskPayloadKind::Capability => {
                let req = PushCapabilityRequest::decode(envelope.push_payload.as_slice()).map_err(
                    |e| {
                        flare_err!(
                            ErrorCode::InvalidParameter,
                            format!("decode PushCapabilityRequest: {}", e)
                        )
                    },
                )?;
                self.gateway_push.push_capability(ctx, req).await
            }
            PushTaskPayloadKind::Unspecified => Err(flare_err!(
                ErrorCode::InvalidParameter,
                "PushTaskPayloadKind unspecified"
            )),
        }
    }

    /// 被 NAK 过的推送（`retry_count > 0`）这次送达时记一行 INFO：broker 重投到重启后的网关
    /// 实例上成功了。以前只看得出它没进 DLQ。
    fn note_redelivered_success(message: &Message, envelope: &PushTaskEnvelope) {
        let retry_count = message.context.retry_count;
        if retry_count > 0 {
            tracing::info!(
                retry_count,
                user_id = %envelope.user_id,
                message_id = %envelope.message_id,
                "Push delivered after broker redelivery"
            );
        }
    }

    async fn apply_route_result(
        &self,
        message: &Message,
        envelope: &PushTaskEnvelope,
        result: Result<(), FlareError>,
    ) -> std::result::Result<MessageResult, ConsumerError> {
        let ctx = &message.context.ctx;
        let payload = message.payload.as_slice();
        match result {
            Ok(()) => {
                Self::note_redelivered_success(message, envelope);
                Ok(MessageResult::Ack)
            }
            Err(e) => {
                if e.is_retryable() {
                    tracing::warn!(
                        error = %e,
                        user_id = %envelope.user_id,
                        message_id = %envelope.message_id,
                        "Push failed with retryable error, nacking for broker redelivery"
                    );
                    return Ok(MessageResult::Nack);
                }

                tracing::error!(
                    error = %e,
                    user_id = %envelope.user_id,
                    message_id = %envelope.message_id,
                    "Push failed with non-retryable error, sending to DLQ"
                );
                if let Err(dlq_err) = self
                    .dlq
                    .publish(ctx, Some(&envelope.conversation_id), payload.to_vec())
                    .await
                {
                    return Err(ConsumerError::DeadLetter(dlq_err.to_string()));
                }
                Ok(MessageResult::Ack)
            }
        }
    }

    async fn apply_group_route_result(
        &self,
        entries: &[&BatchEntry],
        result: Result<(), FlareError>,
    ) -> std::result::Result<MessageResult, ConsumerError> {
        match result {
            Ok(()) => {
                for entry in entries {
                    Self::note_redelivered_success(&entry.message, &entry.envelope);
                }
                Ok(MessageResult::Ack)
            }
            Err(e) if e.is_retryable() => {
                let first = &entries[0].envelope;
                tracing::warn!(
                    error = %e,
                    user_id = %first.user_id,
                    batch_size = entries.len(),
                    "Batched push failed with retryable error, nacking for broker redelivery"
                );
                Ok(MessageResult::Nack)
            }
            Err(e) => {
                let first = &entries[0].envelope;
                tracing::error!(
                    error = %e,
                    user_id = %first.user_id,
                    batch_size = entries.len(),
                    "Batched push failed with non-retryable error, sending entries to DLQ"
                );
                for entry in entries {
                    if let Err(dlq_err) = self
                        .dlq
                        .publish(
                            &entry.message.context.ctx,
                            Some(&entry.envelope.conversation_id),
                            entry.message.payload.clone(),
                        )
                        .await
                    {
                        return Err(ConsumerError::DeadLetter(dlq_err.to_string()));
                    }
                }
                Ok(MessageResult::Ack)
            }
        }
    }
}

#[async_trait::async_trait]
impl MessageHandler for OnlinePushHandler {
    #[instrument(skip(self), fields(
        topic = %message.context.topic,
        partition = message.context.partition,
        offset = message.context.offset,
    ))]
    async fn handle(&self, message: Message) -> std::result::Result<MessageResult, ConsumerError> {
        // 1. 反序列化 PushTaskEnvelope
        let envelope = Self::decode_task_envelope(&message)?;

        tracing::trace!(
            user_id = %envelope.user_id,
            tenant_id = %envelope.tenant_id,
            message_id = %envelope.message_id,
            conversation_id = %envelope.conversation_id,
            payload_kind = ?envelope.payload_kind,
            "Processing PushTaskEnvelope"
        );

        // 2. 获取上下文
        let ctx = &message.context.ctx;
        let user_id = &envelope.user_id;
        let strategy = PushStrategy::AllDevices;

        // 3. 根据 payload_kind 路由
        let result = self
            .route_by_payload_kind(ctx, &envelope, user_id, strategy)
            .await;

        self.apply_route_result(&message, &envelope, result).await
    }

    async fn handle_batch(
        &self,
        messages: Vec<Message>,
    ) -> std::result::Result<Vec<MessageResult>, ConsumerError> {
        let mut entries = Vec::with_capacity(messages.len());
        for (index, message) in messages.into_iter().enumerate() {
            let envelope = Self::decode_task_envelope(&message)?;
            entries.push(BatchEntry {
                index,
                message,
                envelope,
            });
        }

        let mut results = vec![MessageResult::Nack; entries.len()];
        let (message_groups, grouped_indices) = Self::build_message_groups(&entries);
        let entries_ref = &entries;
        let group_results = stream::iter(message_groups.into_values())
            .map(|group| {
                let entries = entries_ref;
                async move {
                    let entry_indices = group.entry_indices;
                    let group_entries = entry_indices
                        .iter()
                        .map(|index| &entries[*index])
                        .collect::<Vec<_>>();
                    let first = group_entries[0];
                    let result = self
                        .gateway_push
                        .push_message(
                            &first.message.context.ctx,
                            &first.envelope.user_id,
                            PushStrategy::AllDevices,
                            group.request,
                        )
                        .await;
                    let route_result = self
                        .apply_group_route_result(&group_entries, result)
                        .await?;
                    Ok::<_, ConsumerError>((entry_indices, route_result))
                }
            })
            .buffer_unordered(MESSAGE_GROUP_FANOUT_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;

        for group_result in group_results {
            let (entry_indices, route_result) = group_result?;
            for index in entry_indices {
                results[index] = route_result.clone();
            }
        }

        let ack_positions = Self::ack_entry_positions(&entries, &grouped_indices);
        let entries_ref = &entries;
        let ack_results = stream::iter(ack_positions)
            .map(|position| async move {
                let entry = &entries_ref[position];
                let result = self
                    .route_by_payload_kind(
                        &entry.message.context.ctx,
                        &entry.envelope,
                        &entry.envelope.user_id,
                        PushStrategy::AllDevices,
                    )
                    .await;
                let route_result = self
                    .apply_route_result(&entry.message, &entry.envelope, result)
                    .await?;
                Ok::<_, ConsumerError>((entry.index, route_result))
            })
            .buffer_unordered(ACK_FANOUT_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;

        let mut ack_indices = HashSet::with_capacity(ack_results.len());
        for ack_result in ack_results {
            let (index, route_result) = ack_result?;
            ack_indices.insert(index);
            results[index] = route_result;
        }

        for entry in &entries {
            if grouped_indices.contains(&entry.index) || ack_indices.contains(&entry.index) {
                continue;
            }
            let result = self
                .route_by_payload_kind(
                    &entry.message.context.ctx,
                    &entry.envelope,
                    &entry.envelope.user_id,
                    PushStrategy::AllDevices,
                )
                .await;
            results[entry.index] = self
                .apply_route_result(&entry.message, &entry.envelope, result)
                .await?;
        }

        Ok(results)
    }

    fn supports_batch(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "push-online-handler"
    }
}

/// 在线推送消费者工厂
pub struct OnlinePushConsumerFactory;

impl OnlinePushConsumerFactory {
    pub fn create_handler(
        gateway_push: Arc<GatewayPushExecutor>,
        dlq: Arc<DlqPublisher>,
    ) -> Arc<dyn MessageHandler> {
        Arc::new(OnlinePushHandler::new(gateway_push, dlq))
    }

    pub fn topic() -> &'static str {
        flare_im_contracts::constants::topics::TOPIC_PUSH_ONLINE
    }

    pub fn consumer_group() -> &'static str {
        flare_im_contracts::constants::groups::PUSH_WORKER_GROUP_DEFAULT
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_proto::common::Message as ProtoMessage;
    use flare_server_core::Context;
    use flare_server_core::mq::consumer::{ContentType, MessageContext};

    pub(super) fn batch_entry(index: usize, user_id: &str, server_id: &str) -> BatchEntry {
        let request = PushMessageRequest {
            user_ids: vec![user_id.to_string()],
            messages: vec![ProtoMessage {
                server_id: server_id.to_string(),
                conversation_id: "conv-1".to_string(),
                ..Default::default()
            }],
            options: None,
        };
        let envelope = PushTaskEnvelope {
            user_id: user_id.to_string(),
            message_id: server_id.to_string(),
            conversation_id: "conv-1".to_string(),
            tenant_id: "tenant-1".to_string(),
            priority: 5,
            expire_at: None,
            push_payload: request.encode_to_vec(),
            headers: Default::default(),
            payload_kind: PushTaskPayloadKind::Message as i32,
        };
        let ctx = Arc::new(Context::with_request_id(format!("req-{server_id}")));
        let message = Message::new(
            envelope.encode_to_vec(),
            ContentType::Protobuf,
            MessageContext::new(ctx, "push.online".to_string()).with_key(user_id.to_string()),
        );
        BatchEntry {
            index,
            message,
            envelope,
        }
    }

    fn ack_entry(index: usize, user_id: &str, ack_id: &str) -> BatchEntry {
        let request = PushAckRequest {
            user_ids: vec![user_id.to_string()],
            ack: Some(Default::default()),
            options: None,
        };
        let envelope = PushTaskEnvelope {
            user_id: user_id.to_string(),
            message_id: ack_id.to_string(),
            conversation_id: "conv-1".to_string(),
            tenant_id: "tenant-1".to_string(),
            priority: 5,
            expire_at: None,
            push_payload: request.encode_to_vec(),
            headers: Default::default(),
            payload_kind: PushTaskPayloadKind::Ack as i32,
        };
        let ctx = Arc::new(Context::with_request_id(format!("req-{ack_id}")));
        let message = Message::new(
            envelope.encode_to_vec(),
            ContentType::Protobuf,
            MessageContext::new(ctx, "push.online".to_string()).with_key(user_id.to_string()),
        );
        BatchEntry {
            index,
            message,
            envelope,
        }
    }

    #[test]
    fn build_message_groups_merges_non_contiguous_same_user_messages_in_order() {
        let entries = vec![
            batch_entry(0, "bob", "m1"),
            batch_entry(1, "alice", "m2"),
            batch_entry(2, "bob", "m3"),
        ];

        let (groups, grouped_indices) = OnlinePushHandler::build_message_groups(&entries);
        let bob = groups
            .values()
            .find(|group| group.request.user_ids == vec!["bob".to_string()])
            .expect("bob group should exist");

        assert_eq!(grouped_indices.len(), 3);
        let server_ids = bob
            .request
            .messages
            .iter()
            .map(|message| message.server_id.clone())
            .collect::<Vec<_>>();
        assert_eq!(server_ids, vec!["m1".to_string(), "m3".to_string()]);
    }

    #[test]
    fn build_message_groups_keeps_different_users_separate() {
        let entries = vec![batch_entry(0, "bob", "m1"), batch_entry(1, "alice", "m2")];

        let (groups, grouped_indices) = OnlinePushHandler::build_message_groups(&entries);

        assert_eq!(grouped_indices.len(), 2);
        assert_eq!(groups.len(), 2);
        let mut users = groups
            .values()
            .map(|group| group.request.user_ids[0].clone())
            .collect::<Vec<_>>();
        users.sort();
        assert_eq!(users, vec!["alice".to_string(), "bob".to_string()]);
        for group in groups.values() {
            assert_eq!(group.request.messages.len(), 1);
        }
    }

    #[test]
    fn build_message_groups_never_merges_same_user_across_tenants() {
        let tenant_a = batch_entry(0, "same-user", "tenant-a-message");
        let mut tenant_b = batch_entry(1, "same-user", "tenant-b-message");
        tenant_b.envelope.tenant_id = "tenant-2".to_string();
        let (groups, grouped_indices) =
            OnlinePushHandler::build_message_groups(&[tenant_a, tenant_b]);

        assert_eq!(grouped_indices.len(), 2);
        assert_eq!(groups.len(), 2);
        let mut grouped_messages = groups
            .into_iter()
            .map(|(key, group)| (key.tenant_id, group.request.messages[0].server_id.clone()))
            .collect::<Vec<_>>();
        grouped_messages.sort();
        assert_eq!(
            grouped_messages,
            vec![
                ("tenant-1".to_string(), "tenant-a-message".to_string()),
                ("tenant-2".to_string(), "tenant-b-message".to_string()),
            ]
        );
    }

    #[test]
    fn build_message_groups_does_not_group_missing_tenant() {
        let mut missing = batch_entry(0, "same-user", "message");
        missing.envelope.tenant_id.clear();
        let (groups, grouped_indices) = OnlinePushHandler::build_message_groups(&[missing]);
        assert!(groups.is_empty());
        assert!(grouped_indices.is_empty());
    }

    #[test]
    fn build_message_groups_ignores_non_message_entries() {
        let mut entry = batch_entry(0, "bob", "m1");
        entry.envelope.payload_kind = PushTaskPayloadKind::Event as i32;
        let entries = vec![entry];

        let (groups, grouped_indices) = OnlinePushHandler::build_message_groups(&entries);

        assert!(groups.is_empty());
        assert!(grouped_indices.is_empty());
    }

    #[test]
    fn build_message_groups_preserves_message_order_per_user() {
        let entries = vec![
            batch_entry(0, "bob", "m1"),
            batch_entry(1, "bob", "m2"),
            batch_entry(2, "bob", "m3"),
        ];

        let (groups, _) = OnlinePushHandler::build_message_groups(&entries);
        let group = groups.values().next().expect("group should exist");
        let server_ids = group
            .request
            .messages
            .iter()
            .map(|message| message.server_id.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            server_ids,
            vec!["m1".to_string(), "m2".to_string(), "m3".to_string()]
        );
    }

    /// 记录 DLQ 发布次数的假 producer。
    #[derive(Default)]
    pub(super) struct CountingProducer {
        pub(super) sends: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl flare_server_core::mq::producer::Producer for CountingProducer {
        async fn send(
            &self,
            _ctx: &flare_im_contracts::Ctx,
            _topic: &str,
            _key: Option<&str>,
            _payload: Vec<u8>,
            _headers: Option<HashMap<String, String>>,
        ) -> std::result::Result<(), flare_server_core::mq::producer::ProducerError> {
            self.sends.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }

        async fn send_batch(
            &self,
            _ctx: &flare_im_contracts::Ctx,
            _messages: Vec<flare_server_core::mq::producer::ProducerMessage>,
        ) -> std::result::Result<(), flare_server_core::mq::producer::ProducerError> {
            Ok(())
        }

        fn name(&self) -> &str {
            "counting-producer"
        }
    }

    /// 停机、重启窗口里 Access Gateway 连不上（W6-S2 F4）：push-worker 必须 Nack 交给 broker
    /// 重投，不能把推送送进 DLQ；网关明确拒绝的仍进 DLQ。
    #[tokio::test]
    async fn gateway_connection_failure_is_nacked_not_dead_lettered() {
        use flare_im_service_kit::gateway::{
            GatewayRouter, GatewayRouterConfig, GatewayRouterTrait,
        };

        let closed = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            format!("http://{}", listener.local_addr().unwrap())
        };
        let router = GatewayRouter::new(GatewayRouterConfig {
            connection_timeout_ms: 1_000,
            static_fallback_endpoint: Some(closed.clone()),
            ..Default::default()
        });
        let gateway_error = router
            .route_push_notification("gw-1", Default::default())
            .await
            .expect_err("nothing listens on the gateway port");

        let online = crate::infrastructure::rpc::OnlineServiceClient::from_channel(
            tonic::transport::Endpoint::from_shared(closed)
                .unwrap()
                .connect_lazy(),
        );
        let producer = Arc::new(CountingProducer::default());
        let handler = OnlinePushHandler::new(
            Arc::new(GatewayPushExecutor::new(Arc::new(online), router)),
            Arc::new(DlqPublisher::from_producer(
                producer.clone(),
                "flare.im.dlq.push",
            )),
        );
        let entry = batch_entry(0, "bob", "m1");

        let outcome = handler
            .apply_route_result(&entry.message, &entry.envelope, Err(gateway_error))
            .await
            .unwrap();
        assert!(matches!(outcome, MessageResult::Nack), "{outcome:?}");
        assert_eq!(producer.sends.load(std::sync::atomic::Ordering::SeqCst), 0);

        let rejected = flare_err!(ErrorCode::InvalidParameter, "rejected by the gateway");
        let outcome = handler
            .apply_route_result(&entry.message, &entry.envelope, Err(rejected))
            .await
            .unwrap();
        assert!(matches!(outcome, MessageResult::Ack), "{outcome:?}");
        assert_eq!(producer.sends.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for CapturedLogs {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buffer);
            Ok(buffer.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl CapturedLogs {
        fn text(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
        }
    }

    /// 被 NAK 过的推送重投后送达时记一行 INFO（W7-S2：以前只能证明它没进 DLQ）；首投送达不记。
    /// 单条与按会话合并的批量两条路径都覆盖。
    #[tokio::test]
    async fn a_redelivered_push_that_succeeds_is_logged() {
        let captured = CapturedLogs::default();
        let writer = captured.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let unused = tonic::transport::Endpoint::from_static("http://127.0.0.1:9").connect_lazy();
        let handler = OnlinePushHandler::new(
            Arc::new(GatewayPushExecutor::new(
                Arc::new(crate::infrastructure::rpc::OnlineServiceClient::from_channel(unused)),
                flare_im_service_kit::gateway::GatewayRouter::new(Default::default()),
            )),
            Arc::new(DlqPublisher::from_producer(
                Arc::new(CountingProducer::default()),
                "flare.im.dlq.push",
            )),
        );

        let first = batch_entry(0, "bob", "m-first");
        let outcome = handler
            .apply_route_result(&first.message, &first.envelope, Ok(()))
            .await
            .unwrap();
        assert!(matches!(outcome, MessageResult::Ack), "{outcome:?}");
        assert!(
            !captured.text().contains("after broker redelivery"),
            "{}",
            captured.text()
        );

        let mut redelivered = batch_entry(1, "bob", "m-again");
        redelivered.message.context.retry_count = 4;
        let outcome = handler
            .apply_route_result(&redelivered.message, &redelivered.envelope, Ok(()))
            .await
            .unwrap();
        assert!(matches!(outcome, MessageResult::Ack), "{outcome:?}");
        let logs = captured.text();
        assert!(
            logs.contains("Push delivered after broker redelivery"),
            "{logs}"
        );
        assert!(logs.contains("retry_count=4"), "{logs}");
        assert!(logs.contains("message_id=m-again"), "{logs}");

        let mut grouped = batch_entry(2, "alice", "m-group");
        grouped.message.context.retry_count = 2;
        let outcome = handler
            .apply_group_route_result(&[&first, &grouped], Ok(()))
            .await
            .unwrap();
        assert!(matches!(outcome, MessageResult::Ack), "{outcome:?}");
        let logs = captured.text();
        assert!(logs.contains("message_id=m-group"), "{logs}");
        assert!(!logs.contains("message_id=m-first"), "{logs}");
    }

    #[test]
    fn ack_entry_positions_finds_ungrouped_ack_entries() {
        let entries = vec![
            batch_entry(10, "bob", "m1"),
            ack_entry(11, "bob", "ack-1"),
            ack_entry(12, "alice", "ack-2"),
        ];
        let mut grouped_indices = HashSet::new();
        grouped_indices.insert(10);

        let positions = OnlinePushHandler::ack_entry_positions(&entries, &grouped_indices);

        assert_eq!(positions, vec![1, 2]);
    }
}

/// W6-S2 F4 复审：Access Gateway 停机或重启期间，推送经生产的消费运行时、推送消费者配置、
/// 记录认证与在线推送 handler 走完整条重投路径。broker 用虚拟时钟模拟 JetStream：NAK 带
/// 延迟时到期后再投，不带延迟时立即重投；`retry_count` 与真实 fetcher 一样是投递次数减一；
/// 一条消息未确认前不会再投。
#[cfg(test)]
mod redelivery_tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use flare_grpc_proto::access_gateway::access_gateway_server::{
        AccessGateway, AccessGatewayServer,
    };
    use flare_grpc_proto::access_gateway::{
        DeliverToConversationRequest, GetUserConnectionsRequest, GetUserConnectionsResponse,
        PushAckResponse, PushNotificationResponse, PushResponse, RelayRealtimeControlRequest,
    };
    use flare_im_service_kit::gateway::{GatewayRouter, GatewayRouterConfig};
    use flare_im_service_kit::push_delivery::{
        AttestedRecordHandler, BrokerRecordIdentity, PushDeliveryMode,
    };
    use flare_server_core::mq::consumer::{
        ConsumerConfig, ConsumerRuntime, DeadLetterPublisher, Dispatcher, FailureContext,
        MessageAck, MessageFetcher, TopicDispatcher,
    };
    use tokio::sync::oneshot;
    use tonic::{Request, Response, Status};

    use super::tests::{CountingProducer, batch_entry};
    use super::*;
    use crate::service::wire::{
        PUSH_DELIVERY_MAX_RETRIES, push_consumer_config, push_delivery_retry_backoff,
    };

    const TENANT: &str = "tenant-1";

    #[derive(Default)]
    struct BrokerState {
        /// 虚拟时钟：取到一条到期的投递时推进到它的到期时刻。
        now: Duration,
        /// 待投递：(到期时刻, retry_count)。
        pending: Vec<(Duration, u32)>,
        in_flight: bool,
        deliveries: Vec<(Duration, u32)>,
        naks: Vec<Option<Duration>>,
        acked_at: Option<Duration>,
        termed_at: Option<Duration>,
        dead_letters: Vec<(Duration, u32)>,
    }

    #[derive(Default)]
    struct SimBroker {
        state: Mutex<BrokerState>,
    }

    impl SimBroker {
        fn state(&self) -> std::sync::MutexGuard<'_, BrokerState> {
            self.state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }

        fn settled(&self) -> bool {
            let state = self.state();
            state.acked_at.is_some() || state.termed_at.is_some()
        }

        fn redeliver(&self, delay: Option<Duration>, retry_count: u32) {
            let mut state = self.state();
            let due = state.now + delay.unwrap_or_default();
            state.pending.push((due, retry_count + 1));
            state.naks.push(delay);
            state.in_flight = false;
        }
    }

    struct SimAck {
        broker: Arc<SimBroker>,
        retry_count: u32,
    }

    #[async_trait::async_trait]
    impl MessageAck for SimAck {
        async fn ack(&self) -> std::result::Result<(), ConsumerError> {
            let mut state = self.broker.state();
            state.acked_at = Some(state.now);
            state.in_flight = false;
            Ok(())
        }

        async fn nack(&self) -> std::result::Result<(), ConsumerError> {
            self.broker.redeliver(None, self.retry_count);
            Ok(())
        }

        async fn nack_with_delay(&self, delay: Duration) -> std::result::Result<(), ConsumerError> {
            self.broker.redeliver(Some(delay), self.retry_count);
            Ok(())
        }

        async fn term(&self) -> std::result::Result<(), ConsumerError> {
            let mut state = self.broker.state();
            state.termed_at = Some(state.now);
            state.in_flight = false;
            Ok(())
        }
    }

    /// 运行时的死信发布器（生产里投到 `flare.im.dlq.push-delivery`），记下虚拟时刻。
    struct SimDeadLetters {
        broker: Arc<SimBroker>,
    }

    #[async_trait::async_trait]
    impl DeadLetterPublisher for SimDeadLetters {
        async fn publish_dead_letter(
            &self,
            _message: &Message,
            failure: FailureContext,
        ) -> std::result::Result<(), ConsumerError> {
            let mut state = self.broker.state();
            let now = state.now;
            state.dead_letters.push((now, failure.retry_count));
            Ok(())
        }
    }

    /// 到虚拟时刻 `at` 时在 `addr` 上把网关拉起来（模拟 core 重启后网关恢复监听）。
    struct GatewayRestart {
        at: Duration,
        addr: std::net::SocketAddr,
        gateway: FakeGateway,
        stop: Option<oneshot::Sender<()>>,
    }

    struct SimFetcher {
        broker: Arc<SimBroker>,
        template: Message,
        restart: Option<GatewayRestart>,
    }

    #[async_trait::async_trait]
    impl MessageFetcher for SimFetcher {
        async fn fetch(&mut self) -> std::result::Result<Option<Message>, ConsumerError> {
            let retry_count = {
                let mut state = self.broker.state();
                if state.in_flight || state.pending.is_empty() {
                    return Ok(None);
                }
                state.pending.sort();
                let (due, retry_count) = state.pending.remove(0);
                state.now = state.now.max(due);
                let now = state.now;
                state.deliveries.push((now, retry_count));
                state.in_flight = true;
                retry_count
            };
            let now = self.broker.state().now;
            if let Some(restart) = self.restart.as_mut()
                && restart.stop.is_none()
                && now >= restart.at
            {
                restart.stop = Some(start_gateway(restart.addr, restart.gateway.clone()).await);
            }
            let mut message = self.template.clone();
            message.context.retry_count = retry_count;
            let ack: Arc<dyn MessageAck> = Arc::new(SimAck {
                broker: self.broker.clone(),
                retry_count,
            });
            Ok(Some(message.with_ack_handle(ack)))
        }
    }

    #[derive(Clone, Default)]
    struct FakeGateway {
        deliveries: Arc<AtomicUsize>,
    }

    #[tonic::async_trait]
    impl AccessGateway for FakeGateway {
        async fn push_message(
            &self,
            _request: Request<PushMessageRequest>,
        ) -> std::result::Result<Response<PushResponse>, Status> {
            Err(Status::unimplemented("not used"))
        }
        async fn push_event(
            &self,
            _request: Request<PushEventRequest>,
        ) -> std::result::Result<Response<PushResponse>, Status> {
            Err(Status::unimplemented("not used"))
        }
        async fn push_notification(
            &self,
            _request: Request<PushNotificationRequest>,
        ) -> std::result::Result<Response<PushNotificationResponse>, Status> {
            Err(Status::unimplemented("not used"))
        }
        async fn push_ack(
            &self,
            _request: Request<PushAckRequest>,
        ) -> std::result::Result<Response<PushAckResponse>, Status> {
            Err(Status::unimplemented("not used"))
        }
        async fn push_custom(
            &self,
            _request: Request<PushCustomRequest>,
        ) -> std::result::Result<Response<PushResponse>, Status> {
            Err(Status::unimplemented("not used"))
        }
        async fn push_capability(
            &self,
            _request: Request<PushCapabilityRequest>,
        ) -> std::result::Result<Response<PushResponse>, Status> {
            Err(Status::unimplemented("not used"))
        }
        async fn deliver_to_conversation(
            &self,
            _request: Request<DeliverToConversationRequest>,
        ) -> std::result::Result<Response<PushResponse>, Status> {
            self.deliveries.fetch_add(1, Ordering::SeqCst);
            Ok(Response::new(PushResponse::default()))
        }
        async fn get_user_connections(
            &self,
            _request: Request<GetUserConnectionsRequest>,
        ) -> std::result::Result<Response<GetUserConnectionsResponse>, Status> {
            Err(Status::unimplemented("not used"))
        }
        async fn relay_realtime_control(
            &self,
            _request: Request<RelayRealtimeControlRequest>,
        ) -> std::result::Result<Response<PushResponse>, Status> {
            Err(Status::unimplemented("not used"))
        }
    }

    async fn start_gateway(
        addr: std::net::SocketAddr,
        gateway: FakeGateway,
    ) -> oneshot::Sender<()> {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .expect("re-bind the gateway port");
        let (stop, stop_rx) = oneshot::channel::<()>();
        tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(AccessGatewayServer::new(gateway))
                .serve_with_incoming_shutdown(
                    tonic::transport::server::TcpIncoming::from(listener),
                    async {
                        let _ = stop_rx.await;
                    },
                )
                .await;
        });
        stop
    }

    fn key_pair() -> (String, String) {
        use base64::Engine as _;
        use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
        // 固定种子的测试密钥，只用于本进程内的签名与校验。
        let key = ed25519_dalek::SigningKey::from_bytes(&[7u8; 32]);
        let private = key
            .to_pkcs8_pem(Default::default())
            .expect("private pem")
            .to_string();
        let public = key
            .verifying_key()
            .to_public_key_pem(Default::default())
            .expect("public pem");
        let b64 = base64::engine::general_purpose::STANDARD;
        (b64.encode(private), b64.encode(public))
    }

    struct Outcome {
        deliveries: Vec<(Duration, u32)>,
        naks: Vec<Option<Duration>>,
        acked_at: Option<Duration>,
        dead_letters: Vec<(Duration, u32)>,
        handler_dlq: usize,
        gateway_deliveries: usize,
    }

    /// 网关地址上没有监听者（停机中）；`restart_at` 为 `Some` 时到该虚拟时刻重新拉起网关。
    async fn run_push_through_gateway_outage(
        consumer: ConsumerConfig,
        restart_at: Option<Duration>,
    ) -> Outcome {
        let addr = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap()
        };
        let router = GatewayRouter::new(GatewayRouterConfig {
            connection_timeout_ms: 1_000,
            static_fallback_endpoint: Some(format!("http://{addr}")),
            ..Default::default()
        });
        let online = crate::infrastructure::rpc::OnlineServiceClient::from_channel(
            tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
                .unwrap()
                .connect_lazy(),
        );
        let handler_dlq = Arc::new(CountingProducer::default());
        let online_handler = OnlinePushConsumerFactory::create_handler(
            Arc::new(GatewayPushExecutor::new(Arc::new(online), router)),
            Arc::new(DlqPublisher::from_producer(
                handler_dlq.clone(),
                "flare.im.dlq.push",
            )),
        );
        let (private, public) = key_pair();
        let identity = BrokerRecordIdentity::new(
            PushDeliveryMode::BrokerIsolated,
            "flare-push-worker",
            Some(&private),
            Some(&public),
        )
        .expect("record identity");
        let topic = OnlinePushConsumerFactory::topic().to_string();
        let mut dispatcher = TopicDispatcher::new();
        Dispatcher::register(
            &mut dispatcher,
            topic.clone(),
            Arc::new(AttestedRecordHandler::new(identity, online_handler)),
        )
        .unwrap();

        let mut template = batch_entry(0, "bob", "m1").message;
        template.context.topic = topic;
        template.context.ctx = Arc::new(template.context.ctx.with_tenant_id(TENANT));

        let broker = Arc::new(SimBroker::default());
        broker.state().pending.push((Duration::ZERO, 0));
        let gateway = FakeGateway::default();
        let mut fetcher = SimFetcher {
            broker: broker.clone(),
            template,
            restart: restart_at.map(|at| GatewayRestart {
                at,
                addr,
                gateway: gateway.clone(),
                stop: None,
            }),
        };
        let runtime = ConsumerRuntime::new(consumer, Arc::new(dispatcher))
            .with_dead_letter_publisher(Arc::new(SimDeadLetters {
                broker: broker.clone(),
            }));
        let (shutdown, shutdown_rx) = oneshot::channel();
        let consumer_task = tokio::spawn(async move {
            let result = runtime.run_with_shutdown(&mut fetcher, shutdown_rx).await;
            if let Some(stop) = fetcher.restart.and_then(|restart| restart.stop) {
                let _ = stop.send(());
            }
            result
        });
        let started = std::time::Instant::now();
        while !broker.settled() && started.elapsed() < Duration::from_secs(20) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let _ = shutdown.send(());
        consumer_task
            .await
            .expect("consumer task")
            .expect("consumer runtime");

        let state = broker.state();
        Outcome {
            deliveries: state.deliveries.clone(),
            naks: state.naks.clone(),
            acked_at: state.acked_at,
            dead_letters: state.dead_letters.clone(),
            handler_dlq: handler_dlq.sends.load(Ordering::SeqCst),
            gateway_deliveries: gateway.deliveries.load(Ordering::SeqCst),
        }
    }

    /// 网关一直不回来：重投按退避摊开，DLQ 只在全部重试用完、跨过整个窗口之后出现。
    #[tokio::test]
    async fn a_push_during_a_long_gateway_outage_backs_off_before_dead_lettering() {
        let backoff = push_delivery_retry_backoff();
        let window = backoff.total_for(PUSH_DELIVERY_MAX_RETRIES);
        let outcome = run_push_through_gateway_outage(push_consumer_config(64, 50), None).await;
        println!(
            "deliveries {:?}\nnaks {:?}\ndead letters {:?}",
            outcome.deliveries, outcome.naks, outcome.dead_letters
        );

        let attempts = PUSH_DELIVERY_MAX_RETRIES as usize + 1;
        assert_eq!(outcome.deliveries.len(), attempts);
        assert_eq!(outcome.naks.len(), attempts - 1);
        for (retry_count, delay) in outcome.naks.iter().enumerate() {
            let expected = backoff.delay_for(retry_count as u32);
            assert_eq!(*delay, Some(expected), "retry {retry_count}");
            assert!(expected > Duration::ZERO);
        }
        for pair in outcome.deliveries.windows(2) {
            let ((earlier, retry), (later, next_retry)) = (pair[0], pair[1]);
            assert_eq!(next_retry, retry + 1);
            assert_eq!(later - earlier, backoff.delay_for(retry));
        }
        assert_eq!(
            outcome.dead_letters,
            vec![(window, PUSH_DELIVERY_MAX_RETRIES)],
            "exactly one dead letter, only after the whole retry window"
        );
        assert!(window >= Duration::from_secs(120), "{window:?}");
        assert_eq!(
            outcome.handler_dlq, 0,
            "a transient failure never takes the handler DLQ"
        );
        assert_eq!(outcome.acked_at, None);
    }

    /// 网关在窗口内恢复（core 重启约 16 s）：推送在恢复后的下一次重投送达，不进任何 DLQ。
    #[tokio::test]
    async fn a_push_during_a_gateway_restart_is_delivered_after_the_restart() {
        let backoff = push_delivery_retry_backoff();
        let restart_at = Duration::from_secs(16);
        let outcome =
            run_push_through_gateway_outage(push_consumer_config(64, 50), Some(restart_at)).await;
        println!(
            "deliveries {:?}\nnaks {:?}\nacked at {:?}",
            outcome.deliveries, outcome.naks, outcome.acked_at
        );

        let acked_at = outcome
            .acked_at
            .expect("delivered once the gateway is back");
        assert!(acked_at >= restart_at);
        // 恢复后最迟一个退避上限内送达。
        assert!(
            acked_at - restart_at <= backoff.delay_for(u32::MAX),
            "{acked_at:?}"
        );
        assert!(
            outcome.dead_letters.is_empty(),
            "{:?}",
            outcome.dead_letters
        );
        assert_eq!(outcome.handler_dlq, 0);
        assert_eq!(outcome.gateway_deliveries, 1);
        assert!(
            outcome
                .naks
                .iter()
                .all(|delay| delay.is_some_and(|d| d > Duration::ZERO))
        );
    }
}
