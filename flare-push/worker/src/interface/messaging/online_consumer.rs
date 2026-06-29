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
    PushAckRequest, PushCustomRequest, PushEventRequest, PushMessageRequest,
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
            let key = MessageBatchKey { user_ids, options };

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
            PushTaskPayloadKind::Unspecified => Err(flare_err!(
                ErrorCode::InvalidParameter,
                "PushTaskPayloadKind unspecified"
            )),
        }
    }

    async fn apply_route_result(
        &self,
        ctx: &flare_server_core::context::Ctx,
        envelope: &PushTaskEnvelope,
        payload: &[u8],
        result: Result<(), FlareError>,
    ) -> std::result::Result<MessageResult, ConsumerError> {
        match result {
            Ok(()) => Ok(MessageResult::Ack),
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
            Ok(()) => Ok(MessageResult::Ack),
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

        self.apply_route_result(ctx, &envelope, &message.payload, result)
            .await
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
                    .apply_route_result(
                        &entry.message.context.ctx,
                        &entry.envelope,
                        &entry.message.payload,
                        result,
                    )
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
                .apply_route_result(
                    &entry.message.context.ctx,
                    &entry.envelope,
                    &entry.message.payload,
                    result,
                )
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

    fn batch_entry(index: usize, user_id: &str, server_id: &str) -> BatchEntry {
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
