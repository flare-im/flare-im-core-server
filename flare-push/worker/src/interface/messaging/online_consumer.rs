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

use std::sync::Arc;

use flare_grpc_proto::access_gateway::{
    PushAckRequest, PushCustomRequest, PushEventRequest, PushMessageRequest,
    PushNotificationRequest, PushOptions,
};
use flare_grpc_proto::signaling::router::PushStrategy;
use flare_proto::common::{PushTaskEnvelope, PushTaskPayloadKind};
use flare_server_core::mq::consumer::{ConsumerError, Message, MessageHandler, MessageResult};
use flare_server_core::{ErrorCode, FlareError, flare_err};
use prost::Message as _;
use tracing::instrument;

use crate::application::GatewayPushExecutor;
use crate::infrastructure::mq::dlq_publisher::DlqPublisher;

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

    fn build_message_group(
        entries: &[BatchEntry],
        start: usize,
    ) -> Option<(usize, PushMessageRequest)> {
        let first = &entries[start].envelope;
        if Self::payload_kind(first) != PushTaskPayloadKind::Message {
            return None;
        }

        let mut request = match Self::decode_push_message_request(first) {
            Ok(request) => request,
            Err(_) => return None,
        };
        let user_id = first.user_id.clone();
        let options: Option<PushOptions> = request.options.clone();
        request.user_ids = vec![user_id.clone()];

        let mut end = start + 1;
        while end < entries.len() {
            let envelope = &entries[end].envelope;
            if envelope.user_id != user_id
                || Self::payload_kind(envelope) != PushTaskPayloadKind::Message
            {
                break;
            }

            let next = match Self::decode_push_message_request(envelope) {
                Ok(next) => next,
                Err(_) => break,
            };
            if next.options != options {
                break;
            }
            request.messages.extend(next.messages);
            end += 1;
        }

        if end == start + 1 {
            return None;
        }

        Some((end, request))
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
        entries: &[BatchEntry],
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
        let mut offset = 0usize;
        while offset < entries.len() {
            if let Some((end, request)) = Self::build_message_group(&entries, offset) {
                let group = &entries[offset..end];
                let first = &group[0];
                let result = self
                    .gateway_push
                    .push_message(
                        &first.message.context.ctx,
                        &first.envelope.user_id,
                        PushStrategy::AllDevices,
                        request,
                    )
                    .await;
                let route_result = self.apply_group_route_result(group, result).await?;
                for entry in group {
                    results[entry.index] = route_result.clone();
                }
                offset = end;
                continue;
            }

            let entry = &entries[offset];
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
            offset += 1;
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

    #[test]
    fn build_message_group_merges_contiguous_same_user_messages_in_order() {
        let entries = vec![
            batch_entry(0, "bob", "m1"),
            batch_entry(1, "bob", "m2"),
            batch_entry(2, "alice", "m3"),
        ];

        let (end, request) = OnlinePushHandler::build_message_group(&entries, 0)
            .expect("first two bob messages should merge");

        assert_eq!(end, 2);
        assert_eq!(request.user_ids, vec!["bob".to_string()]);
        let server_ids = request
            .messages
            .into_iter()
            .map(|message| message.server_id)
            .collect::<Vec<_>>();
        assert_eq!(server_ids, vec!["m1".to_string(), "m2".to_string()]);
    }

    #[test]
    fn build_message_group_does_not_cross_user_boundary() {
        let entries = vec![batch_entry(0, "bob", "m1"), batch_entry(1, "alice", "m2")];

        let group = OnlinePushHandler::build_message_group(&entries, 0);

        assert!(group.is_none());
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
