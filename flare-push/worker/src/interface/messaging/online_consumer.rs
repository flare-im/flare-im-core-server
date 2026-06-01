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
    PushNotificationRequest,
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

impl OnlinePushHandler {
    pub fn new(gateway_push: Arc<GatewayPushExecutor>, dlq: Arc<DlqPublisher>) -> Self {
        Self { gateway_push, dlq }
    }

    fn decode_task_envelope(message: &Message) -> Result<PushTaskEnvelope, ConsumerError> {
        PushTaskEnvelope::decode(message.payload.as_slice())
            .map_err(|e| ConsumerError::Deserialization(format!("PushTaskEnvelope: {}", e)))
    }

    async fn route_by_payload_kind(
        &self,
        ctx: &flare_server_core::context::Ctx,
        envelope: &PushTaskEnvelope,
        user_id: &str,
        strategy: PushStrategy,
    ) -> Result<(), FlareError> {
        let kind = PushTaskPayloadKind::try_from(envelope.payload_kind)
            .unwrap_or(PushTaskPayloadKind::Unspecified);

        match kind {
            PushTaskPayloadKind::Message => {
                let req =
                    PushMessageRequest::decode(envelope.push_payload.as_slice()).map_err(|e| {
                        flare_err!(
                            ErrorCode::InvalidParameter,
                            format!("decode PushMessageRequest: {}", e)
                        )
                    })?;
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

        // 4. 处理结果
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
                    .publish(
                        ctx,
                        Some(&envelope.conversation_id),
                        message.payload.clone(),
                    )
                    .await
                {
                    return Err(ConsumerError::DeadLetter(dlq_err.to_string()));
                }
                Ok(MessageResult::Ack)
            }
        }
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
        flare_im_core::constants::topics::TOPIC_PUSH_ONLINE
    }

    pub fn consumer_group() -> &'static str {
        flare_im_core::constants::groups::PUSH_WORKER_GROUP_DEFAULT
    }
}
