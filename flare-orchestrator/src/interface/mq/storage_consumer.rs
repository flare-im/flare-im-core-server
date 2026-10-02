//! 存储消费者处理器 - 处理 TOPIC_MESSAGE_MAIN 中的消息
//!
//! ## 核心职责
//! 1. 消费 TOPIC_MESSAGE_MAIN 中的 MqEnvelope 消息
//! 2. 消费者组由 [crate::config::MessageOrchestratorConfig] 的 `JetStreamConsumerConfig::consumer_group`（默认 `ORCHESTRATOR_MAIN_GROUP_DEFAULT`）与 [ConsumerConfig::jetstream_consumer_group_override] 决定
//! 3. 反序列化 MqEnvelope 并调用 StorageHandler 处理
//!
//! ## 设计原则
//! - Interface 层：负责 MQ 消息的接收和反序列化
//! - 上下文重建：从 MQ headers 中提取追踪信息
//! - 委托给 Application 层：调用 StorageHandler 处理业务

use std::sync::Arc;

use flare_im_contracts::utils::normalize_optional_tenant_id;
use flare_im_contracts::{Ctx, context_from_mq_metadata};
use flare_proto::common::MqEnvelope;
use flare_server_core::mq::consumer::{ConsumerError, Message, MessageHandler, MessageResult};
use tracing::instrument;

use crate::application::handlers::StorageHandler;

/// 主队列信封的消息头里没有租户：不扇出，首投即进死信（不再按租户 "0" 继续）。
const ENVELOPE_WITHOUT_TENANT: &str = "main queue envelope without a tenant header";

/// 从外层 MQ headers + 内层 MqEnvelope headers 重建上下文。租户只认生产方（ingest）按已验证
/// 上下文写的 `x-tenant-id`；缺失时返回不可重试的 [`ConsumerError::DeadLetter`]。
fn envelope_context(message: &Message, envelope: &MqEnvelope) -> Result<Ctx, ConsumerError> {
    let mut merged_headers = message.context.headers.clone();
    for (key, value) in &envelope.headers {
        merged_headers.insert(key.clone(), value.clone());
    }
    let ctx = context_from_mq_metadata(&merged_headers);
    if normalize_optional_tenant_id(ctx.tenant_id()).is_none() {
        tracing::error!(
            envelope_id = %envelope.envelope_id,
            conversation_id = %envelope.conversation_id,
            reason = ENVELOPE_WITHOUT_TENANT,
            "Rejecting main queue envelope without a tenant header"
        );
        return Err(ConsumerError::DeadLetter(
            ENVELOPE_WITHOUT_TENANT.to_string(),
        ));
    }
    Ok(ctx)
}

/// 存储消费者处理器
///
/// 处理 `TOPIC_MESSAGE_MAIN` 中的消息；JetStream `group.id` 见编排器 `JetStreamConsumerConfig` 与 `wire::initialize` 中的 `ConsumerConfig`。
pub struct StorageConsumerHandler {
    /// 存储处理器（编排层）
    storage_handler: Arc<StorageHandler>,
}

impl StorageConsumerHandler {
    /// 创建新的存储消费者处理器
    pub fn new(storage_handler: Arc<StorageHandler>) -> Self {
        Self { storage_handler }
    }
}

#[async_trait::async_trait]
impl MessageHandler for StorageConsumerHandler {
    /// 处理 MQ 消息
    ///
    /// # 处理流程
    /// 1. 反序列化 MqEnvelope
    /// 2. 从 headers 中重建上下文
    /// 3. 调用 StorageHandler.handle_envelope()
    /// 4. 返回处理结果
    ///
    /// # 参数
    /// - `message`: MQ 消息
    ///
    /// # 返回
    /// - `Ok(MessageResult::Ack)`: 处理成功
    /// - `Err(ConsumerError)`: 处理失败
    #[instrument(skip(self), fields(
        topic = %message.context.topic,
        partition = message.context.partition,
        offset = message.context.offset,
    ))]
    async fn handle(&self, message: Message) -> Result<MessageResult, ConsumerError> {
        // 1. 反序列化 MqEnvelope（直接解码，不需要 content-type 校验）
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
            "Processing MqEnvelope from TOPIC_MESSAGE_MAIN"
        );

        // 2. 从外层 MQ headers + 内层 MqEnvelope headers 中重建上下文；没有租户首投即进死信。
        let ctx = envelope_context(&message, &envelope)?;

        // 3. 调用 StorageHandler 处理
        match self.storage_handler.handle_envelope(&ctx, envelope).await {
            Ok(()) => {
                tracing::trace!(
                    topic = %message.context.topic,
                    partition = message.context.partition,
                    offset = message.context.offset,
                    elapsed_ms = message.context.elapsed_ms(),
                    "Successfully processed MqEnvelope"
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
                // 根据错误类型决定是否重试
                Err(ConsumerError::Handler(format!(
                    "StorageHandler error: {}",
                    e
                )))
            }
        }
    }

    /// 获取处理器名称
    fn name(&self) -> &str {
        "storage-consumer-handler"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_im_contracts::context_to_mq_metadata;
    use flare_server_core::Context;
    use flare_server_core::mq::consumer::{ContentType, MessageContext};

    fn main_queue_message(header_tenant: Option<&str>) -> (Message, MqEnvelope) {
        let mut sender = Context::with_request_id("req-main");
        if let Some(tenant_id) = header_tenant {
            sender = sender.with_tenant_id(tenant_id);
        }
        let headers = context_to_mq_metadata(&std::sync::Arc::new(sender));
        let envelope = MqEnvelope {
            envelope_id: "env-1".to_string(),
            conversation_id: "conv-1".to_string(),
            headers: headers.clone(),
            ..MqEnvelope::default()
        };
        let mut context = MessageContext::new(
            std::sync::Arc::new(Context::with_request_id("transport")),
            "flare.im.message.main".to_string(),
        );
        context.headers = headers;
        (
            Message::new(Vec::new(), ContentType::Protobuf, context),
            envelope,
        )
    }

    /// 主队列信封缺租户时直接进死信，不再以租户 "0" 继续扇出到存储与推送（W11-T）。
    #[test]
    fn envelope_without_tenant_is_dead_lettered() {
        let (message, envelope) = main_queue_message(None);
        let error = envelope_context(&message, &envelope).expect_err("no tenant");
        assert!(matches!(error, ConsumerError::DeadLetter(_)), "{error:?}");
        assert!(!error.is_retryable());

        for tenant in ["tenant-a", "0"] {
            let (message, envelope) = main_queue_message(Some(tenant));
            let ctx = envelope_context(&message, &envelope).expect("tenant header");
            assert_eq!(ctx.tenant_id(), Some(tenant));
        }
    }
}
