//! 操作事件消费者：订阅统一消息事件流 topic（`TOPIC_MESSAGE_EVENTS`），按 `event_type` 过滤操作类事件。
//! 使用 flare-im-core 的 event 模块和 flare-server-core 的 EventEnvelope

use std::sync::Arc;

use async_trait::async_trait;
use flare_im_core::constants::groups::STORAGE_GROUP_DEFAULT;
use flare_im_core::constants::topics::TOPIC_MESSAGE_EVENTS;
use flare_im_core::event::types::types as im_event_types;
use flare_proto::common::Event;
use flare_server_core::context::Ctx;
use flare_server_core::event_bus::{EventEnvelope, EventHandler};
use flare_server_core::{FlareError, Result};
use prost::Message as _;
use tracing::warn;

use crate::application::commands::ProcessEventCommand;
use crate::application::handlers::MessageOperationCommandHandler;
use crate::convert::event_from_proto;

/// 操作事件处理器：专门处理消息操作事件
pub struct OperationEventHandler {
    operation_handler: Arc<MessageOperationCommandHandler>,
}

impl OperationEventHandler {
    /// 创建新的操作事件处理器
    ///
    /// # 参数
    /// - `operation_handler`: 消息操作处理器
    ///
    /// # 返回
    /// - `Self`: 操作事件处理器实例
    pub fn new(operation_handler: Arc<MessageOperationCommandHandler>) -> Self {
        Self { operation_handler }
    }
}

#[async_trait]
impl EventHandler for OperationEventHandler {
    async fn handle(&self, ctx: &Ctx, envelope: EventEnvelope) -> Result<()> {
        // 验证事件类型
        if envelope.event_type != im_event_types::EVENT {
            warn!(
                event_type = %envelope.event_type,
                partition_key = %envelope.partition_key,
                "unexpected event_type in message events topic, skip"
            );
            return Ok(());
        }

        // 从 payload 解析 Event
        let proto_event = Event::decode(&*envelope.payload)
            .map_err(|e| FlareError::deserialization_error(format!("Failed to decode Event: {}", e)))?;

        let event = event_from_proto(&proto_event);

        // 处理事件
        self.operation_handler
            .handle(ctx, ProcessEventCommand { event })
            .await
            .map_err(|e| FlareError::general_error(e.to_string()))?;

        Ok(())
    }

    fn name(&self) -> &str {
        "operation-event-handler"
    }
}

/// 操作事件消费者工厂
///
/// 提供创建操作事件处理器的便捷方法
pub struct OperationEventConsumerFactory;

impl OperationEventConsumerFactory {
    /// 创建操作事件处理器
    ///
    /// # 参数
    /// - `operation_handler`: 消息操作处理器
    ///
    /// # 返回
    /// - `Arc<dyn EventHandler>`: 操作事件处理器实例
    pub fn create_handler(
        operation_handler: Arc<MessageOperationCommandHandler>,
    ) -> Arc<dyn EventHandler> {
        Arc::new(OperationEventHandler::new(operation_handler))
    }

    /// 获取订阅的主题
    ///
    /// # 返回
    /// - `&'static str`: 主题名称
    pub fn topic() -> &'static str {
        TOPIC_MESSAGE_EVENTS
    }

    /// 获取消费者组名称
    ///
    /// # 返回
    /// - `&'static str`: 消费者组名称
    pub fn consumer_group() -> &'static str {
        STORAGE_GROUP_DEFAULT
    }
}
