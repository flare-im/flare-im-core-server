//! 操作事件派发器实现:发布事件到 Kafka

use std::sync::Arc;

use flare_server_core::context::Ctx;

use crate::domain::event::MessageOperationDomainEvent;
use crate::domain::repository::MessageEventPublisher;
use crate::domain::service::operation_event_dispatcher::OperationEventDispatcher;
use crate::error::{Result, to_system_err_with};

/// 操作事件派发器实现:持有 MessageEventPublisher,派发时写 Kafka Event
pub struct OperationEventDispatcherImpl {
    publisher: Arc<dyn MessageEventPublisher>,
}

impl OperationEventDispatcherImpl {
    pub fn new(publisher: Arc<dyn MessageEventPublisher>) -> Self {
        Self { publisher }
    }
}

impl OperationEventDispatcher for OperationEventDispatcherImpl {
    async fn dispatch(
        &self,
        ctx: &Ctx,
        proto_event: flare_proto::common::Event,
        _domain_event: MessageOperationDomainEvent,
    ) -> Result<()> {
        // 只发布事件到 Kafka,推送由 push/server 从主 MQ 消费
        self.publisher
            .publish_event(ctx, proto_event)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "publish_event failed in dispatch");
                to_system_err_with(e, "Failed to publish operation event to Kafka")
            })?;

        Ok(())
    }

    async fn dispatch_event_only(
        &self,
        ctx: &Ctx,
        proto_event: flare_proto::common::Event,
    ) -> Result<()> {
        self.publisher
            .publish_event(ctx, proto_event)
            .await
            .map_err(|e| to_system_err_with(e, "Failed to publish operation event to Kafka"))
    }
}
