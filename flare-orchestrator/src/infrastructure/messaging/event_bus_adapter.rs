//! IM 事件总线端口适配器
//!
//! 将 Orchestrator 的 MQ 发布器适配为 [flare_im_core::ImTopicEventPublisher]，
//! 便于统一依赖「事件总线」抽象，并与 flare-proto / flare-server-core 约定对齐。

use std::sync::Arc;

use flare_im_core::abstractions::topics::{EventBusPublishError, ImTopicEventPublisher};
use flare_im_core::constants::topics::TOPIC_MESSAGE_EVENTS;
use flare_server_core::context::Ctx;

use crate::infrastructure::messaging::mq_publisher::MqMessagePublisher;

/// 使用 Orchestrator 的 MQ 发布器实现统一事件流发布
pub struct OrchestratorEventBusAdapter {
    publisher: Arc<MqMessagePublisher>,
}

impl OrchestratorEventBusAdapter {
    pub fn new(publisher: Arc<MqMessagePublisher>) -> Self {
        Self { publisher }
    }
}

impl ImTopicEventPublisher for OrchestratorEventBusAdapter {
    async fn publish_topic_event(
        &self,
        ctx: &Ctx,
        topic: &str,
        envelope: &flare_proto::common::TopicEventEnvelope,
    ) -> Result<(), EventBusPublishError> {
        if topic != TOPIC_MESSAGE_EVENTS {
            return Ok(());
        }
        self.publisher
            .publish_topic_event_envelope_to_events_topic(ctx, envelope)
            .await
            .map_err(|e| EventBusPublishError::Publish(Box::new(e)))?;
        Ok(())
    }
}
