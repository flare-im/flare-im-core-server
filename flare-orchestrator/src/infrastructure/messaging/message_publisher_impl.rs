//! 编排发布器外壳：实现领域 [MessageEventPublisher] 并暴露 `publish_conversation_ensure`。

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use flare_proto::common::Event;
use flare_proto::push::PushMessageRequest;
use flare_server_core::context::Ctx;

use crate::domain::repository::MessageEventPublisher;
use crate::error::Result;

use super::mq_publisher::MqMessagePublisher;

/// 对外统一类型（原 `MessageEventPublisherItem`）
pub struct OrchestratorPublisher(pub Arc<MqMessagePublisher>);

impl std::fmt::Debug for OrchestratorPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("OrchestratorPublisher").finish()
    }
}

impl MessageEventPublisher for OrchestratorPublisher {
    fn publish_storage<'a>(
        &'a self,
        ctx: &'a Ctx,
        payload: flare_im_core::abstractions::storage_payload::StorageMessagePayload,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        self.0.publish_storage(ctx, payload)
    }

    fn publish_event<'a>(
        &'a self,
        ctx: &'a Ctx,
        event: Event,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        self.0.publish_event(ctx, event)
    }

    fn publish_push<'a>(
        &'a self,
        ctx: &'a Ctx,
        payload: PushMessageRequest,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        self.0.publish_push(ctx, payload)
    }

    fn publish_both<'a>(
        &'a self,
        ctx: &'a Ctx,
        storage_payload: flare_im_core::abstractions::storage_payload::StorageMessagePayload,
        push_payload: PushMessageRequest,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        self.0.publish_both(ctx, storage_payload, push_payload)
    }
}

impl OrchestratorPublisher {
    pub async fn publish_conversation_ensure(
        &self,
        conversation_id: &str,
        tenant_id: &str,
        conversation_type: &str,
        business_type: &str,
        participants: Vec<String>,
    ) -> Result<()> {
        self.0
            .publish_conversation_ensure(
                conversation_id,
                tenant_id,
                conversation_type,
                business_type,
                participants,
            )
            .await
    }
}
