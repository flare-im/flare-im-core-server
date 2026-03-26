//! 将 Push gRPC 语义编排为「入队 Kafka」：委托 `PushProxyMqPublisher`。

use std::sync::Arc;

use anyhow::Result;
use flare_proto::push::{PushCustomRequest, PushMessageRequest, PushNotificationRequest};
use flare_server_core::context::Ctx;
use tracing::instrument;

use crate::infrastructure::PushProxyMqPublisher;

#[derive(Clone)]
pub struct PushProxyCommandHandler {
    publisher: Arc<PushProxyMqPublisher>,
}

impl PushProxyCommandHandler {
    pub fn new(publisher: Arc<PushProxyMqPublisher>) -> Self {
        Self { publisher }
    }

    #[instrument(skip(self, ctx, req), fields(user_count = req.user_ids.len()))]
    pub async fn enqueue_push_message(&self, ctx: &Ctx, req: &PushMessageRequest) -> Result<()> {
        self.publisher.publish_push_message(ctx, req).await
    }

    #[instrument(skip(self, ctx, req), fields(user_count = req.user_ids.len()))]
    pub async fn enqueue_push_notification(
        &self,
        ctx: &Ctx,
        req: &PushNotificationRequest,
    ) -> Result<()> {
        self.publisher.publish_push_notification(ctx, req).await
    }

    #[instrument(skip(self, ctx, req), fields(user_count = req.user_ids.len()))]
    pub async fn enqueue_push_custom(&self, ctx: &Ctx, req: &PushCustomRequest) -> Result<()> {
        self.publisher.publish_push_custom(ctx, req).await
    }
}
