//! 领域事件上行端口（Command 侧，非同步控制面）

use async_trait::async_trait;
use flare_im_core::Ctx;
use flare_proto::common::{Event, OperationResponse};
use flare_server_core::error::Result;

#[async_trait]
pub trait IEventCommandPort: Send + Sync {
    async fn send_event(&self, tx: &Ctx, event: Event) -> Result<OperationResponse>;
}
