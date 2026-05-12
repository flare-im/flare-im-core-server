//! DATA 通道上行端口（Command 侧）

use async_trait::async_trait;
use flare_im_core::Ctx;
use flare_im_core::error::Result;
use flare_proto::common::CustomData;

#[async_trait]
pub trait IDataCommandPort: Send + Sync {
    async fn send_data(&self, tx: &Ctx, data: CustomData) -> Result<Option<Vec<u8>>>;
}
