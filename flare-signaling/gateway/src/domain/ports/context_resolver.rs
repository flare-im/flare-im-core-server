//! 上下文构建：长连接 `connection_id` → 认证/租户上下文 `Ctx`

use async_trait::async_trait;
use flare_core::common::error::Result;
use flare_im_core::Ctx;

#[async_trait]
pub trait IContextResolver: Send + Sync {
    async fn resolve(&self, connection_id: &str) -> Result<Ctx>;
}