//! 热数据缓存仓储（Port）- 使用领域 Message，不依赖 proto

use crate::domain::model::Message;
use flare_im_core::Ctx;

pub trait HotCacheRepository: Send + Sync {
    async fn store_hot(&self, ctx: &Ctx, message: &Message) -> anyhow::Result<()>;

    /// 批量存储消息；默认逐条 store_hot。
    async fn store_hot_batch(&self, ctx: &Ctx, messages: &[Message]) -> anyhow::Result<()> {
        for message in messages {
            self.store_hot(ctx, message).await?;
        }
        Ok(())
    }
}
