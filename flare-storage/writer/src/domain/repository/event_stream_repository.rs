//! 事件流仓储（Port）- 使用领域 Event，不依赖 proto

use crate::domain::model::Event;
use anyhow::Result;
use flare_im_core::Ctx;

pub trait EventStreamRepository: Send + Sync {
    /// 追加一条领域事件到事件流（EVENT_MESSAGE / EVENT_MESSAGE_RECALL / ...）
    async fn append_event_to_stream(&self, ctx: &Ctx, event: &Event) -> Result<()>;

    /// 判断事件是否已存在（用于撤回/编辑等幂等：同一 (tenant_id, conversation_id, seq) 只应用一次）
    async fn event_exists(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        conversation_id: &str,
        seq: i64,
    ) -> Result<bool>;
}
