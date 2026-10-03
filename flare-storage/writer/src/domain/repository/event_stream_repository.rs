//! 事件流仓储（Port）- 使用领域 Event，不依赖 proto

use crate::domain::model::Event;
use flare_im_contracts::Ctx;
use flare_server_core::error::Result;

/// 事件流中与待应用事件同一位置 `(tenant_id, conversation_id, seq)` 上已有的事件与它的关系。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoredEventMatch {
    /// 该位置还没有事件（或该事件不落事件流，例如没有会话 ID）。
    Absent,
    /// 已有的就是同一个事件（重投），按幂等跳过。
    Duplicate,
    /// 该位置上是另一个事件；`existing_event_type` 是已有行的类型号。
    Conflict { existing_event_type: i32 },
}

pub trait EventStreamRepository: Send + Sync {
    /// 追加一条领域事件到事件流（EVENT_MESSAGE / EVENT_MESSAGE_RECALL / ...）
    async fn append_event_to_stream(&self, ctx: &Ctx, event: &Event) -> Result<()>;

    /// 批量追加领域事件到事件流。
    ///
    /// 默认实现保持兼容，具体基础设施实现可覆盖为单 SQL/事务批量写入以降低大批量消息延迟。
    async fn append_events_to_stream(&self, ctx: &Ctx, events: &[Event]) -> Result<()> {
        for event in events {
            self.append_event_to_stream(ctx, event).await?;
        }
        Ok(())
    }

    /// 操作事件应用前的判重：同一 `(tenant_id, conversation_id, seq)` 上已有事件时按内容比较，
    /// 同一个事件的重投为 [`StoredEventMatch::Duplicate`]，另一个事件为 [`StoredEventMatch::Conflict`]。
    /// 比较口径与写入时遇到已有行相同（`append_event_to_stream`）。
    async fn match_stored_event(&self, ctx: &Ctx, event: &Event) -> Result<StoredEventMatch>;
}
