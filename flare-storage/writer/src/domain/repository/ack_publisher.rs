//! 回执发布仓储（Port）
//!
//! **功能**：将写入结果以 ack 形式发布到下游（网关/客户端），通知消息是否落库、是否去重等。
//! - `publish(AckEvent)`：发送包含 message_id、conversation_id、status（Persisted/Duplicate）、
//!   ingestion_ts、persisted_ts、deduplicated 等字段的回执。
//!
//! 典型实现：MQ（如 MqAckPublisher），消费端为网关或推送服务。

use flare_im_contracts::Ctx;
use flare_server_core::error::Result;

use crate::domain::events::AckEvent;

pub trait AckPublisher: Send + Sync {
    async fn publish(&self, ctx: &Ctx, event: AckEvent<'_>) -> Result<()>;
}
