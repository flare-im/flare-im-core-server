//! 领域模型定义（与 proto 解耦，不在 application/domain/infrastructure 使用 proto 类型）

mod context;
mod event;
mod message;

pub use context::{RequestContext, TenantContext};
pub use event::{
    CustomPayload, DeletePayload, EditPayload, Event, EventPayload, EventType, MarkPayload,
    PinPayload, ReactionPayload, ReadPayload, RecallPayload, UnmarkPayload, UnpinPayload,
};
pub use message::{Attachment, Message};

use flare_im_core::utils::TimelineMetadata;

#[derive(Debug, Clone)]
pub struct PreparedMessage {
    pub conversation_id: String,
    pub message_id: String,
    pub message: Message,
    pub timeline: TimelineMetadata,
    pub sync: bool,
}

#[derive(Debug)]
pub struct PersistenceResult {
    pub conversation_id: String,
    pub message_id: String,
    pub timeline: TimelineMetadata,
    pub deduplicated: bool,
}

impl PersistenceResult {
    pub fn new(prepared: &PreparedMessage, deduplicated: bool) -> Self {
        Self {
            conversation_id: prepared.conversation_id.clone(),
            message_id: prepared.message_id.clone(),
            timeline: prepared.timeline.clone(),
            deduplicated,
        }
    }
}
