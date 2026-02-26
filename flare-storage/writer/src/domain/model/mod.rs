//! 领域模型定义

use flare_im_core::utils::TimelineMetadata;

#[derive(Debug, Clone)]
pub struct PreparedMessage {
    pub conversation_id: String,
    pub message_id: String,
    pub message: flare_proto::common::Message,
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
