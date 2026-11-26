use flare_im_core_contracts::utils::TimelineMetadata;
use flare_proto::common::Message;

#[derive(Clone, Debug)]
pub struct MessageSubmission {
    pub message: Message,
    pub message_id: String,
    pub timeline: TimelineMetadata,
}
