use flare_im_contracts::utils::TimelineMetadata;
use flare_im_message_pipeline::SubmittedMessage;
use flare_proto::common::Message;

#[derive(Clone, Debug)]
pub struct MessageSubmission {
    pub message: Message,
    pub message_id: String,
    pub timeline: TimelineMetadata,
}

impl SubmittedMessage for MessageSubmission {
    fn message(&self) -> &Message {
        &self.message
    }

    fn message_id(&self) -> &str {
        &self.message_id
    }
}
