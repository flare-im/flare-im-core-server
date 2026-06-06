use flare_proto::common::SendAckDurability;

#[derive(Debug, Clone)]
pub struct MessageSendOutcome {
    pub server_msg_id: String,
    pub conversation_seq: u64,
    pub durability: SendAckDurability,
}
