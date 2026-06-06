use flare_proto::Message;
use flare_proto::common::{Ack, DataPacket, Event};
use std::collections::HashMap;

// 上行命令

/// 发消息命令（PayloadCommand.type = MESSAGE）
#[derive(Debug, Clone)]
pub struct SendMessageCommand {
    pub connection_id: String,
    pub seq: u64,
    pub msg: Message,
    pub metadata: HashMap<String, Vec<u8>>,
}

impl SendMessageCommand {
    pub fn new(connection_id: String, msg: Message, seq: u64) -> Self {
        Self {
            connection_id,
            msg,
            seq,
            metadata: HashMap::new(),
        }
    }
}

/// 发事件命令（PayloadCommand.type = EVENT）
#[derive(Debug, Clone)]
pub struct SendEventCommand {
    pub connection_id: String,
    pub seq: u64,
    pub event: Event,
    pub metadata: HashMap<String, Vec<u8>>,
}

impl crate::application::SendEventCommand {
    pub fn new(connection_id: String, event: Event, seq: u64) -> Self {
        Self {
            connection_id,
            event,
            seq,
            metadata: HashMap::new(),
        }
    }
}

/// 发数据命令（PayloadCommand.type = DATA，载荷为 [`DataPacket`]，见 `common/data.proto`）
#[derive(Debug, Clone)]
pub struct SendDataCommand {
    pub connection_id: String,
    pub seq: u64,
    pub packet: DataPacket,
    pub metadata: HashMap<String, Vec<u8>>,
}

impl SendDataCommand {
    pub fn new(connection_id: String, packet: DataPacket, seq: u64) -> Self {
        Self {
            connection_id,
            packet,
            seq,
            metadata: HashMap::new(),
        }
    }
}

/// 上行 ACK（PayloadCommand.type = ACK）：载荷为完整 [`Ack`]（Push / Conversation / Batch）
#[derive(Debug, Clone)]
pub struct SendAckCommand {
    pub connection_id: String,
    pub ack_id: Option<String>,
    pub ack: Ack,
    pub metadata: HashMap<String, Vec<u8>>,
}

impl SendAckCommand {
    pub fn new(connection_id: String, ack: Ack, ack_id: Option<String>) -> Self {
        Self {
            connection_id,
            ack,
            ack_id,
            metadata: HashMap::new(),
        }
    }
}
