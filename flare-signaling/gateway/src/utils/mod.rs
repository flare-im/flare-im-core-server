//! 协议工具：解码 `PayloadCommand` 载荷、构造下行 Frame（SendAck / EventAck / DATA）。

use std::collections::HashMap;

use flare_core::common::error::{FlareError as CoreFlareError, Result as CoreResult};
use flare_core::common::protocol::{
    Frame, PayloadCommand, Reliability, frame_with_payload_command,
    payload_command::Type as PayloadType,
};
use flare_proto::common::{Ack, AckType};
use flare_proto::common::{CustomData, DataKind, DataPacket, Event, Message, SendAck, ack};
use prost::Message as ProstMessage;

/// 解码 MESSAGE 上行载荷
pub fn decode_message_payload(payload: &[u8]) -> Result<Message, prost::DecodeError> {
    Message::decode(payload)
}

/// 解码 EVENT 上行载荷
pub fn decode_event_payload(payload: &[u8]) -> Result<Event, prost::DecodeError> {
    Event::decode(payload)
}

/// 解码 DATA 上行载荷
pub fn decode_data_packet(payload: &[u8]) -> Result<DataPacket, prost::DecodeError> {
    DataPacket::decode(payload)
}

/// 解码 ACK 上行载荷
pub fn decode_ack_payload(payload: &[u8]) -> Result<Ack, prost::DecodeError> {
    Ack::decode(payload)
}

/// 发消息结果 → `PayloadCommand.type=ACK`，载荷为 `Ack.send`（`SendAck`）。
pub fn build_message_ack_frame(
    message_id: &str,
    client_msg_id: &str,
    conversation_id: Option<&str>,
    result: std::result::Result<(String, u64), (i32, String)>,
) -> CoreResult<Frame> {
    let send_ack = match result {
        Ok((server_msg_id, seq)) => SendAck {
            client_msg_id: client_msg_id.to_string(),
            server_msg_id,
            seq,
            conversation_id: conversation_id.unwrap_or_default().to_string(),
            success: true,
            error_code: flare_proto::common::ErrorCode::Ok as i32,
            error_message: String::new(),
            server_time: None,
            ack_id: None,
            metadata: Default::default(),
        },
        Err((code, msg)) => SendAck {
            client_msg_id: client_msg_id.to_string(),
            server_msg_id: String::new(),
            seq: 0,
            conversation_id: conversation_id.unwrap_or_default().to_string(),
            success: false,
            error_code: code,
            error_message: msg,
            server_time: None,
            ack_id: None,
            metadata: Default::default(),
        },
    };

    let ack = Ack {
        r#type: AckType::Send as i32,
        ack_id: None,
        at: None,
        payload: Some(ack::Payload::Send(send_ack)),
    };

    let mut buf = Vec::new();
    ack.encode(&mut buf)
        .map_err(|e| CoreFlareError::serialization_error(format!("encode Ack(SendAck): {}", e)))?;

    let cmd = PayloadCommand {
        r#type: PayloadType::Ack as i32,
        message_id: message_id.to_string(),
        payload: buf,
        metadata: Default::default(),
        seq: 0,
    };
    Ok(frame_with_payload_command(cmd, Reliability::AtLeastOnce))
}

/// 领域事件成功 → `Ack(EventAck)`（`PayloadCommand.type=ACK`）
pub fn build_event_ack_operation_frame(message_id: &str, event_id: &str) -> CoreResult<Frame> {
    use flare_proto::common::EventAck;

    let event_ack = EventAck {
        event_id: event_id.to_string(),
        metadata: HashMap::new(),
    };

    let ack = Ack {
        r#type: AckType::Event as i32,
        ack_id: None,
        at: None,
        payload: Some(ack::Payload::Event(event_ack)),
    };

    let mut buf = Vec::new();
    ack.encode(&mut buf)
        .map_err(|e| CoreFlareError::serialization_error(format!("encode Ack(EventAck): {}", e)))?;

    let cmd = PayloadCommand {
        r#type: PayloadType::Ack as i32,
        message_id: message_id.to_string(),
        payload: buf,
        metadata: Default::default(),
        seq: 0,
    };
    Ok(frame_with_payload_command(cmd, Reliability::AtLeastOnce))
}

/// DATA 通道错误回包（`DataKind::USER_CUSTOM`）
pub fn build_data_error_frame(
    message_id: String,
    _kind: &str,
    err: impl std::fmt::Display,
) -> Frame {
    use flare_proto::common::data_packet;

    let inner = CustomData {
        r#type: "error".to_string(),
        payload: err.to_string().into_bytes(),
        metadata: Default::default(),
    };
    let packet = DataPacket {
        kind: DataKind::UserCustom as i32,
        payload: Some(data_packet::Payload::UserCustom(inner)),
    };
    let cmd = PayloadCommand {
        r#type: PayloadType::Data as i32,
        message_id,
        payload: packet.encode_to_vec(),
        metadata: Default::default(),
        seq: 0,
    };
    frame_with_payload_command(cmd, Reliability::AtLeastOnce)
}

/// DATA 通道成功回包（原始 `DataPacket` 编码字节）
pub fn build_data_frame_with_payload(message_id: String, payload: Vec<u8>) -> Frame {
    let cmd = PayloadCommand {
        r#type: PayloadType::Data as i32,
        message_id,
        payload,
        metadata: Default::default(),
        seq: 0,
    };
    frame_with_payload_command(cmd, Reliability::AtLeastOnce)
}
