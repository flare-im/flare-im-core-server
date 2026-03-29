// =============================================================================
// 解码：PayloadCommand.payload → 业务类型
// =============================================================================

use flare_core::common::error::{FlareError as CoreFlareError, Result as CoreResult};
use flare_core::common::protocol::{
    PayloadCommand, Reliability,
    builder::{FrameBuilder, current_timestamp},
    flare::core::commands::command::Type as CommandType,
    frame_with_payload_command,
    payload_command::Type as PayloadType,
};
use flare_proto::common::data_packet::Payload as DataPacketPayload;
use flare_proto::common::{
    Ack, CustomData, DataKind, DataPacket, Event, EventAck, Message as ProtoMessage, SendAck, ack,
};
use prost::Message as _;
use std::collections::HashMap;
/// 解码 MESSAGE 通道 payload 为 Message
#[inline]
pub fn decode_message_payload(payload: &[u8]) -> Result<ProtoMessage, String> {
    ProtoMessage::decode(payload).map_err(|e| format!("decode Message: {e}"))
}

/// 解码 ACK 通道 payload 为 Ack（客户端上行：PushAck/ConversationAck/AckBatch 等）
#[inline]
pub fn decode_ack_payload(payload: &[u8]) -> Result<Ack, String> {
    Ack::decode(payload).map_err(|e| format!("decode Ack: {}", e))
}

/// 解码 EVENT 通道 payload 为 Event
#[inline]
pub fn decode_event_payload(payload: &[u8]) -> Result<Event, String> {
    Event::decode(payload).map_err(|e| format!("decode Event: {}", e))
}

/// 解码 DATA 通道 payload 为 [`DataPacket`]（`common/data.proto`）
#[inline]
pub fn decode_data_packet(payload: &[u8]) -> Result<DataPacket, String> {
    DataPacket::decode(payload).map_err(|e| format!("decode DataPacket: {e}"))
}

// =============================================================================
// MESSAGE 响应：SendAck（封装为 Ack 放入 PayloadCommand）
// =============================================================================

/// 根据发送结果构建 MESSAGE 的 ACK Frame（PayloadCommand.type=ACK, payload=encoded Ack(SendAck)）
pub fn build_message_ack_frame(
    client_message_id: &str,
    conversation_id: Option<&str>,
    result: Result<(String, u64), (i32, String)>,
) -> CoreResult<flare_core::common::protocol::Frame> {
    use flare_proto::common::AckType;
    use prost_types::Timestamp;

    let send_ack = match &result {
        Ok((server_msg_id, seq)) => SendAck {
            client_msg_id: client_message_id.to_string(),
            server_msg_id: server_msg_id.clone(),
            seq: *seq,
            conversation_id: conversation_id.unwrap_or("").to_string(),
            success: true,
            error_code: 0,
            error_message: String::new(),
            server_time: Some(Timestamp {
                seconds: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0),
                nanos: 0,
            }),
            ack_id: None,
            metadata: HashMap::new(),
        },
        Err((code, msg)) => SendAck {
            client_msg_id: client_message_id.to_string(),
            server_msg_id: client_message_id.to_string(),
            seq: 0,
            conversation_id: conversation_id.unwrap_or("").to_string(),
            success: false,
            error_code: *code,
            error_message: msg.clone(),
            server_time: None,
            ack_id: None,
            metadata: HashMap::new(),
        },
    };

    let ack = Ack {
        r#type: AckType::Send as i32,
        ack_id: None,
        at: None,
        payload: Some(ack::Payload::Send(send_ack)),
    };

    let mut payload = Vec::new();
    ack.encode(&mut payload)
        .map_err(|e| CoreFlareError::serialization_error(format!("encode Ack(SendAck): {}", e)))?;

    let ack_cmd = PayloadCommand {
        r#type: PayloadType::Ack as i32,
        message_id: client_message_id.to_string(),
        payload,
        metadata: HashMap::new(),
        seq: 0,
    };
    Ok(frame_with_payload_command(
        ack_cmd,
        Reliability::AtLeastOnce,
    ))
}

// =============================================================================
// DATA 通道：`DataPacket` 序列化体放入 PayloadCommand(type=DATA)
// =============================================================================

/// 将原始 payload 封装为 DATA 类型 Frame（通常为已编码的 [`DataPacket`]）
#[inline]
pub fn build_data_frame_with_payload(
    message_id: String,
    payload: Vec<u8>,
) -> flare_core::common::protocol::Frame {
    let cmd = PayloadCommand {
        r#type: PayloadType::Data as i32,
        message_id: message_id.clone(),
        payload,
        metadata: HashMap::new(),
        seq: 0,
    };
    FrameBuilder::new()
        .with_command(
            flare_core::common::protocol::flare::core::commands::Command {
                r#type: Some(CommandType::Payload(cmd)),
            },
        )
        .with_message_id(message_id)
        .with_reliability(Reliability::AtLeastOnce)
        .with_timestamp(current_timestamp())
        .build()
}

/// 构建 DATA 通道错误响应：`DataPacket { kind=USER_CUSTOM, user_custom }`（`CustomData.type` 为错误类别）
#[inline]
pub fn build_data_error_frame(
    message_id: String,
    error_type: &str,
    error_message: &str,
) -> flare_core::common::protocol::Frame {
    let mut meta = HashMap::new();
    meta.insert("error".to_string(), error_message.to_string());
    let inner = CustomData {
        r#type: error_type.to_string(),
        payload: error_message.as_bytes().to_vec(),
        metadata: meta,
    };
    let packet = DataPacket {
        kind: DataKind::UserCustom as i32,
        payload: Some(DataPacketPayload::UserCustom(inner)),
    };
    build_data_frame_with_payload(message_id, packet.encode_to_vec())
}

// =============================================================================
// EVENT 响应：EventAck（封装为 Ack 放入 PayloadCommand）
// =============================================================================

/// 领域事件回包：`OperationResponse` → `Ack(EventAck)`（`PayloadCommand.type=ACK`）
pub fn build_event_ack_operation_frame(
    message_id: &str,
    event_id: &str,
    operation: &flare_proto::common::OperationResponse,
) -> CoreResult<flare_core::common::protocol::Frame> {
    use flare_proto::common::AckType;
    use flare_proto::common::ErrorCode;

    let status = operation
        .status
        .clone()
        .unwrap_or_else(|| flare_proto::common::RpcStatus {
            code: ErrorCode::Internal as i32,
            message: "missing operation status".to_string(),
            details: Vec::new(),
            context: None,
            localization_key: String::new(),
            localization_params: HashMap::new(),
        });

    let event_ack = EventAck {
        event_id: event_id.to_string(),
        status: Some(status),
        metadata: HashMap::new(),
    };

    let ack = Ack {
        r#type: AckType::Event as i32,
        ack_id: None,
        at: None,
        payload: Some(ack::Payload::Event(event_ack)),
    };

    let mut payload = Vec::new();
    ack.encode(&mut payload)
        .map_err(|e| CoreFlareError::serialization_error(format!("encode Ack(EventAck): {}", e)))?;

    let ack_cmd = PayloadCommand {
        r#type: PayloadType::Ack as i32,
        message_id: message_id.to_string(),
        payload,
        metadata: HashMap::new(),
        seq: 0,
    };
    Ok(frame_with_payload_command(
        ack_cmd,
        Reliability::AtLeastOnce,
    ))
}
