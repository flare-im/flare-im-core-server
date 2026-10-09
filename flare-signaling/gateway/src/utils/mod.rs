//! 协议工具：解码 `PayloadCommand` 载荷、构造下行 Frame（SendAck / EventAck / DATA）。

use flare_core::common::error::{FlareError as CoreFlareError, Result as CoreResult};
use flare_core::common::protocol::{
    Frame, PayloadCommand, Reliability, frame_with_payload_command,
    payload_command::Type as PayloadType,
};
use flare_proto::common::{
    Ack, CustomData, DataPacket, ErrorDetail, Event, Message, SendAccepted, SendAck, ack,
    data_packet, send_ack,
};
use prost::Message as ProstMessage;

use crate::domain::model::MessageSendOutcome;

#[derive(Debug, Clone)]
pub struct SendAckFailure {
    pub code: i32,
    pub message: String,
    pub error_detail: Option<ErrorDetail>,
}

impl SendAckFailure {
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            error_detail: None,
        }
    }

    pub fn with_error_detail(mut self, detail: ErrorDetail) -> Self {
        self.error_detail = Some(detail);
        self
    }
}

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
    result: std::result::Result<MessageSendOutcome, SendAckFailure>,
) -> CoreResult<Frame> {
    let send_ack = match result {
        Ok(outcome) => SendAck {
            client_msg_id: client_msg_id.to_string(),
            conversation_id: conversation_id.unwrap_or_default().to_string(),
            ack_id: None,
            result: Some(send_ack::Result::Accepted(SendAccepted {
                server_msg_id: outcome.server_msg_id,
                conversation_seq: outcome.conversation_seq,
                server_time: now_millis(),
                durability: outcome.durability as i32,
            })),
        },
        Err(failure) => {
            let detail = failure.error_detail.unwrap_or_else(|| ErrorDetail {
                code: failure.code,
                reason: "MESSAGE_SEND_FAILED".to_string(),
                message: failure.message,
                track: String::new(),
            });
            SendAck {
                client_msg_id: client_msg_id.to_string(),
                conversation_id: conversation_id.unwrap_or_default().to_string(),
                ack_id: None,
                result: Some(send_ack::Result::Error(detail)),
            }
        }
    };

    let ack = Ack {
        ack_id: None,
        ack_at: None,
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
        attributes: Default::default(),
    };

    let ack = Ack {
        ack_id: None,
        ack_at: None,
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
pub fn build_data_error_frame(message_id: String, _kind: &str, err: &CoreFlareError) -> Frame {
    let code = match err {
        CoreFlareError::Localized { code, .. } => proto_data_error_code(*code),
        CoreFlareError::System(_) => flare_proto::common::ErrorCode::Internal as i32,
        CoreFlareError::Io(_) => flare_proto::common::ErrorCode::Unavailable as i32,
    };
    let inner = CustomData {
        r#type: "error".to_string(),
        payload: err.to_string().into_bytes(),
        attributes: std::collections::HashMap::from([
            ("error_code".to_string(), code.to_string()),
            ("error_reason".to_string(), err.reason().to_string()),
        ]),
    };
    let packet = DataPacket {
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

fn proto_data_error_code(code: flare_core::common::error::ErrorCode) -> i32 {
    use flare_core::common::error::ErrorCode as CoreCode;
    use flare_proto::common::ErrorCode as ProtoCode;
    match code {
        CoreCode::PermissionDenied => ProtoCode::PermissionDenied as i32,
        CoreCode::AuthenticationFailed
        | CoreCode::AuthenticationExpired
        | CoreCode::AuthenticationInvalid
        | CoreCode::AuthenticationRequired
        | CoreCode::TokenInvalid
        | CoreCode::TokenExpired => ProtoCode::Unauthenticated as i32,
        CoreCode::InvalidParameter
        | CoreCode::MessageFormatError
        | CoreCode::InvalidCommand
        | CoreCode::DeserializationError
        | CoreCode::MessageDecodeFailed => ProtoCode::InvalidArgument as i32,
        CoreCode::MessageNotFound | CoreCode::UserNotFound => ProtoCode::NotFound as i32,
        CoreCode::MessageRateLimitExceeded => ProtoCode::RateLimited as i32,
        CoreCode::ResourceExhausted
        | CoreCode::ConnectionLimitExceeded
        | CoreCode::UserQuotaExceeded => ProtoCode::ResourceExhausted as i32,
        CoreCode::OperationTimeout | CoreCode::ConnectionTimeout | CoreCode::NetworkTimeout => {
            ProtoCode::Timeout as i32
        }
        CoreCode::ServiceUnavailable
        | CoreCode::NetworkError
        | CoreCode::NetworkUnreachable
        | CoreCode::NetworkConnectionLost => ProtoCode::Unavailable as i32,
        CoreCode::OperationNotSupported => ProtoCode::UnsupportedOperation as i32,
        _ => ProtoCode::Internal as i32,
    }
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
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

#[cfg(test)]
mod tests {
    use super::*;
    use flare_core::common::protocol::command;
    use flare_proto::common::{
        SendAckDurability, ack::Payload as AckPayload, send_ack::Result as SendAckResult,
    };

    fn decode_data_error(frame: Frame, expected_message_id: &str) -> CustomData {
        let payload = match frame.command.and_then(|command| command.r#type) {
            Some(command::Type::Payload(payload)) => payload,
            other => panic!("expected payload command, got {other:?}"),
        };
        assert_eq!(payload.r#type, PayloadType::Data as i32);
        assert_eq!(payload.message_id, expected_message_id);
        let packet = DataPacket::decode(payload.payload.as_slice()).unwrap();
        match packet.payload {
            Some(data_packet::Payload::UserCustom(custom)) => custom,
            other => panic!("expected custom DATA error, got {other:?}"),
        }
    }

    #[test]
    fn data_error_frame_carries_official_permission_denied_code_and_legacy_text() {
        let error = CoreFlareError::localized(
            flare_core::common::error::ErrorCode::PermissionDenied,
            "conversation access denied",
        );
        let custom = decode_data_error(
            build_data_error_frame("sync-1".into(), "sync", &error),
            "sync-1",
        );
        assert_eq!(custom.r#type, "error");
        assert_eq!(custom.payload, error.to_string().into_bytes());
        assert_eq!(
            custom.attributes.get("error_code").map(String::as_str),
            Some("21")
        );
        assert_ne!(
            custom.attributes.get("error_code").unwrap(),
            &flare_core::common::error::ErrorCode::PermissionDenied
                .as_u32()
                .to_string(),
            "internal enum numbers are not official wire codes",
        );
        assert_eq!(
            custom.attributes.get("error_reason").map(String::as_str),
            Some(error.reason())
        );
    }

    #[test]
    fn data_error_frame_classifies_types_without_guessing_display_strings() {
        use flare_core::common::error::ErrorCode;
        for (error, expected) in [
            (
                CoreFlareError::localized(ErrorCode::ServiceUnavailable, "unavailable"),
                "51",
            ),
            (
                CoreFlareError::localized(ErrorCode::OperationTimeout, "timeout"),
                "50",
            ),
            (
                CoreFlareError::localized(ErrorCode::DeserializationError, "invalid payload"),
                "10",
            ),
            (CoreFlareError::system("PermissionDenied"), "60"),
        ] {
            let custom = decode_data_error(
                build_data_error_frame("sync-2".into(), "sync", &error),
                "sync-2",
            );
            assert_eq!(
                custom.attributes.get("error_code").map(String::as_str),
                Some(expected)
            );
            assert_eq!(custom.payload, error.to_string().into_bytes());
        }
    }

    #[test]
    fn message_ack_frame_preserves_send_ack_durability() {
        let frame = build_message_ack_frame(
            "frame-1",
            "client-1",
            Some("conversation-1"),
            Ok(MessageSendOutcome {
                server_msg_id: "server-1".to_string(),
                conversation_seq: 42,
                durability: SendAckDurability::BrokerAccepted,
            }),
        )
        .expect("ack frame should build");

        let payload = match frame.command.and_then(|command| command.r#type) {
            Some(command::Type::Payload(payload)) => payload,
            other => panic!("expected payload command, got {other:?}"),
        };
        let ack = Ack::decode(payload.payload.as_slice()).expect("payload should decode as Ack");

        match ack.payload {
            Some(AckPayload::Send(send)) => match send.result {
                Some(SendAckResult::Accepted(accepted)) => {
                    assert_eq!(accepted.server_msg_id, "server-1");
                    assert_eq!(accepted.conversation_seq, 42);
                    assert_eq!(accepted.durability(), SendAckDurability::BrokerAccepted);
                }
                other => panic!("expected accepted SendAck, got {other:?}"),
            },
            other => panic!("expected SendAck payload, got {other:?}"),
        }
    }
}
