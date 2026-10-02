//! 经 Message Ingest 的 `MessageSendService/SendMessage` 发一条单聊文本消息。

use std::time::Duration;

use crate::error::{Result, ensure, err};
use flare_grpc_proto::message::SendMessageRequest;
use flare_grpc_proto::message::message_send_service_client::MessageSendServiceClient;
use flare_proto::common::{
    ConversationType, Message, MessageContent, MessageSource, MessageStatus, MessageType,
    SendAckDurability, TextContent, message_content,
};
use tonic::transport::Endpoint;

use crate::redact::redact;
use crate::signed::{ContextSigner, signed_request};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SmokeMessage {
    pub tenant_id: String,
    pub conversation_id: String,
    pub sender_id: String,
    pub recipient_id: String,
    pub client_msg_id: String,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendOutcome {
    pub server_msg_id: String,
    pub conversation_seq: u64,
    pub durability: SendAckDurability,
}

/// 发送前现签，令牌 60 s 内有效，不跨调用复用。
pub fn build_request(message: &SmokeMessage) -> SendMessageRequest {
    SendMessageRequest {
        conversation_id: message.conversation_id.clone(),
        message: Some(Message {
            conversation_id: message.conversation_id.clone(),
            client_msg_id: message.client_msg_id.clone(),
            sender_id: message.sender_id.clone(),
            source: MessageSource::User as i32,
            conversation_type: ConversationType::Single as i32,
            message_type: MessageType::Text as i32,
            channel_id: message.recipient_id.clone(),
            content: Some(MessageContent {
                content: Some(message_content::Content::Text(TextContent {
                    text: message.text.clone(),
                    mentions: Vec::new(),
                })),
            }),
            status: MessageStatus::Created as i32,
            ..Message::default()
        }),
        sync: false,
        svid: String::new(),
    }
}

pub async fn send(
    endpoint: &str,
    signer: &ContextSigner,
    message: &SmokeMessage,
    timeout: Duration,
) -> Result<SendOutcome> {
    let channel = Endpoint::from_shared(endpoint.to_owned())
        .map_err(|_| err!("invalid ingest endpoint"))?
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(timeout)
        .connect()
        .await
        .map_err(|error| {
            err!(
                "cannot reach message-ingest at {endpoint}: {}",
                redact(&error.to_string())
            )
        })?;
    let mut client = MessageSendServiceClient::new(channel);
    let token = signer.sign_user(&message.tenant_id, &message.sender_id)?;
    let request = signed_request(build_request(message), &token)?;
    let response = client
        .send_message(request)
        .await
        .map_err(|status| {
            err!(
                "message-ingest rejected the signed send: code={:?} message={}",
                status.code(),
                redact(status.message())
            )
        })?
        .into_inner();
    ensure!(response.success, "message-ingest answered success=false");
    ensure!(
        !response.server_msg_id.is_empty()
            && response.server_msg_id.len() <= 160
            && response
                .server_msg_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b':')),
        "message-ingest returned no usable server_msg_id"
    );
    ensure!(
        response.conversation_seq > 0 && response.conversation_seq <= i64::MAX as u64,
        "message-ingest returned no positive conversation seq"
    );
    let durability = response.durability();
    ensure!(
        matches!(
            durability,
            SendAckDurability::BrokerAccepted | SendAckDurability::Persisted
        ),
        "send ACK durability {} is below BROKER_ACCEPTED",
        durability.as_str_name()
    );
    Ok(SendOutcome {
        server_msg_id: response.server_msg_id,
        conversation_seq: response.conversation_seq,
        durability,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_is_a_user_text_message_to_the_recipient() {
        let message = SmokeMessage {
            tenant_id: "e2e-0123456789abcdef0123".into(),
            conversation_id: "1AS05Q2E4KQHG1W05P".into(),
            sender_id: "10001".into(),
            recipient_id: "10002".into(),
            client_msg_id: "smoke-grpc-abc".into(),
            text: "flare message-flow smoke grpc abc".into(),
        };
        let request = build_request(&message);
        assert_eq!(request.conversation_id, "1AS05Q2E4KQHG1W05P");
        assert!(!request.sync);
        let inner = request.message.unwrap();
        assert_eq!(inner.sender_id, "10001");
        assert_eq!(inner.channel_id, "10002");
        assert_eq!(inner.source, MessageSource::User as i32);
        assert_eq!(inner.conversation_type, ConversationType::Single as i32);
        assert_eq!(inner.message_type, MessageType::Text as i32);
        assert!(
            inner.attributes.is_empty(),
            "no identity attributes in the envelope"
        );
        assert!(inner.server_id.is_empty());
        match inner.content.unwrap().content.unwrap() {
            message_content::Content::Text(text) => {
                assert_eq!(text.text, "flare message-flow smoke grpc abc")
            }
            other => panic!("unexpected content {other:?}"),
        }
    }
}
