use std::sync::Arc;

use flare_im_contracts::Ctx;
use flare_im_message_pipeline::{MqPushRepository, PushRepository};
use flare_proto::common::Message;
use flare_server_core::error::Result;
use tracing::instrument;

/// 主消息流 fanout 服务。
///
/// 输入消息已经由 `flare-message-ingest` 完成校验、seq 分配、conversation ensure、WAL 和 Hook。
/// 本服务只负责把主流中的持久消息拆到存储流与推送流。
pub struct MessageFanoutService {
    push_repository: Arc<MqPushRepository>,
}

impl MessageFanoutService {
    pub fn new(push_repository: Arc<MqPushRepository>) -> Self {
        Self { push_repository }
    }

    fn normalize_single_chat_routing(
        &self,
        mut message: Message,
        recipient_user_ids: &[String],
    ) -> Message {
        use crate::domain::model::ConversationType;

        if ConversationType::from_proto(message.conversation_type) != ConversationType::Single {
            return message;
        }

        let sender_id = message.sender_id.trim();
        let Some(peer_id) = recipient_user_ids
            .iter()
            .map(|id| id.trim())
            .find(|id| !id.is_empty() && *id != sender_id)
        else {
            return message;
        };

        if message.channel_id.trim() != peer_id {
            tracing::warn!(
                conversation_id = %message.conversation_id,
                message_id = %message.server_id,
                sender_id = %message.sender_id,
                old_channel_id = %message.channel_id,
                normalized_channel_id = %peer_id,
                "Normalized single chat message channel_id from resolved recipient"
            );
            message.channel_id = peer_id.to_string();
        }

        message
    }

    #[instrument(skip(self, recipient_user_ids), fields(
        conversation_id = %message.conversation_id,
        message_id = %message.server_id,
        recipient_count = recipient_user_ids.len(),
    ))]
    pub async fn persist_and_push_with_recipients(
        &self,
        ctx: &Ctx,
        message: Message,
        recipient_user_ids: Vec<String>,
    ) -> Result<()> {
        tracing::trace!(
            conversation_id = %message.conversation_id,
            message_id = %message.server_id,
            recipient_count = recipient_user_ids.len(),
            "Fanout persistent message to storage and push topics"
        );

        let message = self.normalize_single_chat_routing(message, &recipient_user_ids);
        let conversation_id = message.conversation_id.clone();
        self.push_repository
            .persistence_only_message(ctx, message.clone(), conversation_id.clone())
            .await?;
        self.push_repository
            .push_only_message(ctx, message, recipient_user_ids, conversation_id)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_im_contracts::constants::topics::{TOPIC_MESSAGE_CREATED, TOPIC_PUSH_MESSAGES};
    use flare_proto::common::{ConversationType as ProtoConversationType, MqEnvelope, mq_envelope};
    use flare_server_core::Context;
    use flare_server_core::mq::producer::{Producer, ProducerError, ProducerMessage};
    use prost::Message as _;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Clone)]
    struct CapturedSend {
        topic: String,
        key: Option<String>,
        payload: Vec<u8>,
        headers: Option<HashMap<String, String>>,
    }

    #[derive(Default)]
    struct CapturingProducer {
        sends: Mutex<Vec<CapturedSend>>,
    }

    #[async_trait::async_trait]
    impl Producer for CapturingProducer {
        async fn send(
            &self,
            _ctx: &Ctx,
            topic: &str,
            key: Option<&str>,
            payload: Vec<u8>,
            headers: Option<HashMap<String, String>>,
        ) -> std::result::Result<(), ProducerError> {
            self.sends
                .lock()
                .expect("capture producer poisoned")
                .push(CapturedSend {
                    topic: topic.to_string(),
                    key: key.map(ToString::to_string),
                    payload,
                    headers,
                });
            Ok(())
        }

        async fn send_batch(
            &self,
            _ctx: &Ctx,
            _messages: Vec<ProducerMessage>,
        ) -> std::result::Result<(), ProducerError> {
            Ok(())
        }

        fn name(&self) -> &str {
            "capturing-producer"
        }
    }

    #[tokio::test]
    async fn persistent_message_fanout_writes_storage_before_push() {
        let producer = Arc::new(CapturingProducer::default());
        let repository = MqPushRepository::new(producer.clone());
        let service = MessageFanoutService::new(repository);
        let ctx: Ctx = Arc::new(
            Context::with_request_id("req-mainline-contract")
                .with_trace_id("trace-mainline-contract")
                .with_tenant_id("tenant-a")
                .with_user_id("sender-a"),
        );
        let message = Message {
            server_id: "message-a".to_string(),
            conversation_id: "conversation-a".to_string(),
            conversation_type: ProtoConversationType::Single as i32,
            channel_id: "stale-peer".to_string(),
            sender_id: "sender-a".to_string(),
            conversation_seq: 42,
            created_at: 1_700_000_000_000,
            ..Message::default()
        };

        service
            .persist_and_push_with_recipients(
                &ctx,
                message,
                vec!["sender-a".to_string(), "peer-b".to_string()],
            )
            .await
            .expect("fanout should succeed");

        let captured = producer
            .sends
            .lock()
            .expect("capture producer poisoned")
            .clone();
        assert_eq!(
            captured.len(),
            2,
            "persistent messages must fan out to storage and push topics"
        );

        let storage = &captured[0];
        assert_eq!(storage.topic, TOPIC_MESSAGE_CREATED);
        assert_eq!(storage.key.as_deref(), Some("conversation-a"));
        let storage_headers = storage.headers.as_ref().expect("storage headers");
        assert_eq!(
            storage_headers.get("x-tenant-id").map(String::as_str),
            Some("tenant-a")
        );
        assert_eq!(
            storage_headers.get("x-trace-id").map(String::as_str),
            Some("trace-mainline-contract")
        );
        assert_eq!(
            storage_headers.get("persistence-only").map(String::as_str),
            Some("true")
        );
        let storage_envelope =
            MqEnvelope::decode(storage.payload.as_slice()).expect("decode storage envelope");
        assert!(storage_envelope.persistence_only);
        assert!(!storage_envelope.push_only);
        assert!(storage_envelope.recipient_user_ids.is_empty());

        let Some(mq_envelope::Payload::Message(storage_message)) = storage_envelope.payload else {
            panic!("storage envelope must carry message payload");
        };
        assert_eq!(storage_message.server_id, "message-a");
        assert_eq!(
            storage_message.channel_id, "peer-b",
            "single-chat channel_id must be normalized before storage fanout"
        );

        let push = &captured[1];
        assert_eq!(push.topic, TOPIC_PUSH_MESSAGES);
        assert_eq!(push.key.as_deref(), Some("conversation-a"));
        let push_headers = push.headers.as_ref().expect("push headers");
        assert_eq!(
            push_headers.get("push-only").map(String::as_str),
            Some("true")
        );
        let push_envelope =
            MqEnvelope::decode(push.payload.as_slice()).expect("decode push envelope");
        assert!(push_envelope.push_only);
        assert!(!push_envelope.persistence_only);
        assert_eq!(
            push_envelope.recipient_user_ids,
            vec!["sender-a".to_string(), "peer-b".to_string()]
        );

        let Some(mq_envelope::Payload::Message(push_message)) = push_envelope.payload else {
            panic!("push envelope must carry message payload");
        };
        assert_eq!(push_message.server_id, "message-a");
        assert_eq!(push_message.channel_id, "peer-b");
    }
}
