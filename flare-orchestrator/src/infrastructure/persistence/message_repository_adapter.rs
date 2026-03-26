use crate::error::{FlareError, Result};
use std::sync::Arc;
use flare_server_core::context::Ctx;
use flare_proto::storage::storage_reader_service_client::StorageReaderServiceClient;
use flare_proto::storage::{GetMessageRequest, GetMessageResponse};
use tonic::Request;
use crate::domain::model::{Message, MessageFsmState};
use crate::domain::service::message_operation_service::MessageRepository;
use chrono::{DateTime, Utc};

pub struct StorageReaderMessageRepository {
    client: StorageReaderServiceClient<tonic::transport::Channel>,
}

impl StorageReaderMessageRepository {
    pub fn new(client: Arc<StorageReaderServiceClient<tonic::transport::Channel>>) -> Self {
        Self { client: (*client).clone() }
    }
}

impl MessageRepository for StorageReaderMessageRepository {
    async fn find_by_id(&self, _ctx: &Ctx, message_id: &str) -> Result<Option<Message>> {
        let req = GetMessageRequest {
            message_id: message_id.to_string(),
        };

        let mut client = self.client.clone();
        let resp = client.get_message(Request::new(req)).await
            .map_err(|e| FlareError::system(format!("Failed to get message: {}", e)))?;
        let inner: GetMessageResponse = resp.into_inner();

        if let Some(proto_msg) = inner.message {
            let fsm_state = if proto_msg.status == flare_proto::common::MessageStatus::Recalled as i32 {
                MessageFsmState::Recalled
            } else if proto_msg.status == flare_proto::common::MessageStatus::DeletedHard as i32 {
                MessageFsmState::DeletedHard
            } else {
                MessageFsmState::from_str(
                    proto_msg.extra.get("message_fsm_state")
                        .map(|s| s.as_str())
                        .unwrap_or("SENT")
                ).unwrap_or(MessageFsmState::Sent)
            };

            let created_at_dt = proto_msg.timestamp
                .as_ref()
                .and_then(|ts| DateTime::from_timestamp(ts.seconds, ts.nanos as u32))
                .unwrap_or_else(Utc::now);

            let content_bytes = proto_msg.content.clone();

            let message = Message {
                server_id: proto_msg.server_id.clone(),
                conversation_id: proto_msg.conversation_id.clone(),
                sender_id: proto_msg.sender_id.clone(),
                channel_id: proto_msg.channel_id.clone(),
                content: content_bytes,
                timestamp: created_at_dt,
                fsm_state,
                fsm_state_changed_at: created_at_dt,
                edit_version: proto_msg.extra.get("current_edit_version")
                    .and_then(|v| v.parse::<i32>().ok())
                    .unwrap_or(0),
                edit_history: vec![],
                extra: proto_msg.extra,
                updated_at: created_at_dt,
            };

            Ok(Some(message))
        } else {
            Ok(None)
        }
    }

    async fn save(&self, _ctx: &Ctx, _message: &Message) -> Result<()> {
        Err(FlareError::general_error("Save operation should be handled by Writer via Kafka".to_string()))
    }
}

