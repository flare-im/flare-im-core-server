//! Conversation Read gRPC 客户端池（统一读扩散：登录时拉取用户会话列表用于 eager 订阅）。

use flare_grpc_proto::conversation::conversation_read_service_client::ConversationReadServiceClient;
use flare_im_contracts::service_names::{CONVERSATION, get_service_name};
use flare_im_service_kit::discovery::connect_grpc_channel_resilient;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use tokio::sync::Mutex;
use tonic::transport::Channel;

const DEFAULT_CONVERSATION_URI: &str = "http://127.0.0.1:50090";

pub struct ConversationReadGrpcPool {
    service_name: String,
    client: Mutex<Option<ConversationReadServiceClient<Channel>>>,
}

impl ConversationReadGrpcPool {
    pub fn new() -> Self {
        Self {
            service_name: get_service_name(CONVERSATION),
            client: Mutex::new(None),
        }
    }

    pub async fn ensure_client(&self) -> Result<ConversationReadServiceClient<Channel>> {
        let mut guard = self.client.lock().await;
        if let Some(c) = guard.as_ref() {
            return Ok(c.clone());
        }
        let channel = connect_grpc_channel_resilient(&self.service_name, DEFAULT_CONVERSATION_URI)
            .await
            .map_err(|e| {
                ErrorBuilder::new(ErrorCode::ServiceUnavailable, "conversation read unavailable")
                    .details(e)
                    .build_error()
            })?;
        let client = ConversationReadServiceClient::new(channel);
        *guard = Some(client.clone());
        Ok(client)
    }

    /// 拉取会话全部参与者 user_id（分页聚合）。用于网关首次投递时解析成员订阅（每会话每网关一次，缓存）。
    pub async fn list_participants(
        &self,
        tx: &flare_im_contracts::Ctx,
        conversation_id: &str,
    ) -> Result<Vec<String>> {
        use flare_grpc_proto::conversation::ListConversationParticipantsRequest;
        let mut client = self.ensure_client().await?;
        let mut user_ids = Vec::new();
        let mut cursor = String::new();
        loop {
            let req = flare_server_core::client::request_with_context(
                ListConversationParticipantsRequest {
                    conversation_id: conversation_id.to_string(),
                    cursor: cursor.clone(),
                    limit: 1000,
                    include_removed: false,
                    ext: Default::default(),
                },
                tx,
            );
            let resp = client.list_conversation_participants(req).await.map_err(|e| {
                ErrorBuilder::new(ErrorCode::ServiceUnavailable, "list participants failed")
                    .details(e.to_string())
                    .build_error()
            })?;
            let resp = resp.into_inner();
            for p in resp.participants {
                if !p.user_id.is_empty() {
                    user_ids.push(p.user_id);
                }
            }
            if !resp.has_more || resp.next_cursor.is_empty() {
                break;
            }
            cursor = resp.next_cursor;
        }
        Ok(user_ids)
    }
}

impl Default for ConversationReadGrpcPool {
    fn default() -> Self {
        Self::new()
    }
}
