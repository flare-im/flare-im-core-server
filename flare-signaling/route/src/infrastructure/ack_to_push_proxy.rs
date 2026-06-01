//! 客户端 ACK 上行处理：仅显式已读回执写入 Conversation 服务。
//!
//! PushAck 与普通 Conversation delivery ACK 仅记录/跳过；Conversation / Batch 中显式
//! `ack_kind=read` 的会话 ACK 才调用 `ConversationManageService::MarkConversationAsRead`。
//! 这样避免“收到即已读”，同时仍保证显式已读后重登不会出现服务端未读回弹。
//!
//! **启动解耦**：不在 `wire::initialize` 中连接 Conversation；首条会话 ACK 到达时再解析通道。

use std::sync::Arc;

use flare_grpc_proto::conversation::MarkConversationAsReadRequest;
use flare_grpc_proto::conversation::conversation_manage_service_client::ConversationManageServiceClient;
use flare_im_core::config::FlareAppConfig;
use flare_proto::common::Ack;
use flare_proto::common::ack::Payload as AckPayload;
use flare_proto::common::{ConversationAck, PushAck};
use flare_server_core::client::set_context_metadata;
use flare_server_core::context::Context;
use flare_server_core::error::{ErrorBuilder, ErrorCode};
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::{debug, info, warn};

use crate::error::{Result, require_user_id};

const CONVERSATION_STATIC_FALLBACK: &str = "http://127.0.0.1:50090";
const ACK_KIND_KEY: &str = "ack_kind";
const ACK_KIND_READ: &str = "read";
const ACK_KIND_READ_RECEIPT: &str = "read_receipt";
const READ_SEQ_KEY: &str = "read_seq";

/// 客户端 ACK 转发器：显式会话已读 → Conversation；PushAck / delivery ACK → 占位日志。
pub struct AckToPushProxyForwarder {
    app_config: Arc<FlareAppConfig>,
    /// 首包 ACK 前为 None；解析成功后缓存，避免每条 ACK 重复发现。
    conversation_manage: Mutex<Option<ConversationManageServiceClient<Channel>>>,
}

impl AckToPushProxyForwarder {
    /// 启动期构造：不发起网络 I/O，不依赖 Conversation 已注册/已监听。
    pub fn new_deferred(app_config: Arc<FlareAppConfig>) -> Arc<Self> {
        Arc::new(Self {
            app_config,
            conversation_manage: Mutex::new(None),
        })
    }

    async fn conversation_client(
        &self,
    ) -> std::result::Result<
        ConversationManageServiceClient<Channel>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let mut slot = self.conversation_manage.lock().await;
        if let Some(client) = slot.as_ref() {
            return Ok(client.clone());
        }

        let channel = flare_im_core::discovery::connect_grpc_channel_from_app_config(
            &self.app_config,
            flare_im_core::service_names::CONVERSATION,
            CONVERSATION_STATIC_FALLBACK,
        )
        .await?;
        let client = ConversationManageServiceClient::new(channel);
        *slot = Some(client.clone());
        Ok(client)
    }

    /// 处理客户端上行 ACK。
    pub async fn forward_client_ack(&self, ctx: &Context, ack: Ack) -> Result<()> {
        match ack.payload {
            Some(AckPayload::Conversation(conversation_ack)) => {
                self.apply_conversation_read_ack(ctx, &conversation_ack)
                    .await?;
            }
            Some(AckPayload::Batch(batch)) => {
                for push in batch.push_acks {
                    self.log_push_ack_skip(ctx, &push);
                }
                for conversation_ack in batch.conversation_acks {
                    if let Err(error) = self
                        .apply_conversation_read_ack(ctx, &conversation_ack)
                        .await
                    {
                        warn!(
                            request_id = %ctx.request_id(),
                            conversation_id = %conversation_ack.conversation_id,
                            %error,
                            "batch conversation ack mark read failed"
                        );
                    }
                }
            }
            Some(AckPayload::Push(push_ack)) => {
                self.log_push_ack_skip(ctx, &push_ack);
            }
            _ => {
                debug!(
                    request_id = %ctx.request_id(),
                    ack_type = ack.r#type,
                    "skip client ack: unsupported payload for conversation read"
                );
            }
        }
        Ok(())
    }

    fn log_push_ack_skip(&self, ctx: &Context, push_ack: &PushAck) {
        debug!(
            request_id = %ctx.request_id(),
            user_id = ctx.user_id().unwrap_or_default(),
            window_id = %push_ack.window_id,
            ack_seq = push_ack.ack_seq,
            "skip push ack forward: PushAck RPC removed"
        );
    }

    async fn apply_conversation_read_ack(
        &self,
        ctx: &Context,
        ack: &ConversationAck,
    ) -> Result<()> {
        let conversation_id = ack.conversation_id.trim();
        if conversation_id.is_empty() {
            return Ok(());
        }

        let Some(read_seq) = read_seq_from_ack(ack) else {
            debug!(
                request_id = %ctx.request_id(),
                user_id = ctx.user_id().unwrap_or_default(),
                conversation_id = %conversation_id,
                delivered_seq = ack.last_delivered_seq,
                "skip conversation ack: not an explicit read ack"
            );
            return Ok(());
        };

        let user_id = require_user_id(ctx)?;

        let request = MarkConversationAsReadRequest {
            conversation_id: conversation_id.to_string(),
            read_seq,
        };
        let mut grpc_request = tonic::Request::new(request);
        set_context_metadata(&mut grpc_request, ctx);

        let mut client = self.conversation_client().await.map_err(|e| {
            ErrorBuilder::new(
                ErrorCode::ServiceUnavailable,
                format!("conversation service unavailable for client ack: {e}"),
            )
            .build_error()
        })?;
        client
            .mark_conversation_as_read(grpc_request)
            .await
            .map_err(|status| {
                ErrorBuilder::new(
                    ErrorCode::InternalError,
                    format!("Conversation MarkConversationAsRead failed: {status}"),
                )
                .build_error()
            })?;

        info!(
            request_id = %ctx.request_id(),
            user_id = %user_id,
            conversation_id = %conversation_id,
            read_seq,
            "client conversation ack applied to conversation service"
        );
        Ok(())
    }
}

fn read_seq_from_ack(ack: &ConversationAck) -> Option<i64> {
    let kind = ack.metadata.get(ACK_KIND_KEY)?.trim();
    if kind != ACK_KIND_READ && kind != ACK_KIND_READ_RECEIPT {
        return None;
    }

    let seq = ack
        .metadata
        .get(READ_SEQ_KEY)
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(ack.last_delivered_seq);
    if seq == 0 {
        return None;
    }
    Some(i64::try_from(seq).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn conversation_ack(metadata: HashMap<String, String>, delivered_seq: u64) -> ConversationAck {
        ConversationAck {
            conversation_id: "c1".to_string(),
            server_msg_ids: Vec::new(),
            last_delivered_seq: delivered_seq,
            metadata,
        }
    }

    #[test]
    fn delivery_ack_without_read_kind_is_not_read() {
        let ack = conversation_ack(HashMap::new(), 42);

        assert_eq!(read_seq_from_ack(&ack), None);
    }

    #[test]
    fn explicit_read_ack_uses_read_seq_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert(ACK_KIND_KEY.to_string(), ACK_KIND_READ.to_string());
        metadata.insert(READ_SEQ_KEY.to_string(), "99".to_string());
        let ack = conversation_ack(metadata, 42);

        assert_eq!(read_seq_from_ack(&ack), Some(99));
    }

    #[test]
    fn explicit_read_ack_falls_back_to_delivered_seq() {
        let mut metadata = HashMap::new();
        metadata.insert(ACK_KIND_KEY.to_string(), ACK_KIND_READ_RECEIPT.to_string());
        let ack = conversation_ack(metadata, 42);

        assert_eq!(read_seq_from_ack(&ack), Some(42));
    }

    #[test]
    fn explicit_read_ack_with_zero_seq_is_ignored() {
        let mut metadata = HashMap::new();
        metadata.insert(ACK_KIND_KEY.to_string(), ACK_KIND_READ.to_string());
        metadata.insert(READ_SEQ_KEY.to_string(), "0".to_string());
        let ack = conversation_ack(metadata, 42);

        assert_eq!(read_seq_from_ack(&ack), None);
    }
}
