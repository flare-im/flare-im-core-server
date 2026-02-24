//! 临时消息处理服务
//!
//! 处理临时消息（TYPING、SYSTEM_EVENT）：
//! - 只推送，不持久化
//! - 不经过 WAL
//! - 不分配 seq
//! - 离线消息直接丢弃

use std::sync::Arc;
use anyhow::{Context, Result};
use flare_proto::push::{PushMessageRequest, PushOptions};
use flare_proto::common::{Message, RequestContext, TenantContext};
use flare_server_core::context::{Context as ServerContext, ContextExt};
use tracing::instrument;

use crate::domain::repository::{MessageEventPublisher, MessageEventPublisherItem};

/// 临时消息处理服务
pub struct MessageTemporaryService {
    publisher: Arc<MessageEventPublisherItem>,
}

impl MessageTemporaryService {
    pub fn new(publisher: Arc<MessageEventPublisherItem>) -> Self {
        Self { publisher }
    }

    /// 处理临时消息（只推送，不持久化）
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        message_id = %message.server_id,
        message_type = message.message_type,
        conversation_id = %message.conversation_id
    ))]
    pub async fn handle_temporary_message(
        &self,
        ctx: &ServerContext,
        message: &Message,
    ) -> Result<()> {
        ctx.ensure_not_cancelled()?;
        
        // 构建推送请求
        let push_request = self.build_push_request(message)?;

        // 只发布到推送队列，不持久化
        self.publisher
            .publish_push(push_request)
            .await
            .context("Failed to publish temporary message to push queue")?;

        tracing::debug!(
            message_id = %message.server_id,
            conversation_id = %message.conversation_id,
            "Temporary message published to push queue"
        );

        Ok(())
    }

    /// 构建推送请求
    fn build_push_request(
        &self,
        message: &Message,
    ) -> Result<PushMessageRequest> {
        // 提取接收者ID列表
        let mut user_ids = Vec::new();

        if let Ok(conversation_type) =
            flare_proto::common::ConversationType::try_from(message.conversation_type)
        {
            match conversation_type {
                flare_proto::common::ConversationType::Single => {
                    // 单聊：使用 receiver_id
                    if !message.receiver_id.is_empty() {
                        user_ids.push(message.receiver_id.clone());
                    }
                }
                flare_proto::common::ConversationType::Group
                | flare_proto::common::ConversationType::Channel => {
                    // 群聊、频道：user_ids 留空，由推送服务根据 channel_id/conversation_id 查询成员
                }
                _ => {}
            }
        }

        // 克隆消息
        let message_for_push = message.clone();

        // 构建推送选项（临时消息：require_online=true，不持久化）
        let push_options = PushOptions {
            require_online: true,      // 临时消息只在在线时推送
            persist_if_offline: false, // 离线不持久化
            priority: 3,               // 较低优先级
            metadata: std::collections::HashMap::new(),
            channel: String::new(),
            mute_when_quiet: false,
        };

        Ok(PushMessageRequest {
            user_ids,
            message: Some(message_for_push),
            options: Some(push_options),
            template_id: String::new(),
            template_data: std::collections::HashMap::new(),
        })
    }
}

