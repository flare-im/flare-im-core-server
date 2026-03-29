use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::error::{FlareError, Result};
use flare_proto::conversation::conversation_manage_service_client::ConversationManageServiceClient;
use flare_proto::conversation::{
    ConversationParticipant, CreateConversationRequest, MarkConversationAsReadRequest,
};
use flare_server_core::client::set_context_metadata;
use flare_server_core::context::{Context, ContextExt};
use tonic::transport::Channel;
use tracing::{debug, info, instrument, warn};

use crate::domain::repository::ConversationRepository;

/// gRPC Conversation 客户端（外部依赖）
#[derive(Debug)]
pub struct GrpcConversationClient {
    manage_client: Arc<tokio::sync::Mutex<ConversationManageServiceClient<Channel>>>,
}

impl GrpcConversationClient {
    pub fn new(channel: Channel) -> Self {
        Self {
            manage_client: Arc::new(tokio::sync::Mutex::new(
                ConversationManageServiceClient::new(channel),
            )),
        }
    }
}

impl ConversationRepository for GrpcConversationClient {
    #[instrument(skip(self, ctx, participants), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        conversation_id = %conversation_id,
    ))]
    fn ensure_conversation<'a>(
        &'a self,
        ctx: &'a Context,
        conversation_id: &'a str,
        conversation_type: &'a str,
        business_type: &'a str,
        participants: Vec<String>,
        stored_channel_id: String,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        ctx.ensure_not_cancelled().ok(); // 忽略取消错误，继续处理

        debug!(
            tenant_id = ctx.tenant_id().unwrap_or("none"),
            "Context for ensure_conversation"
        );

        let conversation_id = conversation_id.to_string();
        let conversation_type = conversation_type.to_string();
        let business_type = business_type.to_string();
        let participants = participants; // 移动 participants
        // `stored_channel_id` 已由 [MessageDomainService] 按会话类型归一（单聊为空）

        // 将 conversation_id 放入 attributes，确保会话服务使用传入的 conversation_id
        let mut attributes = std::collections::HashMap::new();
        attributes.insert("conversation_id".to_string(), conversation_id.clone());

        let request = CreateConversationRequest {
            conversation_type: conversation_type.clone(),
            business_type: business_type.clone(),
            participants: participants
                .into_iter()
                .map(|p| ConversationParticipant {
                    user_id: p,
                    roles: vec![],
                    muted: false,
                    pinned: false,
                    attributes: std::collections::HashMap::new(),
                })
                .collect(),
            attributes,
            visibility: 0, // SessionVisibility::SessionVisibilityPrivate
            channel_id: stored_channel_id.clone(),
        };

        let client = Arc::clone(&self.manage_client);
        Box::pin(async move {
            let mut grpc_request = tonic::Request::new(request);
            set_context_metadata(&mut grpc_request, ctx);

            let tenant_id_in_ctx = ctx.tenant_id();
            let tenant_id_in_metadata = grpc_request
                .metadata()
                .get("x-tenant-id")
                .and_then(|v| v.to_str().ok());
            debug!(
                tenant_id_in_ctx = tenant_id_in_ctx.unwrap_or("none"),
                tenant_id_in_metadata = tenant_id_in_metadata.unwrap_or("none"),
                "Context metadata before gRPC call"
            );

            let mut client = client.lock().await;
            match client.create_conversation(grpc_request).await {
                Ok(response) => {
                    let inner = response.into_inner();
                    if let Some(conv) = inner.conversation {
                        debug!(
                            conversation_id = %conv.conversation_id,
                            "Conversation ensured (created or already exists)"
                        );
                        // 验证返回的 conversation_id 是否与请求的一致
                        if conv.conversation_id != conversation_id {
                            warn!(
                                requested_conversation_id = %conversation_id,
                                returned_conversation_id = %conv.conversation_id,
                                "Conversation ID mismatch: requested vs returned"
                            );
                        }
                    }
                    Ok(())
                }
                Err(e) => {
                    // 如果会话已存在，可能会返回错误，这里我们忽略该错误
                    if e.code() == tonic::Code::AlreadyExists {
                        debug!(conversation_id = %conversation_id, "Conversation already exists, skipping creation");
                        Ok(())
                    } else {
                        warn!(
                            error = %e,
                            conversation_id = %conversation_id,
                            "Failed to ensure conversation"
                        );
                        Err(FlareError::system(format!(
                            "Failed to ensure conversation: {}",
                            e
                        )))
                    }
                }
            }
        })
    }

    /// 标记会话已读（供 Message Orchestrator 调用 Conversation 的 MarkConversationAsRead RPC）
    fn mark_conversation_as_read<'a>(
        &'a self,
        ctx: &'a Context,
        conversation_id: &'a str,
        read_seq: i64,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        let request = MarkConversationAsReadRequest {
            conversation_id: conversation_id.to_string(),
            read_seq,
        };
        let mut grpc_request = tonic::Request::new(request);
        set_context_metadata(&mut grpc_request, ctx);
        let client = Arc::clone(&self.manage_client);
        let conversation_id = conversation_id.to_string();
        Box::pin(async move {
            let mut client = client.lock().await;
            client
                .mark_conversation_as_read(grpc_request)
                .await
                .map_err(|e| {
                    FlareError::system(format!("Conversation MarkConversationAsRead failed: {}", e))
                })?;
            info!(
                conversation_id = %conversation_id,
                read_seq,
                "Mark conversation as read via Conversation service"
            );
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// Conversation 仓储枚举（单变体 Grpc，避免 dyn + async trait 不兼容）
// ---------------------------------------------------------------------------

/// ConversationRepository 的枚举封装。
#[derive(Debug)]
pub enum ConversationRepositoryItem {
    Grpc(Arc<GrpcConversationClient>),
}

impl ConversationRepository for ConversationRepositoryItem {
    fn ensure_conversation<'a>(
        &'a self,
        ctx: &'a Context,
        conversation_id: &'a str,
        conversation_type: &'a str,
        business_type: &'a str,
        participants: Vec<String>,
        stored_channel_id: String,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        match self {
            ConversationRepositoryItem::Grpc(repo) => repo.ensure_conversation(
                ctx,
                conversation_id,
                conversation_type,
                business_type,
                participants,
                stored_channel_id,
            ),
        }
    }

    fn mark_conversation_as_read<'a>(
        &'a self,
        ctx: &'a Context,
        conversation_id: &'a str,
        read_seq: i64,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        match self {
            ConversationRepositoryItem::Grpc(repo) => Box::pin(async move {
                repo.mark_conversation_as_read(ctx, conversation_id, read_seq)
                    .await
            }),
        }
    }
}
