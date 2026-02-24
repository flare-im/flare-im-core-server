use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::error::{FlareError, Result};
use flare_proto::conversation::conversation_service_client::ConversationServiceClient;
use flare_proto::conversation::{CreateConversationRequest, ConversationParticipant};
use flare_server_core::context::{Context, ContextExt};
use flare_server_core::client::set_context_metadata;
use tonic::transport::Channel;
use tracing::{debug, warn, instrument};

use crate::domain::repository::ConversationRepository;

/// gRPC Conversation 客户端（外部依赖）
#[derive(Debug)]
pub struct GrpcConversationClient {
    client: Arc<tokio::sync::Mutex<ConversationServiceClient<Channel>>>,
}

impl GrpcConversationClient {
    pub fn new(client: ConversationServiceClient<Channel>) -> Self {
        Self {
            client: Arc::new(tokio::sync::Mutex::new(client)),
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
        };

        let client = Arc::clone(&self.client);
        Box::pin(async move {
            let mut grpc_request = tonic::Request::new(request);
            // 使用 set_context_metadata 注入 Context 到 gRPC 请求的 metadata
            set_context_metadata(&mut grpc_request, ctx);
            
            // 调试：验证 tenant_id 是否正确设置到 metadata
            let tenant_id_in_ctx = ctx.tenant_id();
            let tenant_id_in_metadata = grpc_request.metadata().get("x-tenant-id")
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
                        Err(FlareError::system(format!("Failed to ensure conversation: {}", e)))
                    }
                }
            }
        })
    }
}
