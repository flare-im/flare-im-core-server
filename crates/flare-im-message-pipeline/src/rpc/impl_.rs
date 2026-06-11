//! Tonic 框架的 RPC 客户端实现
//!
//! 本模块提供基于 tonic 的 RPC 客户端具体实现。
//! 未来切换到其他框架时，仅需修改本文件，trait 定义保持不变。

use super::ConversationRpcClient;
use crate::model::ConversationType;
use flare_grpc_proto::conversation::conversation_manage_service_client::ConversationManageServiceClient;
use flare_grpc_proto::conversation::conversation_read_service_client::ConversationReadServiceClient;
use flare_grpc_proto::conversation::{
    CreateConversationRequest, ListConversationParticipantsRequest, ManageParticipantsRequest,
    MarkConversationAsReadRequest,
};
use flare_proto::common::ConversationParticipant;
use flare_server_core::client::set_context_metadata;
use flare_server_core::context::{Context, ContextExt};
use flare_server_core::error::{FlareError, Result};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tonic::transport::Channel;
use tracing::{debug, info, instrument, warn};

/// 会话服务客户端（基于 tonic）
///
/// 使用 tonic 框架实现的会话服务客户端，
/// 通过 gRPC 调用下游的 Conversation 服务。
#[derive(Debug)]
pub struct ConversationClient {
    manage_client: Arc<tokio::sync::Mutex<ConversationManageServiceClient<Channel>>>,
    read_client: Arc<tokio::sync::Mutex<ConversationReadServiceClient<Channel>>>,
}

impl ConversationClient {
    /// 创建新的会话服务客户端
    ///
    /// # 参数
    /// - `channel`: tonic Channel，已连接到 Conversation 服务
    pub fn new(channel: Channel) -> Self {
        Self {
            manage_client: Arc::new(tokio::sync::Mutex::new(
                ConversationManageServiceClient::new(channel.clone()),
            )),
            read_client: Arc::new(tokio::sync::Mutex::new(ConversationReadServiceClient::new(
                channel,
            ))),
        }
    }
}

impl ConversationRpcClient for ConversationClient {
    #[instrument(skip(self, ctx, participants), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        conversation_id = %conversation_id,
    ))]
    fn ensure_conversation<'a>(
        &'a self,
        ctx: &'a Context,
        conversation_id: &'a str,
        conversation_type: ConversationType,
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
        let conversation_type_proto = conversation_type.as_proto();
        let business_type = business_type.to_string();

        // 将 conversation_id 放入 attributes，确保会话服务使用传入的 conversation_id
        let mut attributes = std::collections::HashMap::new();
        attributes.insert("conversation_id".to_string(), conversation_id.clone());

        let participant_protos: Vec<_> = participants
            .into_iter()
            .map(|p| ConversationParticipant {
                user_id: p,
                roles: vec![],
                muted: false,
                pinned: false,
                attributes: std::collections::HashMap::new(),
                joined_at: 0,
            })
            .collect();

        let request = CreateConversationRequest {
            conversation_type: conversation_type_proto,
            business_type: business_type.clone(),
            participants: participant_protos.clone(),
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
                    // 如果会话已存在，仍然要补齐参与者。历史脏数据或并发创建可能留下
                    // 只有发送方的 conversation_participants，直接忽略会导致收件人无法同步会话。
                    if e.code() == tonic::Code::AlreadyExists {
                        debug!(conversation_id = %conversation_id, "Conversation already exists, repairing participants");
                        let repair_request = ManageParticipantsRequest {
                            conversation_id: conversation_id.clone(),
                            to_add: participant_protos,
                            to_remove: vec![],
                            role_updates: vec![],
                        };
                        let mut grpc_request = tonic::Request::new(repair_request);
                        set_context_metadata(&mut grpc_request, ctx);
                        client
                            .manage_participants(grpc_request)
                            .await
                            .map_err(|e| {
                                warn!(
                                    error = %e,
                                    conversation_id = %conversation_id,
                                    "Failed to repair conversation participants"
                                );
                                FlareError::system(format!(
                                    "Failed to repair conversation participants: {}",
                                    e
                                ))
                            })?;
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

    fn get_conversation_members<'a>(
        &'a self,
        ctx: &'a Context,
        conversation_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + 'a>> {
        let client = Arc::clone(&self.read_client);
        let conversation_id = conversation_id.to_string();
        Box::pin(async move {
            let mut member_ids = Vec::new();
            let mut cursor = String::new();
            loop {
                let mut grpc_request = tonic::Request::new(ListConversationParticipantsRequest {
                    conversation_id: conversation_id.clone(),
                    cursor: cursor.clone(),
                    limit: 500,
                    include_removed: false,
                    ext: Default::default(),
                });
                set_context_metadata(&mut grpc_request, ctx);

                let mut client = client.lock().await;
                let response = client
                    .list_conversation_participants(grpc_request)
                    .await
                    .map_err(|e| {
                        FlareError::system(format!("ListConversationParticipants failed: {}", e))
                    })?;
                let page = response.into_inner();
                member_ids.extend(
                    page.participants
                        .into_iter()
                        .map(|p| p.user_id)
                        .filter(|id| !id.trim().is_empty()),
                );
                if !page.has_more {
                    break;
                }
                cursor = page.next_cursor;
            }
            debug!(
                conversation_id = %conversation_id,
                member_count = member_ids.len(),
                "Retrieved conversation members"
            );
            Ok(member_ids)
        })
    }

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

        let client = Arc::clone(&self.manage_client);
        let conversation_id = conversation_id.to_string();
        Box::pin(async move {
            let mut grpc_request = tonic::Request::new(request);
            set_context_metadata(&mut grpc_request, ctx);

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
