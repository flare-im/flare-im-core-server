//! Conversation 服务客户端（用于查询会话参与者）

use std::sync::Arc;

use flare_proto::common::{ActorContext, RequestContext, TenantContext};
use flare_proto::conversation::conversation_service_client::ConversationServiceClient as ConversationServiceClientProto;
use flare_proto::conversation::{UpdateConversationRequest, UpdateConversationResponse};
use flare_server_core::context::{Context, ContextExt};
use flare_server_core::discovery::ServiceClient;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::debug;

/// Conversation 服务客户端
pub struct ConversationServiceClient {
    service_name: String,
    service_client: Mutex<Option<ServiceClient>>,
    client: Mutex<Option<ConversationServiceClientProto<Channel>>>,
}

impl ConversationServiceClient {
    /// 创建新的客户端（使用服务名称，内部创建服务发现）
    pub fn new(service_name: String) -> Arc<Self> {
        Arc::new(Self {
            service_name,
            service_client: Mutex::new(None),
            client: Mutex::new(None),
        })
    }

    /// 使用 ServiceClient 创建新的客户端（推荐，通过 wire 注入）
    pub fn with_service_client(service_client: ServiceClient) -> Arc<Self> {
        Arc::new(Self {
            service_name: String::new(), // 不需要 service_name
            service_client: Mutex::new(Some(service_client)),
            client: Mutex::new(None),
        })
    }

    async fn ensure_client(&self) -> Result<ConversationServiceClientProto<Channel>> {
        let mut guard = self.client.lock().await;
        if let Some(client) = guard.as_ref() {
            return Ok(client.clone());
        }

        // 使用服务发现获取 Channel
        let mut service_client_guard = self.service_client.lock().await;
        if service_client_guard.is_none() {
            // 如果没有注入 ServiceClient，则创建服务发现器
            let discover = flare_im_core::discovery::create_discover(&self.service_name)
                .await
                .map_err(|e| {
                    ErrorBuilder::new(ErrorCode::ServiceUnavailable, "conversation service unavailable")
                        .details(format!(
                            "Failed to create service discover for {}: {}",
                            self.service_name, e
                        ))
                        .build_error()
                })?;

            if let Some(discover) = discover {
                *service_client_guard = Some(ServiceClient::new(discover));
            } else {
                return Err(ErrorBuilder::new(
                    ErrorCode::ServiceUnavailable,
                    "conversation service unavailable",
                )
                .details("Service discovery not configured")
                .build_error());
            }
        }

        let service_client = service_client_guard.as_mut().ok_or_else(|| {
            ErrorBuilder::new(ErrorCode::ServiceUnavailable, "conversation service unavailable")
                .details("Service client not initialized")
                .build_error()
        })?;
        // 添加超时保护，避免服务发现阻塞过长时间
        let channel = tokio::time::timeout(
            std::time::Duration::from_secs(3), // 3秒超时
            service_client.get_channel(),
        )
        .await
        .map_err(|_| {
            ErrorBuilder::new(ErrorCode::ServiceUnavailable, "conversation service unavailable")
                .details("Timeout waiting for service discovery to get channel (3s)")
                .build_error()
        })?
        .map_err(|e| {
            ErrorBuilder::new(ErrorCode::ServiceUnavailable, "conversation service unavailable")
                .details(format!("Failed to get channel: {}", e))
                .build_error()
        })?;

        debug!("Got channel for conversation service from service discovery");

        let client = ConversationServiceClientProto::new(channel);
        *guard = Some(client.clone());
        Ok(client)
    }

    /// 获取会话的所有参与者（通过 UpdateConversation 方法，只传 conversation_id 获取 Conversation 信息）
    #[tracing::instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        conversation_id = %conversation_id,
    ))]
    pub async fn get_conversation_participants(
        &self,
        ctx: &Context,
        conversation_id: &str,
    ) -> Result<Vec<String>> {
        ctx.ensure_not_cancelled().map_err(|e| {
            ErrorBuilder::new(
                ErrorCode::InternalError,
                "Request cancelled",
            )
            .details(e.to_string())
            .build_error()
        })?;
        let mut client = self.ensure_client().await?;

        // 从 Context 中提取 RequestContext 和 TenantContext（用于 protobuf 兼容性）
        let request_context: RequestContext = ctx.request().cloned().map(|rc| rc.into()).unwrap_or_else(|| RequestContext {
            request_id: ctx.request_id().to_string(),
            trace: None,
            actor: Some(ActorContext {
                actor_id: String::new(),
                r#type: 2, // ActorType::ACTOR_TYPE_SERVICE
                roles: vec![],
                attributes: std::collections::HashMap::new(),
            }),
            device: None,
            channel: String::new(),
            user_agent: String::new(),
            attributes: std::collections::HashMap::new(),
        });

        let tenant: Option<TenantContext> = ctx.tenant().cloned().map(|tc| tc.into()).or_else(|| {
            ctx.tenant_id().map(|tenant_id| TenantContext {
                tenant_id: tenant_id.to_string(),
                business_type: String::new(),
                environment: String::new(),
                organization_id: String::new(),
                labels: std::collections::HashMap::new(),
                attributes: std::collections::HashMap::new(),
            })
        });

        // 使用 UpdateConversation 方法，只传 conversation_id，其他字段留空，来获取 Conversation 信息
        let request = UpdateConversationRequest {
            conversation_id: conversation_id.to_string(),
            display_name: String::new(),                  // 留空，不更新
            attributes: std::collections::HashMap::new(), // 留空，不更新
            visibility: 0,                                // 留空，不更新
            lifecycle_state: 0,                           // 留空，不更新
        };

        let response: UpdateConversationResponse = client
            .update_conversation(tonic::Request::new(request))
            .await
            .map_err(|status| {
                ErrorBuilder::new(ErrorCode::ServiceUnavailable, "conversation query failed")
                    .details(format!("Failed to get conversation participants: {}", status))
                    .build_error()
            })?
            .into_inner();

        // 从 Conversation 中提取参与者 user_id 列表
        if let Some(conversation) = response.conversation {
            Ok(conversation
                .participants
                .into_iter()
                .map(|p| p.user_id)
                .collect())
        } else {
            Err(
                ErrorBuilder::new(ErrorCode::InvalidParameter, "conversation not found")
                    .details(format!("Conversation {} not found", conversation_id))
                    .build_error(),
            )
        }
    }
}
