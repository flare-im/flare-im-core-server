//! 应用层命令解析器：根据 message_id 解析会话与目标消息，并构建操作命令基座。
//!
//! 消除 operation_handler 中「查消息 → 取 conversation_id/server_msg_id → 拼 MessageOperationCommand」的重复，
//! 与 CQRS 一致：Query 侧查读模型，Command 侧只做编排。

use std::sync::Arc;

use chrono::Utc;
use flare_im_core::error::Result as FlareResult;
use flare_server_core::context::Ctx;

use crate::application::commands::MessageOperationCommand;
use crate::application::handlers::MessageQueryHandler;
use crate::application::queries::QueryMessageQuery;

/// 将查询错误映射为业务错误：未找到 → MessageNotFound，其余 → system。
#[inline]
pub fn map_message_query_error(e: impl ToString, message_id: &str) -> flare_im_core::error::FlareError {
    let s = e.to_string();
    if s.contains("not found") {
        flare_im_core::error::FlareError::localized(
            flare_im_core::error::ErrorCode::MessageNotFound,
            format!("Message not found: {}", message_id),
        )
    } else {
        flare_im_core::error::FlareError::system(&format!("Internal error: {}", s))
    }
}

/// 应用层操作命令解析器（Port）：按 message_id 查消息，返回 (conversation_id, server_msg_id)。
///
/// 供 MessageOperationHandler 各 handle_*_app 使用，避免重复「查消息 + 拼 base」。
#[derive(Clone)]
pub struct AppCommandResolver {
    query_handler: Arc<MessageQueryHandler>,
}

impl AppCommandResolver {
    pub fn new(query_handler: Arc<MessageQueryHandler>) -> Self {
        Self { query_handler }
    }

    /// 根据 message_id 查询消息，返回 (conversation_id, server_msg_id)。
    /// 若消息不存在则返回 MessageNotFound；server_msg_id 优先用消息的 server_id，否则回退为入参 message_id。
    /// 当 StorageReader 未配置时，若提供 fallback_conversation_id（非空），则直接使用 (fallback_conversation_id, message_id)，便于本地/单机开发。
    pub async fn resolve_message_for_operation(
        &self,
        message_id: &str,
        fallback_conversation_id: Option<&str>,
    ) -> FlareResult<(String, String)> {
        let query = QueryMessageQuery {
            message_id: message_id.to_string(),
            conversation_id: String::new(),
        };
        match self.query_handler.query_message(query).await {
            Ok(msg) => {
                let server_msg_id = if msg.server_id.is_empty() {
                    message_id.to_string()
                } else {
                    msg.server_id
                };
                Ok((msg.conversation_id, server_msg_id))
            }
            Err(e) => {
                if let Some(fallback) = fallback_conversation_id.filter(|s| !s.trim().is_empty()) {
                    tracing::warn!(
                        message_id = %message_id,
                        conversation_id = %fallback,
                        error = %e,
                        "Storage Reader query failed, using fallback conversation_id"
                    );
                    return Ok((fallback.trim().to_string(), message_id.to_string()));
                }
                Err(map_message_query_error(e, message_id))
            }
        }
    }

    /// 从 Context 构建 MessageOperationCommand 基座，供撤回/编辑/删除/已读/反应/置顶/标记等命令复用。
    pub fn build_operation_base(
        &self,
        ctx: &Ctx,
        conversation_id: String,
        message_id: String,
    ) -> MessageOperationCommand {
        MessageOperationCommand {
            message_id,
            operator_id: ctx
                .actor()
                .map(|a| a.actor_id().to_string())
                .unwrap_or_default(),
            timestamp: Utc::now(),
            tenant_id: ctx.tenant_id().unwrap_or("0").to_string(),
            conversation_id,
        }
    }
}
