//! 管理员只读查询（CQRS 读侧）：租户由接口层从已验证的 admin 令牌取得，这里只做参数规整。

use std::sync::Arc;

use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};

use crate::domain::model::{
    AdminConversation, AdminConversationPage, ConversationParticipantsPage,
};
use crate::domain::repository::ConversationAdminRepository;

pub struct ConversationAdminQueryHandler<R: ConversationAdminRepository> {
    repository: Arc<R>,
}

fn required_conversation_id(conversation_id: &str) -> Result<&str> {
    let trimmed = conversation_id.trim();
    if trimmed.is_empty() || trimmed != conversation_id {
        return Err(
            ErrorBuilder::new(ErrorCode::InvalidParameter, "conversation_id is required")
                .build_error(),
        );
    }
    Ok(conversation_id)
}

fn not_found() -> flare_server_core::error::FlareError {
    // 会话不存在与不属于令牌租户同一个回答，不泄露别的租户里有没有这个 ID。
    ErrorBuilder::new(ErrorCode::MessageNotFound, "conversation not found").build_error()
}

impl<R: ConversationAdminRepository> ConversationAdminQueryHandler<R> {
    pub fn new(repository: Arc<R>) -> Self {
        Self { repository }
    }

    pub async fn list_conversations(
        &self,
        tenant_id: &str,
        cursor: Option<&str>,
        limit: i32,
        include_deleted: bool,
    ) -> Result<AdminConversationPage> {
        self.repository
            .admin_list_conversations(tenant_id, cursor, i64::from(limit), include_deleted)
            .await
    }

    pub async fn get_conversation(
        &self,
        tenant_id: &str,
        conversation_id: &str,
    ) -> Result<AdminConversation> {
        let conversation_id = required_conversation_id(conversation_id)?;
        self.repository
            .admin_get_conversation(tenant_id, conversation_id)
            .await?
            .ok_or_else(not_found)
    }

    pub async fn list_participants(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        cursor: Option<&str>,
        limit: i32,
        include_removed: bool,
    ) -> Result<ConversationParticipantsPage> {
        let conversation_id = required_conversation_id(conversation_id)?;
        self.repository
            .admin_list_participants(tenant_id, conversation_id, cursor, limit, include_removed)
            .await?
            .ok_or_else(not_found)
    }
}
