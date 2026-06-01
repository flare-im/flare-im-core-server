use std::sync::Arc;

use flare_server_core::context::Context;

use crate::error::{ErrorBuilder, ErrorCode, Result, require_user_id};
use tracing::debug;

use crate::application::commands::{
    BatchAcknowledgeCommand, CreateConversationCommand, DeleteConversationCommand,
    ForceConversationSyncCommand, ManageParticipantsCommand, MarkConversationAsReadCommand,
    UpdateConversationCommand, UpdateConversationUserSettingsCommand, UpdateCursorCommand,
    UpdatePresenceCommand,
};
use crate::application::queries::{
    ConversationBootstrapQuery, GetConversationDetailQuery, ListConversationsQuery,
    SearchConversationsQuery, SyncMessagesQuery,
};
use crate::domain::service::DefaultConversationDomainService;
use crate::domain::service::conversation_domain_service::ConversationBootstrapOutput;
use crate::infrastructure::persistence::PostgresConversationRepository;
use crate::infrastructure::rpc::StorageReaderClient;

/// 会话命令处理器
///
/// 使用 DefaultConversationDomainService 类型别名
/// 该类型使用具体的 Redis 实现，提供零开销的静态分发
pub struct ConversationCommandHandler {
    domain_service: Arc<DefaultConversationDomainService>,
}

impl ConversationCommandHandler {
    pub fn new(domain_service: Arc<DefaultConversationDomainService>) -> Self {
        Self { domain_service }
    }

    /// 处理批量确认命令
    pub async fn handle_batch_acknowledge(
        &self,
        ctx: &Context,
        command: BatchAcknowledgeCommand,
    ) -> Result<()> {
        let user_id = require_user_id(ctx)?;

        debug!(
            user_id = %user_id,
            count = command.cursors.len(),
            "Handling batch acknowledge command"
        );

        self.domain_service
            .batch_acknowledge(ctx, command.cursors)
            .await?;

        debug!(user_id = %user_id, "Batch acknowledge completed");
        Ok(())
    }

    /// 处理创建会话命令
    pub async fn handle_create_conversation(
        &self,
        ctx: &Context,
        command: CreateConversationCommand,
    ) -> Result<crate::domain::model::Conversation> {
        debug!(
            conversation_type = command.conversation_type.as_int(),
            business_type = %command.business_type,
            "Handling create session command"
        );

        let session = self
            .domain_service
            .create_conversation(
                ctx,
                command.conversation_type,
                command.business_type,
                command.participants,
                command.attributes,
                command.visibility,
                command.channel_id,
            )
            .await?;

        debug!(conversation_id = %session.conversation_id, "Conversation created");
        Ok(session)
    }

    /// 处理删除会话命令
    pub async fn handle_delete_conversation(
        &self,
        ctx: &Context,
        command: DeleteConversationCommand,
    ) -> Result<()> {
        debug!(
            conversation_id = %command.conversation_id,
            hard_delete = command.hard_delete,
            "Handling delete session command"
        );

        self.domain_service
            .delete_conversation(ctx, &command.conversation_id, command.hard_delete)
            .await?;

        debug!(conversation_id = %command.conversation_id, "Conversation deleted");
        Ok(())
    }

    /// 处理强制会话同步命令
    pub async fn handle_force_conversation_sync(
        &self,
        ctx: &Context,
        command: ForceConversationSyncCommand,
    ) -> Result<Vec<String>> {
        let user_id = require_user_id(ctx)?;

        debug!(
            user_id = %user_id,
            session_count = command.conversation_ids.len(),
            "Handling force session sync command"
        );

        let missing = self
            .domain_service
            .force_conversation_sync(ctx, &command.conversation_ids, command.reason.as_deref())
            .await?;

        Ok(missing)
    }

    /// 处理管理参与者命令
    pub async fn handle_manage_participants(
        &self,
        ctx: &Context,
        command: ManageParticipantsCommand,
    ) -> Result<Vec<crate::domain::model::ConversationParticipant>> {
        debug!(
            conversation_id = %command.conversation_id,
            to_add = command.to_add.len(),
            to_remove = command.to_remove.len(),
            "Handling manage participants command"
        );

        let participants = self
            .domain_service
            .manage_participants(
                ctx,
                &command.conversation_id,
                command.to_add,
                command.to_remove,
                command.role_updates,
            )
            .await?;

        debug!(conversation_id = %command.conversation_id, "Participants managed");
        Ok(participants)
    }

    /// 处理更新游标命令
    pub async fn handle_update_cursor(
        &self,
        ctx: &Context,
        command: UpdateCursorCommand,
    ) -> Result<()> {
        let user_id = require_user_id(ctx)?;

        debug!(
            user_id = %user_id,
            conversation_id = %command.conversation_id,
            sync_seq = command.sync_seq,
            "Handling update cursor command"
        );

        self.domain_service
            .update_cursor(ctx, &command.conversation_id, command.sync_seq)
            .await?;

        Ok(())
    }

    /// 标记会话已读（编排 → 领域 `mark_as_read`）
    pub async fn handle_mark_conversation_as_read(
        &self,
        ctx: &Context,
        command: MarkConversationAsReadCommand,
    ) -> Result<()> {
        let user_id = require_user_id(ctx)?;

        debug!(
            user_id = %user_id,
            conversation_id = %command.conversation_id,
            read_seq = command.read_seq,
            "Handling mark conversation as read"
        );

        self.domain_service
            .mark_as_read(ctx, &command.conversation_id, command.read_seq)
            .await?;

        Ok(())
    }

    pub async fn handle_update_conversation_user_settings(
        &self,
        ctx: &Context,
        command: UpdateConversationUserSettingsCommand,
    ) -> Result<crate::domain::model::ConversationUserSettings> {
        let user_id = require_user_id(ctx)?;
        debug!(
            user_id = %user_id,
            conversation_id = %command.conversation_id,
            base_settings_version = command.base_settings_version,
            "Handling update conversation user settings"
        );
        self.domain_service
            .update_user_settings(
                ctx,
                &crate::domain::model::UpdateConversationUserSettingsPatch {
                    conversation_id: command.conversation_id,
                    is_pinned: command.is_pinned,
                    is_muted: command.is_muted,
                    is_archived: command.is_archived,
                    draft: command.draft,
                    base_settings_version: command.base_settings_version,
                },
            )
            .await
    }

    /// 处理更新设备状态命令
    pub async fn handle_update_presence(
        &self,
        ctx: &Context,
        command: UpdatePresenceCommand,
    ) -> Result<()> {
        let user_id = require_user_id(ctx)?;

        debug!(
            user_id = %user_id,
            device_id = %command.device_id,
            state = ?command.state,
            "Handling update presence command"
        );

        self.domain_service
            .update_presence(
                ctx,
                &command.device_id,
                command.device_platform,
                command.state,
                command.conflict_resolution,
                command.notify_conflict,
                command.conflict_reason,
            )
            .await?;

        Ok(())
    }

    /// 处理更新会话命令
    pub async fn handle_update_conversation(
        &self,
        ctx: &Context,
        command: UpdateConversationCommand,
    ) -> Result<crate::domain::model::Conversation> {
        debug!(
            conversation_id = %command.conversation_id,
            "Handling update conversation command"
        );

        let conversation = self
            .domain_service
            .update_conversation(
                ctx,
                &command.conversation_id,
                command.display_name,
                command.attributes,
                command.visibility,
                command.lifecycle_state,
            )
            .await?;

        debug!(conversation_id = %command.conversation_id, "Conversation updated");
        Ok(conversation)
    }
}

/// 会话查询处理器
pub struct ConversationQueryHandler {
    domain_service: Arc<DefaultConversationDomainService>,
}

impl ConversationQueryHandler {
    pub fn new(
        _conversation_repo: Arc<PostgresConversationRepository>,
        _message_provider: Option<Arc<StorageReaderClient>>,
        domain_service: Arc<DefaultConversationDomainService>,
    ) -> Self {
        Self { domain_service }
    }

    /// 处理列出会话查询
    pub async fn handle_list_conversations(
        &self,
        ctx: &Context,
        query: ListConversationsQuery,
    ) -> Result<(
        Vec<crate::domain::model::ConversationSummary>,
        Option<String>,
        bool,
    )> {
        let user_id = require_user_id(ctx)?;

        debug!(
            user_id = %user_id,
            cursor = ?query.cursor,
            limit = query.limit,
            "Handling list sessions query"
        );

        let result = self
            .domain_service
            .list_conversations(ctx, query.cursor.as_deref(), query.limit)
            .await?;

        Ok(result)
    }

    /// 单会话详情：必须已认证，且当前用户须为参与者（非成员与不存在统一返回 NOT_FOUND 语义错误码由 gRPC 层映射）。
    pub async fn handle_get_conversation_detail(
        &self,
        ctx: &Context,
        query: GetConversationDetailQuery,
    ) -> Result<crate::domain::model::Conversation> {
        let user_id = require_user_id(ctx)?;
        if query.conversation_id.trim().is_empty() {
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "conversation_id is required",
            )
            .build_error());
        }

        let Some(conv) = self
            .domain_service
            .get_conversation(ctx, query.conversation_id.trim())
            .await?
        else {
            return Err(
                ErrorBuilder::new(ErrorCode::MessageNotFound, "conversation not found")
                    .build_error(),
            );
        };

        let member = conv.participants.iter().any(|p| p.user_id == user_id);
        if !member {
            return Err(
                ErrorBuilder::new(ErrorCode::MessageNotFound, "conversation not found")
                    .build_error(),
            );
        }

        Ok(conv)
    }

    pub async fn handle_list_conversation_participants(
        &self,
        ctx: &Context,
        query: crate::application::queries::ListConversationParticipantsQuery,
    ) -> Result<crate::domain::model::ConversationParticipantsPage> {
        if query.conversation_id.trim().is_empty() {
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "conversation_id is required",
            )
            .build_error());
        }

        self.domain_service
            .list_conversation_participants(
                ctx,
                query.conversation_id.trim(),
                query.cursor.as_deref(),
                query.limit,
                query.include_removed,
            )
            .await
    }

    /// 处理搜索会话查询
    pub async fn handle_search_conversations(
        &self,
        ctx: &Context,
        query: SearchConversationsQuery,
    ) -> Result<(Vec<crate::domain::model::ConversationSummary>, usize)> {
        debug!(
            filter_count = query.filters.len(),
            sort_count = query.sort.len(),
            limit = query.limit,
            "Handling search conversations query"
        );

        let result = self
            .domain_service
            .search_conversations(ctx, query.filters, query.sort, query.limit, query.offset)
            .await?;

        Ok(result)
    }

    /// 处理会话引导查询
    pub async fn handle_conversation_bootstrap(
        &self,
        ctx: &Context,
        query: ConversationBootstrapQuery,
    ) -> Result<ConversationBootstrapOutput> {
        let user_id = require_user_id(ctx)?;

        debug!(
            user_id = %user_id,
            include_recent = query.include_recent,
            "Handling session bootstrap query"
        );

        let result = self
            .domain_service
            .bootstrap_conversation(
                ctx,
                query.client_cursor,
                query.include_recent,
                query.recent_limit,
            )
            .await?;

        Ok(result)
    }

    /// 处理同步消息查询
    pub async fn handle_sync_messages(
        &self,
        ctx: &Context,
        query: SyncMessagesQuery,
    ) -> Result<crate::domain::model::MessageSyncResult> {
        debug!(
            conversation_id = %query.conversation_id,
            since_ts = query.since_ts,
            cursor = ?query.cursor,
            limit = query.limit,
            "Handling sync messages query"
        );

        let result = self
            .domain_service
            .sync_messages(
                ctx,
                &query.conversation_id,
                query.since_ts,
                query.cursor.as_deref(),
                query.limit,
            )
            .await?;

        Ok(result)
    }
}
