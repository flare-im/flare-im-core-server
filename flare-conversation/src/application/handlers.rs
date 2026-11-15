use std::sync::Arc;

use flare_server_core::context::Context;

use flare_server_core::error::{ErrorBuilder, ErrorCode, Result, require_user_id};
use tracing::debug;

use crate::application::commands::{
    CreateConversationCommand, DeleteConversationCommand, ForceConversationSyncCommand,
    ManageParticipantsCommand, MarkConversationAsReadCommand, PurgeConversationHistoryCommand,
    UpdateConversationCommand, UpdateConversationUserSettingsCommand, UpdateCursorCommand,
    UpdatePresenceCommand, UpdateUserNotificationSettingsCommand,
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

    /// 处理按时间边界清除会话历史命令
    pub async fn handle_purge_conversation_history(
        &self,
        ctx: &Context,
        command: PurgeConversationHistoryCommand,
    ) -> Result<crate::domain::model::ConversationHistoryPurge> {
        debug!(
            conversation_id = %command.conversation_id,
            before_ms = command.before_ms,
            before_epoch = command.before_epoch,
            "Handling purge conversation history command"
        );
        self.domain_service
            .purge_history(
                ctx,
                &command.conversation_id,
                command.before_ms,
                &command.epoch_attribute,
                command.before_epoch,
            )
            .await
    }

    /// Advance the history cutoff after a verified service operation.
    pub async fn handle_clear_history_for_everyone(
        &self,
        ctx: &Context,
        conversation_id: &str,
    ) -> Result<u64> {
        self.domain_service
            .clear_history_for_everyone(ctx, conversation_id)
            .await
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
                    is_mention_only: command.is_mention_only,
                    is_pinned: command.is_pinned,
                    is_muted: command.is_muted,
                    is_archived: command.is_archived,
                    draft: command.draft,
                    base_settings_version: command.base_settings_version,
                    clear_history_through_seq: command.clear_history_through_seq,
                    notify_all: command.notify_all,
                },
            )
            .await
    }

    /// 部分更新调用者本人的用户级通知设置：请求字段先校验成领域补丁（至少改一项、勿扰时段不能
    /// 既设又取消、取值在已知范围内），再按 `base_settings_version` 做乐观并发写入。
    pub async fn handle_update_user_notification_settings(
        &self,
        ctx: &Context,
        command: UpdateUserNotificationSettingsCommand,
    ) -> Result<crate::domain::model::UserNotificationSettings> {
        let user_id = settings_owner(ctx)?;
        let patch = crate::domain::model::UserNotificationSettingsPatch::from_request(
            command.quiet_hours,
            command.clear_quiet_hours,
            command.preview,
            command.multi_device,
            command.base_settings_version,
        )?;
        debug!(
            user_id = %user_id,
            base_settings_version = command.base_settings_version,
            "Handling update user notification settings"
        );
        self.domain_service
            .update_user_notification_settings(ctx, user_id, &patch)
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
    conversation_repo: Arc<PostgresConversationRepository>,
    domain_service: Arc<DefaultConversationDomainService>,
}

impl ConversationQueryHandler {
    pub fn new(
        conversation_repo: Arc<PostgresConversationRepository>,
        _message_provider: Option<Arc<StorageReaderClient>>,
        domain_service: Arc<DefaultConversationDomainService>,
    ) -> Self {
        Self {
            conversation_repo,
            domain_service,
        }
    }

    pub(crate) async fn external_roster_channel(
        &self,
        ctx: &Context,
        conversation_id: &str,
        provider_configured: bool,
    ) -> Result<Option<(String, bool)>> {
        self.conversation_repo
            .external_roster_channel(ctx, conversation_id, provider_configured)
            .await
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
                query.skip_metadata,
                query.scope,
            )
            .await
    }

    /// 处理按 id 查成员（未退出；隐藏了会话的也算）：id 须非空、互不相同、不超过
    /// [`MAX_PARTICIPANT_LOOKUP_IDS`](crate::application::queries::MAX_PARTICIPANT_LOOKUP_IDS)。
    /// 上行事件的成员核对、插件资源桥的成员核对走这条。
    pub async fn handle_find_conversation_members(
        &self,
        ctx: &Context,
        query: crate::application::queries::FindConversationMembersQuery,
    ) -> Result<Vec<crate::domain::model::ConversationParticipant>> {
        let conversation_id = valid_lookup(&query)?;
        self.domain_service
            .find_conversation_members(ctx, conversation_id, &query.user_ids)
            .await
    }

    /// 处理「某用户有新未读消息的会话」查询：`user_id` 非空且不带首尾空白；`since_ms` 须是可
    /// 表示的时刻；`limit` 为 0 或超过上限时取
    /// [`MAX_RECENT_UNREAD_CONVERSATIONS`](crate::application::queries::MAX_RECENT_UNREAD_CONVERSATIONS)。
    /// 调用者的身份（只能是 Cell 内服务）由接口层核对，这里按请求里的用户读。
    pub async fn handle_list_recent_unread_conversations(
        &self,
        ctx: &Context,
        query: crate::application::queries::ListRecentUnreadConversationsQuery,
    ) -> Result<Vec<crate::domain::model::RecentUnreadConversation>> {
        let (user_id, since, limit) = valid_recent_unread_query(&query)?;
        self.domain_service
            .list_recent_unread_conversations(ctx, user_id, since, limit)
            .await
    }

    /// 调用者本人的用户级通知设置；从没设置过时是默认值（版本 0）。
    pub async fn handle_get_user_notification_settings(
        &self,
        ctx: &Context,
    ) -> Result<crate::domain::model::UserNotificationSettings> {
        let user_id = settings_owner(ctx)?;
        self.domain_service
            .get_user_notification_settings(ctx, user_id)
            .await
    }

    /// 一批用户的用户级通知设置（服务身份按用户读，身份由接口层核对）：用户非空、不带首尾空白、
    /// 互不相同，1 到
    /// [`MAX_NOTIFICATION_SETTINGS_BATCH`](crate::domain::model::notification_settings::MAX_NOTIFICATION_SETTINGS_BATCH)
    /// 个。结果与请求一一对应，没设置过的用户是默认值。
    pub async fn handle_batch_get_user_notification_settings(
        &self,
        ctx: &Context,
        query: crate::application::queries::BatchGetUserNotificationSettingsQuery,
    ) -> Result<Vec<(String, crate::domain::model::UserNotificationSettings)>> {
        valid_settings_batch(&query.user_ids)?;
        self.domain_service
            .batch_get_user_notification_settings(ctx, &query.user_ids)
            .await
    }

    /// 会话里开启了「全部消息通知」的成员的一页（服务身份读，身份由接口层核对）：会话 ID 非空且
    /// 不带首尾空白；页令牌是上一页回的；页大小 0 或超过上限时取上限；整个列表封顶
    /// [`MAX_NOTIFICATION_SUBSCRIBERS`](crate::domain::model::notification_settings::MAX_NOTIFICATION_SUBSCRIBERS)。
    pub async fn handle_list_notification_subscribers(
        &self,
        ctx: &Context,
        query: crate::application::queries::ListNotificationSubscribersQuery,
    ) -> Result<crate::domain::model::NotificationSubscriberPage> {
        let (cursor, page_size) = valid_subscriber_query(&query)?;
        self.domain_service
            .list_notification_subscribers(ctx, &query.conversation_id, &cursor, page_size)
            .await
    }

    /// 处理已读摘要查询：会话 ID 非空；消息 ID 非空、互不相同、不超过
    /// [`MAX_READ_SUMMARY_MESSAGES`](crate::domain::model::read_receipt::MAX_READ_SUMMARY_MESSAGES)。
    pub async fn handle_get_message_read_summaries(
        &self,
        ctx: &Context,
        query: crate::application::queries::GetMessageReadSummariesQuery,
    ) -> Result<crate::domain::model::MessageReadSummaries> {
        use crate::domain::model::read_receipt::MAX_READ_SUMMARY_MESSAGES;
        let conversation_id = query.conversation_id.trim();
        let distinct = query
            .server_msg_ids
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        if conversation_id.is_empty()
            || query.server_msg_ids.is_empty()
            || query.server_msg_ids.len() > MAX_READ_SUMMARY_MESSAGES
            || distinct != query.server_msg_ids.len()
            || query.server_msg_ids.iter().any(|id| id.trim().is_empty())
        {
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "conversation_id and 1..=50 distinct server_msg_ids are required",
            )
            .build_error());
        }
        self.domain_service
            .message_read_summaries(ctx, conversation_id, &query.server_msg_ids)
            .await
    }

    /// 处理已读名单查询：会话与消息 ID 非空，`limit` 在 `0..=MAX_READER_PAGE`（0 取默认）。
    pub async fn handle_list_message_readers(
        &self,
        ctx: &Context,
        query: crate::application::queries::ListMessageReadersQuery,
    ) -> Result<crate::domain::model::MessageReaderPage> {
        use crate::domain::model::read_receipt::{DEFAULT_READER_PAGE, MAX_READER_PAGE};
        let conversation_id = query.conversation_id.trim();
        let server_msg_id = query.server_msg_id.trim();
        let limit = match usize::try_from(query.limit) {
            Ok(0) => Some(DEFAULT_READER_PAGE),
            Ok(limit) if limit <= MAX_READER_PAGE => Some(limit),
            _ => None,
        };
        let (Some(limit), false, false) =
            (limit, conversation_id.is_empty(), server_msg_id.is_empty())
        else {
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "conversation_id, server_msg_id and a limit of 0..=200 are required",
            )
            .build_error());
        };
        self.domain_service
            .message_readers(
                ctx,
                conversation_id,
                server_msg_id,
                query.state,
                query.cursor.trim(),
                limit,
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
                query.updated_after_ms,
                query.max_conversations,
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

/// 「某用户有新未读消息的会话」的请求：返回要读的用户、时间下界与条数上限。
fn valid_recent_unread_query(
    query: &crate::application::queries::ListRecentUnreadConversationsQuery,
) -> Result<(&str, chrono::DateTime<chrono::Utc>, usize)> {
    use crate::application::queries::MAX_RECENT_UNREAD_CONVERSATIONS;
    let user_id = query.user_id.as_str();
    if user_id.trim().is_empty() || user_id.trim() != user_id {
        return Err(
            ErrorBuilder::new(ErrorCode::InvalidParameter, "user_id is required").build_error(),
        );
    }
    let Some(since) = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(query.since_ms) else {
        return Err(ErrorBuilder::new(
            ErrorCode::InvalidParameter,
            "since must be a Unix epoch millisecond timestamp",
        )
        .build_error());
    };
    let limit = match usize::try_from(query.limit) {
        Ok(limit) if (1..=MAX_RECENT_UNREAD_CONVERSATIONS).contains(&limit) => limit,
        _ => MAX_RECENT_UNREAD_CONVERSATIONS,
    };
    Ok((user_id, since, limit))
}

/// 用户级通知设置的主人：调用者本人。没有用户、或用户带首尾空白都拒绝，不替调用方猜。
fn settings_owner(ctx: &Context) -> Result<&str> {
    match ctx.user_id() {
        Some(user_id) if !user_id.trim().is_empty() && user_id.trim() == user_id => Ok(user_id),
        _ => Err(
            ErrorBuilder::new(ErrorCode::AuthenticationRequired, "user_id is required")
                .build_error(),
        ),
    }
}

/// 批量读用户级通知设置的用户列表：1 到上限个，非空、不带首尾空白、互不相同。
fn valid_settings_batch(user_ids: &[String]) -> Result<()> {
    use crate::domain::model::notification_settings::MAX_NOTIFICATION_SETTINGS_BATCH;
    if user_ids.is_empty() || user_ids.len() > MAX_NOTIFICATION_SETTINGS_BATCH {
        return Err(ErrorBuilder::new(
            ErrorCode::InvalidParameter,
            "user_ids must hold 1 to 500 users",
        )
        .build_error());
    }
    let mut seen = std::collections::HashSet::with_capacity(user_ids.len());
    for user_id in user_ids {
        if user_id.trim().is_empty() || user_id.trim() != user_id {
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "user_ids must not be blank or padded",
            )
            .build_error());
        }
        if !seen.insert(user_id.as_str()) {
            return Err(ErrorBuilder::new(
                ErrorCode::InvalidParameter,
                "user_ids must be distinct",
            )
            .build_error());
        }
    }
    Ok(())
}

/// 订阅者分页请求：返回解析后的位置与页大小。
fn valid_subscriber_query(
    query: &crate::application::queries::ListNotificationSubscribersQuery,
) -> Result<(crate::domain::model::SubscriberCursor, usize)> {
    use crate::domain::model::notification_settings::subscriber_page_size;
    let conversation_id = query.conversation_id.as_str();
    if conversation_id.trim().is_empty() || conversation_id.trim() != conversation_id {
        return Err(
            ErrorBuilder::new(ErrorCode::InvalidParameter, "conversation_id is required")
                .build_error(),
        );
    }
    let cursor = crate::domain::model::SubscriberCursor::parse(&query.page_token)?;
    Ok((cursor, subscriber_page_size(query.page_size)))
}

/// 按 id 查成员的请求：会话 ID 非空；id 非空、互不相同、不超过
/// [`MAX_PARTICIPANT_LOOKUP_IDS`](crate::application::queries::MAX_PARTICIPANT_LOOKUP_IDS)。
fn valid_lookup(query: &crate::application::queries::FindConversationMembersQuery) -> Result<&str> {
    use crate::application::queries::MAX_PARTICIPANT_LOOKUP_IDS;
    let conversation_id = query.conversation_id.trim();
    let distinct = query
        .user_ids
        .iter()
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    if conversation_id.is_empty()
        || query.user_ids.is_empty()
        || query.user_ids.len() > MAX_PARTICIPANT_LOOKUP_IDS
        || distinct != query.user_ids.len()
        || query.user_ids.iter().any(|id| id.trim().is_empty())
    {
        return Err(ErrorBuilder::new(
            ErrorCode::InvalidParameter,
            "conversation_id and 1..=500 distinct user_ids are required",
        )
        .build_error());
    }
    Ok(conversation_id)
}

#[cfg(test)]
mod recent_unread_query_tests {
    use super::*;
    use crate::application::queries::{
        ListRecentUnreadConversationsQuery, MAX_RECENT_UNREAD_CONVERSATIONS,
    };

    fn query(user_id: &str, since_ms: i64, limit: i32) -> ListRecentUnreadConversationsQuery {
        ListRecentUnreadConversationsQuery {
            user_id: user_id.to_string(),
            since_ms,
            limit,
        }
    }

    /// 条数由服务端封顶：不传、传负数、传得比上限大，都按上限读。
    #[test]
    fn the_limit_is_capped_by_the_server() {
        for (asked, used) in [
            (0, MAX_RECENT_UNREAD_CONVERSATIONS),
            (-3, MAX_RECENT_UNREAD_CONVERSATIONS),
            (1, 1),
            (5, 5),
            (20, 20),
            (21, MAX_RECENT_UNREAD_CONVERSATIONS),
            (i32::MAX, MAX_RECENT_UNREAD_CONVERSATIONS),
        ] {
            let asked_query = query("alice", 1_760_000_000_000, asked);
            let (user_id, since, limit) = valid_recent_unread_query(&asked_query).unwrap();
            assert_eq!(user_id, "alice");
            assert_eq!(since.timestamp_millis(), 1_760_000_000_000);
            assert_eq!(limit, used, "limit {asked}");
        }
        assert_eq!(MAX_RECENT_UNREAD_CONVERSATIONS, 20);
    }

    /// 读谁必须说清楚：空的、带首尾空白的用户标识一律拒绝，不替调用方猜。
    #[test]
    fn a_blank_or_padded_user_is_rejected() {
        for user_id in ["", "  ", " alice", "alice "] {
            let error = valid_recent_unread_query(&query(user_id, 0, 5)).unwrap_err();
            assert_eq!(
                error.code(),
                Some(ErrorCode::InvalidParameter),
                "{user_id:?}"
            );
        }
    }

    /// since 为 0 表示不限下界；放不进时间类型的值是参数错误，不悄悄当成 0。
    #[test]
    fn since_must_be_a_representable_instant() {
        let epoch = query("alice", 0, 5);
        let (_, since, _) = valid_recent_unread_query(&epoch).unwrap();
        assert_eq!(since.timestamp_millis(), 0);
        for out_of_range in [i64::MAX, i64::MIN] {
            let error = valid_recent_unread_query(&query("alice", out_of_range, 5)).unwrap_err();
            assert_eq!(error.code(), Some(ErrorCode::InvalidParameter));
        }
    }
}

#[cfg(test)]
mod notification_settings_request_tests {
    use super::*;
    use crate::application::queries::ListNotificationSubscribersQuery;

    /// 用户级设置只读写调用者本人：没有用户、空白或带首尾空白的用户一律拒绝。
    #[test]
    fn the_settings_owner_is_the_authenticated_user() {
        let alice = Context::with_request_id("req").with_user_id("alice");
        assert_eq!(settings_owner(&alice).unwrap(), "alice");
        for ctx in [
            Context::with_request_id("req"),
            Context::with_request_id("req").with_user_id(""),
            Context::with_request_id("req").with_user_id(" alice"),
        ] {
            let error = settings_owner(&ctx).unwrap_err();
            assert_eq!(error.code(), Some(ErrorCode::AuthenticationRequired));
        }
    }

    #[test]
    fn a_settings_batch_is_one_to_five_hundred_distinct_users() {
        let users = |n: usize| (0..n).map(|i| format!("u{i}")).collect::<Vec<_>>();
        assert!(valid_settings_batch(&users(1)).is_ok());
        assert!(valid_settings_batch(&users(500)).is_ok());
        for bad in [
            users(0),
            users(501),
            vec!["a".to_string(), "a".to_string()],
            vec!["a".to_string(), " b".to_string()],
            vec!["".to_string()],
        ] {
            let error = valid_settings_batch(&bad).unwrap_err();
            assert_eq!(error.code(), Some(ErrorCode::InvalidParameter), "{bad:?}");
        }
    }

    fn subscribers(
        conversation_id: &str,
        page_token: &str,
        page_size: i32,
    ) -> ListNotificationSubscribersQuery {
        ListNotificationSubscribersQuery {
            conversation_id: conversation_id.to_string(),
            page_token: page_token.to_string(),
            page_size,
        }
    }

    #[test]
    fn a_subscriber_query_names_a_conversation_and_a_valid_page() {
        let (cursor, size) = valid_subscriber_query(&subscribers("2G1", "", 0)).unwrap();
        assert_eq!(cursor, crate::domain::model::SubscriberCursor::default());
        assert_eq!(size, 1000);
        let (cursor, size) = valid_subscriber_query(&subscribers("2G1", "1000:u9", 50)).unwrap();
        assert_eq!(cursor.served, 1000);
        assert_eq!(cursor.after_user_id.as_deref(), Some("u9"));
        assert_eq!(size, 50);
        for bad in [
            subscribers("", "", 0),
            subscribers(" 2G1", "", 0),
            subscribers("2G1", "garbage", 0),
        ] {
            let error = valid_subscriber_query(&bad).unwrap_err();
            assert_eq!(error.code(), Some(ErrorCode::InvalidParameter));
        }
    }
}
