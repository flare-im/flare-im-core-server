//! 查询处理器（查询侧）- 直接调用基础设施层，不经过领域服务
//!
//! 在 CQRS 架构中，查询侧通常直接调用基础设施层（仓储实现），
//! 因为查询是只读操作，不涉及业务逻辑，不需要经过领域层。

use chrono::{DateTime, Utc};
use flare_im_core::error::{ErrorCode, Result, map_infra_error};
use flare_im_core::message::{Message, message_to_proto};
use flare_im_core::utils::extract_seq_from_message;
use std::sync::Arc;
use tracing::instrument;

use crate::application::queries::{
    GetMessageQuery, ListMessageTagsQuery, QueryMessageOperationsQuery, QueryMessagesBySeqQuery,
    QueryMessagesQuery, SearchMessagesQuery,
};
use crate::convert::{datetime_to_timestamp, event_to_proto_or_default};
use crate::domain::model::Event;
use crate::domain::repository::MessageStorage;

/// 查询结果结构
#[derive(Debug, Clone)]
pub struct QueryMessagesResult {
    pub messages: Vec<Message>,
    pub next_cursor: String,
    pub has_more: bool,
    pub total_size: i64,
}

/// 消息存储查询处理器（查询侧）
///
/// 直接基于仓储实现查询逻辑，无需领域服务
pub struct MessageStorageQueryHandler<M>
where
    M: MessageStorage + Send + Sync,
{
    storage: Arc<M>,
}

impl<M> MessageStorageQueryHandler<M>
where
    M: MessageStorage + Send + Sync,
{
    pub fn new(storage: Arc<M>) -> Self {
        Self { storage }
    }

    /// 暴露 storage 供 gRPC handler 直接调用仓储（如 query_message_events 等）
    pub fn storage(&self) -> &M {
        self.storage.as_ref()
    }

    /// 查询消息列表
    #[instrument(skip(self, ctx), fields(conversation_id = %query.conversation_id))]
    pub async fn handle_query_messages(
        &self,
        ctx: &flare_server_core::context::Ctx,
        query: QueryMessagesQuery,
    ) -> Result<Vec<Message>> {
        let start_time = if query.start_time == 0 {
            None
        } else {
            DateTime::from_timestamp(query.start_time, 0)
        };

        let end_time = if query.end_time == 0 {
            None
        } else {
            DateTime::from_timestamp(query.end_time, 0)
        };

        self.storage
            .query_messages(
                ctx,
                &query.conversation_id,
                None, // user_id
                start_time,
                end_time,
                query.limit,
            )
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Failed to query messages"))
    }

    /// 查询消息列表（带分页结果）
    #[instrument(skip(self), fields(conversation_id = %query.conversation_id))]
    pub async fn handle_query_messages_with_pagination(
        &self,
        ctx: &flare_server_core::context::Ctx,
        query: QueryMessagesQuery,
    ) -> Result<QueryMessagesResult> {
        let _ = ctx; // 上下文用于日志追踪

        let start_time = if query.start_time == 0 {
            None
        } else {
            DateTime::from_timestamp(query.start_time, 0)
        };

        let end_time = if query.end_time == 0 {
            None
        } else {
            DateTime::from_timestamp(query.end_time, 0)
        };

        // 直接查询消息
        let messages = self
            .storage
            .query_messages(
                ctx,
                &query.conversation_id,
                None, // user_id
                start_time,
                end_time,
                query.limit,
            )
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Database query failed"))?;

        // 构建简化的 QueryMessagesResult
        let message_count = messages.len() as i32;
        let next_cursor = messages
            .last()
            .and_then(|msg| {
                msg.timestamp
                    .as_ref()
                    .map(|ts| format!("{}:{}", ts.seconds, msg.server_id.clone()))
            })
            .unwrap_or_default();
        let has_more = message_count >= query.limit;

        Ok(QueryMessagesResult {
            messages,
            next_cursor,
            has_more,
            total_size: message_count as i64,
        })
    }

    /// 获取单条消息
    #[instrument(skip(self), fields(message_id = %query.message_id))]
    /// 获取消息
    #[instrument(skip(self, ctx), fields(message_id = %query.message_id))]
    pub async fn handle_get_message(
        &self,
        ctx: &flare_server_core::context::Ctx,
        query: GetMessageQuery,
    ) -> Result<Option<Message>> {
        let _ = ctx; // 上下文用于日志追踪
        self.storage
            .get_message(ctx, &query.message_id)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Failed to get message"))
    }

    /// 获取消息的时间戳
    #[instrument(skip(self, ctx), fields(message_id = %message_id))]
    pub async fn handle_get_message_timestamp(
        &self,
        ctx: &flare_server_core::context::Ctx,
        message_id: &str,
    ) -> Result<Option<DateTime<Utc>>> {
        let _ = ctx; // 上下文用于日志追踪
        self.storage
            .get_message_timestamp(ctx, message_id)
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::DatabaseError,
                    "Failed to get message timestamp",
                )
            })
    }

    /// 搜索消息
    #[instrument(skip(self, ctx))]
    pub async fn handle_search_messages(
        &self,
        ctx: &flare_server_core::context::Ctx,
        query: SearchMessagesQuery,
    ) -> Result<Vec<Message>> {
        let _ = ctx; // 上下文用于日志追踪
        let start_time = if query.start_time == 0 {
            None
        } else {
            DateTime::from_timestamp(query.start_time, 0)
        };

        let end_time = if query.end_time == 0 {
            None
        } else {
            DateTime::from_timestamp(query.end_time, 0)
        };

        self.storage
            .search_messages(ctx, &query.filters, start_time, end_time, query.limit)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Failed to search messages"))
    }

    /// 列出所有标签
    #[instrument(skip(self, ctx))]
    pub async fn handle_list_message_tags(
        &self,
        ctx: &flare_server_core::context::Ctx,
        _query: ListMessageTagsQuery,
    ) -> Result<Vec<String>> {
        let _ = ctx; // 上下文用于日志追踪
        self.storage
            .list_all_tags(ctx)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Failed to list tags"))
    }

    /// 基于 seq 查询消息列表
    #[instrument(skip(self, ctx), fields(conversation_id = %query.conversation_id, after_seq = query.after_seq, before_seq = ?query.before_seq))]
    pub async fn handle_query_messages_by_seq(
        &self,
        ctx: &flare_server_core::context::Ctx,
        query: QueryMessagesBySeqQuery,
    ) -> Result<(Vec<Message>, Option<i64>)> {
        let _ = ctx; // 上下文用于日志追踪

        // 直接使用存储层查询
        let messages = self
            .storage
            .query_messages_by_seq(
                ctx,
                &query.conversation_id,
                query.user_id.as_deref(),
                query.after_seq,
                query.before_seq,
                query.limit,
            )
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Database query failed"))?;

        // 提取最后一条消息的 seq（使用工具函数）
        let last_seq = messages
            .last()
            .and_then(|msg| extract_seq_from_message(&message_to_proto(msg)));

        Ok((messages, last_seq))
    }

    /// 查询消息操作历史
    #[instrument(skip(self, ctx), fields(message_id = %query.message_id))]
    pub async fn handle_query_message_operations(
        &self,
        ctx: &flare_server_core::context::Ctx,
        query: QueryMessageOperationsQuery,
    ) -> Result<Vec<Event>> {
        let _ = ctx; // 上下文用于日志追踪
        self.storage
            .query_message_operations(ctx, &query.message_id)
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::DatabaseError,
                    "Failed to query message operations",
                )
            })
    }

    /// 获取同步快照
    #[instrument(skip(self, ctx))]
    pub async fn get_sync_snapshot(
        &self,
        ctx: &flare_server_core::context::Ctx,
        conversation_ids: &[String],
        messages_per_conversation: i32,
    ) -> Result<Vec<(String, Vec<Message>, i64)>> {
        let _ = ctx; // 上下文用于日志追踪
        self.storage
            .get_sync_snapshot(ctx, "", "", conversation_ids, messages_per_conversation)
            .await
            .map_err(|e| {
                map_infra_error(e, ErrorCode::DatabaseError, "Failed to get sync snapshot")
            })
    }

    /// 查询事件
    #[instrument(skip(self, ctx))]
    pub async fn query_events(
        &self,
        ctx: &flare_server_core::context::Ctx,
        conversation_id: &str,
        after_seq: i64,
        before_seq: i64,
        limit: i32,
    ) -> Result<Vec<Event>> {
        let _ = ctx; // 上下文用于日志追踪
        self.storage
            .query_events(
                ctx,
                "",
                conversation_id,
                after_seq,
                before_seq,
                limit,
                vec![],
            )
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Failed to query events"))
    }

    /// 会话事件分页（多取 1 条判断 `has_more`），供 gRPC `QueryConversationEvents` 使用
    #[instrument(skip(self, ctx, event_type_filter))]
    pub async fn query_conversation_events_page(
        &self,
        ctx: &flare_server_core::context::Ctx,
        conversation_id: &str,
        after_seq: i64,
        before_seq: i64,
        limit: i32,
        event_type_filter: Vec<i32>,
    ) -> Result<(Vec<flare_proto::common::Event>, i64, bool, String)> {
        let want = limit.clamp(1, 500);
        let mut rows = self
            .storage
            .query_events(
                ctx,
                "",
                conversation_id,
                after_seq,
                before_seq,
                want + 1,
                event_type_filter,
            )
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Database query failed"))?;
        let has_more = rows.len() as i32 > want;
        if has_more {
            rows.truncate(want as usize);
        }
        let last_seq = rows.last().map(|e| e.seq as i64).unwrap_or(after_seq);
        let events: Vec<flare_proto::common::Event> =
            rows.iter().map(event_to_proto_or_default).collect();
        let next_cursor = events
            .last()
            .map(|e| format!("evt:{}", e.seq))
            .unwrap_or_default();
        Ok((events, last_seq, has_more, next_cursor))
    }

    /// 会话最新消息水位，供 gRPC `GetConversationMessageHead`
    #[instrument(skip(self, ctx))]
    pub async fn get_conversation_message_head_grpc(
        &self,
        ctx: &flare_server_core::context::Ctx,
        conversation_id: &str,
    ) -> Result<Option<(i64, String, Option<prost_types::Timestamp>)>> {
        let head = self
            .storage
            .get_conversation_message_head(ctx, conversation_id)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Database query failed"))?;
        Ok(head.map(|h| {
            let ts = datetime_to_timestamp(h.last_at);
            (h.max_seq, h.last_message_id, ts)
        }))
    }

    /// 获取会话最大序号
    #[instrument(skip(self, ctx))]
    pub async fn get_conversation_max_seq(
        &self,
        ctx: &flare_server_core::context::Ctx,
        conversation_id: &str,
    ) -> Result<Option<i64>> {
        let _ = ctx; // 上下文用于日志追踪
        self.storage
            .get_conversation_max_seq(ctx, "", conversation_id)
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::DatabaseError,
                    "Failed to get conversation max seq",
                )
            })
    }

    /// 获取同步游标
    #[instrument(skip(self, ctx))]
    pub async fn get_sync_cursor(
        &self,
        ctx: &flare_server_core::context::Ctx,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<Option<crate::domain::model::SyncCursor>> {
        let _ = ctx; // 上下文用于日志追踪
        self.storage
            .get_sync_cursor(ctx, "", user_id, conversation_id)
            .await
            .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Failed to get sync cursor"))
    }

    /// 更新同步游标
    #[instrument(skip(self, ctx))]
    pub async fn update_sync_cursor(
        &self,
        ctx: &flare_server_core::context::Ctx,
        user_id: &str,
        conversation_id: &str,
        last_synced_seq: i64,
        last_synced_ts: i64,
    ) -> Result<()> {
        let _ = ctx; // 上下文用于日志追踪
        self.storage
            .update_sync_cursor(
                ctx,
                "",
                user_id,
                conversation_id,
                last_synced_seq,
                last_synced_ts,
                None,
            )
            .await
            .map_err(|e| {
                map_infra_error(e, ErrorCode::DatabaseError, "Failed to update sync cursor")
            })
    }
}
