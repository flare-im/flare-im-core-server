//! 消息存储仓储（Port）
//!
//! **功能**：读侧消息与相关数据的查询与更新入口。
//! - **消息**：`query_messages` / `query_messages_by_seq` / `get_message` / `search_messages` / `count_messages`；
//!   `get_message_timestamp` 用于清除会话等。
//! - **更新**：`update_message`（已读/撤回/属性/标签/反应）、`batch_update_visibility`、`update_message_attributes`。
//! - **操作与事件**：`query_message_operations` / `query_message_events`（按消息拉取事件）、
//!   `query_message_edit_history` / `query_message_read_records` / `query_message_reactions` /
//!   `query_message_visibility` / `query_pinned_messages`。
//! - **同步**：`query_events`（按会话 after_seq 拉取）、`get_conversation_max_seq`、
//!   `get_sync_cursor` / `update_sync_cursor`。
//! - **标签**：`list_all_tags`。
//! 典型实现：PostgreSQL（+ 可选 Redis 缓存）见 `infrastructure::persistence::optimized_postgres_store`。

use crate::domain::model::{
    ConversationMessageHead, EditHistoryEntry, Event, EventType, FilterExpression, Message,
    MessageUpdate, PinnedMessageInfo, ReactionItem, ReadListEntry, SyncCursor, VisibilityStatus,
};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use flare_server_core::context::Ctx;
use std::collections::HashMap;

/// 使用 `async-trait` 保证返回的 Future 为 `Send`，满足 tonic gRPC 对 `async` 方法的线程间调度要求。

#[async_trait]
pub trait MessageStorage: Send + Sync {
    /// CQRS 读侧不落库：写入由 Storage Writer 消费事件完成；此处固定 no-op，仅保留以兼容 trait 对象。
    async fn store_message(
        &self,
        ctx: &Ctx,
        _message: &Message,
        _conversation_id: &str,
    ) -> Result<()> {
        let _ = ctx;
        Ok(())
    }

    async fn query_messages(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        user_id: Option<&str>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
        limit: i32,
    ) -> Result<Vec<Message>>;

    /// 基于 seq 查询消息（after_seq / before_seq / limit，按 seq 升序）
    async fn query_messages_by_seq(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        user_id: Option<&str>,
        after_seq: i64,
        before_seq: Option<i64>,
        limit: i32,
    ) -> Result<Vec<Message>>;

    async fn count_messages(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        user_id: Option<&str>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<i64>;

    async fn get_message(&self, ctx: &Ctx, message_id: &str) -> Result<Option<Message>>;

    /// 获取消息时间戳（用于清除会话等）
    async fn get_message_timestamp(
        &self,
        ctx: &Ctx,
        message_id: &str,
    ) -> Result<Option<DateTime<Utc>>>;

    async fn update_message(
        &self,
        ctx: &Ctx,
        message_id: &str,
        updates: MessageUpdate,
    ) -> Result<()>;

    async fn batch_update_visibility(
        &self,
        ctx: &Ctx,
        message_ids: &[String],
        user_id: &str,
        visibility: VisibilityStatus,
    ) -> Result<usize>;

    async fn search_messages(
        &self,
        ctx: &Ctx,
        filters: &[FilterExpression],
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
        limit: i32,
    ) -> Result<Vec<Message>>;

    async fn update_message_attributes(
        &self,
        ctx: &Ctx,
        message_id: &str,
        attributes: HashMap<String, String>,
        tags: Vec<String>,
    ) -> Result<()>;

    async fn list_all_tags(&self, ctx: &Ctx) -> Result<Vec<String>>;

    /// 查询消息操作历史（与 storage.proto QueryMessageOperationsResponse 对齐，返回 Event 列表）
    async fn query_message_operations(&self, ctx: &Ctx, message_id: &str) -> Result<Vec<Event>>;

    /// 按事件类型查询消息相关事件（支持类型过滤与分页）
    async fn query_message_events(
        &self,
        ctx: &Ctx,
        message_id: &str,
        event_types: Option<&[EventType]>,
        limit: i32,
        offset: i64,
    ) -> Result<(Vec<Event>, bool)>;

    /// 查询消息编辑历史
    async fn query_message_edit_history(
        &self,
        ctx: &Ctx,
        message_id: &str,
    ) -> Result<Vec<EditHistoryEntry>>;

    /// 查询消息已读记录
    async fn query_message_read_records(
        &self,
        ctx: &Ctx,
        message_id: &str,
    ) -> Result<Vec<ReadListEntry>>;

    /// 查询消息可见性状态
    async fn query_message_visibility(
        &self,
        ctx: &Ctx,
        message_id: &str,
        user_id: &str,
    ) -> Result<Option<VisibilityStatus>>;

    /// 查询消息反应
    async fn query_message_reactions(
        &self,
        ctx: &Ctx,
        message_id: &str,
    ) -> Result<Vec<ReactionItem>>;

    /// 查询置顶消息
    async fn query_pinned_messages(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
    ) -> Result<Vec<PinnedMessageInfo>>;

    /// 同步用：按会话拉取事件（`seq > after_seq`，可选 `before_seq` 上界与类型过滤）
    async fn query_events(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        conversation_id: &str,
        after_seq: i64,
        before_seq: i64,
        limit: i32,
        event_type_filter: Vec<i32>,
    ) -> Result<Vec<Event>>;

    /// 同步用：获取会话当前最大消息 seq（无行则 `None`）
    async fn get_conversation_max_seq(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        conversation_id: &str,
    ) -> Result<Option<i64>>;

    /// 同步用：最新消息 seq / server_id / 时间（会话无消息则 `None`）
    async fn get_conversation_message_head(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
    ) -> Result<Option<ConversationMessageHead>>;

    /// 同步用：获取用户在某会话的同步游标
    async fn get_sync_cursor(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<Option<SyncCursor>>;

    /// 同步用：获取同步快照（多个会话的最新消息）
    async fn get_sync_snapshot(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        user_id: &str,
        conversation_ids: &[String],
        messages_per_conversation: i32,
    ) -> Result<Vec<(String, Vec<Message>, i64)>>; // (conversation_id, messages, last_seq)

    /// 同步用：更新用户在某会话的同步游标
    async fn update_sync_cursor(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        user_id: &str,
        conversation_id: &str,
        last_synced_seq: i64,
        last_synced_ts: i64,
        device_id: Option<&str>,
    ) -> Result<()>;
}
