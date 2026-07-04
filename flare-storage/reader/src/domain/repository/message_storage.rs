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
//!
//! 典型实现：PostgreSQL（+ 可选 Redis 缓存）见 `infrastructure::persistence::optimized_postgres_store`。

use crate::domain::model::{
    ConversationMessageHead, EditHistoryEntry, Event, EventType, FilterExpression, MarkEntry,
    Message, MessageExportTaskDraft, MessageUpdate, MessageWriteLedgerEntry,
    MessageWriteLedgerQuery, PinnedMessageInfo, ReactionItem, ReadListEntry, SyncCursor,
    VisibilityStatus,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use flare_im_contracts::Ctx;
use flare_server_core::error::Result;
use std::collections::HashMap;

/// 回溯尾页约定：`after_seq == 0` 且 `before_seq` 有值 ⇒「取最新 limit 条」
/// （存储层按 seq DESC 截断后升序返回；溢出从头部裁剪）。
/// 存储实现与 gRPC 层共用此谓词，防止同一分页约定在两层各自推导后漂移。
pub fn is_backfill_tail_page(after_seq: i64, before_seq: Option<i64>) -> bool {
    after_seq == 0 && before_seq.is_some()
}

/// 使用 `async-trait` 保证返回的 Future 为 `Send`，满足 tonic gRPC 对 `async` 方法的线程间调度要求。
#[allow(clippy::too_many_arguments)]
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
        include_burned_placeholder: bool,
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
        include_burned_placeholder: bool,
    ) -> Result<Vec<Message>>;

    /// 批量窗口：一条 SQL 取多个会话的消息窗口（无默认实现——批量是本方法的
    /// 性能契约，隐式逐会话回退会把 N+1 藏回接口后面）。
    /// `newest_window=true`：每会话取最新 limit 条（忽略 after_seq，冷启首页）；
    /// `false`：每会话取 `seq > after_seq` 升序前 limit 条（增量 catch-up）。
    /// 返回消息一律 seq 升序。
    async fn query_conversations_message_windows(
        &self,
        ctx: &Ctx,
        targets: &[(String, i64)],
        user_id: Option<&str>,
        per_conversation_limit: i32,
        newest_window: bool,
        include_burned_placeholder: bool,
    ) -> Result<Vec<(String, Vec<Message>)>>;

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

    /// 查询消息标记
    async fn query_message_marks(&self, _ctx: &Ctx, _message_id: &str) -> Result<Vec<MarkEntry>> {
        Err(flare_server_core::error::FlareError::system(
            "message mark query is not implemented for this storage backend".to_string(),
        ))
    }

    /// 创建管理面消息导出任务。该方法只登记任务，不在请求线程内同步生成大文件。
    async fn create_message_export_task(
        &self,
        _ctx: &Ctx,
        _draft: MessageExportTaskDraft,
    ) -> Result<String> {
        Err(flare_server_core::error::FlareError::system(
            "message export task persistence is not implemented for this storage backend"
                .to_string(),
        ))
    }

    /// 管理面查询消息写链路账本，用于定位 storage/WAL/ACK 失败或卡住的消息。
    async fn query_message_write_ledger(
        &self,
        ctx: &Ctx,
        query: MessageWriteLedgerQuery,
    ) -> Result<(Vec<MessageWriteLedgerEntry>, bool)>;

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
