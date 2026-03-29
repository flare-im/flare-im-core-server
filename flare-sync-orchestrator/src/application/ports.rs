//! 出站端口（防腐层）：由 `infrastructure` 实现，对接 Conversation / Storage Reader 等。
#![allow(async_fn_in_trait)] // 内部端口，由具体类型实现并 `Send`；与仓库 Rust 2024 异步 trait 风格一致。

use flare_proto::Message;
use flare_proto::common::{Event, MultiDeviceCursor};
use flare_proto::conversation::{
    ConversationBootstrapRequest, ConversationBootstrapResponse, UpdateCursorRequest,
};
use flare_server_core::context::Ctx;
use flare_server_core::error::FlareError;
use std::collections::HashMap;

/// 会话域在同步编排中需要的**原子**能力（经 gRPC：`ConversationReadService` + `ConversationManageService::UpdateCursor`）。
/// 消息按 seq 拉取、事件流等由 `StorageReadPort` / `ConversationEventReadPort` 承担，不经会话聚合 RPC。
pub trait ConversationSyncPort: Send + Sync {
    async fn conversation_bootstrap(
        &self,
        ctx: &Ctx,
        req: ConversationBootstrapRequest,
    ) -> Result<ConversationBootstrapResponse, FlareError>;

    async fn update_read_cursor(
        &self,
        ctx: &Ctx,
        req: UpdateCursorRequest,
    ) -> Result<(), FlareError>;
}

/// 存储读侧返回的会话最新消息水位（`messages` 表，按 `seq` 最大的一行）
#[derive(Debug, Clone, Default)]
pub struct StorageConversationMessageHead {
    pub max_seq: i64,
    pub last_message_id: String,
    pub last_timestamp: Option<prost_types::Timestamp>,
}

/// 存储读侧：按 seq 拉消息页 + 会话消息水位。
pub trait StorageReadPort: Send + Sync {
    async fn query_messages_by_seq(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        after_seq: i64,
        before_seq: i64,
        limit: i32,
        user_id: &str,
    ) -> Result<(Vec<Message>, i64), FlareError>;

    async fn get_conversation_message_head(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
    ) -> Result<StorageConversationMessageHead, FlareError>;
}

/// 会话级事件流（关键事件回放），经 Storage Reader `events` 表。
pub trait ConversationEventReadPort: Send + Sync {
    async fn query_events_page(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        after_seq: i64,
        before_seq: i64,
        limit: i32,
        event_types: &[i32],
        include_deleted: bool,
    ) -> Result<QueryEventsPage, FlareError>;
}

#[derive(Debug, Clone, Default)]
pub struct QueryEventsPage {
    pub events: Vec<Event>,
    pub last_seq: i64,
    pub has_more: bool,
    pub next_cursor: String,
}

/// 进程内 L1 游标缓存（可选）；权威仍以 Conversation / 未来 Redis 为准。
pub trait SyncCursorCachePort: Send + Sync {
    async fn get(&self, user_id: &str, conversation_id: &str) -> Option<MultiDeviceCursor>;

    /// `user_id` 为认证上下文中的用户（`MultiDeviceCursor` 不再携带 user_id）。
    async fn put(&self, user_id: &str, cursor: MultiDeviceCursor);

    /// 返回更新前的 `last_sync_seq`（若存在），用于单调性校验。
    async fn previous_last_seq(&self, user_id: &str, conversation_id: &str) -> Option<i64>;
}

/// 基于 tokio::sync::RwLock<HashMap> 的默认缓存。
#[derive(Clone, Default)]
pub struct MemorySyncCursorCache {
    inner: std::sync::Arc<tokio::sync::RwLock<HashMap<(String, String), MultiDeviceCursor>>>,
}

impl MemorySyncCursorCache {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }
}

impl SyncCursorCachePort for MemorySyncCursorCache {
    async fn get(&self, user_id: &str, conversation_id: &str) -> Option<MultiDeviceCursor> {
        let key = (user_id.to_string(), conversation_id.to_string());
        self.inner.read().await.get(&key).cloned()
    }

    async fn put(&self, user_id: &str, cursor: MultiDeviceCursor) {
        let key = (user_id.to_string(), cursor.conversation_id.clone());
        self.inner.write().await.insert(key, cursor);
    }

    async fn previous_last_seq(&self, user_id: &str, conversation_id: &str) -> Option<i64> {
        let key = (user_id.to_string(), conversation_id.to_string());
        self.inner
            .read()
            .await
            .get(&key)
            .map(|c| c.last_sync_seq as i64)
    }
}
