//! 出站端口（防腐层）：由 `infrastructure` 实现，对接 Conversation / Storage Reader 等。
#![allow(async_fn_in_trait)] // 内部端口，由具体类型实现并 `Send`；与仓库 Rust 2024 异步 trait 风格一致。

use flare_grpc_proto::conversation::{
    ConversationBootstrapRequest, ConversationBootstrapResponse, CreateConversationRequest,
    CreateConversationResponse, GetConversationDetailRequest, GetConversationDetailResponse,
    ListConversationParticipantsRequest, ListConversationParticipantsResponse,
    UpdateConversationUserSettingsRequest, UpdateConversationUserSettingsResponse,
    UpdateCursorRequest,
};
use flare_im_contracts::Ctx;
use flare_proto::Message;
use flare_proto::common::{Event, MultiDeviceCursor};
use flare_server_core::error::FlareError;
use std::collections::{HashMap, VecDeque};

/// 会话域在同步编排中需要的**原子**能力（经 gRPC：`ConversationReadService` + `ConversationManageService::UpdateCursor`）。
/// 消息按 seq 拉取、事件流等由 `StorageReadPort` / `ConversationEventReadPort` 承担，不经会话聚合 RPC。
pub trait ConversationSyncPort: Send + Sync {
    async fn conversation_bootstrap(
        &self,
        ctx: &Ctx,
        req: ConversationBootstrapRequest,
    ) -> Result<ConversationBootstrapResponse, FlareError>;

    async fn update_sync_cursor(
        &self,
        ctx: &Ctx,
        req: UpdateCursorRequest,
    ) -> Result<(), FlareError>;

    async fn conversation_detail(
        &self,
        ctx: &Ctx,
        req: GetConversationDetailRequest,
    ) -> Result<GetConversationDetailResponse, FlareError>;

    async fn list_conversation_participants(
        &self,
        ctx: &Ctx,
        req: ListConversationParticipantsRequest,
    ) -> Result<ListConversationParticipantsResponse, FlareError>;

    async fn update_conversation_user_settings(
        &self,
        ctx: &Ctx,
        req: UpdateConversationUserSettingsRequest,
    ) -> Result<UpdateConversationUserSettingsResponse, FlareError>;

    /// 显式建群：服务端按请求(含 attributes["conversation_id"])创建/确保会话存在(幂等)。
    async fn create_conversation(
        &self,
        ctx: &Ctx,
        req: CreateConversationRequest,
    ) -> Result<CreateConversationResponse, FlareError>;
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

    /// 批量窗口：一次 RPC 取多个会话的消息窗口（冷启 bundle / multi catch-up 共用；
    /// 无默认实现——批量是本方法的性能契约，隐式逐会话回退会把 N+1 藏回接口后面）。
    /// `newest_window=true` 每会话取最新 limit 条；`false` 取 `seq > after_seq` 增量页。
    async fn query_conversations_message_windows(
        &self,
        ctx: &Ctx,
        targets: &[(String, i64)],
        per_conversation_limit: i32,
        newest_window: bool,
        user_id: &str,
    ) -> Result<Vec<(String, Vec<Message>, i64)>, FlareError>;
}

/// 会话级事件流（关键事件回放），经 Storage Reader `events` 表。
pub trait ConversationEventReadPort: Send + Sync {
    #[allow(clippy::too_many_arguments)]
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConversationVersionChange {
    pub conversation_id: String,
    pub version: u64,
    pub max_conversation_seq: u64,
    pub updated_at_ms: i64,
}

/// 用户可见会话摘要版本索引读侧。
///
/// 写侧由消息/事件编排负责；同步编排只根据客户端已知版本查询差异，
/// 用于大群 notify+pull 下的摘要修复提示。
pub trait ConversationVersionIndexPort: Send + Sync {
    async fn diff_known_conversation_versions(
        &self,
        ctx: &Ctx,
        known: &[(String, u64)],
    ) -> Result<Vec<ConversationVersionChange>, FlareError>;
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

    /// 返回更新前的 `last_conversation_seq`（若存在），用于单调性校验。
    async fn previous_last_seq(&self, user_id: &str, conversation_id: &str) -> Option<i64>;
}

/// 默认游标缓存容量上限。这是**可选 L1 缓存**（权威仍以 Conversation 为准），无界会随 (用户×会话) 缓慢泄漏。
const DEFAULT_SYNC_CURSOR_CACHE_CAPACITY: usize = 200_000;

/// 有界游标映射：FIFO 淘汰最旧条目封顶内存。被淘汰条目下次 `get` miss → 由权威源重取（安全）。
struct BoundedCursorMap {
    map: HashMap<(String, String), MultiDeviceCursor>,
    order: VecDeque<(String, String)>,
    capacity: usize,
}

impl Default for BoundedCursorMap {
    fn default() -> Self {
        Self::new(DEFAULT_SYNC_CURSOR_CACHE_CAPACITY)
    }
}

impl BoundedCursorMap {
    fn new(capacity: usize) -> Self {
        Self {
            map: HashMap::new(),
            order: VecDeque::new(),
            capacity: capacity.max(1),
        }
    }

    fn insert(&mut self, key: (String, String), cursor: MultiDeviceCursor) {
        if !self.map.contains_key(&key) {
            self.order.push_back(key.clone());
            while self.order.len() > self.capacity {
                if let Some(evicted) = self.order.pop_front() {
                    self.map.remove(&evicted);
                }
            }
        }
        self.map.insert(key, cursor);
    }

    fn get(&self, key: &(String, String)) -> Option<&MultiDeviceCursor> {
        self.map.get(key)
    }
}

/// 基于 tokio::sync::RwLock<有界映射> 的默认 L1 游标缓存（FIFO 封顶，防长跑泄漏）。
#[derive(Clone)]
pub struct MemorySyncCursorCache {
    inner: std::sync::Arc<tokio::sync::RwLock<BoundedCursorMap>>,
}

/// `Default` 与 `new` 必须同路径（否则 Default 构造悄悄丢掉 env 容量覆盖）。
impl Default for MemorySyncCursorCache {
    fn default() -> Self {
        Self::new()
    }
}

impl MemorySyncCursorCache {
    pub fn new() -> Self {
        let capacity = std::env::var("SYNC_ORCHESTRATOR_CURSOR_CACHE_CAPACITY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(DEFAULT_SYNC_CURSOR_CACHE_CAPACITY);
        Self {
            inner: std::sync::Arc::new(tokio::sync::RwLock::new(BoundedCursorMap::new(capacity))),
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
            .map(|c| c.last_conversation_seq as i64)
    }
}

const DEFAULT_BOOTSTRAP_PAGE_CACHE_TTL_MS: u64 = 1_500;
const BOOTSTRAP_PAGE_CACHE_CAPACITY: usize = 4_096;

/// 分页续拉专用的 bootstrap 快照缓存（短 TTL、有界）。
///
/// - 分页序列的第 1 页（cursor 为空）恒走新鲜 bootstrap 并写入缓存；
/// - **续拉页**（cursor 非空，秒级间隔到达）在 TTL 内复用同一份快照——既消灭
///   "每页全量 bootstrap"的 O(页数 × 全账号 LATERAL) DB 放大，又让同一次分页
///   序列看到一致的数据集（分页遍历变化中的集合本身有漂移风险，快照反而更一致）。
/// - TTL 由 `SYNC_ORCHESTRATOR_BOOTSTRAP_PAGE_CACHE_TTL_MS` 覆盖（0=禁用）。
struct BootstrapCacheEntry {
    stored_at: std::time::Instant,
    /// 该次 bootstrap 使用的存储层增量过滤边界（0=全量）。
    /// 只允许"更旧或相等边界"的条目服务请求（超集规则）：全量(0)可服务任何请求，
    /// 过滤过的条目只能服务过滤边界 ≥ 它的请求——绝不让子集冒充全集。
    updated_after_ms: i64,
    resp: std::sync::Arc<ConversationBootstrapResponse>,
}

pub struct BootstrapPageCache {
    ttl: std::time::Duration,
    inner: std::sync::RwLock<HashMap<(String, String), BootstrapCacheEntry>>,
}

impl Default for BootstrapPageCache {
    fn default() -> Self {
        Self::new()
    }
}

impl BootstrapPageCache {
    pub fn new() -> Self {
        let ttl_ms = std::env::var("SYNC_ORCHESTRATOR_BOOTSTRAP_PAGE_CACHE_TTL_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_BOOTSTRAP_PAGE_CACHE_TTL_MS);
        Self::with_ttl(std::time::Duration::from_millis(ttl_ms))
    }

    pub fn with_ttl(ttl: std::time::Duration) -> Self {
        Self {
            ttl,
            inner: std::sync::RwLock::new(HashMap::new()),
        }
    }

    fn lock_read(
        &self,
    ) -> std::sync::RwLockReadGuard<'_, HashMap<(String, String), BootstrapCacheEntry>> {
        self.inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn lock_write(
        &self,
    ) -> std::sync::RwLockWriteGuard<'_, HashMap<(String, String), BootstrapCacheEntry>> {
        self.inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// `requested_updated_after_ms`：本次请求的增量过滤边界（0=需要全集）。
    /// 只返回"边界 ≤ 请求边界"的条目（超集规则）。
    pub fn get(
        &self,
        tenant_id: &str,
        user_id: &str,
        requested_updated_after_ms: i64,
    ) -> Option<std::sync::Arc<ConversationBootstrapResponse>> {
        if self.ttl.is_zero() {
            return None;
        }
        let key = (tenant_id.to_string(), user_id.to_string());
        let guard = self.lock_read();
        let entry = guard.get(&key)?;
        (entry.stored_at.elapsed() < self.ttl
            && entry.updated_after_ms <= requested_updated_after_ms)
            .then(|| std::sync::Arc::clone(&entry.resp))
    }

    pub fn put(
        &self,
        tenant_id: &str,
        user_id: &str,
        updated_after_ms: i64,
        resp: std::sync::Arc<ConversationBootstrapResponse>,
    ) {
        if self.ttl.is_zero() {
            return;
        }
        let key = (tenant_id.to_string(), user_id.to_string());
        let mut guard = self.lock_write();
        if guard.len() >= BOOTSTRAP_PAGE_CACHE_CAPACITY {
            // 先清过期；仍满则整体清空——微缓存是提示，宁可 miss 不可无界。
            let ttl = self.ttl;
            guard.retain(|_, entry| entry.stored_at.elapsed() < ttl);
            if guard.len() >= BOOTSTRAP_PAGE_CACHE_CAPACITY {
                guard.clear();
            }
        }
        // 更全的条目（更小边界）不被更窄条目覆盖——保住"全集可服务一切"的价值。
        if let Some(existing) = guard.get(&key)
            && existing.stored_at.elapsed() < self.ttl
            && existing.updated_after_ms < updated_after_ms
        {
            return;
        }
        guard.insert(
            key,
            BootstrapCacheEntry {
                stored_at: std::time::Instant::now(),
                updated_after_ms,
                resp,
            },
        );
    }
}

#[cfg(test)]
mod bootstrap_page_cache_tests {
    use super::*;

    #[test]
    fn page_cache_hits_within_ttl_and_expires_after() {
        let cache = BootstrapPageCache::with_ttl(std::time::Duration::from_millis(50));
        let resp = std::sync::Arc::new(ConversationBootstrapResponse::default());
        cache.put("0", "u1", 0, resp.clone());
        assert!(cache.get("0", "u1", 0).is_some(), "hit within ttl");
        assert!(cache.get("0", "u2", 0).is_none(), "other user misses");
        std::thread::sleep(std::time::Duration::from_millis(60));
        assert!(cache.get("0", "u1", 0).is_none(), "expired after ttl");
    }

    #[test]
    fn page_cache_enforces_superset_rule_for_filtered_entries() {
        let cache = BootstrapPageCache::with_ttl(std::time::Duration::from_millis(200));
        let resp = std::sync::Arc::new(ConversationBootstrapResponse::default());
        // 过滤边界 1000 的条目：可服务边界 ≥1000 的请求，绝不服务需要更全集合的请求。
        cache.put("0", "u1", 1_000, resp.clone());
        assert!(cache.get("0", "u1", 1_000).is_some(), "equal boundary hits");
        assert!(
            cache.get("0", "u1", 2_000).is_some(),
            "newer boundary hits (superset)"
        );
        assert!(
            cache.get("0", "u1", 0).is_none(),
            "full-set request must not be served by a filtered subset"
        );
        // 全量(0)条目可服务任何请求，且不被更窄条目覆盖。
        cache.put("0", "u1", 0, resp.clone());
        cache.put("0", "u1", 5_000, resp.clone());
        assert!(
            cache.get("0", "u1", 0).is_some(),
            "full-set entry survives narrower put"
        );
    }

    #[test]
    fn zero_ttl_disables_cache() {
        let cache = BootstrapPageCache::with_ttl(std::time::Duration::ZERO);
        cache.put(
            "0",
            "u1",
            0,
            std::sync::Arc::new(ConversationBootstrapResponse::default()),
        );
        assert!(cache.get("0", "u1", 0).is_none());
    }
}

#[cfg(test)]
mod bounded_cursor_cache_tests {
    use super::*;

    fn cursor(seq: u64) -> MultiDeviceCursor {
        MultiDeviceCursor {
            last_conversation_seq: seq,
            ..Default::default()
        }
    }

    #[test]
    fn bounded_cursor_map_evicts_oldest_and_caps_memory() {
        let mut m = BoundedCursorMap::new(2);
        m.insert(("u".into(), "a".into()), cursor(1));
        m.insert(("u".into(), "b".into()), cursor(2));
        assert!(m.get(&("u".into(), "a".into())).is_some());
        m.insert(("u".into(), "c".into()), cursor(3)); // 越界 → 淘汰最旧 ("u","a")
        assert!(m.get(&("u".into(), "a".into())).is_none(), "oldest evicted");
        assert!(m.get(&("u".into(), "b".into())).is_some());
        assert!(m.get(&("u".into(), "c".into())).is_some());
        assert_eq!(m.order.len(), 2, "memory bounded at capacity");
        // 更新既有键不增长、保留 FIFO 位置
        m.insert(("u".into(), "b".into()), cursor(99));
        assert_eq!(m.order.len(), 2);
        assert_eq!(
            m.get(&("u".into(), "b".into()))
                .unwrap()
                .last_conversation_seq,
            99
        );
    }
}
