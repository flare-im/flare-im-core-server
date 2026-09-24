use std::collections::HashMap;

use flare_server_core::error::Result;

use crate::domain::aggregate::Connection;
use crate::domain::model::OnlineStatusRecord;
use crate::domain::value_object::{ConnectionId, DeviceId, UserId};

pub trait ConversationRepository: Send + Sync {
    async fn save_connection(&self, connection: &Connection) -> Result<()>;
    async fn remove_connection(
        &self,
        conversation_id: &ConnectionId,
        user_id: &UserId,
    ) -> Result<()>;
    async fn touch_connection(
        &self,
        conversation_id: &ConnectionId,
        user_id: &UserId,
    ) -> Result<()>;
    async fn fetch_statuses(
        &self,
        user_ids: &[String],
    ) -> Result<HashMap<String, OnlineStatusRecord>>;
    async fn get_user_connections(&self, user_id: &UserId) -> Result<Vec<Connection>>;
    async fn remove_user_connections(
        &self,
        user_id: &UserId,
        device_ids: Option<&[DeviceId]>,
    ) -> Result<()>;
    async fn get_connection_by_device(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
    ) -> Result<Option<Connection>>;

    async fn list_user_connections(
        &self,
        ctx: &flare_server_core::context::Context,
    ) -> Result<Vec<Connection>>;

    /// 分页读取租户在线索引 `tenant:sessions:{tenant}`（成员形如 `{user}:{device}`）。
    /// 返回 `(next_cursor, members)`；`next_cursor == 0` 表示扫完。
    async fn scan_tenant_sessions(
        &self,
        tenant_id: &str,
        cursor: u64,
        count: usize,
    ) -> Result<(u64, Vec<String>)>;

    /// 从租户在线索引移除一个成员（会话已不存在的陈旧项）。
    async fn unindex_tenant_session(&self, tenant_id: &str, member: &str) -> Result<()>;

    /// 通知各网关关闭该用户的全部长连接（撤销即断的 kick 频道）。
    async fn broadcast_user_kick(&self, user_id: &str) -> Result<()>;
}

/// `tenant:sessions:{tenant}` 的成员编码：`{user}:{device}`。
pub fn tenant_session_member(user_id: &str, device_id: &str) -> String {
    format!("{user_id}:{device_id}")
}

/// 反解 `{user}:{device}`；user_id 不含 `:`，按首个分隔符切分。
pub fn parse_tenant_session_member(member: &str) -> Option<(&str, &str)> {
    member
        .split_once(':')
        .filter(|(user, device)| !user.is_empty() && !device.is_empty())
}

/// 订阅仓库接口
pub trait SubscriptionRepository: Send + Sync {
    /// 添加订阅
    async fn add_subscription(&self, user_id: String, topic: String) -> Result<()>;
    async fn remove_subscription(
        &self,
        ctx: &flare_server_core::context::Context,
        topics: &[String],
    ) -> Result<()>;
    async fn get_user_subscriptions(
        &self,
        ctx: &flare_server_core::context::Context,
    ) -> Result<Vec<(String, HashMap<String, String>)>>;
    /// 获取主题的所有订阅者
    async fn get_topic_subscribers(&self, topic: &str) -> Result<Vec<String>>;
}

/// 信号发布接口
pub trait SignalPublisher: Send + Sync {
    /// 发布信号到主题
    async fn publish_signal(
        &self,
        topic: &str,
        payload: &[u8],
        metadata: &HashMap<String, String>,
    ) -> Result<()>;
}

/// 在线状态发布接口
pub trait PresencePublisher: Send + Sync {
    /// 发布在线状态事件
    async fn publish_presence_event(
        &self,
        event: flare_grpc_proto::signaling::online::PresenceEvent,
    ) -> Result<()>;
    /// 发布用户状态事件
    async fn publish_user_presence_event(
        &self,
        event: flare_grpc_proto::signaling::online::UserPresenceEvent,
    ) -> Result<()>;
}

/// 在线状态监听接口
pub trait PresenceWatcher: Send + Sync {
    /// 监听用户在线状态变化
    async fn watch_presence(
        &self,
        user_ids: &[String],
    ) -> Result<tokio::sync::mpsc::Receiver<flare_server_core::error::Result<PresenceChangeEvent>>>;
}

/// 在线状态变化事件
#[derive(Debug, Clone)]
pub struct PresenceChangeEvent {
    pub user_id: String,
    pub status: OnlineStatusRecord,
    pub occurred_at: chrono::DateTime<chrono::Utc>,
    pub conflict_action: Option<i32>, // ConflictAction enum value
    pub reason: Option<String>,
}
