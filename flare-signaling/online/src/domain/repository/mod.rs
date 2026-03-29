use std::collections::HashMap;

use anyhow::Result;

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
    async fn touch_connection(&self, user_id: &UserId) -> Result<()>;
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
        event: flare_proto::signaling::online::PresenceEvent,
    ) -> Result<()>;
    /// 发布用户状态事件
    async fn publish_user_presence_event(
        &self,
        event: flare_proto::signaling::online::UserPresenceEvent,
    ) -> Result<()>;
}

/// 在线状态监听接口

pub trait PresenceWatcher: Send + Sync {
    /// 监听用户在线状态变化
    async fn watch_presence(
        &self,
        user_ids: &[String],
    ) -> Result<tokio::sync::mpsc::Receiver<anyhow::Result<PresenceChangeEvent>>>;
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
