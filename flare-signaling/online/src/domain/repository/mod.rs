use std::collections::HashMap;

use flare_grpc_proto::signaling::online::DeviceConflictStrategy;
use flare_server_core::error::Result;
use serde::Deserialize;

use crate::domain::aggregate::Connection;
use crate::domain::model::OnlineStatusRecord;
use crate::domain::value_object::{ConnectionId, DeviceId, UserId};

/// A physical gateway connection removed by an atomic login admission.
/// The gateway must close only this connection, never a newer reconnect on
/// the same device ID.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct EvictedConnection {
    pub conversation_id: String,
    pub device_id: String,
    pub connection_id: String,
    /// 为什么被挤掉，决定下发给旧连接的踢线原因（[`EvictionCause::kick_reason`]）。
    #[serde(default)]
    pub cause: EvictionCause,
}

/// 登录准入挤掉一条旧连接的原因（准入脚本按记录给出）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvictionCause {
    /// 同一设备 ID 的新连接接替了旧连接。
    #[default]
    Replaced,
    /// 冲突策略挤掉了同一用户的另一台设备：互斥策略下的任何其他设备，或平台互斥策略下同平台的
    /// 其他设备。
    DeviceConflict,
    /// 设备数超过上限，挤掉了最早登录的设备。
    DeviceCap,
}

impl EvictionCause {
    /// 下发给被挤掉的连接的踢线原因。设备冲突是 [`kick_reason::DEVICE_CONFLICT`]：客户端据此结束
    /// 会话、不再自动重连（否则它连回去又把新设备挤掉，两台设备来回互踢）；同设备接替与超出设备数
    /// 上限仍是 [`kick_reason::SESSION_REPLACED`]。
    pub fn kick_reason(self) -> &'static str {
        match self {
            Self::DeviceConflict => kick_reason::DEVICE_CONFLICT,
            Self::Replaced | Self::DeviceCap => kick_reason::SESSION_REPLACED,
        }
    }
}

pub trait ConversationRepository: Send + Sync {
    async fn save_connection(&self, connection: &Connection) -> Result<()>;
    async fn admit_connection(
        &self,
        connection: &Connection,
        gateway_connection_id: &str,
        strategy: DeviceConflictStrategy,
        max_devices: u32,
    ) -> Result<Vec<EvictedConnection>>;
    async fn remove_connection(
        &self,
        tenant_id: &str,
        conversation_id: &ConnectionId,
        user_id: &UserId,
    ) -> Result<()>;

    /// Publish a physical-session-only kick, then atomically remove that session.
    /// Failed publication leaves it retryable; replacement/index must survive.
    /// Return the old physical gateway ID. Unsupported adapters fail closed.
    async fn remove_connection_for_kick(
        &self,
        _tenant: &str,
        _session: &ConnectionId,
        _user: &UserId,
        _reason: &str,
    ) -> Result<Option<String>> {
        Err(flare_server_core::flare_err!(
            flare_server_core::error::ErrorCode::ServiceUnavailable,
            "conditional session removal unavailable"
        ))
    }
    async fn touch_connection(
        &self,
        tenant_id: &str,
        conversation_id: &ConnectionId,
        user_id: &UserId,
    ) -> Result<()>;
    async fn fetch_statuses(
        &self,
        tenant_id: &str,
        user_ids: &[String],
    ) -> Result<HashMap<String, OnlineStatusRecord>>;
    async fn get_user_connections(
        &self,
        tenant_id: &str,
        user_id: &UserId,
    ) -> Result<Vec<Connection>>;
    async fn remove_user_connections(
        &self,
        tenant_id: &str,
        user_id: &UserId,
        device_ids: Option<&[DeviceId]>,
    ) -> Result<()>;
    async fn get_connection_by_device(
        &self,
        tenant_id: &str,
        user_id: &UserId,
        device_id: &DeviceId,
    ) -> Result<Option<Connection>>;

    async fn list_user_connections(
        &self,
        ctx: &flare_server_core::context::Context,
    ) -> Result<Vec<Connection>>;

    /// 分页读取租户在线索引 `tenant:sessions:{tenant}`（v2 长度编码成员）。
    /// 返回 `(next_cursor, members)`；`next_cursor == 0` 表示扫完。
    async fn scan_tenant_sessions(
        &self,
        tenant_id: &str,
        cursor: u64,
        count: usize,
    ) -> Result<(u64, Vec<String>)>;

    /// 从租户在线索引移除一个成员（会话已不存在的陈旧项）。
    async fn unindex_tenant_session(&self, tenant_id: &str, member: &str) -> Result<()>;

    /// 通知各网关关闭该用户的全部长连接（撤销即断的 kick 频道）。`reason` 是网关关连接前
    /// 下发给客户端的类型化原因（KICKED 帧），见 [`kick_reason`]。
    async fn broadcast_user_kick(
        &self,
        tenant_id: &str,
        user_id: &str,
        device_id: Option<&str>,
        connection_id: Option<&str>,
        reason: &str,
    ) -> Result<()>;
}

/// `tenant:sessions:{tenant}` 的 v2 成员编码：`v2:{user-byte-length}:{user}:{device}`。
/// User/device IDs may contain `:`, so splitting on the first delimiter is unsafe.
pub fn tenant_session_member_prefix(user_id: &str) -> String {
    format!("v2:{}:{user_id}:", user_id.len())
}

pub fn tenant_session_member(user_id: &str, device_id: &str) -> String {
    format!("{}{device_id}", tenant_session_member_prefix(user_id))
}

/// 反解 v2 成员；不双读旧的歧义格式。
pub fn parse_tenant_session_member(member: &str) -> Option<(&str, &str)> {
    let rest = member.strip_prefix("v2:")?;
    let (length, rest) = rest.split_once(':')?;
    let user_len = length.parse::<usize>().ok()?;
    let user = rest.get(..user_len)?;
    let device = rest.get(user_len..)?.strip_prefix(':')?;
    (!user.is_empty() && !device.is_empty()).then_some((user, device))
}

/// 订阅仓库接口
pub trait SubscriptionRepository: Send + Sync {
    /// 添加订阅
    async fn add_subscription(&self, tenant_id: &str, user_id: String, topic: String)
    -> Result<()>;
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
    async fn get_topic_subscribers(&self, tenant_id: &str, topic: &str) -> Result<Vec<String>>;
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
        tenant_id: &str,
        event: flare_grpc_proto::signaling::online::PresenceEvent,
    ) -> Result<()>;
    /// 发布用户状态事件
    async fn publish_user_presence_event(
        &self,
        tenant_id: &str,
        event: flare_grpc_proto::signaling::online::UserPresenceEvent,
    ) -> Result<()>;
}

/// 在线状态监听接口
pub trait PresenceWatcher: Send + Sync {
    /// 监听用户在线状态变化
    async fn watch_presence(
        &self,
        tenant_id: &str,
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

/// 踢线时下发给客户端的类型化原因（KICKED 帧的 message）。
pub mod kick_reason {
    /// 租户停用 / 删除中：与建连拒绝、HTTP 拒绝用同一个错误码。
    pub const TENANT_UNAVAILABLE: &str = "TENANT_UNAVAILABLE";
    /// 运维按设备踢下线。
    pub const DEVICE_KICKED: &str = "DEVICE_KICKED";
    /// 同一用户的另一台设备登录，冲突策略（互斥，或平台互斥下的同平台）把这台设备挤下线
    /// （「已在别处登录」）。与 flare-core `common::kick::DEVICE_CONFLICT_REASON` 是同一个值：
    /// 客户端收到后结束会话、不再自动重连。
    pub const DEVICE_CONFLICT: &str = "DEVICE_CONFLICT";
    /// 同设备的新连接接替了旧连接，或设备数超限挤掉了旧设备。
    pub const SESSION_REPLACED: &str = "SESSION_REPLACED";
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 冲突策略挤掉的其他设备下发 DEVICE_CONFLICT（客户端结束会话、不再重连）；同设备接替与超出
    /// 设备数上限仍是 SESSION_REPLACED。
    #[test]
    fn only_device_conflict_evictions_end_the_kicked_session() {
        assert_eq!(
            EvictionCause::DeviceConflict.kick_reason(),
            kick_reason::DEVICE_CONFLICT
        );
        assert_eq!(
            EvictionCause::Replaced.kick_reason(),
            kick_reason::SESSION_REPLACED
        );
        assert_eq!(
            EvictionCause::DeviceCap.kick_reason(),
            kick_reason::SESSION_REPLACED
        );
    }

    /// 准入脚本的输出：每条带 `cause`；没带的按同设备接替处理。
    #[test]
    fn admission_results_carry_the_eviction_cause() {
        let evicted: Vec<EvictedConnection> = serde_json::from_str(
            r#"[
              {"conversation_id":"c1","device_id":"d1","connection_id":"p1","cause":"device_conflict"},
              {"conversation_id":"c2","device_id":"d2","connection_id":"p2","cause":"replaced"},
              {"conversation_id":"c3","device_id":"d3","connection_id":"p3","cause":"device_cap"},
              {"conversation_id":"c4","device_id":"d4","connection_id":"p4"}
            ]"#,
        )
        .unwrap();
        let causes: Vec<EvictionCause> = evicted.iter().map(|e| e.cause).collect();
        assert_eq!(
            causes,
            vec![
                EvictionCause::DeviceConflict,
                EvictionCause::Replaced,
                EvictionCause::DeviceCap,
                EvictionCause::Replaced,
            ]
        );
    }
}
