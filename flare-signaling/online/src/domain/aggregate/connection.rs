//! Connection 聚合根
//!
//! 职责：管理单个设备的会话生命周期
//!
//! 聚合根特性：
//! 1. 封装业务规则 - 所有状态修改都通过方法，保证不变性约束
//! 2. 发布领域事件 - 每次状态变更都发布事件
//! 3. 富领域模型 - 行为和数据在一起，不是贫血模型
//!
//! 设计参考：微信会话管理、Telegram设备管理

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use super::super::event::DomainEvent;
use super::super::value_object::{
    ConnectionId, ConnectionQuality, DeviceId, DevicePriority, TokenVersion, UserId,
};

/// Connection 聚合根
#[derive(Serialize, Deserialize)]
pub struct Connection {
    // === 聚合根标识 ===
    conversation_id: ConnectionId,
    user_id: UserId,
    device_id: DeviceId,

    // === 连接信息 ===
    device_platform: String, // ios/android/web/pc
    server_id: String,       // Signaling服务ID
    gateway_id: String,      // Gateway服务ID

    // === 会话属性（值对象）===
    device_priority: DevicePriority,
    token_version: TokenVersion,
    connection_quality: Option<ConnectionQuality>,

    // === 生命周期 ===
    created_at: DateTime<Utc>,
    last_heartbeat_at: DateTime<Utc>,

    // === 领域事件（未提交的事件）===
    #[serde(skip)]
    domain_events: Vec<Box<dyn DomainEvent>>,
}

/// Connection 创建参数
pub struct ConnectionCreateParams {
    pub user_id: UserId,
    pub device_id: DeviceId,
    pub device_platform: String,
    pub server_id: String,
    pub gateway_id: String,
    pub device_priority: DevicePriority,
    pub token_version: TokenVersion,
    pub initial_quality: Option<ConnectionQuality>,
}

impl Connection {
    // ==================== 工厂方法 ====================

    /// 创建新会话（工厂方法）
    ///
    /// 业务规则：
    /// 1. 自动生成会话ID（UUID v4）
    /// 2. 初始化心跳时间为当前时间
    /// 3. 发布 ConnectionCreated 事件
    pub fn create(params: ConnectionCreateParams) -> Self {
        let now = Utc::now();
        let conversation_id = ConnectionId::new();

        let mut session = Self {
            conversation_id: conversation_id.clone(),
            user_id: params.user_id.clone(),
            device_id: params.device_id.clone(),
            device_platform: params.device_platform,
            server_id: params.server_id,
            gateway_id: params.gateway_id,
            device_priority: params.device_priority,
            token_version: params.token_version,
            connection_quality: params.initial_quality,
            created_at: now,
            last_heartbeat_at: now,
            domain_events: Vec::new(),
        };

        // 发布会话创建事件
        session.add_event(crate::domain::event::ConnectionCreatedEvent {
            conversation_id,
            user_id: params.user_id,
            device_id: params.device_id,
            device_priority: params.device_priority,
            token_version: params.token_version,
            occurred_at: now,
        });

        session
    }

    /// 从持久化数据重建聚合根（仓储专用）
    ///
    /// 注意：此方法不发布事件，因为是从已存在的数据重建
    #[allow(clippy::too_many_arguments)]
    pub fn reconstitute(
        conversation_id: ConnectionId,
        user_id: UserId,
        device_id: DeviceId,
        device_platform: String,
        server_id: String,
        gateway_id: String,
        device_priority: DevicePriority,
        token_version: TokenVersion,
        connection_quality: Option<ConnectionQuality>,
        created_at: DateTime<Utc>,
        last_heartbeat_at: DateTime<Utc>,
    ) -> Self {
        Self {
            conversation_id,
            user_id,
            device_id,
            device_platform,
            server_id,
            gateway_id,
            device_priority,
            token_version,
            connection_quality,
            created_at,
            last_heartbeat_at,
            domain_events: Vec::new(),
        }
    }

    // ==================== 命令方法（修改状态）====================

    /// 刷新心跳
    ///
    /// 业务规则：
    /// 1. 更新最后心跳时间
    /// 2. 如果提供了质量数据，更新链接质量
    /// 3. 如果质量显著变化，发布 QualityChanged 事件
    pub fn refresh_heartbeat(&mut self, quality: Option<ConnectionQuality>) -> Result<(), String> {
        let now = Utc::now();
        let old_quality = self.connection_quality.clone();

        self.last_heartbeat_at = now;

        if let Some(new_quality) = quality {
            // 检测质量是否显著变化（等级变化）
            let quality_changed = match &old_quality {
                Some(old) => old.quality_level() != new_quality.quality_level(),
                None => true, // 首次上报质量
            };

            self.connection_quality = Some(new_quality.clone());

            if quality_changed {
                // 发布质量变化事件
                self.add_event(crate::domain::event::QualityChangedEvent {
                    conversation_id: self.conversation_id.clone(),
                    user_id: self.user_id.clone(),
                    device_id: self.device_id.clone(),
                    old_quality,
                    new_quality: Some(new_quality),
                    occurred_at: now, // 使用方法开始时的时间戳保持一致性
                });
            }
        }

        Ok(())
    }

    /// 更新链接质量
    ///
    /// 业务规则：
    /// 1. 验证质量数据有效性
    /// 2. 如果质量等级变化，发布事件
    pub fn update_quality(&mut self, quality: ConnectionQuality) -> Result<(), String> {
        let old_quality = self.connection_quality.clone();

        // 检测质量等级是否变化
        let quality_changed = match &old_quality {
            Some(old) => old.quality_level() != quality.quality_level(),
            None => true,
        };

        self.connection_quality = Some(quality.clone());

        if quality_changed {
            self.add_event(crate::domain::event::QualityChangedEvent {
                conversation_id: self.conversation_id.clone(),
                user_id: self.user_id.clone(),
                device_id: self.device_id.clone(),
                old_quality,
                new_quality: Some(quality),
                occurred_at: Utc::now(),
            });
        }

        Ok(())
    }

    /// 更新设备优先级
    ///
    /// 业务规则：
    /// 1. 只允许提升优先级或降低到Normal
    /// 2. 不允许直接降到Low（需要通过其他机制）
    pub fn update_priority(&mut self, new_priority: DevicePriority) -> Result<(), String> {
        if new_priority == DevicePriority::Low && self.device_priority != DevicePriority::Low {
            return Err("Cannot directly downgrade to Low priority".to_string());
        }

        let old_priority = self.device_priority;
        self.device_priority = new_priority;

        self.add_event(crate::domain::event::PriorityChangedEvent {
            conversation_id: self.conversation_id.clone(),
            user_id: self.user_id.clone(),
            device_id: self.device_id.clone(),
            old_priority,
            new_priority,
            occurred_at: Utc::now(),
        });

        Ok(())
    }

    /// 踢出会话（强制下线）
    ///
    /// 业务规则：
    /// 1. 发布 ConnectionKicked 事件
    /// 2. 标记会话为已失效（实际删除由仓储完成）
    pub fn kick(&mut self, reason: String) -> Result<(), String> {
        self.add_event(crate::domain::event::ConnectionKickedEvent {
            conversation_id: self.conversation_id.clone(),
            user_id: self.user_id.clone(),
            device_id: self.device_id.clone(),
            reason,
            occurred_at: Utc::now(),
        });

        Ok(())
    }

    // ==================== 查询方法（不修改状态）====================

    /// 获取会话ID
    pub fn id(&self) -> &ConnectionId {
        &self.conversation_id
    }

    /// 获取用户ID
    pub fn user_id(&self) -> &UserId {
        &self.user_id
    }

    /// 获取设备ID
    pub fn device_id(&self) -> &DeviceId {
        &self.device_id
    }

    /// 获取设备平台
    pub fn device_platform(&self) -> &str {
        &self.device_platform
    }

    /// 获取Gateway ID
    pub fn gateway_id(&self) -> &str {
        &self.gateway_id
    }

    /// 获取Server ID
    pub fn server_id(&self) -> &str {
        &self.server_id
    }

    /// 获取设备优先级
    pub fn device_priority(&self) -> DevicePriority {
        self.device_priority
    }

    /// 获取Token版本
    pub fn token_version(&self) -> TokenVersion {
        self.token_version
    }

    /// 获取链接质量
    pub fn connection_quality(&self) -> Option<&ConnectionQuality> {
        self.connection_quality.as_ref()
    }

    /// 获取最后心跳时间
    pub fn last_heartbeat_at(&self) -> DateTime<Utc> {
        self.last_heartbeat_at
    }

    /// 计算质量评分（0-100）
    ///
    /// 业务规则：
    /// - 如果没有质量数据，返回默认分数50
    /// - 如果质量数据过期（超过30秒），降低评分
    pub fn quality_score(&self) -> f64 {
        match &self.connection_quality {
            Some(quality) => {
                let base_score = quality.quality_score();

                // 如果质量数据过期，降低评分
                if quality.is_stale() {
                    base_score * 0.7
                } else {
                    base_score
                }
            }
            None => 50.0, // 默认中等分数
        }
    }

    /// 判断会话是否过期
    ///
    /// 业务规则：超过指定时间未心跳视为过期
    ///
    /// 参考：微信30秒心跳超时，钉钉60秒
    pub fn is_expired(&self, timeout: Duration) -> bool {
        let now = Utc::now();
        let elapsed = now.signed_duration_since(self.last_heartbeat_at);
        elapsed > timeout
    }

    /// 判断链接质量是否健康
    pub fn is_healthy(&self) -> bool {
        self.connection_quality
            .as_ref()
            .map(|q| q.is_healthy())
            .unwrap_or(false)
    }

    /// 判断是否应该被Token版本踢出
    pub fn should_be_kicked_by_version(&self, new_version: TokenVersion) -> bool {
        new_version.should_kick(&self.token_version)
    }

    // ==================== 领域事件管理 ====================

    /// 添加领域事件
    fn add_event(&mut self, event: impl DomainEvent + 'static) {
        self.domain_events.push(Box::new(event));
    }

    /// 获取所有未提交的领域事件
    pub fn domain_events(&self) -> &[Box<dyn DomainEvent>] {
        &self.domain_events
    }

    /// 清空领域事件（事件发布后调用）
    pub fn clear_events(&mut self) {
        self.domain_events.clear();
    }
}

// 手动实现 Debug 和 Clone，避免 domain_events 的 trait 对象限制
impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("conversation_id", &self.conversation_id)
            .field("user_id", &self.user_id)
            .field("device_id", &self.device_id)
            .field("device_platform", &self.device_platform)
            .field("server_id", &self.server_id)
            .field("gateway_id", &self.gateway_id)
            .field("device_priority", &self.device_priority)
            .field("token_version", &self.token_version)
            .field("connection_quality", &self.connection_quality)
            .field("created_at", &self.created_at)
            .field("last_heartbeat_at", &self.last_heartbeat_at)
            .finish()
    }
}

impl Clone for Connection {
    fn clone(&self) -> Self {
        Self {
            conversation_id: self.conversation_id.clone(),
            user_id: self.user_id.clone(),
            device_id: self.device_id.clone(),
            device_platform: self.device_platform.clone(),
            server_id: self.server_id.clone(),
            gateway_id: self.gateway_id.clone(),
            device_priority: self.device_priority,
            token_version: self.token_version,
            connection_quality: self.connection_quality.clone(),
            created_at: self.created_at,
            last_heartbeat_at: self.last_heartbeat_at,
            domain_events: Vec::new(),
        }
    }
}

// ==================== 比较与排序 ====================

impl PartialEq for Connection {
    fn eq(&self, other: &Self) -> bool {
        self.conversation_id == other.conversation_id
    }
}

impl Eq for Connection {}

/// 为Connection实现排序（按质量评分降序）
impl PartialOrd for Connection {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // 首先按优先级降序
        match other.device_priority.cmp(&self.device_priority) {
            std::cmp::Ordering::Equal => {
                // 同优先级按质量评分降序
                let self_score = self.quality_score();
                let other_score = other.quality_score();
                other_score.partial_cmp(&self_score)
            }
            ordering => Some(ordering),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_session() -> Connection {
        Connection::create(ConnectionCreateParams {
            user_id: UserId::new("test_user".to_string()).unwrap(),
            device_id: DeviceId::new("test_device".to_string()).unwrap(),
            device_platform: "ios".to_string(),
            server_id: "server1".to_string(),
            gateway_id: "gateway1".to_string(),
            device_priority: DevicePriority::Normal,
            token_version: TokenVersion::new(1).unwrap(),
            initial_quality: None,
        })
    }

    #[test]
    fn test_session_creation() {
        let session = create_test_session();

        assert_eq!(session.device_platform(), "ios");
        assert_eq!(session.device_priority(), DevicePriority::Normal);
        assert_eq!(session.domain_events().len(), 1); // ConnectionCreatedEvent
    }

    #[test]
    fn test_refresh_heartbeat() {
        let mut session = create_test_session();
        let old_heartbeat = session.last_heartbeat_at();

        std::thread::sleep(std::time::Duration::from_millis(10));

        session.refresh_heartbeat(None).unwrap();

        assert!(session.last_heartbeat_at() > old_heartbeat);
    }

    #[test]
    fn test_quality_score() {
        let mut session = create_test_session();

        // 没有质量数据，默认50分
        assert_eq!(session.quality_score(), 50.0);

        // 添加优质链接
        let quality = ConnectionQuality::new(
            30,
            0.001,
            crate::domain::value_object::NetworkType::Wifi,
            None,
        )
        .unwrap();

        session.update_quality(quality).unwrap();
        assert!(session.quality_score() > 80.0);
    }

    #[test]
    fn test_token_version_kick() {
        let mut session = create_test_session();

        let new_version = TokenVersion::new(2).unwrap();
        assert!(session.should_be_kicked_by_version(new_version));

        session.kick("Token version outdated".to_string()).unwrap();
        assert_eq!(session.domain_events().len(), 2); // Created + Kicked
    }

    #[test]
    fn test_session_expiration() {
        let session = create_test_session();

        assert!(!session.is_expired(Duration::seconds(60)));
        assert!(session.is_expired(Duration::seconds(0)));
    }
}
