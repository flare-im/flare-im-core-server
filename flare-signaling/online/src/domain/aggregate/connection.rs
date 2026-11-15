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
    ConnectionId, ConnectionQuality, DeviceAppState, DeviceAttention, DeviceId, DevicePriority,
    DeviceStateUpdate, TokenVersion, UserId,
};

/// Connection 聚合根
#[derive(Serialize, Deserialize)]
pub struct Connection {
    // === 聚合根标识 ===
    conversation_id: ConnectionId,
    user_id: UserId,
    device_id: DeviceId,
    /// 数据面租户 ID（默认 "0"）；`tenant:sessions:{tenant}` 索引按它维护
    tenant_id: String,
    /// Authenticated Social session binding; absent only on pre-upgrade/Core-native records.
    #[serde(default)]
    identity_session_id: Option<String>,

    // === 连接信息 ===
    device_platform: String, // ios/android/web/pc
    server_id: String,       // Signaling服务ID
    gateway_id: String,      // Gateway服务ID

    // === 会话属性（值对象）===
    device_priority: DevicePriority,
    token_version: TokenVersion,
    connection_quality: Option<ConnectionQuality>,

    // === 设备可达状态（客户端经网关上报）===
    #[serde(default)]
    app_state: DeviceAppState,
    #[serde(default)]
    attention: DeviceAttention,
    /// 进入当前应用状态的时刻：只在应用状态变化时前移，注意力与角标的更新不动它
    #[serde(default)]
    state_since: DateTime<Utc>,
    /// 最近一次上报的未读总数
    #[serde(default)]
    badge: u32,

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
    pub tenant_id: String,
    pub device_platform: String,
    pub server_id: String,
    pub gateway_id: String,
    pub device_priority: DevicePriority,
    pub token_version: TokenVersion,
    pub initial_quality: Option<ConnectionQuality>,
    /// 建连时的应用状态（CONNECT 元数据带来）
    pub app_state: DeviceAppState,
    /// 建连时的用户注意力
    pub attention: DeviceAttention,
}

impl Connection {
    // ==================== 工厂方法 ====================

    /// 创建新会话（工厂方法）
    ///
    /// 业务规则：
    /// 1. 自动生成会话ID（UUID v4）
    /// 2. 初始化心跳时间为当前时间
    /// 3. 设备可达状态取建连时带来的应用状态与注意力，角标从 0 起，等第一次上报
    /// 4. 发布 ConnectionCreated 事件
    pub fn create(params: ConnectionCreateParams) -> Self {
        let now = Utc::now();
        let conversation_id = ConnectionId::new();

        let mut session = Self {
            conversation_id: conversation_id.clone(),
            user_id: params.user_id.clone(),
            device_id: params.device_id.clone(),
            tenant_id: params.tenant_id,
            identity_session_id: None,
            device_platform: params.device_platform,
            server_id: params.server_id,
            gateway_id: params.gateway_id,
            device_priority: params.device_priority,
            token_version: params.token_version,
            connection_quality: params.initial_quality,
            app_state: params.app_state,
            attention: params.attention,
            state_since: now,
            badge: 0,
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
    /// 注意：此方法不发布事件，因为是从已存在的数据重建。设备可达状态由
    /// [`Self::with_device_state`] 补上；不补时是前台、自建连起。
    #[allow(clippy::too_many_arguments)]
    pub fn reconstitute(
        conversation_id: ConnectionId,
        user_id: UserId,
        device_id: DeviceId,
        tenant_id: String,
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
            tenant_id,
            identity_session_id: None,
            device_platform,
            server_id,
            gateway_id,
            device_priority,
            token_version,
            connection_quality,
            app_state: DeviceAppState::default(),
            attention: DeviceAttention::default(),
            state_since: created_at,
            badge: 0,
            created_at,
            last_heartbeat_at,
            domain_events: Vec::new(),
        }
    }

    /// 重建时补上持久化的设备可达状态（仓储专用）
    pub fn with_device_state(
        mut self,
        app_state: DeviceAppState,
        attention: DeviceAttention,
        state_since: DateTime<Utc>,
        badge: u32,
    ) -> Self {
        self.app_state = app_state;
        self.attention = attention;
        self.state_since = state_since;
        self.badge = badge;
        self
    }

    // ==================== 命令方法（修改状态）====================

    /// 记录一次设备状态上报
    ///
    /// 业务规则：
    /// 1. 应用状态、注意力、角标整体覆盖为上报值
    /// 2. 只有应用状态真的变了才把 `state_since` 前移到 `at`：同一状态下只改注意力或角标，
    ///    「进入当前状态多久」不重新计时
    pub fn report_device_state(&mut self, update: DeviceStateUpdate, at: DateTime<Utc>) {
        if self.app_state != update.app_state {
            self.state_since = at;
        }
        self.app_state = update.app_state;
        self.attention = update.attention;
        self.badge = update.badge;
    }

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

    /// 获取租户ID
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
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

    /// 获取设备应用状态
    pub fn app_state(&self) -> DeviceAppState {
        self.app_state
    }

    /// 获取用户注意力
    pub fn attention(&self) -> DeviceAttention {
        self.attention
    }

    /// 进入当前应用状态的时刻
    pub fn state_since(&self) -> DateTime<Utc> {
        self.state_since
    }

    /// 最近一次上报的未读总数
    pub fn badge(&self) -> u32 {
        self.badge
    }

    /// 获取最后心跳时间
    pub fn last_heartbeat_at(&self) -> DateTime<Utc> {
        self.last_heartbeat_at
    }

    pub fn identity_session_id(&self) -> Option<&str> {
        self.identity_session_id.as_deref()
    }

    pub fn with_identity_session_id(mut self, sid: Option<String>) -> Self {
        self.identity_session_id = sid;
        self
    }

    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
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

    /// 一次身份会话吊销是否覆盖一条连接
    ///
    /// 业务规则：
    /// 1. 带身份会话 ID 的连接按 ID 精确匹配，与截止时刻无关
    /// 2. 没有身份会话 ID 的连接（早于身份会话下发）按登记时刻不晚于截止时刻算；截止时刻是吊销
    ///    发生的那一刻，重试时不能换成当前时间
    ///
    /// 仓储对存储里的记录、领域服务对重建的聚合用的是这同一条规则。
    pub fn identity_revocation_covers(
        identity_session_id: Option<&str>,
        created_at_ms: i64,
        revoked_session_ids: &[String],
        cutoff_ms: i64,
    ) -> bool {
        match identity_session_id {
            Some(session_id) => revoked_session_ids
                .iter()
                .any(|revoked| revoked == session_id),
            None => created_at_ms <= cutoff_ms,
        }
    }

    /// 这条连接是否被一次身份会话吊销结束（见 [`Self::identity_revocation_covers`]）
    pub fn ended_by_identity_revocation(
        &self,
        revoked_session_ids: &[String],
        cutoff_ms: i64,
    ) -> bool {
        Self::identity_revocation_covers(
            self.identity_session_id(),
            self.created_at.timestamp_millis(),
            revoked_session_ids,
            cutoff_ms,
        )
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
            .field("tenant_id", &self.tenant_id)
            .field("device_platform", &self.device_platform)
            .field("server_id", &self.server_id)
            .field("gateway_id", &self.gateway_id)
            .field("device_priority", &self.device_priority)
            .field("token_version", &self.token_version)
            .field("connection_quality", &self.connection_quality)
            .field("app_state", &self.app_state)
            .field("attention", &self.attention)
            .field("state_since", &self.state_since)
            .field("badge", &self.badge)
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
            tenant_id: self.tenant_id.clone(),
            identity_session_id: self.identity_session_id.clone(),
            device_platform: self.device_platform.clone(),
            server_id: self.server_id.clone(),
            gateway_id: self.gateway_id.clone(),
            device_priority: self.device_priority,
            token_version: self.token_version,
            connection_quality: self.connection_quality.clone(),
            app_state: self.app_state,
            attention: self.attention,
            state_since: self.state_since,
            badge: self.badge,
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
            tenant_id: "0".to_string(),
            device_platform: "ios".to_string(),
            server_id: "server1".to_string(),
            gateway_id: "gateway1".to_string(),
            device_priority: DevicePriority::Normal,
            token_version: TokenVersion::new(1).unwrap(),
            initial_quality: None,
            app_state: DeviceAppState::Standby,
            attention: DeviceAttention::Idle,
        })
    }

    #[test]
    fn test_session_creation() {
        let session = create_test_session();

        assert_eq!(session.device_platform(), "ios");
        assert_eq!(session.device_priority(), DevicePriority::Normal);
        assert_eq!(session.domain_events().len(), 1); // ConnectionCreatedEvent
    }

    /// 建连带来的应用状态与注意力就是初始状态：自建连起计时，角标等第一次上报。
    #[test]
    fn a_new_session_starts_in_the_state_it_connected_with() {
        let session = create_test_session();

        assert_eq!(session.app_state(), DeviceAppState::Standby);
        assert_eq!(session.attention(), DeviceAttention::Idle);
        assert_eq!(session.state_since(), session.created_at());
        assert_eq!(session.badge(), 0);
    }

    /// 上报整体覆盖应用状态、注意力与角标；`state_since` 只在应用状态变化时前移。
    #[test]
    fn state_since_moves_only_when_the_app_state_changes() {
        let mut session = create_test_session();
        let connected_at = session.state_since();
        let later = connected_at + Duration::seconds(30);

        // 同一应用状态下只改注意力与角标：不重新计时。
        session.report_device_state(
            DeviceStateUpdate {
                app_state: DeviceAppState::Standby,
                attention: DeviceAttention::Attentive,
                badge: 7,
            },
            later,
        );
        assert_eq!(session.attention(), DeviceAttention::Attentive);
        assert_eq!(session.badge(), 7);
        assert_eq!(session.state_since(), connected_at);

        session.report_device_state(
            DeviceStateUpdate {
                app_state: DeviceAppState::Parking,
                attention: DeviceAttention::Unspecified,
                badge: 9,
            },
            later,
        );
        assert_eq!(session.app_state(), DeviceAppState::Parking);
        assert_eq!(session.attention(), DeviceAttention::Unspecified);
        assert_eq!(session.badge(), 9);
        assert_eq!(session.state_since(), later);
    }

    /// 身份会话吊销：带身份会话 ID 的连接只按 ID 匹配；没有 ID 的按登记时刻不晚于截止时刻算。
    #[test]
    fn an_identity_revocation_covers_named_sessions_and_older_unbound_connections() {
        let revoked = vec!["sid-old".to_string()];
        let bound = |sid: &str| create_test_session().with_identity_session_id(Some(sid.into()));
        let created_ms = |session: &Connection| session.created_at().timestamp_millis();

        let named = bound("sid-old");
        assert!(named.ended_by_identity_revocation(&revoked, 0));
        let other = bound("sid-new");
        assert!(!other.ended_by_identity_revocation(&revoked, i64::MAX));
        // 没点名任何会话：带 ID 的连接一条都不覆盖。
        assert!(!named.ended_by_identity_revocation(&[], i64::MAX));

        let unbound = create_test_session();
        assert!(unbound.ended_by_identity_revocation(&[], created_ms(&unbound)));
        assert!(!unbound.ended_by_identity_revocation(&revoked, created_ms(&unbound) - 1));
    }

    /// 重建时不补设备状态就是前台、自建连起；补了以持久化的值为准。
    #[test]
    fn reconstitution_restores_the_persisted_device_state() {
        let created_at = Utc::now() - Duration::minutes(5);
        let rebuild = || {
            Connection::reconstitute(
                ConnectionId::new(),
                UserId::new("test_user".to_string()).unwrap(),
                DeviceId::new("test_device".to_string()).unwrap(),
                "0".to_string(),
                "ios".to_string(),
                "server1".to_string(),
                "gateway1".to_string(),
                DevicePriority::Normal,
                TokenVersion::new(1).unwrap(),
                None,
                created_at,
                created_at,
            )
        };

        let untouched = rebuild();
        assert_eq!(untouched.app_state(), DeviceAppState::Active);
        assert_eq!(untouched.attention(), DeviceAttention::Unspecified);
        assert_eq!(untouched.state_since(), created_at);
        assert_eq!(untouched.badge(), 0);

        let parked_at = created_at + Duration::minutes(2);
        let parked = rebuild().with_device_state(
            DeviceAppState::Parking,
            DeviceAttention::Idle,
            parked_at,
            12,
        );
        assert_eq!(parked.app_state(), DeviceAppState::Parking);
        assert_eq!(parked.attention(), DeviceAttention::Idle);
        assert_eq!(parked.state_since(), parked_at);
        assert_eq!(parked.badge(), 12);
        // 克隆带着设备状态走。
        assert_eq!(parked.clone().badge(), 12);
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
