//! 连接实体(聚合根)
//!
//! 封装连接管理逻辑,提供状态转换方法,作为连接上下文的聚合根。
//! 基于 DDD 原则,Connection 是访问连接相关数据的唯一入口。

use chrono::{DateTime, Utc};
use flare_core::common::device::DeviceInfo;
use std::collections::HashMap;

/// 连接状态枚举
///
/// 定义连接的生命周期状态,支持显式状态机建模。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectionState {
    /// 已断开
    Disconnected,
    /// 连接中
    Connecting,
    /// 已认证
    Authenticated,
    /// 活跃中
    Active,
}

impl ConnectionState {
    /// 获取状态的字符串表示
    pub fn as_str(&self) -> &'static str {
        match self {
            ConnectionState::Disconnected => "disconnected",
            ConnectionState::Connecting => "connecting",
            ConnectionState::Authenticated => "authenticated",
            ConnectionState::Active => "active",
        }
    }
}

/// 连接质量
///
/// 记录连接的网络质量指标,用于监控和优化连接体验。
#[derive(Debug, Clone)]
pub struct ConnectionQuality {
    /// 往返时延(Round-Trip Time),单位:毫秒
    pub rtt_ms: u32,
    /// 丢包率,单位:百分比
    pub packet_loss_rate: u32,
    /// 网络类型(如 "wifi", "4g", "5g")
    pub network_type: String,
    /// 最后测量时间
    pub last_measure_ts: DateTime<Utc>,
}

impl ConnectionQuality {
    /// 创建新的连接质量指标
    pub fn new() -> Self {
        Self {
            rtt_ms: 0,
            packet_loss_rate: 0,
            network_type: "unknown".to_string(),
            last_measure_ts: Utc::now(),
        }
    }

    /// 创建指定质量的连接指标
    pub fn with_quality(rtt_ms: u32, packet_loss_rate: u32, network_type: String) -> Self {
        Self {
            rtt_ms,
            packet_loss_rate,
            network_type,
            last_measure_ts: Utc::now(),
        }
    }

    /// 更新测量时间
    pub fn update_timestamp(&mut self) {
        self.last_measure_ts = Utc::now();
    }
}

/// 连接实体(聚合根)
///
/// 封装连接管理的核心逻辑,包括状态转换、质量更新等。
/// 作为连接上下文的聚合根,确保业务不变式的一致性。
#[derive(Debug, Clone)]
pub struct Connection {
    /// 连接ID
    pub connection_id: String,
    /// 用户ID
    pub user_id: String,
    /// 租户ID
    pub tenant_id: String,
    /// 设备信息
    pub device_info: DeviceInfo,
    /// 连接状态
    pub state: ConnectionState,
    /// 连接元数据
    pub metadata: HashMap<String, String>,
    /// 连接质量
    pub quality: ConnectionQuality,
    /// 会话ID(来自 Signaling Online)
    pub conversation_id: Option<String>,
    /// 连接建立时间
    pub connected_at: DateTime<Utc>,
    /// 最后活跃时间
    pub last_active_at: DateTime<Utc>,
    /// 传输协议
    pub protocol: String,
}

impl Connection {
    /// 创建新连接
    ///
    /// 初始化连接状态为 Connecting,记录连接建立时间。
    ///
    /// # 参数
    /// - `connection_id`: 连接唯一标识符
    /// - `user_id`: 用户ID
    /// - `tenant_id`: 租户ID
    /// - `device_info`: 设备信息
    /// - `protocol`: 传输协议(如 "websocket", "quic")
    pub fn new(
        connection_id: String,
        user_id: String,
        tenant_id: String,
        device_info: DeviceInfo,
        protocol: String,
    ) -> Self {
        let now = Utc::now();
        Self {
            connection_id,
            user_id,
            tenant_id,
            device_info,
            state: ConnectionState::Connecting,
            metadata: HashMap::new(),
            quality: ConnectionQuality::new(),
            conversation_id: None,
            connected_at: now,
            last_active_at: now,
            protocol,
        }
    }

    /// 认证连接
    ///
    /// 将连接状态从 Connecting 转换为 Authenticated,并设置会话ID。
    ///
    /// # 参数
    /// - `conversation_id`: 会话ID
    ///
    /// # 返回
    /// - `Ok(())`: 认证成功
    /// - `Err(DomainError)`: 状态转换失败
    pub fn authenticate(&mut self, conversation_id: String) -> Result<(), DomainError> {
        if self.state != ConnectionState::Connecting {
            return Err(DomainError::InvalidState {
                expected: ConnectionState::Connecting,
                actual: self.state,
            });
        }

        self.state = ConnectionState::Authenticated;
        self.conversation_id = Some(conversation_id);
        self.update_last_active();
        Ok(())
    }

    /// 激活连接(心跳成功)
    ///
    /// 将连接状态从 Authenticated 转换为 Active。
    ///
    /// # 返回
    /// - `Ok(())`: 激活成功
    /// - `Err(DomainError)`: 状态转换失败
    pub fn activate(&mut self) -> Result<(), DomainError> {
        if self.state != ConnectionState::Authenticated {
            return Err(DomainError::InvalidState {
                expected: ConnectionState::Authenticated,
                actual: self.state,
            });
        }

        self.state = ConnectionState::Active;
        self.update_last_active();
        Ok(())
    }

    /// 断开连接
    ///
    /// 将连接状态转换为 Disconnected。
    ///
    /// # 返回
    /// - `Ok(())`: 断开成功
    /// - `Err(DomainError)`: 状态转换失败
    pub fn disconnect(&mut self) -> Result<(), DomainError> {
        if self.state == ConnectionState::Disconnected {
            return Err(DomainError::AlreadyDisconnected);
        }

        self.state = ConnectionState::Disconnected;
        Ok(())
    }

    /// 更新连接质量
    ///
    /// 更新连接的网络质量指标。
    ///
    /// # 参数
    /// - `quality`: 新的连接质量指标
    pub fn update_quality(&mut self, quality: ConnectionQuality) {
        self.quality = quality;
    }

    /// 更新最后活跃时间
    pub fn update_last_active(&mut self) {
        self.last_active_at = Utc::now();
    }

    /// 检查连接是否活跃
    pub fn is_active(&self) -> bool {
        self.state == ConnectionState::Active
    }

    /// 检查连接是否已认证
    pub fn is_authenticated(&self) -> bool {
        matches!(
            self.state,
            ConnectionState::Authenticated | ConnectionState::Active
        )
    }

    /// 获取连接时长(毫秒)
    pub fn duration_ms(&self) -> i64 {
        (Utc::now() - self.connected_at).num_milliseconds()
    }

    /// 添加元数据
    pub fn add_metadata(&mut self, key: String, value: String) {
        self.metadata.insert(key, value);
    }

    /// 获取元数据
    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.metadata.get(key)
    }
}

/// 领域错误
///
/// 定义连接管理过程中的业务错误。
#[derive(Debug, thiserror::Error)]
pub enum DomainError {
    /// 状态转换错误
    #[error("Invalid state: expected {expected:?}, actual {actual:?}")]
    InvalidState {
        expected: ConnectionState,
        actual: ConnectionState,
    },

    /// 连接已断开
    #[error("Connection already disconnected")]
    AlreadyDisconnected,

    /// 连接未找到
    #[error("Connection not found: {0}")]
    ConnectionNotFound(String),

    /// 认证失败
    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_core::common::device::DevicePlatform;

    #[test]
    fn test_new_connection() {
        let device_info = DeviceInfo::new("device123".to_string(), DevicePlatform::Web);
        let connection = Connection::new(
            "conn123".to_string(),
            "user123".to_string(),
            "tenant123".to_string(),
            device_info,
            "websocket".to_string(),
        );

        assert_eq!(connection.connection_id, "conn123");
        assert_eq!(connection.user_id, "user123");
        assert_eq!(connection.state, ConnectionState::Connecting);
        assert!(connection.conversation_id.is_none());
    }

    #[test]
    fn test_authenticate_success() {
        let device_info = DeviceInfo::new("device123".to_string(), DevicePlatform::Web);
        let mut connection = Connection::new(
            "conn123".to_string(),
            "user123".to_string(),
            "tenant123".to_string(),
            device_info,
            "websocket".to_string(),
        );

        let result = connection.authenticate("conv123".to_string());
        assert!(result.is_ok());
        assert_eq!(connection.state, ConnectionState::Authenticated);
        assert_eq!(connection.conversation_id, Some("conv123".to_string()));
    }

    #[test]
    fn test_authenticate_invalid_state() {
        let device_info = DeviceInfo::new("device123".to_string(), DevicePlatform::Web);
        let mut connection = Connection::new(
            "conn123".to_string(),
            "user123".to_string(),
            "tenant123".to_string(),
            device_info,
            "websocket".to_string(),
        );

        // 先认证
        connection.authenticate("conv123".to_string()).unwrap();

        // 再次认证应该失败
        let result = connection.authenticate("conv456".to_string());
        assert!(result.is_err());
        assert!(matches!(result, Err(DomainError::InvalidState { .. })));
    }

    #[test]
    fn test_activate_success() {
        let device_info = DeviceInfo::new("device123".to_string(), DevicePlatform::Web);
        let mut connection = Connection::new(
            "conn123".to_string(),
            "user123".to_string(),
            "tenant123".to_string(),
            device_info,
            "websocket".to_string(),
        );

        connection.authenticate("conv123".to_string()).unwrap();
        let result = connection.activate();
        assert!(result.is_ok());
        assert_eq!(connection.state, ConnectionState::Active);
    }

    #[test]
    fn test_activate_invalid_state() {
        let device_info = DeviceInfo::new("device123".to_string(), DevicePlatform::Web);
        let mut connection = Connection::new(
            "conn123".to_string(),
            "user123".to_string(),
            "tenant123".to_string(),
            device_info,
            "websocket".to_string(),
        );

        // 未认证就激活应该失败
        let result = connection.activate();
        assert!(result.is_err());
        assert!(matches!(result, Err(DomainError::InvalidState { .. })));
    }

    #[test]
    fn test_disconnect_success() {
        let device_info = DeviceInfo::new("device123".to_string(), DevicePlatform::Web);
        let mut connection = Connection::new(
            "conn123".to_string(),
            "user123".to_string(),
            "tenant123".to_string(),
            device_info,
            "websocket".to_string(),
        );

        connection.authenticate("conv123".to_string()).unwrap();
        connection.activate().unwrap();

        let result = connection.disconnect();
        assert!(result.is_ok());
        assert_eq!(connection.state, ConnectionState::Disconnected);
    }

    #[test]
    fn test_disconnect_already_disconnected() {
        let device_info = DeviceInfo::new("device123".to_string(), DevicePlatform::Web);
        let mut connection = Connection::new(
            "conn123".to_string(),
            "user123".to_string(),
            "tenant123".to_string(),
            device_info,
            "websocket".to_string(),
        );

        connection.authenticate("conv123".to_string()).unwrap();
        connection.activate().unwrap();
        connection.disconnect().unwrap();

        // 再次断开应该失败
        let result = connection.disconnect();
        assert!(result.is_err());
        assert!(matches!(result, Err(DomainError::AlreadyDisconnected)));
    }

    #[test]
    fn test_is_active() {
        let device_info = DeviceInfo::new("device123".to_string(), DevicePlatform::Web);
        let mut connection = Connection::new(
            "conn123".to_string(),
            "user123".to_string(),
            "tenant123".to_string(),
            device_info,
            "websocket".to_string(),
        );

        assert!(!connection.is_active());

        connection.authenticate("conv123".to_string()).unwrap();
        assert!(!connection.is_active());

        connection.activate().unwrap();
        assert!(connection.is_active());
    }

    #[test]
    fn test_is_authenticated() {
        let device_info = DeviceInfo::new("device123".to_string(), DevicePlatform::Web);
        let mut connection = Connection::new(
            "conn123".to_string(),
            "user123".to_string(),
            "tenant123".to_string(),
            device_info,
            "websocket".to_string(),
        );

        assert!(!connection.is_authenticated());

        connection.authenticate("conv123".to_string()).unwrap();
        assert!(connection.is_authenticated());

        connection.activate().unwrap();
        assert!(connection.is_authenticated());
    }

    #[test]
    fn test_update_quality() {
        let device_info = DeviceInfo::new("device123".to_string(), DevicePlatform::Web);
        let mut connection = Connection::new(
            "conn123".to_string(),
            "user123".to_string(),
            "tenant123".to_string(),
            device_info,
            "websocket".to_string(),
        );

        let quality = ConnectionQuality::with_quality(50, 1, "wifi".to_string());
        connection.update_quality(quality);

        assert_eq!(connection.quality.rtt_ms, 50);
        assert_eq!(connection.quality.packet_loss_rate, 1);
        assert_eq!(connection.quality.network_type, "wifi");
    }

    #[test]
    fn test_metadata() {
        let device_info = DeviceInfo::new("device123".to_string(), DevicePlatform::Web);
        let mut connection = Connection::new(
            "conn123".to_string(),
            "user123".to_string(),
            "tenant123".to_string(),
            device_info,
            "websocket".to_string(),
        );

        connection.add_metadata("key1".to_string(), "value1".to_string());
        connection.add_metadata("key2".to_string(), "value2".to_string());

        assert_eq!(connection.get_metadata("key1"), Some(&"value1".to_string()));
        assert_eq!(connection.get_metadata("key2"), Some(&"value2".to_string()));
        assert_eq!(connection.get_metadata("key3"), None);
    }

    #[test]
    fn test_duration_ms() {
        let device_info = DeviceInfo::new("device123".to_string(), DevicePlatform::Web);
        let connection = Connection::new(
            "conn123".to_string(),
            "user123".to_string(),
            "tenant123".to_string(),
            device_info,
            "websocket".to_string(),
        );

        let duration = connection.duration_ms();
        assert!(duration >= 0);
    }
}
