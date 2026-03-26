//! 连接上下文值对象
//!
//! 表示当前长连接请求的认证与设备信息，由接口层解析 connection_id 后注入应用层。
//! 查询连接列表时由 ConnectionQuery 填充 protocol / connected_at / last_active_at。

use std::collections::HashMap;

use chrono::{DateTime, Utc};

/// 连接上下文（值对象）
///
/// 由 ConnectionContextResolver 根据 connection_id 解析得到，供 Command/Query 使用。
/// 飞书/Telegram/WhatsApp 风格：device_id + platform 用于多端路由与推送策略。
#[derive(Clone, Debug)]
pub struct ConnectionInfo {
    pub connection_id: String,
    pub user_id: String,
    pub tenant_id: String,
    pub device_id: String,
    /// 设备平台（ios/android/web/pc），用于推送策略与多端区分
    pub platform: Option<String>,
    /// 连接元数据（用于 gRPC 等构建请求上下文），可选
    pub metadata: Option<HashMap<String, String>>,
    /// 传输协议（如 websocket/quic），查询连接列表时由基础设施填充
    pub protocol: Option<String>,
    /// 连接建立时间，查询连接列表时由基础设施填充
    pub connected_at: Option<DateTime<Utc>>,
    /// 最后活跃时间，查询连接列表时由基础设施填充
    pub last_active_at: Option<DateTime<Utc>>,
}

impl ConnectionInfo {
    pub fn new(
        connection_id: String,
        user_id: String,
        tenant_id: String,
        device_id: String,
    ) -> Self {
        Self {
            connection_id,
            user_id,
            tenant_id,
            device_id,
            platform: None,
            metadata: None,
            protocol: None,
            connected_at: None,
            last_active_at: None,
        }
    }

    pub fn with_platform(mut self, platform: String) -> Self {
        self.platform = Some(platform);
        self
    }

    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = Some(metadata);
        self
    }
}
