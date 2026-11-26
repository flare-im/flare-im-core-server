//! 连接/会话状态抽象（State 模式）
//!
//! 用于网关与信令层：连接中、已认证、离线等状态流转，便于在线状态、多端漫游与水平扩展。
//! 领域层只定义状态与转换接口，具体实现由 infrastructure 完成。

use std::fmt;

/// 连接级状态（网关单连接）
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ConnectionState {
    /// 已建立传输，未完成认证
    #[default]
    Connecting,
    /// 已认证，可收发业务消息
    Authenticated,
    /// 已关闭或即将关闭
    Disconnected,
}

/// 会话级状态（用户/设备维度，可多端）
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum SessionState {
    /// 未登录或已登出
    #[default]
    Offline,
    /// 已登录，当前无活跃连接
    OnlineIdle,
    /// 已登录，至少一端在线
    OnlineActive,
}

/// 连接状态转换端口（由网关/信令基础设施实现）
///
/// 用于上报连接建立、认证成功、断开等，驱动在线状态与路由表更新。
pub trait ConnectionStateNotifier: Send + Sync {
    /// 通知状态变更（connection_id, user_id 可选, 新状态）
    fn notify_connection_state(
        &self,
        connection_id: &str,
        user_id: Option<&str>,
        state: ConnectionState,
    ) -> Box<dyn std::future::Future<Output = ()> + Send + Unpin>;
}

/// 无操作实现，便于测试与默认装配
pub struct NoopConnectionStateNotifier;

impl ConnectionStateNotifier for NoopConnectionStateNotifier {
    fn notify_connection_state(
        &self,
        _connection_id: &str,
        _user_id: Option<&str>,
        _state: ConnectionState,
    ) -> Box<dyn std::future::Future<Output = ()> + Send + Unpin> {
        Box::new(std::future::ready(()))
    }
}

impl fmt::Debug for NoopConnectionStateNotifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NoopConnectionStateNotifier").finish()
    }
}
