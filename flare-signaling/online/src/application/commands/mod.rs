//! 命令结构体定义（Command DTO）

use flare_proto::signaling::online::{
    HeartbeatRequest, LoginRequest, LogoutRequest, SubscribeUserPresenceRequest,
    WatchPresenceRequest,
};
use flare_server_core::context::Context;

/// 登录命令
#[derive(Debug, Clone)]
pub struct LoginCommand {
    /// 原始请求
    pub request: LoginRequest,
    /// 上下文
    pub ctx: Context,
}

/// 登出命令
#[derive(Debug, Clone)]
pub struct LogoutCommand {
    /// 原始请求
    pub request: LogoutRequest,
    /// 上下文
    pub ctx: Context,
}

/// 心跳命令
#[derive(Debug, Clone)]
pub struct HeartbeatCommand {
    /// 原始请求
    pub request: HeartbeatRequest,
    /// 上下文
    pub ctx: Context,
}

/// 订阅用户状态命令
#[derive(Debug, Clone)]
pub struct SubscribeUserPresenceCommand {
    /// 原始请求
    pub request: SubscribeUserPresenceRequest,
}

/// 订阅在线状态命令
#[derive(Debug, Clone)]
pub struct WatchPresenceCommand {
    /// 原始请求
    pub request: WatchPresenceRequest,
}
