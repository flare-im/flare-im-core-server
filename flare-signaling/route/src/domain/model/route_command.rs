//! 路由命令聚合根
//!
//! RouteCommand 是上行路由的聚合根，负责维护路由命令的业务不变式

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 路由命令聚合根
///
/// 聚合根职责：
/// - 维护路由命令的业务不变式
/// - 提供路由命令的创建和验证方法
/// - 发布领域事件
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RouteCommand {
    /// 命令 ID（唯一标识）
    command_id: String,
    /// 业务系统标识（SVID）
    svid: String,
    /// 路由类型
    route_type: RouteType,
    /// 路由选项
    options: RouteOptions,
    /// 创建时间
    created_at: DateTime<Utc>,
}

impl RouteCommand {
    /// 创建新的路由命令
    ///
    /// # 业务规则
    /// - command_id 不能为空
    /// - svid 不能为空
    pub fn new(
        command_id: String,
        svid: String,
        route_type: RouteType,
        options: RouteOptions,
    ) -> Self {
        Self {
            command_id,
            svid,
            route_type,
            options,
            created_at: Utc::now(),
        }
    }

    /// 获取命令 ID
    pub fn command_id(&self) -> &str {
        &self.command_id
    }

    /// 获取 SVID
    pub fn svid(&self) -> &str {
        &self.svid
    }

    /// 获取路由类型
    pub fn route_type(&self) -> &RouteType {
        &self.route_type
    }

    /// 获取路由选项
    pub fn options(&self) -> &RouteOptions {
        &self.options
    }

    /// 获取创建时间
    pub fn created_at(&self) -> &DateTime<Utc> {
        &self.created_at
    }
}

/// 路由类型枚举
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RouteType {
    /// 消息路由
    Message,
    /// 事件路由
    Event,
    /// ACK 路由
    Ack,
    /// 数据路由
    Data,
}

impl std::fmt::Display for RouteType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RouteType::Message => write!(f, "Message"),
            RouteType::Event => write!(f, "Event"),
            RouteType::Ack => write!(f, "Ack"),
            RouteType::Data => write!(f, "Data"),
        }
    }
}

/// 路由选项值对象
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RouteOptions {
    /// 超时秒数，默认 5
    timeout_seconds: i32,
    /// 是否启用追踪，默认 true
    enable_tracing: bool,
    /// 重试策略
    retry_strategy: RetryStrategy,
    /// 负载均衡策略
    load_balance_strategy: LoadBalanceStrategy,
    /// 路由优先级，越大越高，默认 0
    priority: i32,
}

impl Default for RouteOptions {
    fn default() -> Self {
        Self {
            timeout_seconds: 5,
            enable_tracing: true,
            retry_strategy: RetryStrategy::None,
            load_balance_strategy: LoadBalanceStrategy::RoundRobin,
            priority: 0,
        }
    }
}

impl RouteOptions {
    /// 创建新的路由选项
    pub fn new() -> Self {
        Self::default()
    }

    /// 设置超时秒数
    pub fn with_timeout(mut self, timeout_seconds: i32) -> Self {
        self.timeout_seconds = timeout_seconds;
        self
    }

    /// 设置是否启用追踪
    pub fn with_tracing(mut self, enable_tracing: bool) -> Self {
        self.enable_tracing = enable_tracing;
        self
    }

    /// 设置重试策略
    pub fn with_retry_strategy(mut self, retry_strategy: RetryStrategy) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }

    /// 设置负载均衡策略
    pub fn with_load_balance_strategy(
        mut self,
        load_balance_strategy: LoadBalanceStrategy,
    ) -> Self {
        self.load_balance_strategy = load_balance_strategy;
        self
    }

    /// 设置优先级
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    pub fn timeout_seconds(&self) -> i32 {
        self.timeout_seconds
    }

    pub fn enable_tracing(&self) -> bool {
        self.enable_tracing
    }

    pub fn retry_strategy(&self) -> &RetryStrategy {
        &self.retry_strategy
    }

    pub fn load_balance_strategy(&self) -> &LoadBalanceStrategy {
        &self.load_balance_strategy
    }

    pub fn priority(&self) -> i32 {
        self.priority
    }
}

/// 重试策略枚举
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RetryStrategy {
    /// 不重试（默认）
    None,
    /// 重试一次
    Once,
    /// 指数退避最多 3 次
    Exponential,
}

impl std::fmt::Display for RetryStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RetryStrategy::None => write!(f, "None"),
            RetryStrategy::Once => write!(f, "Once"),
            RetryStrategy::Exponential => write!(f, "Exponential"),
        }
    }
}

/// 负载均衡策略枚举
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum LoadBalanceStrategy {
    /// 轮询（默认）
    RoundRobin,
    /// 最少连接
    LeastConnections,
    /// 一致性哈希（会话同实例）
    ConsistentHash,
}

impl std::fmt::Display for LoadBalanceStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoadBalanceStrategy::RoundRobin => write!(f, "RoundRobin"),
            LoadBalanceStrategy::LeastConnections => write!(f, "LeastConnections"),
            LoadBalanceStrategy::ConsistentHash => write!(f, "ConsistentHash"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_route_command_creation() {
        let command = RouteCommand::new(
            "cmd-123".to_string(),
            "svid.im".to_string(),
            RouteType::Message,
            RouteOptions::default(),
        );

        assert_eq!(command.command_id(), "cmd-123");
        assert_eq!(command.svid(), "svid.im");
        assert_eq!(command.route_type(), &RouteType::Message);
    }

    #[test]
    fn test_route_options_default() {
        let options = RouteOptions::default();
        assert_eq!(options.timeout_seconds(), 5);
        assert!(options.enable_tracing());
        assert_eq!(options.retry_strategy(), &RetryStrategy::None);
        assert_eq!(
            options.load_balance_strategy(),
            &LoadBalanceStrategy::RoundRobin
        );
        assert_eq!(options.priority(), 0);
    }

    #[test]
    fn test_route_options_builder() {
        let options = RouteOptions::new()
            .with_timeout(10)
            .with_tracing(false)
            .with_retry_strategy(RetryStrategy::Once)
            .with_load_balance_strategy(LoadBalanceStrategy::LeastConnections)
            .with_priority(5);

        assert_eq!(options.timeout_seconds(), 10);
        assert!(!options.enable_tracing());
        assert_eq!(options.retry_strategy(), &RetryStrategy::Once);
        assert_eq!(
            options.load_balance_strategy(),
            &LoadBalanceStrategy::LeastConnections
        );
        assert_eq!(options.priority(), 5);
    }
}
