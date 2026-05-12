//! ACK 路由 Port
//!
//! 定义 ACK 路由的抽象接口

use anyhow::Result;
use async_trait::async_trait;

use crate::domain::model::{RouteCommand, RoutedEndpoint};

/// ACK 路由 Port（Trait）
///
/// 负责将 ACK 路由到 Push Proxy 或 JetStream
#[async_trait]
pub trait AckRoutingPort: Send + Sync {
    /// 路由 ACK 到目标端点
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `command`: 路由命令
    ///
    /// # 返回
    /// - `Ok(RoutedEndpoint)`: 路由到的目标端点
    /// - `Err`: 路由失败
    async fn route(&self, ctx: &crate::Ctx, command: &RouteCommand) -> Result<RoutedEndpoint>;

    /// 发布 ACK 到 JetStream
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `payload`: ACK 载荷
    ///
    /// # 返回
    /// - `Ok(())`: 发布成功
    /// - `Err`: 发布失败
    async fn publish_to_jetstream(&self, ctx: &crate::Ctx, payload: Vec<u8>) -> Result<()>;
}
