//! 数据路由 Port
//!
//! 定义 CustomData 路由的抽象接口

use async_trait::async_trait;
use flare_server_core::error::Result;

use crate::domain::model::{RouteCommand, RoutedEndpoint};

/// 数据路由 Port（Trait）
///
/// 负责将 CustomData 路由到目标服务端点
#[async_trait]
pub trait DataRoutingPort: Send + Sync {
    /// 路由数据到目标端点
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `command`: 路由命令
    ///
    /// # 返回
    /// - `Ok(RoutedEndpoint)`: 路由到的目标端点
    /// - `Err`: 路由失败
    async fn route(&self, ctx: &crate::Ctx, command: &RouteCommand) -> Result<RoutedEndpoint>;

    /// 调用下游服务
    ///
    /// # 参数
    /// - `ctx`: 上下文
    /// - `endpoint`: 目标端点
    /// - `payload`: 数据载荷
    ///
    /// # 返回
    /// - `Ok(Vec<u8>)`: 下游响应
    /// - `Err`: 调用失败
    async fn invoke_downstream(
        &self,
        ctx: &crate::Ctx,
        endpoint: &RoutedEndpoint,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>>;
}
