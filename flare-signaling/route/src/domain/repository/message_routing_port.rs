//! 消息路由 Port
//!
//! 定义消息路由的抽象接口，遵循 DDD 的 Port 模式

use anyhow::Result;
use async_trait::async_trait;

use crate::domain::model::{RouteCommand, RoutedEndpoint};

/// 消息路由 Port（Trait）
///
/// 负责将消息路由到目标服务端点
#[async_trait]
pub trait MessageRoutingPort: Send + Sync {
    /// 路由消息到目标端点
    ///
    /// # 参数
    /// - `ctx`: 上下文（TraceID、UserID 等）
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
    /// - `payload`: 消息载荷
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

/// Ctx 定义（简化版，实际应从 flare-core 导入）
///
/// 承载 TraceID、UserID、TenantID 等上下文信息
#[derive(Debug, Clone)]
pub struct Ctx {
    /// TraceID（追踪 ID）
    trace_id: String,
    /// UserID（用户 ID）
    user_id: Option<String>,
    /// TenantID（租户 ID）
    tenant_id: Option<String>,
}

impl Ctx {
    /// 创建新的上下文
    pub fn new(trace_id: String, user_id: Option<String>) -> Self {
        Self {
            trace_id,
            user_id,
            tenant_id: None,
        }
    }

    /// 创建带有租户 ID 的上下文
    pub fn with_tenant(mut self, tenant_id: String) -> Self {
        self.tenant_id = Some(tenant_id);
        self
    }

    /// 获取 TraceID
    pub fn trace_id(&self) -> &str {
        &self.trace_id
    }

    /// 获取 UserID
    pub fn user_id(&self) -> Option<&str> {
        self.user_id.as_deref()
    }

    /// 获取 TenantID
    pub fn tenant_id(&self) -> Option<&str> {
        self.tenant_id.as_deref()
    }
}
