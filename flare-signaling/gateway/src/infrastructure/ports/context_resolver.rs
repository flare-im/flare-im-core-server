//! [`IContextResolver`] 与 **网关 `Ctx` 构建**（与 `domain/ports/context_resolver.rs` 对应）
//!
//! - [`build_gateway_ctx_from_info`]：从领域 [`ConnectionInfo`](crate::domain::model::ConnectionInfo) 构建 `Ctx`；
//!   由 [`super::connection_port::ConnectionRepository::build_ctx`] 在 `get_connection_info` 之后调用。
//! - [`ConnectionContextResolver`]：委托 [`IConnectionPort::build_ctx`]（与仓储共用同一构建路径）。

use std::sync::Arc;

use async_trait::async_trait;
use flare_core::common::error::Result as CoreResult;
use flare_im_core::Ctx;

use crate::domain::model::ConnectionInfo as DomainConnectionInfo;
use crate::domain::ports::{IConnectionPort, IContextResolver};
use crate::infrastructure::error::server_error_to_core;

/// 从已解析的网关连接信息构建上行用 [`Ctx`]（租户 / 用户 / 设备来自 metadata）
pub fn build_gateway_ctx_from_info(
    connection_info: &DomainConnectionInfo,
    default_tenant_id: &str,
) -> Ctx {
    Arc::new(crate::infrastructure::connection_context::build_context_from_connection(
        connection_info.metadata.as_ref(),
        Some(connection_info.user_id.as_str()),
        default_tenant_id,
    ))
}

/// 基于 [`IConnectionPort::build_ctx`] 解析 `Ctx`
pub struct ConnectionContextResolver {
    connection_port: Arc<dyn IConnectionPort>,
}

impl ConnectionContextResolver {
    pub fn new(connection_port: Arc<dyn IConnectionPort>) -> Self {
        Self { connection_port }
    }
}

#[async_trait]
impl IContextResolver for ConnectionContextResolver {
    async fn resolve(&self, connection_id: &str) -> CoreResult<Ctx> {
        self.connection_port
            .build_ctx(connection_id)
            .await
            .map_err(server_error_to_core)
    }
}
