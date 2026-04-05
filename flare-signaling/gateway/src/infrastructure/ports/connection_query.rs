//! [`ConnectionQuery`] 实现（与 `domain/ports/connection_query.rs` 对应）
//!
//! 基于 [`ConnectionManagerTrait`] 的本地连接读模型（CQRS 查询侧）。

use std::sync::Arc;

use async_trait::async_trait;
use flare_core::server::connection::ConnectionManagerTrait;
use flare_im_core::Ctx;
use flare_server_core::context::Context;
use flare_im_core::error::Result;

use super::connection_port::core_info_to_domain;
use crate::domain::model::ConnectionInfo as DomainConnectionInfo;
use crate::domain::ports::ConnectionQuery;

/// 从连接管理器查询用户连接列表
pub struct ManagerConnectionQuery {
    manager: Arc<dyn ConnectionManagerTrait>,
    default_tenant_id: String,
}

impl ManagerConnectionQuery {
    pub fn new(manager: Arc<dyn ConnectionManagerTrait>, default_tenant_id: String) -> Self {
        Self {
            manager,
            default_tenant_id,
        }
    }
}

#[async_trait]
impl ConnectionQuery for ManagerConnectionQuery {
    async fn query_user_connections(
        &self,
        _tx: &Ctx,
        user_id: &str,
    ) -> Result<Vec<DomainConnectionInfo>> {
        let connection_ids = self.manager.get_user_connections(user_id).await;
        let mut list = Vec::with_capacity(connection_ids.len());
        for connection_id in connection_ids {
            if let Some((_, core_info)) = self.manager.get_connection(&connection_id).await {
                list.push(core_info_to_domain(
                    connection_id,
                    &core_info,
                    &self.default_tenant_id,
                ));
            }
        }
        Ok(list)
    }

    async fn list_user_connections(&self, user_id: &str) -> Result<Vec<DomainConnectionInfo>> {
        let root = Arc::new(Context::root());
        self.query_user_connections(&root, user_id).await
    }
}
