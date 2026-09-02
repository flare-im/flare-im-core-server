//! [`ConnectionQuery`] 实现（与 `domain/ports/connection_query.rs` 对应）
//!
//! 基于 [`ConnectionManagerTrait`] 的本地连接读模型（CQRS 查询侧）。

use std::sync::Arc;

use async_trait::async_trait;
use flare_core::server::connection::ConnectionManagerTrait;
use flare_im_contracts::Ctx;
use flare_server_core::context::Context;
use flare_server_core::error::Result;

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

    /// 直接读连接表，跳过读模型组装。
    ///
    /// `get_user_connections` 是分片 HashMap 的同步读，代价极低；
    /// 而默认路径会为每个连接再做一次 `get_connection().await` 去拼
    /// 设备/平台字段——订阅路径拿到后直接丢掉。
    async fn list_user_connection_ids(&self, user_id: &str) -> Result<Vec<String>> {
        Ok(self.manager.get_user_connections(user_id).await)
    }

    /// 分片 HashMap 的点查，代价与 `get_user_connections` 同量级。
    async fn connection_exists(&self, connection_id: &str) -> bool {
        self.manager.get_connection(connection_id).await.is_some()
    }
}
