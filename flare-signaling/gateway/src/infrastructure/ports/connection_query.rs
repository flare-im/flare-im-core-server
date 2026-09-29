//! [`ConnectionQuery`] 实现（与 `domain/ports/connection_query.rs` 对应）
//!
//! 基于 [`ConnectionManagerTrait`] 的本地连接读模型（CQRS 查询侧）。

use std::sync::Arc;

use async_trait::async_trait;
use flare_core::server::connection::ConnectionManagerTrait;
use flare_im_contracts::Ctx;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};

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
        tx: &Ctx,
        user_id: &str,
    ) -> Result<Vec<DomainConnectionInfo>> {
        let tenant_id = required_tenant(tx)?;
        let connection_ids = self.manager.get_user_connections(user_id).await;
        let mut list = Vec::with_capacity(connection_ids.len());
        for connection_id in connection_ids {
            if let Some((_, core_info)) = self.manager.get_connection(&connection_id).await {
                if !matches_tenant_connection(&core_info, tenant_id, user_id) {
                    continue;
                }
                list.push(core_info_to_domain(
                    connection_id,
                    &core_info,
                    &self.default_tenant_id,
                ));
            }
        }
        Ok(list)
    }

    async fn list_user_connections(
        &self,
        tx: &Ctx,
        user_id: &str,
    ) -> Result<Vec<DomainConnectionInfo>> {
        self.query_user_connections(tx, user_id).await
    }

    /// 直接读连接表，跳过读模型组装。
    ///
    /// `get_user_connections` 是分片 HashMap 的同步读，代价极低；
    /// 而默认路径会为每个连接再做一次 `get_connection().await` 去拼
    /// 设备/平台字段——订阅路径拿到后直接丢掉。
    async fn list_user_connection_ids(&self, tx: &Ctx, user_id: &str) -> Result<Vec<String>> {
        let tenant_id = required_tenant(tx)?;
        let mut scoped = Vec::new();
        for connection_id in self.manager.get_user_connections(user_id).await {
            if let Some((_, info)) = self.manager.get_connection(&connection_id).await
                && matches_tenant_connection(&info, tenant_id, user_id)
            {
                scoped.push(connection_id);
            }
        }
        Ok(scoped)
    }

    /// 分片 HashMap 的点查，代价与 `get_user_connections` 同量级。
    async fn connection_exists(&self, connection_id: &str) -> bool {
        self.manager.get_connection(connection_id).await.is_some()
    }
}

fn required_tenant(tx: &Ctx) -> Result<&str> {
    let tenant_id = tx.tenant_id().ok_or_else(|| {
        ErrorBuilder::new(
            ErrorCode::PermissionDenied,
            "tenant required for connection query",
        )
        .build_error()
    })?;
    if !flare_im_contracts::domain::tenant::is_valid_tenant_id(tenant_id) {
        return Err(ErrorBuilder::new(
            ErrorCode::PermissionDenied,
            "invalid connection query tenant",
        )
        .build_error());
    }
    Ok(tenant_id)
}

fn matches_tenant_connection(
    info: &flare_core::server::connection::TraitConnectionInfo,
    tenant_id: &str,
    user_id: &str,
) -> bool {
    info.authenticated
        && info.user_id.as_deref() == Some(user_id)
        && info.metadata.get("tenant_id").map(String::as_str) == Some(tenant_id)
}
