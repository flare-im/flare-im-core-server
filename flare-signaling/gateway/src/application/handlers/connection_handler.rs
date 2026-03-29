//! 连接管理处理器（Connection BC 应用层）
//!
//! 编排连接建立/断开，与 message_event_flow 中 Signaling Gateway 一致：on_connect → 注册会话；on_disconnect → 清理。
//! **架构落地**（ARCHITECTURE_REFACTOR §5 State）：通过 `ConnectionStateNotifier` 上报连接状态，
//! 驱动在线状态与路由表更新；默认 Noop，wire 中可替换为与 Online 打通的实现。

use std::sync::Arc;

use crate::domain::ports::IConnectionPort;
use flare_im_core::Ctx;
use flare_im_core::abstractions::state::{ConnectionState, ConnectionStateNotifier};
use flare_server_core::error::Result;
use tracing::{debug, instrument, warn};

use crate::domain::service::ConnectionDomainService;

/// 连接管理处理器：仅编排会话领域服务与指标，连接查询由 QueryHandler / PushDomainService 使用 ConnectionQuery 独立完成。
pub struct ConnectionHandler {
    session_domain_service: Arc<ConnectionDomainService>,
    metrics: Arc<flare_im_core::metrics::AccessGatewayMetrics>,
    /// State 模式：连接状态通知，默认 Noop，可注入实现与 Online 打通
    state_notifier: Arc<dyn ConnectionStateNotifier>,
    connection_port: Arc<dyn IConnectionPort>,
}

impl ConnectionHandler {
    pub fn new(
        session_domain_service: Arc<ConnectionDomainService>,
        metrics: Arc<flare_im_core::metrics::AccessGatewayMetrics>,
        state_notifier: Arc<dyn ConnectionStateNotifier>,
        connection_port: Arc<dyn IConnectionPort>,
    ) -> Self {
        Self {
            session_domain_service,
            metrics,
            state_notifier,
            connection_port,
        }
    }

    /// 为长连接请求构建请求级 `Ctx`（租户/用户/trace 等），供领域层与下游 RPC 透传。
    pub async fn build_request_ctx(&self, connection_id: &str) -> Result<Ctx> {
        self.connection_port.build_ctx(connection_id).await
    }

    /// 处理连接建立
    ///
    /// 流程：
    /// 1. 获取连接信息(认证已在基础层完成)
    /// 2. 注册会话到 Signaling Online(业务端会话管理)
    /// 3. 通知连接状态(业务端通知)
    /// 4. 记录指标和日志
    #[instrument(skip(self))]
    pub async fn handle_connect(&self, connection_id: &str) -> Result<String> {
        let connection_info = self
            .connection_port
            .get_connection_info(connection_id)
            .await?;
        let user_id = connection_info.user_id.clone();

        // 注册会话到 Signaling Online(业务端会话管理)
        let conversation_id = self
            .session_domain_service
            .register_connection(
                &user_id,
                &connection_info.device_id,
                Some(connection_id),
                connection_info.metadata.as_ref(),
            )
            .await?;

        // 通知连接状态(业务端通知)
        self.state_notifier
            .notify_connection_state(
                connection_id,
                Some(user_id.as_str()),
                ConnectionState::Authenticated,
            )
            .await;

        // 更新活跃连接数(从基础层获取)
        let list = self.connection_port.list_user_connections(&user_id).await?;
        let active_count = list.len() as i64;
        self.metrics.connections_active.set(active_count);

        debug!(
            user_id = %user_id,
            connection_id = %connection_id,
            conversation_id = %conversation_id,
            active_connections = active_count,
            "Connection established (authentication done in base layer, storage done by flare-core)"
        );

        Ok(conversation_id)
    }

    /// 处理连接断开
    ///
    /// 流程：
    /// 1. 获取连接信息
    /// 2. 通知连接状态
    /// 3. 决定是否注销会话
    /// 4. 记录指标和日志
    #[instrument(skip(self))]
    pub async fn handle_disconnect(&self, connection_id: &str) -> Result<()> {
        let connection_info = self
            .connection_port
            .get_connection_info(connection_id)
            .await?;
        let user_id = connection_info.user_id.clone();

        // 检查用户是否还有其他连接
        let user_connections = self.connection_port.list_user_connections(&user_id).await?;
        let has_other_connections = !user_connections.is_empty();

        // 通知连接状态
        self.state_notifier
            .notify_connection_state(
                connection_id,
                Some(user_id.as_str()),
                ConnectionState::Disconnected,
            )
            .await;

        // 更新活跃连接数
        let active_count = user_connections.len() as i64;
        self.metrics.connection_disconnected_total.inc();
        self.metrics.connections_active.set(active_count);

        debug!(
            connection_id = %connection_id,
            user_id = %user_id,
            active_connections = active_count,
            "Connection disconnected"
        );

        // 如果是最后一个连接,注销会话
        if !has_other_connections {
            if let Err(err) = self
                .session_domain_service
                .unregister_connection(&user_id, None)
                .await
            {
                warn!(
                    ?err,
                    user_id = %user_id,
                    connection_id = %connection_id,
                    "Failed to unregister online status"
                );
                return Err(err);
            }

            debug!(
                user_id = %user_id,
                connection_id = %connection_id,
                "User disconnected"
            );
        }

        Ok(())
    }

    /// 刷新会话心跳
    ///
    /// 流程：
    /// 1. 获取连接信息
    /// 2. 刷新 Signaling Online 的心跳
    /// 3. 记录日志
    #[instrument(skip(self))]
    pub async fn refresh_session(&self, connection_id: &str) -> Result<()> {
        let connection_info = self
            .connection_port
            .get_connection_info(connection_id)
            .await?;
        let user_id = connection_info.user_id.clone();
        let metadata = self
            .connection_port
            .get_connection_metadata(connection_id)
            .await?;
        let conversation_id = metadata.get("conversation_id").cloned().ok_or_else(|| {
            flare_server_core::error::ErrorBuilder::new(
                flare_server_core::error::ErrorCode::InvalidParameter,
                "conversation_id not in connection metadata",
            )
            .build_error()
        })?;

        // 刷新 Signaling Online 的心跳
        self.session_domain_service
            .refresh_heartbeat(&user_id, &conversation_id, Some(connection_id))
            .await
            .map_err(|e| {
                flare_server_core::error::ErrorBuilder::new(
                    flare_server_core::error::ErrorCode::InternalError,
                    format!("Failed to refresh session: {e}"),
                )
                .build_error()
            })?;

        debug!(
            user_id = %user_id,
            connection_id = %connection_id,
            conversation_id = %conversation_id,
            "Session heartbeat refreshed"
        );

        Ok(())
    }

    // pub async fn spawn_refresh_session(&self, connection_id: &str) {
    //     let handler = self.clone();
    //     let cid = connection_id.to_string();
    //     tokio::spawn(async move {
    //         if let Err(err) = handler.refresh_session(&cid).await {
    //             tracing::warn!(?err, %cid, "spawn_refresh_session: failed to refresh session heartbeat");
    //         }
    //     });
    // }
}
