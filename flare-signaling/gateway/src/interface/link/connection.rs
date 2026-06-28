//! 连接处理器（interface 层）
//!
//! 仅保留：disconnect_connection、refresh_session、on_connect，全部委托 application ConnectionHandler。

use std::collections::HashSet;
use std::sync::Arc;

use flare_core::common::error::Result as CoreResult;
use flare_core::server::connection::ConnectionManagerTrait;
use flare_core::server::handle::ServerHandle;
use tokio::sync::Mutex;
use tracing::{instrument, warn};

use crate::application::handlers::{ConnectionHandler, SendHandler};
use crate::infrastructure::error::server_error_to_core;

/// 长连接处理器：断开/刷新/连接建立均委托 ConnectionHandler，依赖显式注入。
#[derive(Clone)]
pub struct LongConnectionHandler {
    pub connection_handler: Arc<ConnectionHandler>,
    pub send_handler: Arc<SendHandler>,
    refresh_in_flight: Arc<Mutex<HashSet<String>>>,
}

impl LongConnectionHandler {
    pub fn new(connection_handler: Arc<ConnectionHandler>, send_handler: Arc<SendHandler>) -> Self {
        Self {
            connection_handler,
            send_handler,
            refresh_in_flight: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// 断开连接：组装 DisconnectCommand，委托 ConnectionHandler 做会话清理。
    pub async fn disconnect_connection(&self, connection_id: &str) {
        if let Err(err) = self
            .connection_handler
            .handle_disconnect(connection_id)
            .await
        {
            warn!(?err, %connection_id, "failed to handle disconnect");
        }
    }

    pub async fn refresh_session(&self, connection_id: &str) -> CoreResult<()> {
        if let Err(err) = self.connection_handler.refresh_session(connection_id).await {
            warn!(?err, connection_id = %connection_id, "Failed to refresh session");
            return Err(server_error_to_core(err));
        }
        Ok(())
    }

    /// 在后台任务中刷新会话心跳，不阻塞当前调用方；失败仅打日志。
    #[inline]
    pub fn spawn_refresh_session(&self, connection_id: &str) {
        let handler = self.clone();
        let cid = connection_id.to_string();
        tokio::spawn(async move {
            {
                let mut in_flight = handler.refresh_in_flight.lock().await;
                if !in_flight.insert(cid.clone()) {
                    tracing::trace!(%cid, "skip duplicate refresh_session heartbeat");
                    return;
                }
            }
            if let Err(err) = handler.refresh_session(&cid).await {
                tracing::warn!(?err, %cid, "spawn_refresh_session: failed to refresh session heartbeat");
            }
            handler.refresh_in_flight.lock().await.remove(&cid);
        });
    }

    /// 连接建立：从上下文组装 ConnectCommand，委托 application ConnectionHandler。
    #[instrument(skip(self))]
    pub async fn on_connect(&self, connection_id: &str) -> CoreResult<()> {
        if let Err(err) = self.connection_handler.handle_connect(connection_id).await {
            warn!(?err, connection_id = %connection_id, "Failed to handle connection");
            return Err(server_error_to_core(err));
        }
        Ok(())
    }

    /// 注入 `ServerHandle`（供 PushRepository 等组件使用）；当前无状态，占位以匹配启动装配。
    pub async fn set_server_handle(&self, _handle: Arc<dyn ServerHandle>) {}

    /// 注入连接管理器；当前无状态，占位以匹配启动装配。
    pub async fn set_connection_manager(&self, _manager: Arc<dyn ConnectionManagerTrait>) {}
}
