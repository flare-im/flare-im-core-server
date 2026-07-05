//! 连接处理器（interface 层）
//!
//! 仅保留：disconnect_connection、refresh_session、on_connect，全部委托 application ConnectionHandler。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use flare_core::common::error::Result as CoreResult;
use flare_core::server::connection::ConnectionManagerTrait;
use flare_core::server::handle::ServerHandle;
use tokio::sync::Mutex;
use tracing::{instrument, warn};

use crate::application::handlers::{ConnectionHandler, SendHandler};
use crate::infrastructure::error::server_error_to_core;

/// Minimum interval between presence-refresh RPCs for a single connection.
/// Presence TTL (signaling-online `online_ttl_seconds`, default 3600s) is far larger, so a
/// short throttle keeps presence alive with wide margin while collapsing the per-frame flood.
const REFRESH_THROTTLE: Duration = Duration::from_secs(10);

/// 长连接处理器：断开/刷新/连接建立均委托 ConnectionHandler，依赖显式注入。
#[derive(Clone)]
pub struct LongConnectionHandler {
    pub connection_handler: Arc<ConnectionHandler>,
    pub send_handler: Arc<SendHandler>,
    /// Per-connection last presence-refresh time, used to throttle heartbeat refreshes.
    /// Every inbound frame (ping/pong/message/event) would otherwise spawn a refresh RPC to
    /// signaling-online; collapsing to at most one per `REFRESH_THROTTLE` removes redundant load.
    last_refresh: Arc<Mutex<HashMap<String, Instant>>>,
}

impl LongConnectionHandler {
    pub fn new(connection_handler: Arc<ConnectionHandler>, send_handler: Arc<SendHandler>) -> Self {
        Self {
            connection_handler,
            send_handler,
            last_refresh: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// 断开连接：组装 DisconnectCommand，委托 ConnectionHandler 做会话清理。
    pub async fn disconnect_connection(&self, connection_id: &str) {
        // Bound the throttle map: drop the connection's entry on disconnect.
        self.last_refresh.lock().await.remove(connection_id);
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
                let mut last = handler.last_refresh.lock().await;
                let now = Instant::now();
                match last.get(&cid) {
                    Some(&at) if now.duration_since(at) < REFRESH_THROTTLE => {
                        tracing::trace!(%cid, "skip throttled refresh_session heartbeat");
                        return;
                    }
                    // Record the start time before the RPC so concurrent frames within the
                    // window also skip (this subsumes the previous in-flight dedup).
                    _ => {
                        last.insert(cid.clone(), now);
                    }
                }
            }
            if let Err(err) = handler.refresh_session(&cid).await {
                tracing::warn!(?err, %cid, "spawn_refresh_session: failed to refresh session heartbeat");
            }
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
