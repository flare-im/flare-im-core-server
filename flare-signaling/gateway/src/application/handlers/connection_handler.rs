//! 连接管理处理器（Connection BC 应用层）
//!
//! 编排连接建立/断开，与 message_event_flow 中 Signaling Gateway 一致：on_connect → 注册会话；on_disconnect → 清理。
//! **架构落地**（ARCHITECTURE_REFACTOR §5 State）：通过 `ConnectionStateNotifier` 上报连接状态，
//! 驱动在线状态与路由表更新；默认 Noop，wire 中可替换为与 Online 打通的实现。

use std::sync::Arc;

use crate::domain::ports::IConnectionPort;
use flare_im_contracts::Ctx;
use flare_im_contracts::abstractions::state::{ConnectionState, ConnectionStateNotifier};
use flare_server_core::error::Result;
use std::collections::HashMap;
use tokio::sync::RwLock;
use tracing::{debug, instrument, warn};

use crate::domain::service::ConnectionDomainService;

#[derive(Clone, Debug)]
struct OnlineSession {
    user_id: String,
    conversation_id: String,
}

/// 连接管理处理器：仅编排会话领域服务与指标，连接查询由 QueryHandler / PushDomainService 使用 ConnectionQuery 独立完成。
pub struct ConnectionHandler {
    session_domain_service: Arc<ConnectionDomainService>,
    metrics: Arc<flare_im_service_kit::metrics::AccessGatewayMetrics>,
    /// State 模式：连接状态通知，默认 Noop，可注入实现与 Online 打通
    state_notifier: Arc<dyn ConnectionStateNotifier>,
    connection_port: Arc<dyn IConnectionPort>,
    online_sessions: Arc<RwLock<HashMap<String, OnlineSession>>>,
    /// 会话级在线订阅注册表（统一读扩散地基）：断线时清扫该连接的全部会话订阅。
    conversation_subscriptions: Arc<crate::domain::service::ConversationSubscriptionRegistry>,
    /// Conversation 读服务客户端池：登录时拉取用户会话列表用于 eager 订阅。
    conversation_read: Arc<crate::infrastructure::ports::ConversationReadGrpcPool>,
}

impl ConnectionHandler {
    pub fn new(
        session_domain_service: Arc<ConnectionDomainService>,
        metrics: Arc<flare_im_service_kit::metrics::AccessGatewayMetrics>,
        state_notifier: Arc<dyn ConnectionStateNotifier>,
        connection_port: Arc<dyn IConnectionPort>,
        conversation_subscriptions: Arc<crate::domain::service::ConversationSubscriptionRegistry>,
        conversation_read: Arc<crate::infrastructure::ports::ConversationReadGrpcPool>,
    ) -> Self {
        Self {
            session_domain_service,
            metrics,
            state_notifier,
            connection_port,
            online_sessions: Arc::new(RwLock::new(HashMap::new())),
            conversation_subscriptions,
            conversation_read,
        }
    }

    /// 会话列表 sync 时重跑 eager 订阅：客户端建群/被拉群后刷新会话列表，即可订阅到新会话
    /// （覆盖"登录在建群之前"的时序）。按 connection_id 解析 user_id。best-effort。
    pub async fn resubscribe_conversations(&self, connection_id: &str) {
        let user_id = {
            let sessions = self.online_sessions.read().await;
            sessions.get(connection_id).map(|s| s.user_id.clone())
        };
        if let Some(user_id) = user_id {
            self.eager_subscribe_user_conversations(connection_id, &user_id)
                .await;
        }
    }

    /// 统一读扩散：登录即按用户会话列表 eager 订阅，确保在线成员 race-free 收到会话 publish。
    /// best-effort——失败仅日志，不阻断连接（send/进会话仍会惰性 join 兜底）。
    async fn eager_subscribe_user_conversations(&self, connection_id: &str, user_id: &str) {
        let ctx = match self.build_request_ctx(connection_id).await {
            Ok(ctx) => ctx,
            Err(error) => {
                warn!(%user_id, %connection_id, ?error, "eager subscribe: build ctx failed");
                return;
            }
        };
        let mut client = match self.conversation_read.ensure_client().await {
            Ok(client) => client,
            Err(error) => {
                warn!(%user_id, %connection_id, ?error, "eager subscribe: conversation read client unavailable");
                return;
            }
        };
        let request = flare_server_core::client::request_with_context(
            flare_grpc_proto::conversation::ListConversationsRequest {
                cursor: String::new(),
                limit: 0,
                order: 0,
            },
            &ctx,
        );
        match client.list_conversations(request).await {
            Ok(response) => {
                let conversations = response.into_inner().conversations;
                let count = conversations.len();
                for summary in conversations {
                    self.conversation_subscriptions
                        .join(&summary.conversation_id, connection_id);
                }
                debug!(%user_id, %connection_id, subscribed = count, "eager subscribed user conversations");
            }
            Err(error) => {
                warn!(%user_id, %connection_id, ?error, "eager subscribe: list_conversations failed");
            }
        }
    }

    /// 会话订阅注册表（供投递/订阅路径共享同一实例）。
    pub fn conversation_subscriptions(
        &self,
    ) -> Arc<crate::domain::service::ConversationSubscriptionRegistry> {
        self.conversation_subscriptions.clone()
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
                connection_info.platform.as_deref(),
                Some(connection_id),
                connection_info.metadata.as_ref(),
            )
            .await?;
        self.online_sessions.write().await.insert(
            connection_id.to_string(),
            OnlineSession {
                user_id: user_id.clone(),
                conversation_id: conversation_id.clone(),
            },
        );

        // 统一读扩散：登录即订阅用户全部会话（race-free 收会话 publish）；best-effort。
        self.eager_subscribe_user_conversations(connection_id, &user_id)
            .await;

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
        // 统一读扩散地基：断线先清扫该连接的全部会话订阅（幂等，O(该连接订阅的会话数)）。
        self.conversation_subscriptions.remove_connection(connection_id);
        let session = self
            .online_sessions
            .write()
            .await
            .remove(connection_id)
            .ok_or_else(|| {
                flare_server_core::error::ErrorBuilder::new(
                    flare_server_core::error::ErrorCode::InvalidParameter,
                    "online session not found for connection",
                )
                .build_error()
            })?;
        let user_id = session.user_id;
        let conversation_id = session.conversation_id;

        // 底层连接可能已先被移除，断开清理必须以 Online 会话缓存为准。
        let user_connections = self
            .connection_port
            .list_user_connections(&user_id)
            .await
            .unwrap_or_default();
        let active_count_after_disconnect = user_connections
            .iter()
            .filter(|conn| conn.connection_id != connection_id)
            .count();

        // 通知连接状态
        self.state_notifier
            .notify_connection_state(
                connection_id,
                Some(user_id.as_str()),
                ConnectionState::Disconnected,
            )
            .await;

        // 更新活跃连接数
        let active_count = active_count_after_disconnect as i64;
        self.metrics.connection_disconnected_total.inc();
        self.metrics.connections_active.set(active_count);

        debug!(
            connection_id = %connection_id,
            user_id = %user_id,
            active_connections = active_count,
            "Connection disconnected"
        );

        if let Err(err) = self
            .session_domain_service
            .unregister_connection(&user_id, &conversation_id)
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
            active_connections = active_count,
            "User connection unregistered"
        );

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
        let session = match self
            .online_sessions
            .read()
            .await
            .get(connection_id)
            .cloned()
        {
            Some(session) => session,
            None => {
                self.handle_connect(connection_id).await?;
                self.online_sessions
                    .read()
                    .await
                    .get(connection_id)
                    .cloned()
                    .ok_or_else(|| {
                        flare_server_core::error::ErrorBuilder::new(
                            flare_server_core::error::ErrorCode::InvalidParameter,
                            "online conversation_id not found for connection",
                        )
                        .build_error()
                    })?
            }
        };
        let user_id = session.user_id;
        let conversation_id = session.conversation_id;

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
