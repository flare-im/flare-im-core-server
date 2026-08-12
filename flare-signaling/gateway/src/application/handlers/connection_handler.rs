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
    /// 连接注册去重：`on_connect` 与心跳补偿（`refresh_session` 的 None 分支）可能并发触发
    /// `handle_connect`；在 `register_connection` 的 await 窗口内 `online_sessions` 尚未写入 →
    /// 双重注册 → 双 login + `platform_exclusive` 自我清理 → 会话订阅抖动 → 接收方在线连接漏订阅
    /// → 实时投递 fanout `success=1`（对端实时收不到，仅重登录 history-sync 才补回）。
    /// 用该集合序列化同一 connection 的注册，保证幂等。
    connecting: Arc<tokio::sync::Mutex<std::collections::HashSet<String>>>,
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
            connecting: Arc::new(tokio::sync::Mutex::new(std::collections::HashSet::new())),
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
    /// best-effort——失败仅日志，不阻断连接（send/进会话仍会惰性 join 兜底 + 首投递成员 bootstrap）。
    async fn eager_subscribe_user_conversations(&self, connection_id: &str, user_id: &str) {
        Self::eager_subscribe_impl(
            self.connection_port.clone(),
            self.conversation_read.clone(),
            self.conversation_subscriptions.clone(),
            connection_id.to_string(),
            user_id.to_string(),
        )
        .await;
    }

    /// 静态实现：克隆所需 Arc 即可在后台任务中执行，使 eager 订阅**不阻塞 CONNECT_ACK 关键路径**
    /// （连接风暴下 conversation 服务 list_conversations 慢会拖垮连接建立）。
    async fn eager_subscribe_impl(
        connection_port: Arc<dyn IConnectionPort>,
        conversation_read: Arc<crate::infrastructure::ports::ConversationReadGrpcPool>,
        conversation_subscriptions: Arc<crate::domain::service::ConversationSubscriptionRegistry>,
        connection_id: String,
        user_id: String,
    ) {
        let ctx = match connection_port.build_ctx(&connection_id).await {
            Ok(ctx) => ctx,
            Err(error) => {
                warn!(%user_id, %connection_id, ?error, "eager subscribe: build ctx failed");
                return;
            }
        };
        let mut client = match conversation_read.ensure_client().await {
            Ok(client) => client,
            Err(error) => {
                warn!(%user_id, %connection_id, ?error, "eager subscribe: conversation read client unavailable");
                return;
            }
        };
        // 用户自己的 sync 收件箱：业务侧把「还没有会话」的通知（好友申请等）发到
        // `sync:<user_id>`，它**不是真实会话**、永远不会出现在 list_conversations 里。
        // 不在这里显式订阅，这类通知就没有订阅者、被静默丢弃——表现为「对方发来好友申请，
        // 我这边毫无动静」，只能靠客户端下次主动拉列表才发现。
        conversation_subscriptions.join(
            &flare_im_contracts::constants::sync_inbox::sync_inbox_conversation_id(&user_id),
            &connection_id,
        );

        // 订阅用户的**全部**会话(分页),否则会话数 > 服务端默认 limit(20)时,超出部分得不到实时
        // 读扩散推送(被动接收方在非前 20 会话里收不到消息,要等安全轮询)。后台任务执行,不阻塞 CONNECT_ACK。
        const EAGER_PAGE_LIMIT: i32 = 500;
        const EAGER_MAX_TOTAL: usize = 10_000; // 封顶,避免极端会话数撑爆单连接订阅内存
        let mut cursor = String::new();
        let mut total = 0usize;
        loop {
            let request = flare_server_core::client::request_with_context(
                flare_grpc_proto::conversation::ListConversationsRequest {
                    cursor: cursor.clone(),
                    limit: EAGER_PAGE_LIMIT,
                    order: 0,
                },
                &ctx,
            );
            match client.list_conversations(request).await {
                Ok(response) => {
                    let page = response.into_inner();
                    for summary in &page.conversations {
                        conversation_subscriptions.join(&summary.conversation_id, &connection_id);
                    }
                    total += page.conversations.len();
                    if page.conversations.is_empty()
                        || !page.has_more
                        || page.next_cursor.is_empty()
                        || total >= EAGER_MAX_TOTAL
                    {
                        debug!(%user_id, %connection_id, subscribed = total, "eager subscribed user conversations");
                        break;
                    }
                    cursor = page.next_cursor;
                }
                Err(error) => {
                    warn!(%user_id, %connection_id, ?error, "eager subscribe: list_conversations failed");
                    break;
                }
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
        // 单飞：同一 connection 的并发注册（on_connect × 心跳补偿）只放行一个。
        // 已注册 → 幂等返回；他人注册中 → 短轮询等其完成后复用结果。
        loop {
            if let Some(session) = self.online_sessions.read().await.get(connection_id) {
                return Ok(session.conversation_id.clone());
            }
            if self
                .connecting
                .lock()
                .await
                .insert(connection_id.to_string())
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let result = self.handle_connect_inner(connection_id).await;
        self.connecting.lock().await.remove(connection_id);
        result
    }

    /// 注册主体（仅经 `handle_connect` 单飞入口进入）。
    async fn handle_connect_inner(&self, connection_id: &str) -> Result<String> {
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
        // **不阻塞 CONNECT_ACK**：后台执行，避免连接风暴下 list_conversations 慢拖垮连接建立
        // （首投递成员 bootstrap + send 惰性 join 兜底订阅时序）。
        tokio::spawn(Self::eager_subscribe_impl(
            self.connection_port.clone(),
            self.conversation_read.clone(),
            self.conversation_subscriptions.clone(),
            connection_id.to_string(),
            user_id.clone(),
        ));

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
        self.conversation_subscriptions
            .remove_connection(connection_id);
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
        // 读锁必须在进入 None 分支前释放：match 的 scrutinee 临时量（含锁守卫）
        // 存活到整个 match 结束，若在 None 分支持锁调用 handle_connect（内部
        // write().await 同一把锁）= 异步自死锁——之后**所有**连接的 handle_connect
        // 永久排队，eager 订阅全部失效（实时下行只剩首投递兜底）。
        let existing = {
            let sessions = self.online_sessions.read().await;
            sessions.get(connection_id).cloned()
        };
        let session = match existing {
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
