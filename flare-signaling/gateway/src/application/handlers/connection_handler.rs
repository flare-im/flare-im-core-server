//! 连接管理处理器（Connection BC 应用层）
//!
//! 编排连接建立/断开，与 message_event_flow 中 Signaling Gateway 一致：on_connect → 注册会话；on_disconnect → 清理。
//! **架构落地**（ARCHITECTURE_REFACTOR §5 State）：通过 `ConnectionStateNotifier` 上报连接状态，
//! 驱动在线状态与路由表更新；默认 Noop，wire 中可替换为与 Online 打通的实现。

use std::sync::Arc;

use crate::domain::ports::IConnectionPort;
use flare_im_contracts::Ctx;
use flare_im_contracts::abstractions::state::{ConnectionState, ConnectionStateNotifier};
use flare_im_usage::UsageRecorder;
use flare_server_core::error::Result;
use std::collections::HashMap;
use tokio::sync::RwLock;
use tracing::{debug, instrument, warn};

use crate::domain::service::ConnectionDomainService;

#[derive(Clone)]
struct OnlineSession {
    user_id: String,
    tenant_id: String,
    conversation_id: String,
    authenticated_ctx: Ctx,
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
    /// 用该表序列化同一 connection 的注册，保证幂等。
    ///
    /// 值表示「登记途中已收到断开」：断开处理在该窗口里找不到会话，只能在这里留标记，由登记
    /// 完成后自己注销（见 `handle_connect_inner`）。锁顺序固定为先本表、后 `online_sessions`。
    connecting: Arc<tokio::sync::Mutex<HashMap<String, bool>>>,
    /// 会话级在线订阅注册表（统一读扩散地基）：断线时清扫该连接的全部会话订阅。
    conversation_subscriptions: Arc<crate::domain::service::ConversationSubscriptionRegistry>,
    /// Conversation 读服务客户端池：登录时拉取用户会话列表用于 eager 订阅。
    conversation_read: Arc<crate::infrastructure::ports::ConversationReadGrpcPool>,
    /// 用量计数点：`online_sessions` 写入与取出成对计连接（`connections.peak`、
    /// `connection.minutes`），写入时计当日活跃用户（`users.active_daily`）。
    usage: UsageRecorder,
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
            connecting: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            conversation_subscriptions,
            conversation_read,
            usage: UsageRecorder::disabled(),
        }
    }

    pub fn with_usage(mut self, usage: UsageRecorder) -> Self {
        self.usage = usage;
        self
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
        let Some(tenant_id) = ctx.tenant_id().filter(|tenant| {
            flare_im_contracts::domain::tenant::is_valid_tenant_id(tenant)
                && flare_im_contracts::utils::normalize_tenant_id(tenant) == *tenant
        }) else {
            warn!(%user_id, %connection_id, "eager subscribe: canonical tenant missing");
            return;
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
            tenant_id,
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
            let request = match conversation_read.user_read_request(
                flare_grpc_proto::conversation::ListConversationsRequest {
                    cursor: cursor.clone(),
                    limit: EAGER_PAGE_LIMIT,
                    order: 0,
                },
                &ctx,
            ) {
                Ok(request) => request,
                Err(error) => {
                    warn!(%user_id, %connection_id, ?error, "eager subscribe: read identity unavailable");
                    break;
                }
            };
            match client.list_conversations(request).await {
                Ok(response) => {
                    let page = response.into_inner();
                    for summary in &page.conversations {
                        conversation_subscriptions.join(
                            tenant_id,
                            &summary.conversation_id,
                            &connection_id,
                        );
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
            {
                let mut connecting = self.connecting.lock().await;
                if !connecting.contains_key(connection_id) {
                    connecting.insert(connection_id.to_string(), false);
                    break;
                }
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
        let authenticated_ctx = self.connection_port.build_ctx(connection_id).await?;
        if authenticated_ctx.user_id() != Some(connection_info.user_id.as_str())
            || authenticated_ctx.tenant_id() != Some(connection_info.tenant_id.as_str())
        {
            return Err(flare_server_core::error::ErrorBuilder::new(
                flare_server_core::error::ErrorCode::PermissionDenied,
                "online connection identity mismatch",
            )
            .build_error());
        }
        let user_id = connection_info.user_id.clone();

        // 注册会话到 Signaling Online(业务端会话管理)
        let conversation_id = self
            .session_domain_service
            .register_connection(
                &authenticated_ctx,
                &user_id,
                &connection_info.device_id,
                connection_info.platform.as_deref(),
                Some(connection_id),
                connection_info.metadata.as_ref(),
            )
            .await?;

        // 登记的 await 窗口里连接可能已经断开：断开处理那时找不到会话，什么都没清。登记完成后
        // 复查两件事再写会话——断开处理留下的标记（断开发生在登记途中），以及底层连接表里是否
        // 还有这条连接（断开处理跑在本次注册占位之前，底层随后移除了连接）。任一成立都不写
        // `online_sessions`、立即注销刚登记的 Online 会话；否则会话与 Online 登录一直留到 TTL。
        // 写会话与读标记在同一把 `connecting` 锁里：之后到达的断开一定能找到会话。
        let still_connected = self
            .connection_port
            .get_connection_info(connection_id)
            .await
            .is_ok();
        let registered = {
            let connecting = self.connecting.lock().await;
            let closed_while_registering = connecting.get(connection_id).copied().unwrap_or(false);
            if still_connected && !closed_while_registering {
                let replaced = self.online_sessions.write().await.insert(
                    connection_id.to_string(),
                    OnlineSession {
                        user_id: user_id.clone(),
                        tenant_id: connection_info.tenant_id.clone(),
                        conversation_id: conversation_id.clone(),
                        authenticated_ctx: authenticated_ctx.clone(),
                    },
                );
                // 并发的第二次登记会覆盖同一连接的会话：被覆盖的那条不会再被断开处理取出，
                // 在这里与它登记时的 `connection_opened` 配对，连接数不会只增不减。
                if let Some(replaced) = replaced {
                    self.usage.connection_closed(&replaced.tenant_id);
                }
                self.usage.connection_opened(&connection_info.tenant_id);
                self.usage.user_active(&connection_info.tenant_id, &user_id);
                true
            } else {
                false
            }
        };
        if !registered {
            debug!(
                user_id = %user_id,
                connection_id = %connection_id,
                conversation_id = %conversation_id,
                "connection closed during online registration; unregistering the late login"
            );
            if let Err(err) = self
                .session_domain_service
                .unregister_connection(&authenticated_ctx, &user_id, &conversation_id)
                .await
            {
                // 注销失败时这条 Online 会话只能等 TTL 回收，与断开处理注销失败相同。
                warn!(
                    ?err,
                    user_id = %user_id,
                    connection_id = %connection_id,
                    "Failed to unregister online status of a connection closed during registration"
                );
            }
            return Err(flare_server_core::error::ErrorBuilder::new(
                flare_server_core::error::ErrorCode::ConnectionClosed,
                "connection closed during online registration",
            )
            .build_error());
        }

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
        let list = self
            .connection_port
            .list_user_connections(&connection_info.tenant_id, &user_id)
            .await?;
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
        // 没有 Online 会话是预期路径，不是错误：连接在注册会话之前就被拒（租户停用、鉴权失败，
        // 底层仍会回调断开），或同一连接先 `on_error` 再 `on_disconnect`、第二次清理时会话已被
        // 第一次取走。以前这里报 InvalidParameter，调用方每次记一条 WARN，租户停用演练期间成百
        // 上千行（W10-S2 593、W12-S2 292）。
        //
        // 注册正在途中（`register_connection` 还没返回）时同样找不到会话：在 `connecting` 里留下
        // 断开标记，由登记完成后自己注销。取会话与留标记在同一把 `connecting` 锁里，与登记一侧
        // 「读标记、写会话」互斥，不会两边都落空。
        let session = {
            let mut connecting = self.connecting.lock().await;
            let session = self.online_sessions.write().await.remove(connection_id);
            if session.is_none()
                && let Some(closed_while_registering) = connecting.get_mut(connection_id)
            {
                *closed_while_registering = true;
                debug!(
                    connection_id = %connection_id,
                    "connection closed while its online registration is in flight; the registration unregisters itself"
                );
            }
            session
        };
        let Some(session) = session else {
            debug!(
                connection_id = %connection_id,
                "online session not found for connection; nothing to unregister"
            );
            return Ok(());
        };
        let user_id = session.user_id;
        let tenant_id = session.tenant_id;
        let conversation_id = session.conversation_id;
        let authenticated_ctx = session.authenticated_ctx;
        // 与登记时的 `connection_opened` 成对：会话只会从 `online_sessions` 取出一次。
        self.usage.connection_closed(&tenant_id);

        // 底层连接可能已先被移除，断开清理必须以 Online 会话缓存为准。
        let user_connections = self
            .connection_port
            .list_user_connections(&tenant_id, &user_id)
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
            .unregister_connection(&authenticated_ctx, &user_id, &conversation_id)
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
        let authenticated_ctx = session.authenticated_ctx;

        // 刷新 Signaling Online 的心跳
        self.session_domain_service
            .refresh_heartbeat(
                &authenticated_ctx,
                &user_id,
                &conversation_id,
                Some(connection_id),
            )
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

#[cfg(test)]
mod disconnect_tests {
    use super::*;
    use crate::domain::model::{ConnectionDomainServiceConfig, ConnectionInfo};
    use crate::domain::service::{ConnectionQualityService, ConversationSubscriptionRegistry};
    use async_trait::async_trait;
    use flare_grpc_proto::signaling::{
        GetOnlineStatusRequest, GetOnlineStatusResponse, HeartbeatRequest, HeartbeatResponse,
        LoginRequest, LoginResponse, LogoutRequest, LogoutResponse,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// 只数 Online 注销次数的连接端口；其余调用不应在断开清理中出现。
    #[derive(Default)]
    struct CountingPort {
        logouts: AtomicUsize,
    }

    #[async_trait]
    impl IConnectionPort for CountingPort {
        async fn login(&self, _ctx: &Ctx, _request: LoginRequest) -> Result<LoginResponse> {
            unreachable!("disconnect never logs in")
        }

        async fn logout(&self, _ctx: &Ctx, _request: LogoutRequest) -> Result<LogoutResponse> {
            self.logouts.fetch_add(1, Ordering::SeqCst);
            Ok(LogoutResponse::default())
        }

        async fn heartbeat(
            &self,
            _ctx: &Ctx,
            _request: HeartbeatRequest,
        ) -> Result<HeartbeatResponse> {
            unreachable!("disconnect never sends heartbeats")
        }

        async fn get_online_status(
            &self,
            _ctx: &Ctx,
            _request: GetOnlineStatusRequest,
        ) -> Result<GetOnlineStatusResponse> {
            unreachable!("disconnect never reads presence")
        }

        async fn list_user_connections(
            &self,
            _tenant_id: &str,
            _user_id: &str,
        ) -> Result<Vec<ConnectionInfo>> {
            Ok(Vec::new())
        }

        async fn get_connection_info(&self, _connection_id: &str) -> Result<ConnectionInfo> {
            unreachable!("disconnect works from the online session cache")
        }

        async fn get_connection_metadata(
            &self,
            _connection_id: &str,
        ) -> Result<HashMap<String, String>> {
            Ok(HashMap::new())
        }

        async fn build_ctx(&self, _connection_id: &str) -> Result<Ctx> {
            unreachable!("disconnect works from the online session cache")
        }
    }

    fn handler(port: Arc<dyn IConnectionPort>) -> ConnectionHandler {
        ConnectionHandler::new(
            Arc::new(ConnectionDomainService::new(
                port.clone(),
                Arc::new(ConnectionQualityService::new()),
                ConnectionDomainServiceConfig {
                    gateway_id: "gateway-test".to_string(),
                },
            )),
            Arc::new(flare_im_service_kit::metrics::AccessGatewayMetrics::new()),
            Arc::new(flare_im_contracts::abstractions::state::NoopConnectionStateNotifier),
            port,
            Arc::new(ConversationSubscriptionRegistry::new()),
            Arc::new(crate::infrastructure::ports::ConversationReadGrpcPool::new()),
        )
    }

    /// 连接在注册 Online 会话之前就被拒（租户停用、鉴权失败），或同一连接先报错再断开时，
    /// 断开清理找不到会话：这是预期路径，不报错（调用方据此不再记 WARN），也不去注销 Online。
    #[tokio::test]
    async fn disconnecting_a_connection_without_an_online_session_is_a_quiet_no_op() {
        let port = Arc::new(CountingPort::default());
        let handler = handler(port.clone());

        handler
            .handle_disconnect("conn-rejected-before-registration")
            .await
            .expect("a connection that never registered has nothing to clean up");
        assert_eq!(port.logouts.load(Ordering::SeqCst), 0);
    }

    /// 已注册的连接只注销一次：`on_error` 之后的 `on_disconnect` 再清理一次时什么都不做。
    #[tokio::test]
    async fn a_registered_connection_is_unregistered_once() {
        let port = Arc::new(CountingPort::default());
        let handler = handler(port.clone());
        handler.online_sessions.write().await.insert(
            "conn-1".to_string(),
            OnlineSession {
                user_id: "u1".to_string(),
                tenant_id: "tenant-a".to_string(),
                conversation_id: "presence-1".to_string(),
                authenticated_ctx: Arc::new(
                    flare_server_core::Context::with_request_id("req")
                        .with_tenant_id("tenant-a")
                        .with_user_id("u1"),
                ),
            },
        );

        handler.handle_disconnect("conn-1").await.expect("first");
        handler.handle_disconnect("conn-1").await.expect("second");
        assert_eq!(port.logouts.load(Ordering::SeqCst), 1);
    }

    /// 连接表里有一条已认证连接、Online 登录要等测试放行的端口：用来在 `register_connection`
    /// 的 await 窗口里断开连接。
    struct GatedLoginPort {
        /// 连接是否还在底层连接表里（底层在业务断开处理之后才移除连接）。
        connected: std::sync::atomic::AtomicBool,
        login_entered: tokio::sync::Notify,
        release_login: tokio::sync::Notify,
        logins: AtomicUsize,
        logouts: AtomicUsize,
    }

    impl GatedLoginPort {
        fn new() -> Self {
            Self {
                connected: std::sync::atomic::AtomicBool::new(true),
                login_entered: tokio::sync::Notify::new(),
                release_login: tokio::sync::Notify::new(),
                logins: AtomicUsize::new(0),
                logouts: AtomicUsize::new(0),
            }
        }

        fn connection_info(&self, connection_id: &str) -> Result<ConnectionInfo> {
            if !self.connected.load(Ordering::SeqCst) {
                return Err(flare_server_core::error::ErrorBuilder::new(
                    flare_server_core::error::ErrorCode::InvalidParameter,
                    "connection not found",
                )
                .build_error());
            }
            let mut info = ConnectionInfo::new(
                connection_id.to_string(),
                "u1".to_string(),
                "tenant-a".to_string(),
                "d1".to_string(),
            );
            info.platform = Some("web".to_string());
            Ok(info)
        }
    }

    #[async_trait]
    impl IConnectionPort for GatedLoginPort {
        async fn login(&self, _ctx: &Ctx, _request: LoginRequest) -> Result<LoginResponse> {
            self.logins.fetch_add(1, Ordering::SeqCst);
            // notify_one 在没有等待者时留一个许可，测试晚一步等待也不会错过。
            self.login_entered.notify_one();
            self.release_login.notified().await;
            Ok(LoginResponse {
                success: true,
                conversation_id: "presence-1".to_string(),
                ..Default::default()
            })
        }

        async fn logout(&self, _ctx: &Ctx, request: LogoutRequest) -> Result<LogoutResponse> {
            assert_eq!(request.conversation_id, "presence-1");
            self.logouts.fetch_add(1, Ordering::SeqCst);
            Ok(LogoutResponse::default())
        }

        async fn heartbeat(
            &self,
            _ctx: &Ctx,
            _request: HeartbeatRequest,
        ) -> Result<HeartbeatResponse> {
            unreachable!("registration never sends heartbeats")
        }

        async fn get_online_status(
            &self,
            _ctx: &Ctx,
            _request: GetOnlineStatusRequest,
        ) -> Result<GetOnlineStatusResponse> {
            unreachable!("registration never reads presence")
        }

        async fn list_user_connections(
            &self,
            _tenant_id: &str,
            _user_id: &str,
        ) -> Result<Vec<ConnectionInfo>> {
            Ok(Vec::new())
        }

        async fn get_connection_info(&self, connection_id: &str) -> Result<ConnectionInfo> {
            self.connection_info(connection_id)
        }

        async fn get_connection_metadata(
            &self,
            _connection_id: &str,
        ) -> Result<HashMap<String, String>> {
            Ok(HashMap::new())
        }

        async fn build_ctx(&self, connection_id: &str) -> Result<Ctx> {
            self.connection_info(connection_id)?;
            Ok(Arc::new(
                flare_server_core::Context::with_request_id("req")
                    .with_tenant_id("tenant-a")
                    .with_user_id("u1"),
            ))
        }
    }

    /// 在 Online 登录途中开始注册，返回注册任务；返回时登录已进入等待。
    async fn connect_until_login_waits(
        handler: &Arc<ConnectionHandler>,
        port: &Arc<GatedLoginPort>,
    ) -> tokio::task::JoinHandle<Result<String>> {
        let connect = tokio::spawn({
            let handler = handler.clone();
            async move { handler.handle_connect("conn-1").await }
        });
        port.login_entered.notified().await;
        connect
    }

    fn assert_closed_during_registration(result: Result<String>) {
        let err = result.expect_err("registration of a closed connection must not succeed");
        assert_eq!(
            err.code(),
            Some(flare_server_core::error::ErrorCode::ConnectionClosed),
            "{err}"
        );
    }

    /// 断开发生在 `register_connection` 的 await 窗口里：断开处理找不到会话，随后完成的登记
    /// 不能再写进 `online_sessions`（否则会话与 Online 登录一直留到 TTL），而要立即注销一次。
    #[tokio::test]
    async fn a_disconnect_during_registration_unregisters_the_late_login() {
        let port = Arc::new(GatedLoginPort::new());
        let usage = UsageRecorder::unpublished("signaling-gateway-test");
        let handler = Arc::new(handler(port.clone()).with_usage(usage.clone()));
        let connect = connect_until_login_waits(&handler, &port).await;

        handler
            .handle_disconnect("conn-1")
            .await
            .expect("disconnect while registering is not an error");
        assert_eq!(port.logouts.load(Ordering::SeqCst), 0);
        port.release_login.notify_one();

        assert_closed_during_registration(connect.await.expect("connect task"));
        assert_eq!(port.logouts.load(Ordering::SeqCst), 1);
        assert!(!handler.online_sessions.read().await.contains_key("conn-1"));
        assert!(handler.connecting.lock().await.is_empty());

        // 底层随后照常再报一次断开（on_error 之后的 on_disconnect）：不再注销。
        handler.handle_disconnect("conn-1").await.expect("second");
        assert_eq!(port.logouts.load(Ordering::SeqCst), 1);
        assert_eq!(port.logins.load(Ordering::SeqCst), 1);
        // 没写进 `online_sessions` 的登记不算连接，也不算活跃用户。
        assert!(usage.live().is_empty());
    }

    /// 断开处理在注册占位之前就跑完了（什么都没清），底层随后把连接移出连接表：登记完成时
    /// 连接已不在，同样不写会话、立即注销。
    #[tokio::test]
    async fn a_connection_gone_when_registration_completes_is_unregistered() {
        let port = Arc::new(GatedLoginPort::new());
        let handler = Arc::new(handler(port.clone()));
        let connect = connect_until_login_waits(&handler, &port).await;

        port.connected.store(false, Ordering::SeqCst);
        port.release_login.notify_one();

        assert_closed_during_registration(connect.await.expect("connect task"));
        assert_eq!(port.logouts.load(Ordering::SeqCst), 1);
        assert!(!handler.online_sessions.read().await.contains_key("conn-1"));
    }

    /// 没有断开时照常登记：会话写入、不注销；之后的断开注销一次。
    #[tokio::test]
    async fn a_connection_still_open_after_registration_keeps_its_session() {
        let port = Arc::new(GatedLoginPort::new());
        let usage = UsageRecorder::unpublished("signaling-gateway-test");
        let handler = Arc::new(handler(port.clone()).with_usage(usage.clone()));
        let connect = connect_until_login_waits(&handler, &port).await;
        port.release_login.notify_one();

        assert_eq!(
            connect.await.expect("connect task").expect("registered"),
            "presence-1"
        );
        let live: Vec<_> = usage
            .live()
            .into_iter()
            .map(|reading| (reading.app, reading.meter, reading.value))
            .collect();
        assert_eq!(live, [("tenant-a".to_string(), "connections.peak", 1)]);
        assert!(handler.online_sessions.read().await.contains_key("conn-1"));
        assert!(handler.connecting.lock().await.is_empty());
        assert_eq!(port.logouts.load(Ordering::SeqCst), 0);

        handler
            .handle_disconnect("conn-1")
            .await
            .expect("disconnect");
        assert_eq!(port.logouts.load(Ordering::SeqCst), 1);
        assert!(!handler.online_sessions.read().await.contains_key("conn-1"));

        // 断开把连接数减回去：再登记一次，窗口峰值仍是 1 而不是 2。
        let reconnect = connect_until_login_waits(&handler, &port).await;
        port.release_login.notify_one();
        reconnect.await.expect("connect task").expect("registered");
        let live: Vec<_> = usage
            .live()
            .into_iter()
            .map(|reading| (reading.app, reading.meter, reading.value))
            .collect();
        assert_eq!(live, [("tenant-a".to_string(), "connections.peak", 1)]);
    }

    /// 同一连接的会话被并发登记覆盖时，被覆盖的那条按断开计，连接数不累积。
    #[tokio::test]
    async fn a_replaced_session_does_not_leave_a_connection_counted() {
        let port = Arc::new(GatedLoginPort::new());
        let usage = UsageRecorder::unpublished("signaling-gateway-test");
        let handler = Arc::new(handler(port.clone()).with_usage(usage.clone()));
        let first = connect_until_login_waits(&handler, &port).await;
        port.release_login.notify_one();
        first.await.expect("connect task").expect("registered");

        // 单飞判空之后、拿到占位之前对方已登记完成的竞态，结果是第二次登记直接覆盖会话。
        handler.online_sessions.write().await.remove("conn-1");
        let second = connect_until_login_waits(&handler, &port).await;
        handler.online_sessions.write().await.insert(
            "conn-1".to_string(),
            OnlineSession {
                user_id: "u1".to_string(),
                tenant_id: "tenant-a".to_string(),
                conversation_id: "presence-1".to_string(),
                authenticated_ctx: Arc::new(
                    flare_server_core::Context::with_request_id("req")
                        .with_tenant_id("tenant-a")
                        .with_user_id("u1"),
                ),
            },
        );
        port.release_login.notify_one();
        second.await.expect("connect task").expect("registered");

        handler
            .handle_disconnect("conn-1")
            .await
            .expect("disconnect");
        let third = connect_until_login_waits(&handler, &port).await;
        port.release_login.notify_one();
        third.await.expect("connect task").expect("registered");
        let live: Vec<_> = usage
            .live()
            .into_iter()
            .map(|reading| (reading.app, reading.meter, reading.value))
            .collect();
        assert_eq!(live, [("tenant-a".to_string(), "connections.peak", 1)]);
    }
}
