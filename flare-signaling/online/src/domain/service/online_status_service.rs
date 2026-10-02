//! 在线状态领域服务 - 包含所有业务逻辑实现

use std::collections::HashMap;
use std::sync::Arc;

use flare_grpc_proto::signaling::online::{
    DeviceConflictStrategy, GetOnlineStatusResponse, HeartbeatResponse, LoginRequest,
    LoginResponse, LogoutRequest, LogoutResponse, OnlineStatus,
};
use flare_im_contracts::ConnectionEvent;
use flare_im_contracts::domain::tenant::is_valid_tenant_id;
use flare_im_service_kit::tenant_runtime::{TenantAccessPolicy, TenantLookup, TenantRuntimeCache};
use flare_server_core::context::Context;
use flare_server_core::error::{ErrorCode, Result, map_infra_error};
use flare_server_core::flare_err;
use prost_types::Timestamp;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::domain::aggregate::{Connection, ConnectionCreateParams};
use crate::domain::connection_event_publisher::{
    ConnectionEventPublisher, NoopConnectionEventPublisher,
};
use crate::domain::model::OnlineStatusRecord;
use crate::domain::repository::{ConversationRepository, EvictedConnection, kick_reason};
use crate::domain::value_object::{
    ConnectionId, ConnectionQuality, DeviceId, DevicePriority, TokenVersion, UserId,
};

#[derive(Debug, Clone)]
struct InMemoryConnection {
    session: Connection,
}

/// 在线状态领域服务 - 包含所有业务逻辑
///
/// 使用泛型参数以支持 Rust 2024 原生 async fn in traits（禁止 `dyn` 异步 trait）
pub struct OnlineStatusService<CR, P = NoopConnectionEventPublisher> {
    repository: Arc<CR>,
    sessions: Arc<RwLock<HashMap<String, InMemoryConnection>>>,
    gateway_id: String,
    connection_event_publisher: Option<Arc<P>>,
    tenant_runtime: TenantRuntimeCache,
    tenant_policy: TenantAccessPolicy,
    max_online_devices_per_user: u32,
}

impl<CR, P> OnlineStatusService<CR, P>
where
    CR: ConversationRepository + Send + Sync,
    P: ConnectionEventPublisher + Send + Sync,
{
    pub fn new(repository: Arc<CR>, gateway_id: String) -> Self {
        Self {
            repository,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            gateway_id,
            connection_event_publisher: None,
            tenant_runtime: TenantRuntimeCache::disabled(),
            tenant_policy: TenantAccessPolicy::Lenient,
            max_online_devices_per_user: 0,
        }
    }

    pub fn with_connection_event_publisher(mut self, publisher: Option<Arc<P>>) -> Self {
        self.connection_event_publisher = publisher;
        self
    }

    pub fn with_tenant_runtime(
        mut self,
        tenant_runtime: TenantRuntimeCache,
        tenant_policy: TenantAccessPolicy,
        max_online_devices_per_user: u32,
    ) -> Self {
        self.tenant_runtime = tenant_runtime;
        self.tenant_policy = tenant_policy;
        self.max_online_devices_per_user = max_online_devices_per_user;
        self
    }

    async fn remove_memory_evicted(&self, evicted: &[EvictedConnection]) {
        let mut map = self.sessions.write().await;
        for item in evicted {
            map.remove(&item.conversation_id);
        }
    }

    pub async fn login(&self, ctx: &Context, request: LoginRequest) -> Result<LoginResponse> {
        let tenant_id = required_tenant(ctx)?;
        tracing::trace!(
            trace_id = %ctx.trace_id(),
            user_id = %request.user_id,
            device_id = %request.device_id,
            "Handling login request"
        );

        // 参数验证
        if request.user_id.is_empty() {
            return Err(flare_err!(
                ErrorCode::InvalidParameter,
                "user_id is required"
            ));
        }
        if request.device_id.is_empty() {
            return Err(flare_err!(
                ErrorCode::InvalidParameter,
                "device_id is required"
            ));
        }

        let user_id = &request.user_id;
        let device_id = &request.device_id;
        let device_platform = request.device_platform.as_str();
        let desired_strategy = request.desired_conflict_strategy();
        if !matches!(
            desired_strategy,
            DeviceConflictStrategy::Exclusive
                | DeviceConflictStrategy::PlatformExclusive
                | DeviceConflictStrategy::Coexist
        ) {
            return Err(flare_err!(
                ErrorCode::InvalidParameter,
                "desired_conflict_strategy is required"
            ));
        }
        let applied_strategy = desired_strategy;

        let user_vo = UserId::new(user_id.clone()).map_err(|e| {
            flare_err!(
                ErrorCode::InvalidParameter,
                format!("invalid user_id: {}", e)
            )
        })?;
        let lookup = self.tenant_runtime.lookup(tenant_id).await;
        match &lookup {
            // 投影读不到（数据库抖动等）：不论策略都拒绝登记（fail-closed），但这是暂时故障，
            // 不是租户结论。不能用 `TENANT_UNAVAILABLE`：客户端收到它会终止会话、不再重连；
            // 这里按服务不可用上报，网关照常按内部错误关连接，客户端随后重连。与 HTTP 租户门
            // 回 503 而不是 403 一致。
            TenantLookup::Unavailable => {
                return Err(flare_err!(
                    ErrorCode::ServiceUnavailable,
                    "tenant runtime projection unavailable"
                ));
            }
            // 已知的停用 / 删除中租户，或严格策略下没有投影的租户：类型化原因，网关据此在
            // CONNECT_ACK 之后给客户端下发 KICKED `TENANT_UNAVAILABLE`，与建连拒绝、踢线、
            // HTTP 拒绝是同一个错误码。
            _ if !lookup.admits_connection(self.tenant_policy) => {
                return Err(flare_err!(
                    ErrorCode::PermissionDenied,
                    kick_reason::TENANT_UNAVAILABLE
                ));
            }
            _ => {}
        }
        let projected_cap = lookup.core_quota().max_online_devices_per_user;
        let max_devices = if projected_cap > 0 {
            projected_cap
        } else {
            self.max_online_devices_per_user
        };

        // 从 metadata 中提取 gateway_id（用于跨地区路由）
        // 如果 metadata 中没有 gateway_id，使用配置的默认值
        let gateway_id = request
            .metadata
            .get("gateway_id")
            .cloned()
            .unwrap_or_else(|| self.gateway_id.clone());

        // 提取设备优先级（默认为普通优先级=2）
        let device_priority = request.device_priority;

        // 提取 Token 版本（默认为0）
        let token_version = request.token_version;

        // 提取初始链接质量
        let connection_quality = request
            .initial_quality
            .as_ref()
            .and_then(|q| ConnectionQuality::from_proto(q).ok());

        // 创建新会话
        let device_vo = DeviceId::new(device_id.clone()).map_err(|e| {
            flare_err!(
                ErrorCode::InvalidParameter,
                format!("invalid device_id: {}", e)
            )
        })?;
        let priority_vo = DevicePriority::from_i32(device_priority);
        let token_vo = TokenVersion::from(token_version);
        let params = ConnectionCreateParams {
            user_id: user_vo.clone(),
            device_id: device_vo.clone(),
            tenant_id: tenant_id.to_owned(),
            device_platform: device_platform.to_string(),
            server_id: request.server_id.clone(),
            gateway_id: gateway_id.clone(),
            device_priority: priority_vo,
            token_version: token_vo,
            initial_quality: connection_quality.clone(),
        };
        let session = Connection::create(params);
        let conversation_id = session.id().as_str().to_string();

        let gateway_connection_id = request.metadata.get("connection_id").ok_or_else(|| {
            flare_err!(
                ErrorCode::PermissionDenied,
                "authenticated gateway connection ID required"
            )
        })?;
        let evicted = self
            .repository
            .admit_connection(
                &session,
                gateway_connection_id,
                applied_strategy,
                max_devices,
            )
            .await
            .map_err(|error| {
                map_infra_error(
                    error,
                    ErrorCode::DatabaseError,
                    "Failed to atomically admit Online connection",
                )
            })?;
        self.remove_memory_evicted(&evicted).await;
        for old in &evicted {
            // 原因随挤掉的缘由：冲突策略挤掉的其他设备收到 DEVICE_CONFLICT，结束会话、不再重连。
            // 多网关时新设备所在网关的设备管理器不认识旧设备，旧设备只会收到这一条踢线；若报成
            // SESSION_REPLACED，它会自动重连回来再把新设备挤掉，两台设备来回互踢。
            if let Err(error) = self
                .repository
                .broadcast_user_kick(
                    tenant_id,
                    user_id,
                    Some(&old.device_id),
                    Some(&old.connection_id),
                    old.cause.kick_reason(),
                )
                .await
            {
                // Redis admission is already committed. Returning a login error
                // would leave the new physical socket registered with no
                // conversation ID at the gateway. The old session is gone from
                // authoritative storage and its next heartbeat will fail.
                warn!(tenant_id, user_id, connection_id = %old.connection_id, %error, "old connection kick publish failed after admission");
            }
        }

        {
            let mut map = self.sessions.write().await;
            map.insert(
                conversation_id.clone(),
                InMemoryConnection {
                    session: session.clone(),
                },
            );
        }

        if let (Some(publisher), Some(_connection_id)) = (
            &self.connection_event_publisher,
            request.metadata.get("connection_id"),
        ) {
            let event = ConnectionEvent {
                user_id: user_id.clone(),
                device_id: Some(device_id.clone()),
                state: "registered".to_string(),
            };
            if let Err(e) = publisher.publish(&event).await {
                warn!(error = %e, "Failed to publish ConnectionRegistered event");
            }
        }

        info!(
            user_id = %user_id,
            tenant_id = %tenant_id,
            conversation_id = %conversation_id,
            device_id = %device_id,
            gateway_id = %gateway_id,
            "User logged in successfully"
        );

        Ok(LoginResponse {
            success: true,
            conversation_id,
            route_server: request.server_id,
            error_message: String::new(),
            applied_conflict_strategy: applied_strategy as i32,
        })
    }

    pub async fn logout(&self, ctx: &Context, request: LogoutRequest) -> Result<LogoutResponse> {
        let tenant_id = required_tenant(ctx)?;
        tracing::trace!(
            trace_id = %ctx.trace_id(),
            user_id = %request.user_id,
            conversation_id = %request.conversation_id,
            "Handling logout request"
        );

        let user_id = request.user_id.trim();
        let conversation_id = request.conversation_id.trim();
        if user_id.is_empty() {
            return Err(flare_err!(
                ErrorCode::InvalidParameter,
                "user_id is required"
            ));
        }
        let user_vo = UserId::new(user_id.to_string()).map_err(|e| {
            flare_err!(
                ErrorCode::InvalidParameter,
                format!("invalid user_id: {}", e)
            )
        })?;

        if conversation_id.is_empty() {
            return Err(flare_err!(
                ErrorCode::InvalidParameter,
                "conversation_id is required"
            ));
        }

        let session_vo = ConnectionId::from_string(conversation_id.to_string()).map_err(|e| {
            flare_err!(
                ErrorCode::InvalidParameter,
                format!("invalid conversation_id: {}", e)
            )
        })?;
        {
            let mut map = self.sessions.write().await;
            if map.get(conversation_id).is_some_and(|entry| {
                entry.session.tenant_id() == tenant_id
                    && entry.session.user_id().as_str() == user_id
            }) {
                map.remove(conversation_id);
            }
        }
        self.repository
            .remove_connection(tenant_id, &session_vo, &user_vo)
            .await
            .map_err(|e| {
                map_infra_error(e, ErrorCode::DatabaseError, "Failed to remove connection")
            })?;

        info!(
            user_id = %user_id,
            conversation_id = %conversation_id,
            "User logged out successfully"
        );

        Ok(LogoutResponse { success: true })
    }

    pub async fn heartbeat(
        &self,
        ctx: &Context,
        conversation_id: &str,
        user_id: &str,
        connection_quality: Option<&flare_proto::common::ConnectionQuality>,
    ) -> Result<HeartbeatResponse> {
        let tenant_id = required_tenant(ctx)?;
        tracing::trace!(
            trace_id = %ctx.trace_id(),
            user_id = %user_id,
            conversation_id = %conversation_id,
            "Handling heartbeat request"
        );

        {
            let mut map = self.sessions.write().await;
            if let Some(session) = map.get_mut(conversation_id).filter(|entry| {
                entry.session.tenant_id() == tenant_id
                    && entry.session.user_id().as_str() == user_id
            }) {
                let quality_opt =
                    connection_quality.and_then(|q| ConnectionQuality::from_proto(q).ok());
                session
                    .session
                    .refresh_heartbeat(quality_opt)
                    .map_err(|e| flare_server_core::error::FlareError::system((e).to_string()))?;
            }
        }

        let user_vo = UserId::new(user_id.to_string()).map_err(|e| {
            flare_err!(
                ErrorCode::InvalidParameter,
                format!("invalid user_id: {}", e)
            )
        })?;
        let session_vo = ConnectionId::from_string(conversation_id.to_string()).map_err(|e| {
            flare_err!(
                ErrorCode::InvalidParameter,
                format!("invalid conversation_id: {}", e)
            )
        })?;
        self.repository
            .touch_connection(tenant_id, &session_vo, &user_vo)
            .await
            .map_err(|e| {
                map_infra_error(e, ErrorCode::DatabaseError, "Failed to touch connection")
            })?;

        Ok(HeartbeatResponse { success: true })
    }

    pub async fn get_online_status(
        &self,
        ctx: &Context,
        user_ids: &[String],
    ) -> Result<GetOnlineStatusResponse> {
        let tenant_id = required_tenant(ctx)?;
        tracing::trace!(
            trace_id = %ctx.trace_id(),
            user_count = %user_ids.len(),
            "Handling get online status request"
        );

        let statuses = self
            .repository
            .fetch_statuses(tenant_id, user_ids)
            .await
            .map_err(|e| {
                map_infra_error(e, ErrorCode::DatabaseError, "Failed to fetch statuses")
            })?;

        let mut result = HashMap::new();
        for user_id in user_ids {
            let status = statuses
                .get(user_id)
                .cloned()
                .unwrap_or_else(|| OnlineStatusRecord {
                    online: false,
                    server_id: String::new(),
                    gateway_id: None,
                    cluster_id: None,
                    last_seen: None,
                    device_id: None,
                    device_platform: None,
                });
            result.insert(
                user_id.clone(),
                OnlineStatus {
                    online: status.online,
                    server_id: status.server_id,
                    cluster_id: status.cluster_id.unwrap_or_default(),
                    last_seen: status.last_seen.as_ref().map(|dt| Timestamp {
                        seconds: dt.timestamp(),
                        nanos: dt.timestamp_subsec_nanos() as i32,
                    }),
                    device_id: status.device_id.unwrap_or_default(),
                    device_platform: status.device_platform.unwrap_or_default(),
                    gateway_id: status.gateway_id.unwrap_or_default(), // 返回 gateway_id 用于跨地区路由
                },
            );
        }

        Ok(GetOnlineStatusResponse { statuses: result })
    }
}

fn required_tenant(ctx: &Context) -> Result<&str> {
    let tenant_id = ctx
        .tenant_id()
        .ok_or_else(|| flare_err!(ErrorCode::PermissionDenied, "verified tenant is required"))?;
    if !is_valid_tenant_id(tenant_id) {
        return Err(flare_err!(
            ErrorCode::PermissionDenied,
            "invalid tenant in context"
        ));
    }
    Ok(tenant_id)
}

/// Noop ConversationRepository 实现
pub struct NoopConversationRepository;

impl ConversationRepository for NoopConversationRepository {
    async fn save_connection(
        &self,
        _connection: &Connection,
    ) -> flare_server_core::error::Result<()> {
        Ok(())
    }
    async fn admit_connection(
        &self,
        _connection: &Connection,
        _gateway_connection_id: &str,
        _strategy: DeviceConflictStrategy,
        _max_devices: u32,
    ) -> flare_server_core::error::Result<Vec<EvictedConnection>> {
        Ok(Vec::new())
    }
    async fn remove_connection(
        &self,
        _tenant_id: &str,
        _conversation_id: &ConnectionId,
        _user_id: &UserId,
    ) -> flare_server_core::error::Result<()> {
        Ok(())
    }
    async fn touch_connection(
        &self,
        _tenant_id: &str,
        _conversation_id: &ConnectionId,
        _user_id: &UserId,
    ) -> flare_server_core::error::Result<()> {
        Ok(())
    }
    async fn fetch_statuses(
        &self,
        _tenant_id: &str,
        _user_ids: &[String],
    ) -> flare_server_core::error::Result<HashMap<String, OnlineStatusRecord>> {
        Ok(HashMap::new())
    }
    async fn get_user_connections(
        &self,
        _tenant_id: &str,
        _user_id: &UserId,
    ) -> flare_server_core::error::Result<Vec<Connection>> {
        Ok(vec![])
    }
    async fn remove_user_connections(
        &self,
        _tenant_id: &str,
        _user_id: &UserId,
        _device_ids: Option<&[DeviceId]>,
    ) -> flare_server_core::error::Result<()> {
        Ok(())
    }
    async fn get_connection_by_device(
        &self,
        _tenant_id: &str,
        _user_id: &UserId,
        _device_id: &DeviceId,
    ) -> flare_server_core::error::Result<Option<Connection>> {
        Ok(None)
    }
    async fn list_user_connections(
        &self,
        _ctx: &Context,
    ) -> flare_server_core::error::Result<Vec<Connection>> {
        Ok(vec![])
    }
    async fn scan_tenant_sessions(
        &self,
        _tenant_id: &str,
        _cursor: u64,
        _count: usize,
    ) -> flare_server_core::error::Result<(u64, Vec<String>)> {
        Ok((0, vec![]))
    }
    async fn unindex_tenant_session(
        &self,
        _tenant_id: &str,
        _member: &str,
    ) -> flare_server_core::error::Result<()> {
        Ok(())
    }
    async fn broadcast_user_kick(
        &self,
        _tenant_id: &str,
        _user_id: &str,
        _device_id: Option<&str>,
        _connection_id: Option<&str>,
        _reason: &str,
    ) -> flare_server_core::error::Result<()> {
        Ok(())
    }
}

/// 默认的在线状态服务类型
pub type DefaultOnlineStatusService =
    OnlineStatusService<NoopConversationRepository, NoopConnectionEventPublisher>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::repository::EvictionCause;
    use flare_im_contracts::domain::tenant::{TenantRuntimeSnapshot, TenantStatus};
    use flare_im_service_kit::tenant_runtime::{TenantRuntimeCacheOptions, TenantRuntimeSource};

    struct OneTenant(TenantStatus);

    #[async_trait::async_trait]
    impl TenantRuntimeSource for OneTenant {
        async fn load(
            &self,
            tenant_id: &str,
        ) -> std::result::Result<Option<TenantRuntimeSnapshot>, String> {
            Ok(Some(TenantRuntimeSnapshot {
                tenant_id: tenant_id.to_string(),
                status: self.0,
                quota: Default::default(),
                version: 1,
            }))
        }
    }

    /// 控制面从没投影过这个租户。
    struct NoProjection;

    #[async_trait::async_trait]
    impl TenantRuntimeSource for NoProjection {
        async fn load(
            &self,
            _tenant_id: &str,
        ) -> std::result::Result<Option<TenantRuntimeSnapshot>, String> {
            Ok(None)
        }
    }

    /// 投影库读不到（如 PG 抖动）。
    struct UnreachableProjection;

    #[async_trait::async_trait]
    impl TenantRuntimeSource for UnreachableProjection {
        async fn load(
            &self,
            _tenant_id: &str,
        ) -> std::result::Result<Option<TenantRuntimeSnapshot>, String> {
            Err("connection refused".to_string())
        }
    }

    fn service_with(
        source: impl TenantRuntimeSource,
        policy: TenantAccessPolicy,
    ) -> DefaultOnlineStatusService {
        DefaultOnlineStatusService::new(
            Arc::new(NoopConversationRepository),
            "gateway-test".to_string(),
        )
        .with_tenant_runtime(
            TenantRuntimeCache::with_source(Arc::new(source), TenantRuntimeCacheOptions::default()),
            policy,
            0,
        )
    }

    async fn login_alice(service: &DefaultOnlineStatusService) -> Result<LoginResponse> {
        let ctx = Context::root()
            .with_tenant_id("tenant-a")
            .with_user_id("alice");
        service
            .login(
                &ctx,
                LoginRequest {
                    user_id: "alice".to_string(),
                    device_id: "device-a".to_string(),
                    device_platform: "web".to_string(),
                    desired_conflict_strategy: DeviceConflictStrategy::Coexist as i32,
                    metadata: HashMap::from([("connection_id".to_string(), "conn-a".to_string())]),
                    ..Default::default()
                },
            )
            .await
    }

    /// 停用 / 删除中的租户登记会话被拒，原因是类型化的 `TENANT_UNAVAILABLE`：网关据此在 CONNECT_ACK
    /// 之后给客户端下发同一个原因，而不是无原因关连接。
    #[tokio::test]
    async fn login_of_an_unavailable_tenant_is_rejected_with_a_typed_reason() {
        for status in [TenantStatus::Suspended, TenantStatus::Deleting] {
            let service = service_with(OneTenant(status), TenantAccessPolicy::Lenient);

            let error = login_alice(&service)
                .await
                .expect_err("unavailable tenant must not log in");

            assert_eq!(
                error.code(),
                Some(ErrorCode::PermissionDenied),
                "{status:?}"
            );
            assert_eq!(
                error.reason(),
                kick_reason::TENANT_UNAVAILABLE,
                "{status:?}"
            );
        }
    }

    /// 没有投影的租户：严格策略按租户不可用拒绝（类型化原因），宽松策略放行。
    #[tokio::test]
    async fn an_unprojected_tenant_follows_the_policy() {
        let strict = service_with(NoProjection, TenantAccessPolicy::Strict);
        let error = login_alice(&strict)
            .await
            .expect_err("strict policy rejects an unprojected tenant");
        assert_eq!(error.code(), Some(ErrorCode::PermissionDenied));
        assert_eq!(error.reason(), kick_reason::TENANT_UNAVAILABLE);

        let lenient = service_with(NoProjection, TenantAccessPolicy::Lenient);
        login_alice(&lenient)
            .await
            .expect("lenient policy admits an unprojected tenant");
    }

    /// 一次踢线广播：`(device_id, connection_id, reason)`。
    type Kick = (Option<String>, Option<String>, String);

    /// 准入挤掉的连接各按缘由收到踢线；其余操作与 [`NoopConversationRepository`] 相同。
    struct EvictingRepository {
        evicted: Vec<EvictedConnection>,
        kicks: std::sync::Mutex<Vec<Kick>>,
    }

    impl ConversationRepository for EvictingRepository {
        async fn save_connection(&self, connection: &Connection) -> Result<()> {
            NoopConversationRepository.save_connection(connection).await
        }
        async fn admit_connection(
            &self,
            _connection: &Connection,
            _gateway_connection_id: &str,
            _strategy: DeviceConflictStrategy,
            _max_devices: u32,
        ) -> Result<Vec<EvictedConnection>> {
            Ok(self.evicted.clone())
        }
        async fn remove_connection(
            &self,
            tenant_id: &str,
            conversation_id: &ConnectionId,
            user_id: &UserId,
        ) -> Result<()> {
            NoopConversationRepository
                .remove_connection(tenant_id, conversation_id, user_id)
                .await
        }
        async fn touch_connection(
            &self,
            tenant_id: &str,
            conversation_id: &ConnectionId,
            user_id: &UserId,
        ) -> Result<()> {
            NoopConversationRepository
                .touch_connection(tenant_id, conversation_id, user_id)
                .await
        }
        async fn fetch_statuses(
            &self,
            tenant_id: &str,
            user_ids: &[String],
        ) -> Result<HashMap<String, OnlineStatusRecord>> {
            NoopConversationRepository
                .fetch_statuses(tenant_id, user_ids)
                .await
        }
        async fn get_user_connections(
            &self,
            tenant_id: &str,
            user_id: &UserId,
        ) -> Result<Vec<Connection>> {
            NoopConversationRepository
                .get_user_connections(tenant_id, user_id)
                .await
        }
        async fn remove_user_connections(
            &self,
            tenant_id: &str,
            user_id: &UserId,
            device_ids: Option<&[DeviceId]>,
        ) -> Result<()> {
            NoopConversationRepository
                .remove_user_connections(tenant_id, user_id, device_ids)
                .await
        }
        async fn get_connection_by_device(
            &self,
            tenant_id: &str,
            user_id: &UserId,
            device_id: &DeviceId,
        ) -> Result<Option<Connection>> {
            NoopConversationRepository
                .get_connection_by_device(tenant_id, user_id, device_id)
                .await
        }
        async fn list_user_connections(&self, ctx: &Context) -> Result<Vec<Connection>> {
            NoopConversationRepository.list_user_connections(ctx).await
        }
        async fn scan_tenant_sessions(
            &self,
            tenant_id: &str,
            cursor: u64,
            count: usize,
        ) -> Result<(u64, Vec<String>)> {
            NoopConversationRepository
                .scan_tenant_sessions(tenant_id, cursor, count)
                .await
        }
        async fn unindex_tenant_session(&self, tenant_id: &str, member: &str) -> Result<()> {
            NoopConversationRepository
                .unindex_tenant_session(tenant_id, member)
                .await
        }
        async fn broadcast_user_kick(
            &self,
            _tenant_id: &str,
            _user_id: &str,
            device_id: Option<&str>,
            connection_id: Option<&str>,
            reason: &str,
        ) -> Result<()> {
            self.kicks.lock().unwrap().push((
                device_id.map(str::to_owned),
                connection_id.map(str::to_owned),
                reason.to_owned(),
            ));
            Ok(())
        }
    }

    fn evicted(device: &str, physical: &str, cause: EvictionCause) -> EvictedConnection {
        EvictedConnection {
            conversation_id: format!("session-{physical}"),
            device_id: device.to_string(),
            connection_id: physical.to_string(),
            cause,
        }
    }

    /// 冲突策略挤掉的其他设备收到 DEVICE_CONFLICT（结束会话、不再重连），同设备接替与超出设备数
    /// 上限收到 SESSION_REPLACED。多网关时新设备所在网关不认识旧设备，旧设备只收到这一条踢线：
    /// 报成 SESSION_REPLACED 的话它会重连回来再挤掉新设备，两台设备来回互踢。
    #[tokio::test]
    async fn evicted_connections_are_kicked_with_the_reason_for_their_eviction() {
        let repository = Arc::new(EvictingRepository {
            evicted: vec![
                evicted("pc-old", "conn-pc-old", EvictionCause::DeviceConflict),
                evicted("device-a", "conn-a-old", EvictionCause::Replaced),
                evicted("web-oldest", "conn-web-oldest", EvictionCause::DeviceCap),
            ],
            kicks: std::sync::Mutex::new(Vec::new()),
        });
        let service = OnlineStatusService::<_, NoopConnectionEventPublisher>::new(
            Arc::clone(&repository),
            "gateway-test".to_string(),
        )
        .with_tenant_runtime(
            TenantRuntimeCache::with_source(
                Arc::new(NoProjection),
                TenantRuntimeCacheOptions::default(),
            ),
            TenantAccessPolicy::Lenient,
            0,
        );
        let ctx = Context::root()
            .with_tenant_id("tenant-a")
            .with_user_id("alice");

        service
            .login(
                &ctx,
                LoginRequest {
                    user_id: "alice".to_string(),
                    device_id: "device-a".to_string(),
                    device_platform: "pc".to_string(),
                    desired_conflict_strategy: DeviceConflictStrategy::PlatformExclusive as i32,
                    metadata: HashMap::from([("connection_id".to_string(), "conn-a".to_string())]),
                    ..Default::default()
                },
            )
            .await
            .expect("login admits the new device");

        let kick = |device: &str, physical: &str, reason: &str| {
            (
                Some(device.to_string()),
                Some(physical.to_string()),
                reason.to_string(),
            )
        };
        assert_eq!(
            *repository.kicks.lock().unwrap(),
            vec![
                kick("pc-old", "conn-pc-old", kick_reason::DEVICE_CONFLICT),
                kick("device-a", "conn-a-old", kick_reason::SESSION_REPLACED),
                kick(
                    "web-oldest",
                    "conn-web-oldest",
                    kick_reason::SESSION_REPLACED
                ),
            ]
        );
    }

    /// 投影库读不到时仍然拒绝登记（两种策略都是），但报的是暂时故障，不是 `TENANT_UNAVAILABLE`：
    /// 后者会让客户端终止会话、不再重连，一次数据库抖动就会把这段时间里连进来的客户端全部挂在离线。
    #[tokio::test]
    async fn a_projection_outage_fails_the_login_transiently() {
        for policy in [TenantAccessPolicy::Lenient, TenantAccessPolicy::Strict] {
            let service = service_with(UnreachableProjection, policy);

            let error = login_alice(&service)
                .await
                .expect_err("login must fail closed while the projection is unreachable");

            assert_eq!(
                error.code(),
                Some(ErrorCode::ServiceUnavailable),
                "{policy:?}"
            );
            assert_ne!(
                error.reason(),
                kick_reason::TENANT_UNAVAILABLE,
                "{policy:?}"
            );
        }
    }
}
