//! 用户领域服务 - 包含所有业务逻辑实现

use std::sync::Arc;

use chrono::{DateTime, Utc};
use flare_grpc_proto::signaling::online::{
    BatchGetUserPresenceRequest, BatchGetUserPresenceResponse, DeviceInfo, GetDeviceRequest,
    GetDeviceResponse, GetUserPresenceRequest, GetUserPresenceResponse, KickDeviceRequest,
    KickDeviceResponse, KickTenantRequest, KickTenantResponse, ListUserDevicesRequest,
    ListUserDevicesResponse, UserPresence,
};
use flare_im_contracts::domain::tenant::is_valid_tenant_id;
use flare_im_contracts::utils::normalize_tenant_id;
use flare_server_core::error::{ErrorCode, Result, map_infra_error};
use flare_server_core::flare_err;
use prost_types::Timestamp;
use tracing::{info, warn};

use super::loggable_reason;
use crate::domain::aggregate::Connection;
use crate::domain::push_binding::{
    BindingRevocation, BindingRevocationReason, BindingRevocationSink,
};
use crate::domain::repository::{
    ConversationRepository, is_kick_pending_confirmation, kick_reason, parse_tenant_session_member,
};
use crate::domain::value_object::UserId;

const ONLINE_SESSION_FRESHNESS_SECONDS: i64 = 90;
/// `KickTenant` 每页 SSCAN 的成员数。
const KICK_TENANT_SCAN_PAGE: usize = 256;
/// 请求未带原因时日志里记的原因。连接端收到的始终是类型化的
/// [`kick_reason::TENANT_UNAVAILABLE`]，与建连拒绝、HTTP 拒绝同一个错误码，客户端据此判断租户不可用；
/// 请求里的原因是运维说明，只进日志。
const KICK_TENANT_DEFAULT_REASON: &str = "TENANT_UNAVAILABLE";

/// 一次踢线针对的范围。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KickScope {
    /// 点名踢一台设备：运维踢，或用户踢自己的设备。
    Device,
    /// 踢整个租户时按在线索引逐台踢。
    Tenant,
}

impl KickScope {
    /// 下发给被踢连接的类型化原因。
    fn wire_reason(self) -> &'static str {
        match self {
            Self::Device => kick_reason::DEVICE_KICKED,
            Self::Tenant => kick_reason::TENANT_UNAVAILABLE,
        }
    }

    /// 吊销被踢设备推送绑定的原因。
    fn binding_reason(self) -> BindingRevocationReason {
        match self {
            Self::Device => BindingRevocationReason::Kicked,
            Self::Tenant => BindingRevocationReason::TenantKicked,
        }
    }
}

/// 用户领域服务 - 包含所有业务逻辑（泛型仓储，避免 `dyn` 异步 trait）
pub struct UserService<R: ConversationRepository + Send + Sync> {
    conversation_repository: Arc<R>,
    /// 推送绑定吊销的出口；没接推送通道时为空，踢线与身份吊销都不吊销绑定。
    binding_revocations: Option<Arc<dyn BindingRevocationSink>>,
}

impl<R: ConversationRepository + Send + Sync> UserService<R> {
    pub fn new(conversation_repository: Arc<R>) -> Self {
        Self {
            conversation_repository,
            binding_revocations: None,
        }
    }

    /// 接上推送绑定吊销的出口：踢设备、踢租户、吊销身份会话时，吊销被结束的设备的绑定。
    pub fn with_binding_revocations(
        mut self,
        sink: Option<Arc<dyn BindingRevocationSink>>,
    ) -> Self {
        self.binding_revocations = sink;
        self
    }

    /// 吊销一台被踢设备的推送绑定。只提交请求，不等推送侧。
    ///
    /// 登记时刻的上限取提交的这一刻：踢线已经生效（或已经登记、必然生效），在这之前登记的绑定
    /// 都属于被踢掉的那次登录；设备之后重新登录登记的绑定不在范围内，吊销送得再晚也不会清掉它。
    fn revoke_device_bindings(
        &self,
        tenant_id: &str,
        user_id: &str,
        device_id: &str,
        reason: BindingRevocationReason,
    ) {
        if let Some(sink) = self.binding_revocations.as_ref() {
            let kicked_at_ms = Utc::now().timestamp_millis();
            sink.submit(BindingRevocation::device(
                tenant_id,
                user_id,
                device_id,
                reason,
                kicked_at_ms,
            ));
        }
    }

    /// 身份会话被吊销的设备，推送绑定一并吊销。
    ///
    /// 在仓储结束这些会话之前算出涉及的设备并提交：会话一旦移除就再也对不上设备。身份侧的吊销
    /// 此时已成事实，哪怕这一次网关还没确认断开（仓储返回错误、调用方稍后重试），绑定也该吊销；
    /// 重试会重复提交，吊销可以重复执行。
    ///
    /// - 请求没有点名会话：这次吊销针对该用户的全部登录，凡是还留着的绑定都失效，一次吊销他在
    ///   本租户下全部设备的绑定。
    /// - 请求点名了会话：只吊销被这次吊销结束的在线会话所属的设备。已托管的设备没有在线会话，
    ///   这里对不上它属于哪个身份会话，不会被吊销；要覆盖它，推送绑定得先带上身份会话 ID。
    ///
    /// 登记时刻的上限是身份侧吊销发生的时刻，即请求的 `cutoff_ms`：调用方重投时它不变，用户在那
    /// 之后重新登录登记的绑定不受影响。请求没带截止时刻时退到收到请求的时刻 `received_at_ms`。
    async fn revoke_bindings_of_identity_sessions(
        &self,
        tenant: &str,
        user: &UserId,
        sessions: &[String],
        cutoff_ms: i64,
        received_at_ms: i64,
    ) -> Result<()> {
        let Some(sink) = self.binding_revocations.as_ref() else {
            return Ok(());
        };
        let registered_before_ms = if cutoff_ms > 0 {
            cutoff_ms
        } else {
            received_at_ms
        };
        if sessions.is_empty() {
            sink.submit(BindingRevocation::all_devices(
                tenant,
                user.as_str(),
                BindingRevocationReason::IdentityRevoked,
                registered_before_ms,
            ));
            return Ok(());
        }
        let live = self
            .conversation_repository
            .get_user_connections(tenant, user)
            .await?;
        let mut devices: Vec<&str> = live
            .iter()
            .filter(|connection| connection.ended_by_identity_revocation(sessions, cutoff_ms))
            .map(|connection| connection.device_id().as_str())
            .collect();
        // 一台设备可以同时挂着几条连接，只报一次。
        devices.sort_unstable();
        devices.dedup();
        for device_id in devices {
            sink.submit(BindingRevocation::device(
                tenant,
                user.as_str(),
                device_id,
                BindingRevocationReason::IdentityRevoked,
                registered_before_ms,
            ));
        }
        Ok(())
    }

    /// 查询用户在线状态
    pub async fn get_user_presence(
        &self,
        tenant_id: &str,
        request: GetUserPresenceRequest,
    ) -> Result<GetUserPresenceResponse> {
        let user_id = &request.user_id;

        // 获取用户的所有会话
        let user_id_vo = UserId::new(user_id.to_string()).map_err(|e| {
            flare_err!(
                ErrorCode::InvalidParameter,
                format!("invalid user_id: {}", e)
            )
        })?;
        let sessions = self
            .conversation_repository
            .get_user_connections(tenant_id, &user_id_vo)
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::DatabaseError,
                    "Failed to get user connections",
                )
            })?;
        let now = Utc::now();
        let sessions = sessions
            .into_iter()
            .filter(|s| {
                now.signed_duration_since(s.last_heartbeat_at())
                    .num_seconds()
                    <= ONLINE_SESSION_FRESHNESS_SECONDS
            })
            .collect::<Vec<_>>();

        // 计算在线状态
        let is_online = !sessions.is_empty();
        let last_seen = sessions
            .iter()
            .map(|s| s.last_heartbeat_at())
            .max()
            .unwrap_or_else(Utc::now);

        let presence = UserPresence {
            user_id: user_id.clone(),
            is_online,
            devices: sessions.iter().map(device_info).collect(),
            last_seen: Some(timestamp(last_seen)),
        };

        Ok(GetUserPresenceResponse {
            presence: Some(presence),
        })
    }

    /// 批量查询在线状态
    pub async fn batch_get_user_presence(
        &self,
        tenant_id: &str,
        request: BatchGetUserPresenceRequest,
    ) -> Result<BatchGetUserPresenceResponse> {
        let user_ids = &request.user_ids;

        if user_ids.is_empty() {
            return Ok(BatchGetUserPresenceResponse {
                presences: std::collections::HashMap::new(),
            });
        }

        if user_ids.len() > 100 {
            return Err(flare_err!(
                ErrorCode::InvalidParameter,
                "maximum 100 user_ids allowed"
            ));
        }

        let mut presences = std::collections::HashMap::new();

        for user_id in user_ids {
            match self
                .get_user_presence(
                    tenant_id,
                    GetUserPresenceRequest {
                        user_id: user_id.clone(),
                    },
                )
                .await
            {
                Ok(response) => {
                    if let Some(presence) = response.presence {
                        presences.insert(user_id.clone(), presence);
                    }
                }
                Err(err) => {
                    warn!(user_id = %user_id, error = %err, "failed to get user presence");
                }
            }
        }

        Ok(BatchGetUserPresenceResponse { presences })
    }

    pub async fn list_user_devices(
        &self,
        tenant_id: &str,
        request: ListUserDevicesRequest,
    ) -> Result<ListUserDevicesResponse> {
        let user_id = request.user_id;

        let user_id_vo = crate::domain::value_object::UserId::new(user_id)
            .map_err(|e| flare_server_core::error::FlareError::system((e).to_string()))?;
        let sessions = self
            .conversation_repository
            .get_user_connections(tenant_id, &user_id_vo)
            .await?;
        let now = Utc::now();
        let sessions = sessions
            .into_iter()
            .filter(|s| {
                now.signed_duration_since(s.last_heartbeat_at())
                    .num_seconds()
                    <= ONLINE_SESSION_FRESHNESS_SECONDS
            })
            .collect::<Vec<_>>();

        Ok(ListUserDevicesResponse {
            devices: sessions.iter().map(device_info).collect(),
        })
    }

    /// Private identity-owner effect; no Staff impersonation or device-wide wildcard kick.
    pub async fn revoke_identity_sessions(
        &self,
        tenant: &str,
        user: &str,
        sessions: &[String],
        cutoff_ms: i64,
        request_id: &str,
        signing_key_id: &str,
    ) -> Result<u32> {
        let received_at_ms = Utc::now().timestamp_millis();
        let user = crate::domain::value_object::UserId::new(user.to_owned())
            .map_err(|error| flare_server_core::error::FlareError::system(error.to_string()))?;
        self.revoke_bindings_of_identity_sessions(
            tenant,
            &user,
            sessions,
            cutoff_ms,
            received_at_ms,
        )
        .await?;
        self.conversation_repository
            .revoke_identity_sessions(
                tenant,
                &user,
                sessions,
                cutoff_ms,
                request_id,
                signing_key_id,
            )
            .await
    }

    /// 踢出设备（运维操作，或用户踢自己的设备）：连接端收到 [`kick_reason::DEVICE_KICKED`]，请求
    /// 里的原因只进日志。被踢设备的推送绑定一并吊销，设备当时不在线也一样。
    pub async fn kick_device(
        &self,
        request: KickDeviceRequest,
        expected_tenant_id: &str,
    ) -> Result<KickDeviceResponse> {
        self.kick_device_because(request, expected_tenant_id, KickScope::Device)
            .await
    }

    /// `scope` 决定下发给被踢连接的原因、吊销推送绑定的原因，以及设备没有在线会话时吊不吊销。
    async fn kick_device_because(
        &self,
        request: KickDeviceRequest,
        expected_tenant_id: &str,
        scope: KickScope,
    ) -> Result<KickDeviceResponse> {
        let wire_reason = scope.wire_reason();
        let binding_reason = scope.binding_reason();
        let user_id = &request.user_id;
        let device_id = &request.device_id;

        // 查找设备对应的会话
        let user_vo = crate::domain::value_object::UserId::new(user_id.to_string())
            .map_err(|e| flare_server_core::error::FlareError::system((e).to_string()))?;
        let device_vo = crate::domain::value_object::DeviceId::new(device_id.to_string())
            .map_err(|e| flare_server_core::error::FlareError::system((e).to_string()))?;
        let session = self
            .conversation_repository
            .get_connection_by_device(expected_tenant_id, &user_vo, &device_vo)
            .await?;

        if let Some(session) = session {
            if session.tenant_id() != expected_tenant_id {
                return Err(flare_err!(
                    ErrorCode::PermissionDenied,
                    "device tenant does not match control identity"
                ));
            }
            // 删除会话
            let physical = match self
                .conversation_repository
                .remove_connection_for_kick(expected_tenant_id, session.id(), &user_vo, wire_reason)
                .await
            {
                Ok(physical) => physical,
                Err(error) => {
                    // 踢线已经持久登记、只是网关还没确认：这条会话必然结束，但结束时不再经过
                    // 这里，绑定现在就吊销。其余失败会话还在，绑定不动。
                    if is_kick_pending_confirmation(&error) {
                        self.revoke_device_bindings(
                            expected_tenant_id,
                            user_id,
                            device_id,
                            binding_reason,
                        );
                    }
                    return Err(error);
                }
            };
            // 同设备的新连接在移除之前接替了它：设备仍然在线，没有被踢，绑定不动。
            let Some(physical) = physical else {
                return Ok(KickDeviceResponse { success: false });
            };
            // 被踢的设备可能根本收不到踢线通知（不在线、连接半死），不会自己吊销绑定。
            self.revoke_device_bindings(expected_tenant_id, user_id, device_id, binding_reason);

            info!(
                user_id = %user_id,
                device_id = %device_id,
                conversation_id = %session.id().as_str(),
                connection_id = %physical,
                reason = %loggable_reason(&request.reason),
                "device kicked"
            );

            Ok(KickDeviceResponse { success: true })
        } else {
            // 点名踢的设备没有在线会话（已托管、已断开）：它收不到踢线，推送却还在往它的绑定上发，
            // 这正是踢它要止住的。绑定照样吊销，调用方拿到的仍是「设备不存在」。
            // 踢租户时走到这里的是只剩索引的陈旧成员，不是谁点名要踢的设备，不吊销。
            if scope == KickScope::Device {
                self.revoke_device_bindings(expected_tenant_id, user_id, device_id, binding_reason);
            }
            Err(flare_err!(ErrorCode::UserNotFound, "device not found"))
        }
    }

    /// 踢出整个租户：按 `tenant:sessions:{tenant}` 索引分页 SSCAN，每个成员复用 `kick_device`；
    /// 陈旧索引以条件删除清理；只通知旧物理连接，绝不广播踢整个用户。
    pub async fn kick_tenant(&self, request: KickTenantRequest) -> Result<KickTenantResponse> {
        let tenant_id = normalize_tenant_id(&request.tenant_id);
        if !is_valid_tenant_id(&tenant_id) {
            return Err(flare_err!(
                ErrorCode::InvalidParameter,
                format!("invalid tenant_id: {}", request.tenant_id)
            ));
        }
        let reason = match request.reason.trim() {
            "" => KICK_TENANT_DEFAULT_REASON,
            reason => reason,
        };

        let mut cursor = 0u64;
        let mut kicked = 0u32;
        let mut stale = 0u32;
        let mut failed = 0u32;
        loop {
            let (next, members) = self
                .conversation_repository
                .scan_tenant_sessions(&tenant_id, cursor, KICK_TENANT_SCAN_PAGE)
                .await?;
            for member in members {
                let Some((user_id, device_id)) = parse_tenant_session_member(&member) else {
                    warn!(tenant_id = %tenant_id, member = %member, "malformed tenant session member; dropping");
                    self.conversation_repository
                        .unindex_tenant_session(&tenant_id, &member)
                        .await?;
                    continue;
                };
                match self
                    .kick_device_because(
                        KickDeviceRequest {
                            user_id: user_id.to_string(),
                            device_id: device_id.to_string(),
                            reason: reason.to_string(),
                        },
                        &tenant_id,
                        KickScope::Tenant,
                    )
                    .await
                {
                    Ok(response) if response.success => {
                        kicked = kicked.saturating_add(1);
                    }
                    Ok(_) => {} // Replaced before removal: keep the replacement's index.
                    Err(error) if error.code() == Some(ErrorCode::UserNotFound) => {
                        // 会话已经过期 / 被别处删掉，只剩索引成员。
                        stale = stale.saturating_add(1);
                        self.conversation_repository
                            .unindex_tenant_session(&tenant_id, &member)
                            .await?;
                    }
                    Err(error) => {
                        failed = failed.saturating_add(1);
                        warn!(tenant_id = %tenant_id, member = %member, %error, "kick device failed; continuing");
                    }
                }
            }
            if next == 0 {
                break;
            }
            cursor = next;
        }

        info!(
            tenant_id = %tenant_id,
            reason = %loggable_reason(reason),
            kicked,
            stale,
            failed,
            "tenant kicked"
        );
        if failed > 0 {
            return Err(flare_err!(
                ErrorCode::ServiceUnavailable,
                "tenant kick incomplete"
            ));
        }
        Ok(KickTenantResponse {
            kicked,
            superseded: false,
        })
    }

    pub async fn get_device(
        &self,
        tenant_id: &str,
        request: GetDeviceRequest,
    ) -> Result<GetDeviceResponse> {
        let user_id = request.user_id.as_str();
        let device_id = &request.device_id;

        let user_id_vo = crate::domain::value_object::UserId::new(user_id.to_string())
            .map_err(|e| flare_server_core::error::FlareError::system((e).to_string()))?;
        let device_vo = crate::domain::value_object::DeviceId::new(device_id.to_string())
            .map_err(|e| flare_server_core::error::FlareError::system((e).to_string()))?;
        let session = self
            .conversation_repository
            .get_connection_by_device(tenant_id, &user_id_vo, &device_vo)
            .await?;

        if let Some(session) = session {
            Ok(GetDeviceResponse {
                device: Some(device_info(&session)),
            })
        } else {
            Err(flare_err!(ErrorCode::InvalidParameter, "device not found"))
        }
    }
}

/// 一条会话对外的设备信息：设备列表、在线状态与单设备查询（含管理员入口）共用这一处，
/// 设备可达状态（应用状态、注意力、进入当前状态的时刻、角标）随会话记录带出。
fn device_info(session: &Connection) -> DeviceInfo {
    DeviceInfo {
        device_id: session.device_id().as_str().to_string(),
        platform: session.device_platform().to_string(),
        model: String::new(),
        os_version: String::new(),
        last_active_time: Some(timestamp(session.last_heartbeat_at())),
        priority: session.device_priority() as i32,
        token_version: session.token_version().value(),
        connection_quality: session.connection_quality().cloned().map(|cq| cq.into()),
        conversation_id: session.id().as_str().to_string(),
        gateway_id: session.gateway_id().to_string(),
        server_id: session.server_id().to_string(),
        app_state: session.app_state().to_proto() as i32,
        attention: session.attention().to_proto() as i32,
        state_since: Some(timestamp(session.state_since())),
        badge: session.badge(),
    }
}

fn timestamp(at: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: at.timestamp(),
        nanos: at.timestamp_subsec_nanos() as i32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::aggregate::{Connection, ConnectionCreateParams};
    use crate::domain::model::OnlineStatusRecord;
    use crate::domain::repository::tenant_session_member;
    use crate::domain::value_object::{
        ConnectionId, DeviceAppState, DeviceAttention, DeviceId, DevicePriority, DeviceStateUpdate,
        TokenVersion,
    };
    use std::collections::{BTreeSet, HashMap};
    use std::sync::Mutex;

    /// 内存版会话存储：复刻 Redis 实现的索引维护规则（save 加成员、remove 删成员）。
    #[derive(Default)]
    struct MemoryRepo {
        sessions: Mutex<HashMap<String, Vec<Connection>>>,
        index: Mutex<HashMap<String, BTreeSet<String>>>,
        /// 成员插入顺序日志：SSCAN 的游标不随删除位移，这里用它模拟。
        index_log: Mutex<HashMap<String, Vec<String>>>,
        kicks: Mutex<Vec<(String, String, Option<String>)>>,
        /// 每次 kick 广播带给连接端的原因。
        kick_reasons: Mutex<Vec<String>>,
        page_size_seen: Mutex<Vec<usize>>,
        /// 踢线时仓储的表现；缺省是真的移除。
        kick_behaviour: Mutex<KickBehaviour>,
        /// 身份会话吊销时网关迟迟不确认：仓储报错，调用方稍后重试。
        identity_revocation_pending: Mutex<bool>,
    }

    /// 踢线时仓储的表现。
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    enum KickBehaviour {
        /// 通知连接端并移除会话。
        #[default]
        Remove,
        /// 踢线已登记，网关还没确认。
        Pending,
        /// 同设备的新连接已经接替，没有可移除的。
        Replaced,
        /// 存储不可用。
        Unavailable,
    }

    impl MemoryRepo {
        fn index_add(&self, tenant: &str, member: &str) {
            let inserted = self
                .index
                .lock()
                .unwrap()
                .entry(tenant.to_string())
                .or_default()
                .insert(member.to_string());
            if inserted {
                self.index_log
                    .lock()
                    .unwrap()
                    .entry(tenant.to_string())
                    .or_default()
                    .push(member.to_string());
            }
        }

        fn seed_stale(&self, tenant: &str, member: &str) {
            self.index_add(tenant, member);
        }

        fn members(&self, tenant: &str) -> Vec<String> {
            self.index
                .lock()
                .unwrap()
                .get(tenant)
                .map(|set| set.iter().cloned().collect())
                .unwrap_or_default()
        }
    }

    impl ConversationRepository for MemoryRepo {
        async fn save_connection(&self, connection: &Connection) -> Result<()> {
            self.sessions
                .lock()
                .unwrap()
                .entry(format!(
                    "{}:{}",
                    connection.tenant_id(),
                    connection.user_id().as_str()
                ))
                .or_default()
                .push(connection.clone());
            self.index_add(
                connection.tenant_id(),
                &tenant_session_member(
                    connection.user_id().as_str(),
                    connection.device_id().as_str(),
                ),
            );
            Ok(())
        }

        async fn admit_connection(
            &self,
            connection: &Connection,
            _gateway_connection_id: &str,
            _strategy: flare_grpc_proto::signaling::online::DeviceConflictStrategy,
            _max_devices: u32,
        ) -> Result<Vec<crate::domain::repository::EvictedConnection>> {
            self.save_connection(connection).await?;
            Ok(Vec::new())
        }

        async fn remove_connection(
            &self,
            tenant_id: &str,
            conversation_id: &ConnectionId,
            user_id: &UserId,
        ) -> Result<()> {
            let removed = {
                let mut sessions = self.sessions.lock().unwrap();
                let list = sessions
                    .entry(format!("{}:{}", tenant_id, user_id.as_str()))
                    .or_default();
                let pos = list.iter().position(|c| c.id() == conversation_id);
                pos.map(|i| list.remove(i))
            };
            if let Some(removed) = removed {
                self.index
                    .lock()
                    .unwrap()
                    .entry(removed.tenant_id().to_string())
                    .or_default()
                    .remove(&tenant_session_member(
                        user_id.as_str(),
                        removed.device_id().as_str(),
                    ));
            }
            Ok(())
        }

        async fn touch_connection(&self, _: &str, _: &ConnectionId, _: &UserId) -> Result<()> {
            Ok(())
        }

        async fn remove_connection_for_kick(
            &self,
            tenant: &str,
            id: &ConnectionId,
            user: &UserId,
            reason: &str,
        ) -> Result<Option<String>> {
            let session = self
                .get_user_connections(tenant, user)
                .await?
                .into_iter()
                .find(|s| s.id() == id);
            let Some(session) = session else {
                return Ok(None);
            };
            let behaviour = *self.kick_behaviour.lock().unwrap();
            match behaviour {
                KickBehaviour::Remove => {}
                KickBehaviour::Pending => {
                    return Err(flare_err!(
                        ErrorCode::ServiceUnavailable,
                        crate::domain::repository::KICK_PENDING_CONFIRMATION
                    ));
                }
                KickBehaviour::Replaced => return Ok(None),
                KickBehaviour::Unavailable => {
                    return Err(flare_err!(
                        ErrorCode::DatabaseError,
                        "conditional session removal failed"
                    ));
                }
            }
            self.broadcast_user_kick(
                tenant,
                user.as_str(),
                Some(session.device_id().as_str()),
                Some(&format!("physical-{}", id.as_str())),
                reason,
            )
            .await?;
            self.remove_connection(tenant, id, user).await?;
            Ok(Some(format!("physical-{}", id.as_str())))
        }

        async fn revoke_identity_sessions(
            &self,
            tenant: &str,
            user: &UserId,
            session_ids: &[String],
            cutoff_ms: i64,
            _request_id: &str,
            _signing_key_id: &str,
        ) -> Result<u32> {
            if *self.identity_revocation_pending.lock().unwrap() {
                return Err(flare_err!(
                    ErrorCode::ServiceUnavailable,
                    "identity connection retirement pending receiver confirmation"
                ));
            }
            let mut sessions = self.sessions.lock().unwrap();
            let list = sessions
                .entry(format!("{}:{}", tenant, user.as_str()))
                .or_default();
            let before = list.len();
            list.retain(|c| !c.ended_by_identity_revocation(session_ids, cutoff_ms));
            Ok((before - list.len()) as u32)
        }

        async fn fetch_statuses(
            &self,
            _tenant_id: &str,
            _user_ids: &[String],
        ) -> Result<HashMap<String, OnlineStatusRecord>> {
            Ok(HashMap::new())
        }

        async fn get_user_connections(
            &self,
            tenant_id: &str,
            user_id: &UserId,
        ) -> Result<Vec<Connection>> {
            Ok(self
                .sessions
                .lock()
                .unwrap()
                .get(&format!("{}:{}", tenant_id, user_id.as_str()))
                .cloned()
                .unwrap_or_default())
        }

        async fn remove_user_connections(
            &self,
            _tenant_id: &str,
            _user_id: &UserId,
            _device_ids: Option<&[DeviceId]>,
        ) -> Result<()> {
            unreachable!("not used by kick_tenant")
        }

        async fn get_connection_by_device(
            &self,
            tenant_id: &str,
            user_id: &UserId,
            device_id: &DeviceId,
        ) -> Result<Option<Connection>> {
            Ok(self
                .get_user_connections(tenant_id, user_id)
                .await?
                .into_iter()
                .find(|c| c.device_id() == device_id))
        }

        async fn list_user_connections(
            &self,
            _ctx: &flare_server_core::context::Context,
        ) -> Result<Vec<Connection>> {
            unreachable!("not used by kick_tenant")
        }

        async fn scan_tenant_sessions(
            &self,
            tenant_id: &str,
            cursor: u64,
            count: usize,
        ) -> Result<(u64, Vec<String>)> {
            self.page_size_seen.lock().unwrap().push(count);
            // 与 SSCAN 一样：游标是稳定位置，扫描期间被删的成员只是不再出现，不会让后面的成员被跳过。
            let log = self
                .index_log
                .lock()
                .unwrap()
                .get(tenant_id)
                .cloned()
                .unwrap_or_default();
            let present = self
                .index
                .lock()
                .unwrap()
                .get(tenant_id)
                .cloned()
                .unwrap_or_default();
            let start = (cursor as usize).min(log.len());
            let end = (start + count.max(1)).min(log.len());
            let page = log[start..end]
                .iter()
                .filter(|m| present.contains(*m))
                .cloned()
                .collect();
            let next = if end >= log.len() { 0 } else { end as u64 };
            Ok((next, page))
        }

        async fn unindex_tenant_session(&self, tenant_id: &str, member: &str) -> Result<()> {
            self.index
                .lock()
                .unwrap()
                .entry(tenant_id.to_string())
                .or_default()
                .remove(member);
            Ok(())
        }

        async fn broadcast_user_kick(
            &self,
            tenant_id: &str,
            user_id: &str,
            device_id: Option<&str>,
            _connection_id: Option<&str>,
            reason: &str,
        ) -> Result<()> {
            self.kicks.lock().unwrap().push((
                tenant_id.to_owned(),
                user_id.to_owned(),
                device_id.map(str::to_owned),
            ));
            self.kick_reasons.lock().unwrap().push(reason.to_owned());
            Ok(())
        }
    }

    fn connection(tenant: &str, user: &str, device: &str) -> Connection {
        Connection::create(ConnectionCreateParams {
            user_id: UserId::new(user.to_string()).unwrap(),
            device_id: DeviceId::new(device.to_string()).unwrap(),
            tenant_id: tenant.to_string(),
            device_platform: "ios".to_string(),
            server_id: "s".to_string(),
            gateway_id: "g".to_string(),
            device_priority: DevicePriority::Normal,
            token_version: TokenVersion::from(0),
            initial_quality: None,
            app_state: DeviceAppState::Active,
            attention: DeviceAttention::Unspecified,
        })
    }

    /// 设备列表、在线状态与单设备查询带出同一份设备可达状态：应用状态、注意力、进入当前状态的
    /// 时刻与角标。
    #[tokio::test]
    async fn device_queries_expose_the_reported_device_state() {
        use flare_proto::common::{
            DeviceAppState as ProtoAppState, DeviceAttention as ProtoAttention,
        };

        let repo = Arc::new(MemoryRepo::default());
        let mut parked = connection("acme", "u1", "phone");
        let parked_at = parked.created_at() + chrono::Duration::seconds(3);
        parked.report_device_state(
            DeviceStateUpdate {
                app_state: DeviceAppState::Parking,
                attention: DeviceAttention::Idle,
                badge: 4,
            },
            parked_at,
        );
        repo.save_connection(&parked).await.unwrap();
        let untouched = connection("acme", "u1", "desk");
        repo.save_connection(&untouched).await.unwrap();
        let service = UserService::new(repo);

        let assert_parked = |device: &DeviceInfo| {
            assert_eq!(device.app_state, ProtoAppState::Parking as i32);
            assert_eq!(device.attention, ProtoAttention::Idle as i32);
            assert_eq!(device.badge, 4);
            assert_eq!(device.state_since, Some(timestamp(parked_at)));
        };
        let find = |devices: Vec<DeviceInfo>, id: &str| {
            devices
                .into_iter()
                .find(|device| device.device_id == id)
                .unwrap()
        };

        let listed = service
            .list_user_devices(
                "acme",
                ListUserDevicesRequest {
                    user_id: "u1".into(),
                },
            )
            .await
            .unwrap()
            .devices;
        assert_parked(&find(listed.clone(), "phone"));
        // 没上报过的设备：建连时的状态、自建连起、角标 0。
        let desk = find(listed, "desk");
        assert_eq!(desk.app_state, ProtoAppState::Active as i32);
        assert_eq!(desk.attention, ProtoAttention::Unspecified as i32);
        assert_eq!(desk.badge, 0);
        assert_eq!(desk.state_since, Some(timestamp(untouched.created_at())));

        let presence = service
            .get_user_presence(
                "acme",
                GetUserPresenceRequest {
                    user_id: "u1".into(),
                },
            )
            .await
            .unwrap()
            .presence
            .unwrap();
        assert_parked(&find(presence.devices, "phone"));

        let device = service
            .get_device(
                "acme",
                GetDeviceRequest {
                    user_id: "u1".into(),
                    device_id: "phone".into(),
                },
            )
            .await
            .unwrap()
            .device
            .unwrap();
        assert_parked(&device);
    }

    async fn seeded() -> (Arc<MemoryRepo>, UserService<MemoryRepo>) {
        let repo = Arc::new(MemoryRepo::default());
        for (t, u, d) in [
            ("acme", "u1", "d1"),
            ("acme", "u1", "d2"),
            ("acme", "u2", "d1"),
            ("other", "u9", "d1"),
        ] {
            repo.save_connection(&connection(t, u, d)).await.unwrap();
        }
        let service = UserService::new(repo.clone());
        (repo, service)
    }

    /// 一条吊销请求针对谁：`(租户, 用户, 设备, 原因)`；设备为 `None` 是全部设备。
    type Target = (String, String, Option<String>, BindingRevocationReason);

    fn target(
        tenant: &str,
        user: &str,
        device: Option<&str>,
        reason: BindingRevocationReason,
    ) -> Target {
        (
            tenant.to_string(),
            user.to_string(),
            device.map(str::to_string),
            reason,
        )
    }

    /// 记下提交的推送绑定吊销请求。
    #[derive(Default)]
    struct RecordedRevocations(Mutex<Vec<BindingRevocation>>);

    impl RecordedRevocations {
        /// 各条请求针对谁，按 (用户, 设备) 排序：踢租户按索引顺序走，顺序不定。
        fn targets(&self) -> Vec<Target> {
            let mut targets: Vec<Target> = self
                .0
                .lock()
                .unwrap()
                .iter()
                .map(|r| {
                    (
                        r.tenant_id.clone(),
                        r.user_id.clone(),
                        r.device_id.clone(),
                        r.reason,
                    )
                })
                .collect();
            targets.sort_by(|a, b| (&a.1, &a.2).cmp(&(&b.1, &b.2)));
            targets
        }

        /// 各条请求的登记时刻上限。
        fn cutoffs(&self) -> Vec<i64> {
            self.0
                .lock()
                .unwrap()
                .iter()
                .map(|r| r.registered_before_ms)
                .collect()
        }
    }

    impl BindingRevocationSink for RecordedRevocations {
        fn submit(&self, revocation: BindingRevocation) {
            self.0.lock().unwrap().push(revocation);
        }
    }

    fn now_ms() -> i64 {
        Utc::now().timestamp_millis()
    }

    /// [`seeded`] 的会话，外加接好的吊销出口。
    async fn seeded_with_revocations() -> (
        Arc<MemoryRepo>,
        UserService<MemoryRepo>,
        Arc<RecordedRevocations>,
    ) {
        let (repo, service) = seeded().await;
        let revocations = Arc::new(RecordedRevocations::default());
        let service = service
            .with_binding_revocations(Some(revocations.clone() as Arc<dyn BindingRevocationSink>));
        (repo, service, revocations)
    }

    fn kick(user: &str, device: &str) -> KickDeviceRequest {
        KickDeviceRequest {
            user_id: user.into(),
            device_id: device.into(),
            reason: "lost phone".into(),
        }
    }

    /// 踢设备（运维踢或用户踢自己的设备走的是同一个入口）：被踢设备的推送绑定吊销，别的设备、
    /// 别的租户不动。只吊销踢线那一刻之前登记的绑定。
    #[tokio::test]
    async fn a_kicked_device_loses_its_push_bindings() {
        let (_repo, service, revocations) = seeded_with_revocations().await;

        let before_ms = now_ms();
        assert!(
            service
                .kick_device(kick("u1", "d1"), "acme")
                .await
                .unwrap()
                .success
        );
        let after_ms = now_ms();

        assert_eq!(
            revocations.targets(),
            vec![target(
                "acme",
                "u1",
                Some("d1"),
                BindingRevocationReason::Kicked
            )]
        );
        let cutoffs = revocations.cutoffs();
        assert!(
            (before_ms..=after_ms).contains(&cutoffs[0]),
            "the cutoff {} is the moment of the kick ({before_ms}..={after_ms})",
            cutoffs[0]
        );
    }

    /// 点名踢的设备没有在线会话（已托管的手机）：调用方拿到的仍是「设备不存在」，但它的推送绑定
    /// 照样吊销，推送到此为止。
    #[tokio::test]
    async fn a_kick_of_a_device_without_a_live_session_still_revokes_its_bindings() {
        let (_repo, service, revocations) = seeded_with_revocations().await;

        let before_ms = now_ms();
        let missing = service
            .kick_device(kick("u1", "parked-phone"), "acme")
            .await;
        let after_ms = now_ms();

        assert_eq!(missing.unwrap_err().code(), Some(ErrorCode::UserNotFound));
        assert_eq!(
            revocations.targets(),
            vec![target(
                "acme",
                "u1",
                Some("parked-phone"),
                BindingRevocationReason::Kicked
            )]
        );
        assert!((before_ms..=after_ms).contains(&revocations.cutoffs()[0]));
    }

    /// 设备仍然在线的踢线不吊销绑定：同设备的新连接已经接替（没有被踢），或存储报错（会话还在，
    /// 调用方重试）。
    #[tokio::test]
    async fn a_kick_that_leaves_the_device_online_revokes_nothing() {
        let (repo, service, revocations) = seeded_with_revocations().await;

        *repo.kick_behaviour.lock().unwrap() = KickBehaviour::Replaced;
        assert!(
            !service
                .kick_device(kick("u1", "d1"), "acme")
                .await
                .unwrap()
                .success
        );

        *repo.kick_behaviour.lock().unwrap() = KickBehaviour::Unavailable;
        assert!(service.kick_device(kick("u1", "d1"), "acme").await.is_err());

        assert!(revocations.targets().is_empty());
    }

    /// 踢线已登记、网关还没确认：这次调用报错，但会话必然结束、结束时不再经过领域服务，所以
    /// 绑定现在就吊销。
    #[tokio::test]
    async fn a_kick_awaiting_gateway_confirmation_still_revokes_the_bindings() {
        let (repo, service, revocations) = seeded_with_revocations().await;
        *repo.kick_behaviour.lock().unwrap() = KickBehaviour::Pending;

        let error = service
            .kick_device(kick("u1", "d1"), "acme")
            .await
            .unwrap_err();

        assert!(is_kick_pending_confirmation(&error));
        assert_eq!(
            revocations.targets(),
            vec![target(
                "acme",
                "u1",
                Some("d1"),
                BindingRevocationReason::Kicked
            )]
        );
    }

    /// 踢整个租户：每条被移除的会话所属设备各吊销一次，原因是租户被踢；只剩索引的陈旧成员（没有
    /// 在线会话，也不是谁点名要踢的设备）与别的租户不吊销。
    #[tokio::test]
    async fn kicking_a_tenant_revokes_the_bindings_of_each_removed_device() {
        let (repo, service, revocations) = seeded_with_revocations().await;
        repo.seed_stale("acme", &tenant_session_member("ghost", "d1"));

        let response = service
            .kick_tenant(KickTenantRequest {
                tenant_id: "acme".to_string(),
                reason: "suspended".to_string(),
                expected_tenant_version: None,
            })
            .await
            .unwrap();

        assert_eq!(response.kicked, 3);
        let tenant_kicked = |user: &str, device: &str| {
            target(
                "acme",
                user,
                Some(device),
                BindingRevocationReason::TenantKicked,
            )
        };
        assert_eq!(
            revocations.targets(),
            vec![
                tenant_kicked("u1", "d1"),
                tenant_kicked("u1", "d2"),
                tenant_kicked("u2", "d1"),
            ]
        );
    }

    fn bound(tenant: &str, user: &str, device: &str, identity_session: &str) -> Connection {
        connection(tenant, user, device).with_identity_session_id(Some(identity_session.into()))
    }

    /// 身份吊销点名了会话：只吊销被这次吊销结束的在线会话所属的设备（一台设备挂几条连接也只报
    /// 一次），包括截止时刻之前登记、没有身份会话 ID 的连接；别的身份会话的设备不动。登记时刻的
    /// 上限是请求的截止时刻。
    #[tokio::test]
    async fn an_identity_revocation_with_named_sessions_revokes_the_devices_it_ends() {
        let repo = Arc::new(MemoryRepo::default());
        for session in [
            bound("acme", "u1", "phone", "sid-old"),
            bound("acme", "u1", "phone", "sid-old"),
            bound("acme", "u1", "tablet", "sid-older"),
            bound("acme", "u1", "desk", "sid-new"),
            connection("acme", "u1", "legacy"),
            bound("other", "u1", "phone-b", "sid-old"),
        ] {
            repo.save_connection(&session).await.unwrap();
        }
        let revocations = Arc::new(RecordedRevocations::default());
        let service = UserService::new(repo.clone())
            .with_binding_revocations(Some(revocations.clone() as Arc<dyn BindingRevocationSink>));
        let sessions = vec!["sid-old".to_string(), "sid-older".to_string()];
        let cutoff_ms = now_ms() + 1_000;

        let kicked = service
            .revoke_identity_sessions("acme", "u1", &sessions, cutoff_ms, "req-1", "legacy")
            .await
            .unwrap();

        assert_eq!(kicked, 4);
        let identity_revoked = |device: &str| {
            target(
                "acme",
                "u1",
                Some(device),
                BindingRevocationReason::IdentityRevoked,
            )
        };
        assert_eq!(
            revocations.targets(),
            vec![
                identity_revoked("legacy"),
                identity_revoked("phone"),
                identity_revoked("tablet"),
            ]
        );
        assert_eq!(revocations.cutoffs(), vec![cutoff_ms; 3]);
        // 没被吊销的身份会话还在线。
        let remaining = repo
            .get_user_connections("acme", &UserId::new("u1".into()).unwrap())
            .await
            .unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].device_id().as_str(), "desk");
    }

    /// 身份吊销没有点名会话：一次吊销该用户在本租户下全部设备的绑定，不逐台列。
    #[tokio::test]
    async fn an_identity_revocation_without_named_sessions_revokes_every_device() {
        let (_repo, service, revocations) = seeded_with_revocations().await;
        let cutoff_ms = 1_700_000_000_000;

        service
            .revoke_identity_sessions("acme", "u1", &[], cutoff_ms, "req-1", "legacy")
            .await
            .unwrap();

        assert_eq!(
            revocations.targets(),
            vec![target(
                "acme",
                "u1",
                None,
                BindingRevocationReason::IdentityRevoked
            )]
        );
        assert_eq!(revocations.cutoffs(), vec![cutoff_ms]);
    }

    /// 调用方重投同一次身份吊销（截止时刻不变）：再提交的吊销带的仍是原来的截止时刻，用户在那
    /// 之后重新登录登记的绑定不会被迟到的重投清掉。
    #[tokio::test]
    async fn a_redelivered_identity_revocation_keeps_its_original_cutoff() {
        let (_repo, service, revocations) = seeded_with_revocations().await;
        let cutoff_ms = now_ms() - 3_600_000;

        for _ in 0..2 {
            service
                .revoke_identity_sessions("acme", "u1", &[], cutoff_ms, "req-1", "legacy")
                .await
                .unwrap();
        }

        assert_eq!(revocations.cutoffs(), vec![cutoff_ms, cutoff_ms]);
    }

    /// 请求没带截止时刻：以收到请求的时刻为上限，不发不带上限的吊销。
    #[tokio::test]
    async fn an_identity_revocation_without_a_cutoff_uses_the_time_it_was_received() {
        let (_repo, service, revocations) = seeded_with_revocations().await;

        let before_ms = now_ms();
        service
            .revoke_identity_sessions("acme", "u1", &[], 0, "req-1", "legacy")
            .await
            .unwrap();
        let after_ms = now_ms();

        let cutoffs = revocations.cutoffs();
        assert_eq!(cutoffs.len(), 1);
        assert!((before_ms..=after_ms).contains(&cutoffs[0]));
    }

    /// 网关还没确认断开、仓储报错（调用方稍后重试）时，绑定照样吊销：身份侧的吊销已成事实，
    /// 重试时这些会话可能已经被后台移除，再也对不上设备。
    #[tokio::test]
    async fn an_identity_revocation_revokes_bindings_even_while_retirement_is_pending() {
        let repo = Arc::new(MemoryRepo::default());
        repo.save_connection(&bound("acme", "u1", "phone", "sid-old"))
            .await
            .unwrap();
        *repo.identity_revocation_pending.lock().unwrap() = true;
        let revocations = Arc::new(RecordedRevocations::default());
        let service = UserService::new(repo)
            .with_binding_revocations(Some(revocations.clone() as Arc<dyn BindingRevocationSink>));
        let cutoff_ms = now_ms();

        let outcome = service
            .revoke_identity_sessions(
                "acme",
                "u1",
                &["sid-old".to_string()],
                cutoff_ms,
                "req-1",
                "legacy",
            )
            .await;

        assert!(outcome.is_err());
        assert_eq!(
            revocations.targets(),
            vec![target(
                "acme",
                "u1",
                Some("phone"),
                BindingRevocationReason::IdentityRevoked
            )]
        );
        assert_eq!(revocations.cutoffs(), vec![cutoff_ms]);
    }

    /// 没接推送通道：踢线、踢租户、身份吊销照旧，身份吊销也不多读一次会话。
    #[tokio::test]
    async fn without_a_revocation_sink_nothing_changes() {
        let (repo, service) = seeded().await;

        assert!(
            service
                .kick_device(kick("u1", "d1"), "acme")
                .await
                .unwrap()
                .success
        );
        let missing = service.kick_device(kick("u1", "gone"), "acme").await;
        assert_eq!(missing.unwrap_err().code(), Some(ErrorCode::UserNotFound));
        let kicked = service
            .revoke_identity_sessions("acme", "u1", &[], i64::MAX, "req-1", "legacy")
            .await
            .unwrap();
        // 剩下的 u1/d2 没有身份会话 ID，按截止时刻被结束。
        assert_eq!(kicked, 1);
        assert!(
            repo.get_user_connections("acme", &UserId::new("u1".into()).unwrap())
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn operator_device_kick_tells_the_connection_a_typed_reason() {
        let (repo, service) = seeded().await;
        service
            .kick_device(
                KickDeviceRequest {
                    user_id: "u1".into(),
                    device_id: "d1".into(),
                    reason: "lost phone".into(),
                },
                "acme",
            )
            .await
            .unwrap();
        assert_eq!(
            *repo.kick_reasons.lock().unwrap(),
            vec![kick_reason::DEVICE_KICKED.to_string()]
        );
    }

    /// 文本日志的落点：`fmt` 订阅者写进这块共享缓冲。
    #[derive(Clone, Default)]
    struct LogBuffer(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for LogBuffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// admin-gateway 转来的原因是解码后的文本，可以含 `\n`、`\r`。用 tracing-subscriber 的文本
    /// 格式（`fmt`）记一次踢设备、一次踢租户：每个事件仍只占一行，原因以转义形式出现。
    #[tokio::test]
    async fn a_kick_reason_with_line_breaks_cannot_add_a_log_line() {
        let logs = LogBuffer::default();
        let writer = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .without_time()
            .with_max_level(tracing::Level::INFO)
            .finish();
        // 进程里只登记了一个调度器时，tracing 按触发线程自己的默认调度器缓存 callsite 兴趣：别的
        // 测试线程先触发 "tenant kicked" 就会把它缓存成 never，这里便收不到。多登记一个空调度器，
        // 兴趣改按全部调度器计算，再整体重算一次。
        let _second = tracing::Dispatch::new(tracing_subscriber::registry());
        let guard = tracing::subscriber::set_default(subscriber);
        tracing::callsite::rebuild_interest_cache();

        let forged = "清理\nINFO forged line\r\nWARN x";
        let (repo, service) = seeded().await;
        service
            .kick_device(
                KickDeviceRequest {
                    user_id: "u1".into(),
                    device_id: "d1".into(),
                    reason: forged.into(),
                },
                "acme",
            )
            .await
            .unwrap();
        service
            .kick_tenant(KickTenantRequest {
                tenant_id: "acme".to_string(),
                reason: forged.to_string(),
                expected_tenant_version: None,
            })
            .await
            .unwrap();
        drop(guard);

        let output = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
        assert!(!output.contains('\r'), "{output}");
        let lines: Vec<&str> = output.lines().collect();
        // kick_device 一行；kick_tenant 踢剩下的两台设备各一行，再加汇总一行。
        assert_eq!(lines.len(), 4, "{output}");
        assert_eq!(
            lines.iter().filter(|l| l.contains("device kicked")).count(),
            3,
            "{output}"
        );
        assert_eq!(
            lines.iter().filter(|l| l.contains("tenant kicked")).count(),
            1,
            "{output}"
        );
        let escaped = "reason=清理\\nINFO forged line\\r\\nWARN x";
        assert!(lines.iter().all(|l| l.contains(escaped)), "{output}");
        // 连接端收到的仍是类型化原因，与请求里的原因无关。
        assert!(
            repo.kick_reasons
                .lock()
                .unwrap()
                .iter()
                .all(|r| r == kick_reason::DEVICE_KICKED || r == kick_reason::TENANT_UNAVAILABLE)
        );
    }

    #[tokio::test]
    async fn kick_device_cannot_remove_another_tenants_session() {
        let (repo, service) = seeded().await;
        let result = service
            .kick_device(
                KickDeviceRequest {
                    user_id: "u9".into(),
                    device_id: "d1".into(),
                    reason: "test".into(),
                },
                "acme",
            )
            .await;
        assert_eq!(result.unwrap_err().code(), Some(ErrorCode::UserNotFound));
        assert_eq!(
            repo.members("other"),
            vec![tenant_session_member("u9", "d1")]
        );
    }

    #[tokio::test]
    async fn same_user_and_device_are_isolated_between_tenants() {
        let repo = Arc::new(MemoryRepo::default());
        repo.save_connection(&connection("acme", "same", "d1"))
            .await
            .unwrap();
        repo.save_connection(&connection("other", "same", "d1"))
            .await
            .unwrap();
        let service = UserService::new(repo.clone());
        service
            .kick_device(
                KickDeviceRequest {
                    user_id: "same".into(),
                    device_id: "d1".into(),
                    reason: "self".into(),
                },
                "acme",
            )
            .await
            .unwrap();
        assert!(
            repo.get_user_connections("acme", &UserId::new("same".into()).unwrap())
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            repo.get_user_connections("other", &UserId::new("same".into()).unwrap())
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(
            repo.kicks
                .lock()
                .unwrap()
                .iter()
                .all(|(tenant, _, _)| tenant == "acme")
        );
    }

    #[tokio::test]
    async fn kick_tenant_removes_every_session_of_that_tenant_only() {
        let (repo, service) = seeded().await;

        let response = service
            .kick_tenant(KickTenantRequest {
                tenant_id: "acme".to_string(),
                reason: "suspended".to_string(),
                expected_tenant_version: None,
            })
            .await
            .unwrap();

        assert_eq!(response.kicked, 3);
        assert!(repo.members("acme").is_empty(), "index emptied");
        assert!(
            repo.get_user_connections("acme", &UserId::new("u1".into()).unwrap())
                .await
                .unwrap()
                .is_empty()
        );
        // 其他租户不受影响。
        assert_eq!(
            repo.members("other"),
            vec![tenant_session_member("u9", "d1")]
        );
        assert_eq!(
            repo.get_user_connections("other", &UserId::new("u9".into()).unwrap())
                .await
                .unwrap()
                .len(),
            1
        );
        // 连接端收到的是类型化错误码，不是请求里的运维说明。
        let reasons = repo.kick_reasons.lock().unwrap().clone();
        assert!(!reasons.is_empty());
        assert!(
            reasons.iter().all(|r| r == kick_reason::TENANT_UNAVAILABLE),
            "{reasons:?}"
        );
        // Only device/physical-session kicks: never a user-wide reconnect-unsafe fallback.
        let mut kicks = repo.kicks.lock().unwrap().clone();
        kicks.sort();
        assert!(kicks.iter().all(|(tenant, _, _)| tenant == "acme"));
        assert_eq!(kicks.len(), 3);
        assert!(kicks.iter().all(|(_, _, device)| device.is_some()));
    }

    #[tokio::test]
    async fn kick_tenant_drops_stale_index_members_without_counting_them() {
        let (repo, service) = seeded().await;
        repo.seed_stale("acme", "ghost:d1");
        repo.seed_stale("acme", "malformed");

        let response = service
            .kick_tenant(KickTenantRequest {
                tenant_id: "acme".to_string(),
                reason: String::new(),
                expected_tenant_version: None,
            })
            .await
            .unwrap();

        assert_eq!(response.kicked, 3);
        assert!(repo.members("acme").is_empty());
        assert!(
            !repo
                .kicks
                .lock()
                .unwrap()
                .iter()
                .any(|(_, user, _)| user == "ghost")
        );
    }

    #[tokio::test]
    async fn kick_tenant_pages_through_the_index() {
        let repo = Arc::new(MemoryRepo::default());
        for i in 0..(KICK_TENANT_SCAN_PAGE * 2 + 5) {
            repo.save_connection(&connection("big", &format!("u{i}"), "d"))
                .await
                .unwrap();
        }
        let service = UserService::new(repo.clone());
        let response = service
            .kick_tenant(KickTenantRequest {
                tenant_id: "big".to_string(),
                reason: String::new(),
                expected_tenant_version: None,
            })
            .await
            .unwrap();
        assert_eq!(response.kicked as usize, KICK_TENANT_SCAN_PAGE * 2 + 5);
        assert!(
            repo.page_size_seen.lock().unwrap().len() >= 3,
            "at least three SSCAN pages"
        );
        assert!(repo.members("big").is_empty());
    }

    #[tokio::test]
    async fn kick_tenant_normalizes_and_validates_tenant_id() {
        let (repo, service) = seeded().await;
        repo.save_connection(&connection("0", "u0", "d0"))
            .await
            .unwrap();

        let response = service
            .kick_tenant(KickTenantRequest {
                tenant_id: "default".to_string(),
                reason: String::new(),
                expected_tenant_version: None,
            })
            .await
            .unwrap();
        assert_eq!(response.kicked, 1);

        let err = service
            .kick_tenant(KickTenantRequest {
                tenant_id: "*".to_string(),
                reason: String::new(),
                expected_tenant_version: None,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Some(ErrorCode::InvalidParameter));
    }
}
