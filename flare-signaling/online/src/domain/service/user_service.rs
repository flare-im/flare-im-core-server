//! 用户领域服务 - 包含所有业务逻辑实现

use std::sync::Arc;

use chrono::Utc;
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

use crate::domain::repository::{ConversationRepository, parse_tenant_session_member};
use crate::domain::value_object::UserId;

const ONLINE_SESSION_FRESHNESS_SECONDS: i64 = 90;
/// `KickTenant` 每页 SSCAN 的成员数。
const KICK_TENANT_SCAN_PAGE: usize = 256;
/// 踢出原因缺省值（原样下发给连接端）。
const KICK_TENANT_DEFAULT_REASON: &str = "TENANT_UNAVAILABLE";

/// 用户领域服务 - 包含所有业务逻辑（泛型仓储，避免 `dyn` 异步 trait）
pub struct UserService<R: ConversationRepository + Send + Sync> {
    conversation_repository: Arc<R>,
}

impl<R: ConversationRepository + Send + Sync> UserService<R> {
    pub fn new(conversation_repository: Arc<R>) -> Self {
        Self {
            conversation_repository,
        }
    }

    /// 查询用户在线状态
    pub async fn get_user_presence(
        &self,
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
            .get_user_connections(&user_id_vo)
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
            devices: sessions
                .into_iter()
                .map(|s| DeviceInfo {
                    device_id: s.device_id().as_str().to_string(),
                    platform: s.device_platform().to_string(),
                    model: String::new(),
                    os_version: String::new(),
                    last_active_time: Some(Timestamp {
                        seconds: s.last_heartbeat_at().timestamp(),
                        nanos: s.last_heartbeat_at().timestamp_subsec_nanos() as i32,
                    }),
                    priority: s.device_priority() as i32,
                    token_version: s.token_version().value(),
                    connection_quality: s.connection_quality().cloned().map(|cq| cq.into()),
                    conversation_id: s.id().as_str().to_string(),
                    gateway_id: s.gateway_id().to_string(),
                    server_id: s.server_id().to_string(),
                })
                .collect(),
            last_seen: Some(Timestamp {
                seconds: last_seen.timestamp(),
                nanos: last_seen.timestamp_subsec_nanos() as i32,
            }),
        };

        Ok(GetUserPresenceResponse {
            presence: Some(presence),
        })
    }

    /// 批量查询在线状态
    pub async fn batch_get_user_presence(
        &self,
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
                .get_user_presence(GetUserPresenceRequest {
                    user_id: user_id.clone(),
                })
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
        ctx: &flare_server_core::context::Context,
        request: ListUserDevicesRequest,
    ) -> Result<ListUserDevicesResponse> {
        // Router / push-worker 等服务间调用会在 proto 中携带目标 user_id；上下文里的 user_id 常为调用方而非被查询用户。
        let user_id = if !request.user_id.is_empty() {
            request.user_id
        } else {
            ctx.user_id()
                .ok_or_else(|| {
                    flare_server_core::error::FlareError::system(
                        "user_id is required in ListUserDevicesRequest or context".to_string(),
                    )
                })?
                .to_string()
        };

        let user_id_vo = crate::domain::value_object::UserId::new(user_id)
            .map_err(|e| flare_server_core::error::FlareError::system((e).to_string()))?;
        let sessions = self
            .conversation_repository
            .get_user_connections(&user_id_vo)
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
            devices: sessions
                .into_iter()
                .map(|s| DeviceInfo {
                    device_id: s.device_id().as_str().to_string(),
                    platform: s.device_platform().to_string(),
                    model: String::new(),
                    os_version: String::new(),
                    last_active_time: Some(Timestamp {
                        seconds: s.last_heartbeat_at().timestamp(),
                        nanos: s.last_heartbeat_at().timestamp_subsec_nanos() as i32,
                    }),
                    priority: s.device_priority() as i32,
                    token_version: s.token_version().value(),
                    connection_quality: s.connection_quality().cloned().map(|cq| cq.into()),
                    conversation_id: s.id().as_str().to_string(),
                    gateway_id: s.gateway_id().to_string(),
                    server_id: s.server_id().to_string(),
                })
                .collect(),
        })
    }

    /// 踢出设备
    pub async fn kick_device(&self, request: KickDeviceRequest) -> Result<KickDeviceResponse> {
        let user_id = &request.user_id;
        let device_id = &request.device_id;

        // 查找设备对应的会话
        let user_vo = crate::domain::value_object::UserId::new(user_id.to_string())
            .map_err(|e| flare_server_core::error::FlareError::system((e).to_string()))?;
        let device_vo = crate::domain::value_object::DeviceId::new(device_id.to_string())
            .map_err(|e| flare_server_core::error::FlareError::system((e).to_string()))?;
        let session = self
            .conversation_repository
            .get_connection_by_device(&user_vo, &device_vo)
            .await?;

        if let Some(session) = session {
            // 删除会话
            self.conversation_repository
                .remove_connection(session.id(), &user_vo)
                .await?;

            info!(
                user_id = %user_id,
                device_id = %device_id,
                conversation_id = %session.id().as_str(),
                "device kicked"
            );

            Ok(KickDeviceResponse { success: true })
        } else {
            Err(flare_err!(ErrorCode::UserNotFound, "device not found"))
        }
    }

    /// 踢出整个租户：按 `tenant:sessions:{tenant}` 索引分页 SSCAN，每个成员复用 `kick_device`；
    /// 索引里已无会话的陈旧成员顺手清掉；最后按用户去重通知网关关 socket。
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
        let mut kicked_users = std::collections::BTreeSet::new();
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
                    .kick_device(KickDeviceRequest {
                        user_id: user_id.to_string(),
                        device_id: device_id.to_string(),
                        reason: reason.to_string(),
                    })
                    .await
                {
                    Ok(_) => {
                        kicked = kicked.saturating_add(1);
                        kicked_users.insert(user_id.to_string());
                    }
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

        for user_id in &kicked_users {
            if let Err(error) = self
                .conversation_repository
                .broadcast_user_kick(user_id)
                .await
            {
                warn!(tenant_id = %tenant_id, user_id = %user_id, %error, "kick broadcast failed");
            }
        }

        info!(
            tenant_id = %tenant_id,
            reason = %reason,
            kicked,
            stale,
            failed,
            users = kicked_users.len(),
            "tenant kicked"
        );
        Ok(KickTenantResponse { kicked })
    }

    pub async fn get_device(
        &self,
        ctx: &flare_server_core::context::Context,
        request: GetDeviceRequest,
    ) -> Result<GetDeviceResponse> {
        let user_id = ctx.user_id().ok_or_else(|| {
            flare_server_core::error::FlareError::system(
                "user_id is required in context".to_string(),
            )
        })?;
        let device_id = &request.device_id;

        let user_id_vo = crate::domain::value_object::UserId::new(user_id.to_string())
            .map_err(|e| flare_server_core::error::FlareError::system((e).to_string()))?;
        let device_vo = crate::domain::value_object::DeviceId::new(device_id.to_string())
            .map_err(|e| flare_server_core::error::FlareError::system((e).to_string()))?;
        let session = self
            .conversation_repository
            .get_connection_by_device(&user_id_vo, &device_vo)
            .await?;

        if let Some(session) = session {
            Ok(GetDeviceResponse {
                device: Some(DeviceInfo {
                    device_id: session.device_id().as_str().to_string(),
                    platform: session.device_platform().to_string(),
                    model: String::new(),
                    os_version: String::new(),
                    last_active_time: Some(Timestamp {
                        seconds: session.last_heartbeat_at().timestamp(),
                        nanos: session.last_heartbeat_at().timestamp_subsec_nanos() as i32,
                    }),
                    priority: session.device_priority() as i32,
                    token_version: session.token_version().value(),
                    connection_quality: session.connection_quality().cloned().map(|cq| cq.into()),
                    conversation_id: session.id().as_str().to_string(),
                    gateway_id: session.gateway_id().to_string(),
                    server_id: session.server_id().to_string(),
                }),
            })
        } else {
            Err(flare_err!(ErrorCode::InvalidParameter, "device not found"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::aggregate::{Connection, ConnectionCreateParams};
    use crate::domain::model::OnlineStatusRecord;
    use crate::domain::repository::tenant_session_member;
    use crate::domain::value_object::{ConnectionId, DeviceId, DevicePriority, TokenVersion};
    use std::collections::{BTreeSet, HashMap};
    use std::sync::Mutex;

    /// 内存版会话存储：复刻 Redis 实现的索引维护规则（save 加成员、remove 删成员）。
    #[derive(Default)]
    struct MemoryRepo {
        sessions: Mutex<HashMap<String, Vec<Connection>>>,
        index: Mutex<HashMap<String, BTreeSet<String>>>,
        /// 成员插入顺序日志：SSCAN 的游标不随删除位移，这里用它模拟。
        index_log: Mutex<HashMap<String, Vec<String>>>,
        kicks: Mutex<Vec<String>>,
        page_size_seen: Mutex<Vec<usize>>,
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
                .entry(connection.user_id().as_str().to_string())
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

        async fn remove_connection(
            &self,
            conversation_id: &ConnectionId,
            user_id: &UserId,
        ) -> Result<()> {
            let removed = {
                let mut sessions = self.sessions.lock().unwrap();
                let list = sessions.entry(user_id.as_str().to_string()).or_default();
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

        async fn touch_connection(&self, _: &ConnectionId, _: &UserId) -> Result<()> {
            Ok(())
        }

        async fn fetch_statuses(
            &self,
            _user_ids: &[String],
        ) -> Result<HashMap<String, OnlineStatusRecord>> {
            Ok(HashMap::new())
        }

        async fn get_user_connections(&self, user_id: &UserId) -> Result<Vec<Connection>> {
            Ok(self
                .sessions
                .lock()
                .unwrap()
                .get(user_id.as_str())
                .cloned()
                .unwrap_or_default())
        }

        async fn remove_user_connections(
            &self,
            _user_id: &UserId,
            _device_ids: Option<&[DeviceId]>,
        ) -> Result<()> {
            unreachable!("not used by kick_tenant")
        }

        async fn get_connection_by_device(
            &self,
            user_id: &UserId,
            device_id: &DeviceId,
        ) -> Result<Option<Connection>> {
            Ok(self
                .get_user_connections(user_id)
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

        async fn broadcast_user_kick(&self, user_id: &str) -> Result<()> {
            self.kicks.lock().unwrap().push(user_id.to_string());
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
        })
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

    #[tokio::test]
    async fn kick_tenant_removes_every_session_of_that_tenant_only() {
        let (repo, service) = seeded().await;

        let response = service
            .kick_tenant(KickTenantRequest {
                tenant_id: "acme".to_string(),
                reason: "suspended".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(response.kicked, 3);
        assert!(repo.members("acme").is_empty(), "index emptied");
        assert!(
            repo.get_user_connections(&UserId::new("u1".into()).unwrap())
                .await
                .unwrap()
                .is_empty()
        );
        // 其他租户不受影响。
        assert_eq!(repo.members("other"), vec!["u9:d1".to_string()]);
        assert_eq!(
            repo.get_user_connections(&UserId::new("u9".into()).unwrap())
                .await
                .unwrap()
                .len(),
            1
        );
        // 网关 kick 通知按用户去重。
        let mut kicks = repo.kicks.lock().unwrap().clone();
        kicks.sort();
        assert_eq!(kicks, vec!["u1".to_string(), "u2".to_string()]);
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
            })
            .await
            .unwrap();

        assert_eq!(response.kicked, 3);
        assert!(repo.members("acme").is_empty());
        assert!(!repo.kicks.lock().unwrap().contains(&"ghost".to_string()));
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
            })
            .await
            .unwrap();
        assert_eq!(response.kicked, 1);

        let err = service
            .kick_tenant(KickTenantRequest {
                tenant_id: "*".to_string(),
                reason: String::new(),
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Some(ErrorCode::InvalidParameter));
    }
}
