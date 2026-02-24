//! 在线状态领域服务 - 包含所有业务逻辑实现

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use flare_proto::signaling::online::{
    DeviceConflictStrategy, GetOnlineStatusResponse, HeartbeatResponse, LoginRequest,
    LoginResponse, LogoutRequest, LogoutResponse, OnlineStatus,
};
use prost_types::Timestamp;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::domain::aggregate::{Connection, ConnectionCreateParams};
use crate::domain::model::OnlineStatusRecord;
use crate::domain::repository::ConversationRepository;
use crate::domain::value_object::{
    ConnectionQuality, DeviceId, DevicePriority, ConnectionId, TokenVersion, UserId,
};
use crate::util;

#[derive(Debug, Clone)]
struct InMemoryConnection {
    session: Connection,
}

/// 在线状态领域服务 - 包含所有业务逻辑
///
/// 注意：领域服务不依赖基础设施层的配置，配置由应用层传入必要参数
pub struct OnlineStatusService {
    repository: Arc<dyn ConversationRepository + Send + Sync>,
    sessions: Arc<RwLock<HashMap<String, InMemoryConnection>>>,
    gateway_id: String,
}

impl OnlineStatusService {
    pub fn new(repository: Arc<dyn ConversationRepository + Send + Sync>, gateway_id: String) -> Self {
        Self {
            repository,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            gateway_id,
        }
    }

    pub async fn login(&self, request: LoginRequest) -> Result<LoginResponse> {
        let user_id = &request.user_id;
        let device_id = &request.device_id;
        let device_platform = request.device_platform.as_str();
        let desired_strategy = request.desired_conflict_strategy();
        let applied_strategy = desired_strategy;

        // 检查现有会话
        let user_vo = UserId::new(user_id.clone()).unwrap();
        let existing_sessions = self.repository.get_user_connections(&user_vo).await?;

        // 根据冲突策略处理现有会话
        if !existing_sessions.is_empty() {
            match applied_strategy {
                DeviceConflictStrategy::Exclusive => {
                    // 互斥：踢出所有旧设备
                    info!(
                        user_id = %user_id,
                        device_id = %device_id,
                        "Exclusive strategy: removing all existing sessions"
                    );
                    self.repository.remove_user_connections(&user_vo, None).await?;
                }
                DeviceConflictStrategy::PlatformExclusive => {
                    // 平台互斥：只踢出同平台的旧设备
                    let same_platform_devices: Vec<DeviceId> = existing_sessions
                        .iter()
                        .filter(|s| s.device_platform() == device_platform)
                        .map(|s| s.device_id().clone())
                        .collect();
                    if !same_platform_devices.is_empty() {
                        info!(
                            user_id = %user_id,
                            device_id = %device_id,
                            platform = %device_platform,
                            "Platform exclusive strategy: removing same platform devices"
                        );
                        self.repository
                            .remove_user_connections(&user_vo, Some(&same_platform_devices))
                            .await?;
                    }
                }
                DeviceConflictStrategy::Coexist => {
                    // 共存：允许多设备同时在线
                    info!(
                        user_id = %user_id,
                        device_id = %device_id,
                        "Coexist strategy: allowing multiple devices"
                    );
                }
                _ => {
                    // 未指定策略，默认使用互斥
                    warn!(
                        user_id = %user_id,
                        "No conflict strategy specified, using Exclusive"
                    );
                    self.repository.remove_user_connections(&user_vo, None).await?;
                }
            }
        }

        // 从 metadata 中提取 gateway_id（用于跨地区路由）
        // 如果 metadata 中没有 gateway_id，使用配置的默认值
        let gateway_id = request
            .metadata
            .get("gateway_id")
            .map(|s| s.clone())
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
        let user_vo = UserId::new(user_id.clone()).unwrap();
        let device_vo = DeviceId::new(device_id.clone()).unwrap();
        let priority_vo = DevicePriority::from_i32(device_priority);
        let token_vo = TokenVersion::from(token_version);
        let params = ConnectionCreateParams {
            user_id: user_vo.clone(),
            device_id: device_vo.clone(),
            device_platform: device_platform.to_string(),
            server_id: request.server_id.clone(),
            gateway_id: gateway_id.clone(),
            device_priority: priority_vo,
            token_version: token_vo,
            initial_quality: connection_quality.clone(),
        };
        let session = Connection::create(params);
        let conversation_id = session.id().as_str().to_string();

        {
            let mut map = self.sessions.write().await;
            map.insert(
                conversation_id.clone(),
                InMemoryConnection {
                    session: session.clone(),
                },
            );
        }

        self.repository.save_connection(&session).await?;

        info!(
            user_id = %user_id,
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
            status: util::rpc_status_ok(),
            applied_conflict_strategy: applied_strategy as i32,
        })
    }

    pub async fn logout(&self, request: LogoutRequest) -> Result<LogoutResponse> {
        let user_id = &request.user_id;
        let conversation_id = &request.conversation_id;

        // 从内存中移除会话
        {
            let mut map = self.sessions.write().await;
            map.remove(conversation_id);
        }

        // 从Redis中移除会话
        let user_vo = UserId::new(user_id.clone()).unwrap();
        let session_vo = ConnectionId::from_string(conversation_id.clone()).unwrap();
        self.repository
            .remove_connection(&session_vo, &user_vo)
            .await?;

        info!(
            user_id = %user_id,
            conversation_id = %conversation_id,
            "User logged out successfully"
        );

        Ok(LogoutResponse {
            success: true,
            status: util::rpc_status_ok(),
        })
    }

    pub async fn heartbeat(
        &self,
        conversation_id: &str,
        user_id: &str,
        connection_quality: Option<&flare_proto::common::ConnectionQuality>,
    ) -> Result<HeartbeatResponse> {
        // 检查会话是否存在
        {
            let map = self.sessions.read().await;
            if !map.contains_key(conversation_id) {
                return Ok(HeartbeatResponse {
                    success: false,
                    status: util::rpc_status_error(
                        flare_server_core::error::ErrorCode::InvalidParameter,
                        "Connection not found",
                    ),
                });
            }
        }

        // 更新内存中的last_seen和链接质量
        {
            let mut map = self.sessions.write().await;
            if let Some(session) = map.get_mut(conversation_id) {
                // 刷新心跳（含质量）
                let quality_opt =
                    connection_quality.and_then(|q| ConnectionQuality::from_proto(q).ok());
                session
                    .session
                    .refresh_heartbeat(quality_opt)
                    .map_err(|e| anyhow::anyhow!(e))?;
            }
        }

        // 更新Redis中的会话TTL
        let user_vo = UserId::new(user_id.to_string()).unwrap();
        self.repository.touch_connection(&user_vo).await?;

        Ok(HeartbeatResponse {
            success: true,
            status: util::rpc_status_ok(),
        })
    }

    pub async fn get_online_status(&self, user_ids: &[String]) -> Result<GetOnlineStatusResponse> {
        let statuses = self.repository.fetch_statuses(user_ids).await?;

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

        Ok(GetOnlineStatusResponse {
            statuses: result,
            status: util::rpc_status_ok(),
        })
    }
}
