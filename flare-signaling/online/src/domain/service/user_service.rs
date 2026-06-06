//! 用户领域服务 - 包含所有业务逻辑实现

use std::sync::Arc;

use chrono::Utc;
use flare_grpc_proto::signaling::online::{
    BatchGetUserPresenceRequest, BatchGetUserPresenceResponse, DeviceInfo, GetDeviceRequest,
    GetDeviceResponse, GetUserPresenceRequest, GetUserPresenceResponse, KickDeviceRequest,
    KickDeviceResponse, ListUserDevicesRequest, ListUserDevicesResponse, UserPresence,
};
use flare_server_core::error::{ErrorCode, Result, map_infra_error};
use flare_server_core::flare_err;
use prost_types::Timestamp;
use tracing::{info, warn};

use crate::domain::repository::ConversationRepository;
use crate::domain::value_object::UserId;

const ONLINE_SESSION_FRESHNESS_SECONDS: i64 = 90;

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
