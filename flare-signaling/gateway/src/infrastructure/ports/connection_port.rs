//! [`IConnectionPort`] 基础设施实现（与 `domain/ports/connection_port.rs` 对应）
//!
//! - RPC（login/logout/heartbeat/get_online_status）：服务发现连接 Signaling Online。
//! - 本地连接信息：从 `ConnectionManager` 读取并映射为领域 `ConnectionInfo`。

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use flare_core::server::connection::{ConnectionManagerTrait, TraitConnectionInfo};
use flare_grpc_proto::signaling::online::online_service_client::OnlineServiceClient;
use flare_grpc_proto::signaling::online::{
    GetOnlineStatusRequest, GetOnlineStatusResponse, HeartbeatRequest, HeartbeatResponse,
    LoginRequest, LoginResponse, LogoutRequest, LogoutResponse,
};
use flare_im_core::ServiceClient;
use flare_im_core::service_names::{SIGNALING_ONLINE, get_service_name};
use flare_server_core::error::{ErrorBuilder, ErrorCode, InfraResult, InfraResultExt, Result};
use tokio::sync::Mutex;
use tonic::transport::Channel;

use flare_im_core::Ctx;

use crate::constants::METADATA_KEY_TENANT_ID;
use crate::domain::model::ConnectionInfo as DomainConnectionInfo;
use crate::domain::ports::IConnectionPort;

pub struct ConnectionRepository {
    connection_manager: Arc<dyn ConnectionManagerTrait>,
    service_name: String,
    service_client: Mutex<Option<ServiceClient>>,
    client: Mutex<Option<OnlineServiceClient<Channel>>>,
    default_tenant_id: String,
}

impl ConnectionRepository {
    pub fn new(
        connection_manager: Arc<dyn ConnectionManagerTrait>,
        default_tenant_id: String,
    ) -> Self {
        let service_name = get_service_name(SIGNALING_ONLINE);
        Self {
            connection_manager,
            service_name,
            service_client: Mutex::new(None),
            client: Mutex::new(None),
            default_tenant_id,
        }
    }

    pub fn with_service_client(
        connection_manager: Arc<dyn ConnectionManagerTrait>,
        default_tenant_id: String,
        service_client: ServiceClient,
    ) -> Self {
        let service_name = get_service_name(SIGNALING_ONLINE);
        Self {
            connection_manager,
            service_name,
            service_client: Mutex::new(Some(service_client)),
            client: Mutex::new(None),
            default_tenant_id,
        }
    }

    async fn ensure_client(&self) -> InfraResult<OnlineServiceClient<Channel>> {
        let mut guard = self.client.lock().await;
        if let Some(client) = guard.as_ref() {
            return Ok(client.clone());
        }

        let mut service_client_guard = self.service_client.lock().await;
        if service_client_guard.is_none() {
            let discover = flare_im_core::discovery::create_discover(&self.service_name)
                .await
                .map_err(|e| {
                    ErrorBuilder::new(
                        ErrorCode::ServiceUnavailable,
                        "signaling online service unavailable",
                    )
                    .details(format!(
                        "Failed to create service discover for {}: {}",
                        self.service_name, e
                    ))
                    .build_error()
                })?;

            if let Some(discover) = discover {
                *service_client_guard = Some(ServiceClient::new(discover));
            } else {
                return Err(anyhow::anyhow!("Service discovery not configured"));
            }
        }

        let service_client = service_client_guard
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Service client not initialized"))?;
        let channel = service_client.get_channel().await.map_err(|e| {
            ErrorBuilder::new(
                ErrorCode::ServiceUnavailable,
                "signaling online unavailable",
            )
            .details(format!("Failed to get channel: {}", e))
            .build_error()
        })?;

        tracing::trace!(
            service_name = %self.service_name,
            "ConnectionRepository: got channel from service discovery"
        );

        let client = OnlineServiceClient::new(channel);
        *guard = Some(client.clone());
        Ok(client)
    }
}

#[async_trait]
impl IConnectionPort for ConnectionRepository {
    async fn login(&self, request: LoginRequest) -> Result<LoginResponse> {
        let mut client = self.ensure_client().await.into_flare(
            ErrorCode::ServiceUnavailable,
            "failed to connect signaling online service",
        )?;
        client
            .login(request)
            .await
            .map(|resp| resp.into_inner())
            .map_err(|status| {
                ErrorBuilder::new(ErrorCode::ServiceUnavailable, "signaling login failed")
                    .details(status.to_string())
                    .build_error()
            })
    }

    async fn logout(&self, request: LogoutRequest) -> Result<LogoutResponse> {
        let mut client = self.ensure_client().await.into_flare(
            ErrorCode::ServiceUnavailable,
            "failed to connect signaling online service",
        )?;
        client
            .logout(request)
            .await
            .map(|resp| resp.into_inner())
            .map_err(|status| {
                ErrorBuilder::new(ErrorCode::ServiceUnavailable, "signaling logout failed")
                    .details(status.to_string())
                    .build_error()
            })
    }

    async fn heartbeat(&self, request: HeartbeatRequest) -> Result<HeartbeatResponse> {
        let mut client = self.ensure_client().await.into_flare(
            ErrorCode::ServiceUnavailable,
            "failed to connect signaling online service",
        )?;
        client
            .heartbeat(request)
            .await
            .map(|resp| resp.into_inner())
            .map_err(|status| {
                ErrorBuilder::new(ErrorCode::ServiceUnavailable, "signaling heartbeat failed")
                    .details(status.to_string())
                    .build_error()
            })
    }

    async fn get_online_status(
        &self,
        request: GetOnlineStatusRequest,
    ) -> Result<GetOnlineStatusResponse> {
        let mut client = self.ensure_client().await.into_flare(
            ErrorCode::ServiceUnavailable,
            "failed to connect signaling online service",
        )?;
        client
            .get_online_status(request)
            .await
            .map(|resp| resp.into_inner())
            .map_err(|status| {
                ErrorBuilder::new(
                    ErrorCode::ServiceUnavailable,
                    "signaling get_online_status failed",
                )
                .details(status.to_string())
                .build_error()
            })
    }

    async fn list_user_connections(&self, user_id: &str) -> Result<Vec<DomainConnectionInfo>> {
        let connection_ids = self.connection_manager.get_user_connections(user_id).await;
        let mut list = Vec::with_capacity(connection_ids.len());
        for connection_id in connection_ids {
            if let Some((_, core_info)) =
                self.connection_manager.get_connection(&connection_id).await
            {
                list.push(core_info_to_domain(
                    connection_id,
                    &core_info,
                    &self.default_tenant_id,
                ));
            }
        }
        Ok(list)
    }

    async fn get_connection_info(&self, connection_id: &str) -> Result<DomainConnectionInfo> {
        let (_, core_info) = self
            .connection_manager
            .get_connection(connection_id)
            .await
            .ok_or_else(|| {
                ErrorBuilder::new(ErrorCode::InvalidParameter, "connection not found")
                    .details(format!("connection_id={}", connection_id))
                    .build_error()
            })?;
        Ok(core_info_to_domain(
            connection_id.to_string(),
            &core_info,
            &self.default_tenant_id,
        ))
    }

    async fn get_connection_metadata(
        &self,
        connection_id: &str,
    ) -> Result<HashMap<String, String>> {
        let (_, core_info) = self
            .connection_manager
            .get_connection(connection_id)
            .await
            .ok_or_else(|| {
                ErrorBuilder::new(ErrorCode::InvalidParameter, "connection not found")
                    .details(format!("connection_id={}", connection_id))
                    .build_error()
            })?;
        Ok(core_info.metadata)
    }

    async fn build_ctx(&self, connection_id: &str) -> Result<Ctx> {
        let connection_info = self.get_connection_info(connection_id).await?;
        Ok(super::context_resolver::build_gateway_ctx_from_info(
            &connection_info,
            &self.default_tenant_id,
        ))
    }
}

/// 将 flare_core trait 侧 `ConnectionInfo`（[`TraitConnectionInfo`]）转为领域 [`DomainConnectionInfo`]
pub(crate) fn core_info_to_domain(
    connection_id: String,
    core_info: &TraitConnectionInfo,
    default_tenant_id: &str,
) -> DomainConnectionInfo {
    let user_id = core_info.user_id.clone().unwrap_or_else(String::new);
    let device_id = core_info
        .device_info
        .as_ref()
        .map(|d| d.device_id.clone())
        .unwrap_or_else(|| "unknown".to_string());
    let platform = core_info
        .device_info
        .as_ref()
        .map(|d| d.platform.as_str().to_string());
    let mut metadata = core_info.metadata.clone();
    if let Some(device_info) = core_info.device_info.as_ref() {
        if let Some(app_version) = device_info.app_version.as_ref()
            && !app_version.trim().is_empty()
        {
            metadata.insert("app_version".to_string(), app_version.clone());
        }
        if let Some(system_version) = device_info.system_version.as_ref()
            && !system_version.trim().is_empty()
        {
            metadata.insert("system_version".to_string(), system_version.clone());
        }
        if let Some(model) = device_info.model.as_ref()
            && !model.trim().is_empty()
        {
            metadata.insert("model".to_string(), model.clone());
        }
    }
    let tenant_id = core_info
        .metadata
        .get(METADATA_KEY_TENANT_ID)
        .cloned()
        .unwrap_or_else(|| default_tenant_id.to_string());
    let mut info = DomainConnectionInfo::new(connection_id, user_id, tenant_id, device_id);
    if let Some(p) = platform {
        info = info.with_platform(p);
    }
    info.with_metadata(metadata)
}
