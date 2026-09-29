//! 用户在线状态 / 设备相关编排（应用层），委托领域 `UserService`。
//!
//! 接口层仅依赖本模块，不直接调用领域服务。

use std::sync::Arc;

use flare_grpc_proto::signaling::online::{
    BatchGetUserPresenceRequest, BatchGetUserPresenceResponse, GetDeviceRequest, GetDeviceResponse,
    GetUserPresenceRequest, GetUserPresenceResponse, KickDeviceRequest, KickDeviceResponse,
    KickTenantRequest, KickTenantResponse, ListUserDevicesRequest, ListUserDevicesResponse,
};

use crate::domain::repository::ConversationRepository;
use crate::domain::service::UserService;
use flare_server_core::error::Result;

#[derive(Clone)]
pub struct OnlineUserHandler<R: ConversationRepository + Send + Sync> {
    inner: Arc<UserService<R>>,
}

impl<R: ConversationRepository + Send + Sync> OnlineUserHandler<R> {
    pub fn new(inner: Arc<UserService<R>>) -> Self {
        Self { inner }
    }

    pub async fn get_user_presence(
        &self,
        tenant_id: &str,
        request: GetUserPresenceRequest,
    ) -> Result<GetUserPresenceResponse> {
        self.inner.get_user_presence(tenant_id, request).await
    }

    pub async fn batch_get_user_presence(
        &self,
        tenant_id: &str,
        request: BatchGetUserPresenceRequest,
    ) -> Result<BatchGetUserPresenceResponse> {
        self.inner.batch_get_user_presence(tenant_id, request).await
    }

    pub async fn list_user_devices(
        &self,
        tenant_id: &str,
        request: ListUserDevicesRequest,
    ) -> Result<ListUserDevicesResponse> {
        self.inner.list_user_devices(tenant_id, request).await
    }

    pub async fn kick_device(
        &self,
        request: KickDeviceRequest,
        expected_tenant_id: &str,
    ) -> Result<KickDeviceResponse> {
        self.inner.kick_device(request, expected_tenant_id).await
    }

    pub async fn kick_tenant(&self, request: KickTenantRequest) -> Result<KickTenantResponse> {
        self.inner.kick_tenant(request).await
    }

    pub async fn get_device(
        &self,
        tenant_id: &str,
        request: GetDeviceRequest,
    ) -> Result<GetDeviceResponse> {
        self.inner.get_device(tenant_id, request).await
    }
}
