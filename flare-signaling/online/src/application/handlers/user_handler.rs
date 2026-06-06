//! 用户在线状态 / 设备相关编排（应用层），委托领域 `UserService`。
//!
//! 接口层仅依赖本模块，不直接调用领域服务。

use std::sync::Arc;

use flare_grpc_proto::signaling::online::{
    BatchGetUserPresenceRequest, BatchGetUserPresenceResponse, GetDeviceRequest, GetDeviceResponse,
    GetUserPresenceRequest, GetUserPresenceResponse, KickDeviceRequest, KickDeviceResponse,
    ListUserDevicesRequest, ListUserDevicesResponse,
};
use flare_server_core::context::Context;

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
        request: GetUserPresenceRequest,
    ) -> Result<GetUserPresenceResponse> {
        self.inner.get_user_presence(request).await
    }

    pub async fn batch_get_user_presence(
        &self,
        request: BatchGetUserPresenceRequest,
    ) -> Result<BatchGetUserPresenceResponse> {
        self.inner.batch_get_user_presence(request).await
    }

    pub async fn list_user_devices(
        &self,
        ctx: &Context,
        request: ListUserDevicesRequest,
    ) -> Result<ListUserDevicesResponse> {
        self.inner.list_user_devices(ctx, request).await
    }

    pub async fn kick_device(&self, request: KickDeviceRequest) -> Result<KickDeviceResponse> {
        self.inner.kick_device(request).await
    }

    pub async fn get_device(
        &self,
        ctx: &Context,
        request: GetDeviceRequest,
    ) -> Result<GetDeviceResponse> {
        self.inner.get_device(ctx, request).await
    }
}
