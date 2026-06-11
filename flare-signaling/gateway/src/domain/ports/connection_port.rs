//! 连接端口接口（领域层）
//!
//! 请求/响应类型与 [`flare_grpc_proto::signaling`]（`online` 服务）一致；网关基础设施实现见
//! [`crate::infrastructure::ports::connection_port::ConnectionRepository`]。

use crate::domain::model::ConnectionInfo;
use async_trait::async_trait;
use flare_grpc_proto::signaling::{
    GetOnlineStatusRequest, GetOnlineStatusResponse, HeartbeatRequest, HeartbeatResponse,
    LoginRequest, LoginResponse, LogoutRequest, LogoutResponse,
};
use flare_im_contracts::Ctx;
use flare_server_core::error::Result;
use std::collections::HashMap;

#[async_trait]
pub trait IConnectionPort: Send + Sync {
    async fn login(&self, request: LoginRequest) -> Result<LoginResponse>;
    async fn logout(&self, request: LogoutRequest) -> Result<LogoutResponse>;
    async fn heartbeat(&self, request: HeartbeatRequest) -> Result<HeartbeatResponse>;
    async fn get_online_status(
        &self,
        request: GetOnlineStatusRequest,
    ) -> Result<GetOnlineStatusResponse>;
    async fn list_user_connections(&self, user_id: &str) -> Result<Vec<ConnectionInfo>>;
    async fn get_connection_info(&self, connection_id: &str) -> Result<ConnectionInfo>;
    async fn get_connection_metadata(&self, connection_id: &str)
    -> Result<HashMap<String, String>>;

    /// 从本地连接态构建上行处理用 `Ctx`（租户/用户/设备等）
    async fn build_ctx(&self, connection_id: &str) -> Result<Ctx>;
}
