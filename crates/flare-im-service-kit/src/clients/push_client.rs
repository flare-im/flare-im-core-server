use flare_server_core::error::Result;
use tonic::transport::Channel;

use crate::staff_context::staff_context_request;
use flare_grpc_proto::push::push_admin_service_client::PushAdminServiceClient;
use flare_grpc_proto::push::{AdminQueryPushTraceRequest, AdminQueryPushTraceResponse};

/// Push Proxy 的管理员只读客户端。只带 admin-gateway 验过后原样转发的 Staff `x-flare-ctx`
/// （`staff_token`），租户由 Push Proxy 从令牌里取。
#[derive(Clone)]
pub struct PushAdminClientWrapper {
    admin: PushAdminServiceClient<Channel>,
}

impl PushAdminClientWrapper {
    pub fn from_channel(channel: Channel) -> Self {
        Self {
            admin: PushAdminServiceClient::new(channel),
        }
    }

    /// 一个用户的推送决策链：Push Proxy 要求 `core:users:read` 的 Staff 上下文。
    pub async fn admin_query_push_trace(
        &mut self,
        staff_token: &str,
        request: AdminQueryPushTraceRequest,
    ) -> Result<AdminQueryPushTraceResponse> {
        let response = self
            .admin
            .admin_query_push_trace(staff_context_request(request, staff_token)?)
            .await?;
        Ok(response.into_inner())
    }
}
