use std::sync::Arc;

use anyhow::Result;
use flare_grpc_proto::signaling::online::GetOnlineStatusRequest;
use flare_grpc_proto::signaling::online::online_service_client::OnlineServiceClient;
use flare_im_core::Ctx;
use tonic::transport::Channel;

use crate::config::PushServerConfig;

pub struct OnlineStatusService {
    config: Arc<PushServerConfig>,
    client: OnlineServiceClient<Channel>,
}

impl OnlineStatusService {
    pub async fn new(config: Arc<PushServerConfig>) -> Result<Self> {
        let channel = Channel::from_shared(config.online_service_endpoint.clone())?
            .connect()
            .await?;
        let client = OnlineServiceClient::new(channel);
        Ok(Self { config, client })
    }

    pub async fn is_online(&self, ctx: &Ctx, user_id: &str) -> Result<bool> {
        let mut client = self.client.clone();
        let mut req = tonic::Request::new(GetOnlineStatusRequest {
            user_ids: vec![user_id.to_string()],
        });
        flare_server_core::grpc::client::encode_context_to_metadata(
            req.metadata_mut(),
            ctx.as_ref(),
        );
        let resp = client.get_online_status(req).await?.into_inner();
        Ok(resp
            .statuses
            .get(user_id)
            .map(|s| s.online)
            .unwrap_or(false))
    }

    pub fn default_tenant_id(&self) -> &str {
        &self.config.default_tenant_id
    }
}
