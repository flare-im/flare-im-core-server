use std::collections::HashMap;
use std::sync::Arc;

use flare_grpc_proto::signaling::online::GetOnlineStatusRequest;
use flare_grpc_proto::signaling::online::online_service_client::OnlineServiceClient;
use flare_im_contracts::Ctx;
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError, Result};
use redis::aio::ConnectionManager;
use tonic::transport::Channel;

use crate::config::PushServerConfig;

enum OnlineStatusBackend {
    Grpc(OnlineServiceClient<Channel>),
    Redis(ConnectionManager),
}

pub struct OnlineStatusService {
    config: Arc<PushServerConfig>,
    backend: OnlineStatusBackend,
}

impl OnlineStatusService {
    pub async fn new(config: Arc<PushServerConfig>) -> Result<Self> {
        let backend = match config.online_status_backend.as_str() {
            "redis" => {
                let client = redis::Client::open(config.redis_url.clone()).map_err(|err| {
                    FlareError::system(format!(
                        "invalid push server redis uri {}: {err}",
                        config.redis_url
                    ))
                })?;
                let manager = ConnectionManager::new(client).await.map_err(|err| {
                    FlareError::system(format!("connect push server redis: {err}"))
                })?;
                OnlineStatusBackend::Redis(manager)
            }
            "grpc" => {
                let endpoint = config.online_service_endpoint.clone();
                let channel = Channel::from_shared(endpoint.clone())
                    .map_err(|err| {
                        FlareError::system(format!("invalid online grpc uri {endpoint}: {err}"))
                    })?
                    .connect()
                    .await
                    .map_err(|err| {
                        FlareError::system(format!("connect online grpc {endpoint}: {err}"))
                    })?;
                OnlineStatusBackend::Grpc(OnlineServiceClient::new(channel))
            }
            other => {
                return Err(ErrorBuilder::new(
                    ErrorCode::ConfigurationError,
                    "unsupported push server online status backend",
                )
                .param("backend", other.to_string())
                .build_error());
            }
        };
        Ok(Self { config, backend })
    }

    pub async fn is_online(&self, ctx: &Ctx, user_id: &str) -> Result<bool> {
        let statuses = self.online_statuses(ctx, &[user_id.to_string()]).await?;
        Ok(statuses.get(user_id).copied().unwrap_or(false))
    }

    pub async fn online_statuses(
        &self,
        ctx: &Ctx,
        user_ids: &[String],
    ) -> Result<HashMap<String, bool>> {
        if user_ids.is_empty() {
            return Ok(HashMap::new());
        }

        match &self.backend {
            OnlineStatusBackend::Grpc(client) => {
                self.grpc_online_statuses(ctx, client, user_ids).await
            }
            OnlineStatusBackend::Redis(manager) => {
                self.redis_online_statuses(manager, user_ids).await
            }
        }
    }

    pub async fn conversation_online_user_ids(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
    ) -> Result<Vec<String>> {
        match &self.backend {
            OnlineStatusBackend::Redis(manager) => {
                self.redis_conversation_online_user_ids(manager, ctx, conversation_id)
                    .await
            }
            OnlineStatusBackend::Grpc(_) => Err(ErrorBuilder::new(
                ErrorCode::ConfigurationError,
                "conversation online index requires redis online status backend",
            )
            .param("backend", "grpc")
            .build_error()),
        }
    }

    async fn grpc_online_statuses(
        &self,
        ctx: &Ctx,
        client: &OnlineServiceClient<Channel>,
        user_ids: &[String],
    ) -> Result<HashMap<String, bool>> {
        let mut client = client.clone();
        let mut req = tonic::Request::new(GetOnlineStatusRequest {
            user_ids: user_ids.to_vec(),
        });
        flare_server_core::grpc::client::encode_context_to_metadata(
            req.metadata_mut(),
            ctx.as_ref(),
        );
        let resp = client.get_online_status(req).await?.into_inner();
        Ok(resp
            .statuses
            .into_iter()
            .map(|(user_id, status)| (user_id, status.online))
            .collect())
    }

    async fn redis_online_statuses(
        &self,
        manager: &ConnectionManager,
        user_ids: &[String],
    ) -> Result<HashMap<String, bool>> {
        let mut conn = manager.clone();
        let mut pipe = redis::pipe();
        for user_id in user_ids {
            pipe.hlen(format!("session:{user_id}"));
        }
        let counts: Vec<usize> = pipe
            .query_async(&mut conn)
            .await
            .map_err(|err| FlareError::system(format!("query redis online status batch: {err}")))?;
        Ok(user_ids
            .iter()
            .cloned()
            .zip(counts.into_iter().map(|count| count > 0))
            .collect())
    }

    async fn redis_conversation_online_user_ids(
        &self,
        manager: &ConnectionManager,
        ctx: &Ctx,
        conversation_id: &str,
    ) -> Result<Vec<String>> {
        let tenant_id = ctx.tenant_id().unwrap_or_else(|| self.default_tenant_id());
        let key = format!("conv:online:{tenant_id}:{conversation_id}");
        let mut conn = manager.clone();
        let mut user_ids: Vec<String> = redis::cmd("SMEMBERS")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .map_err(|err| {
                FlareError::system(format!(
                    "query redis conversation online index {key}: {err}"
                ))
            })?;
        user_ids.retain(|user_id| !user_id.trim().is_empty());
        user_ids.sort();
        user_ids.dedup();
        Ok(user_ids)
    }

    pub fn default_tenant_id(&self) -> &str {
        &self.config.default_tenant_id
    }
}
