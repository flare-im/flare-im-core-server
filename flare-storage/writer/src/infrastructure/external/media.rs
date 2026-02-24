use anyhow::{Result, anyhow};
use async_trait::async_trait;
use flare_proto::media::{media_service_client::MediaServiceClient, GetFileInfoRequest};
use flare_server_core::context::{Context, ContextExt};
use tonic::transport::Channel;
use tracing::{warn, instrument};

use crate::domain::model::MediaAttachmentMetadata;
use crate::domain::repository::MediaAttachmentVerifier;

pub struct MediaAttachmentClient {
    endpoint: String,
    client: tokio::sync::Mutex<Option<MediaServiceClient<Channel>>>,
}

impl MediaAttachmentClient {
    pub fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            client: tokio::sync::Mutex::new(None),
        }
    }

    async fn ensure_client(&self) -> Result<MediaServiceClient<Channel>> {
        let mut guard = self.client.lock().await;
        if let Some(client) = guard.as_ref() {
            return Ok(client.clone());
        }

        let client = MediaServiceClient::connect(self.endpoint.clone())
            .await
            .map_err(|err| anyhow!("Failed to connect media service: {err}"))?;

        *guard = Some(client.clone());
        Ok(client)
    }
}

#[async_trait]
impl MediaAttachmentVerifier for MediaAttachmentClient {
    #[instrument(skip(self, ctx), fields(
        request_id = %ctx.request_id(),
        trace_id = %ctx.trace_id(),
        file_count = file_ids.len(),
    ))]
    async fn fetch_metadata(&self, ctx: &Context, file_ids: &[String]) -> Result<Vec<MediaAttachmentMetadata>> {
        ctx.ensure_not_cancelled().map_err(|e| {
            anyhow!("Request cancelled: {}", e)
        })?;
        let mut client = self.ensure_client().await?;
        let mut result = Vec::with_capacity(file_ids.len());

        // 从 Context 中提取 RequestContext 和 TenantContext（用于 protobuf 兼容性）
        let _request_context: flare_proto::common::RequestContext = ctx.request()
            .cloned()
            .map(|req_ctx| req_ctx.into())
            .unwrap_or_else(|| {
                let request_id = if ctx.request_id().is_empty() {
                    uuid::Uuid::new_v4().to_string()
                } else {
                    ctx.request_id().to_string()
                };
                flare_proto::common::RequestContext {
                    request_id,
                    trace: None,
                    actor: None,
                    device: None,
                    channel: String::new(),
                    user_agent: String::new(),
                    attributes: std::collections::HashMap::new(),
                }
            });

        let _tenant: flare_proto::common::TenantContext = ctx.tenant()
            .cloned()
            .map(|t| t.into())
            .or_else(|| {
                ctx.tenant_id().map(|tenant_id| {
                    let tenant: flare_server_core::context::TenantContext = 
                        flare_server_core::context::TenantContext::new(tenant_id);
                    tenant.into()
                })
            })
            .unwrap_or_else(|| {
                flare_proto::common::TenantContext::default()
            });

        for file_id in file_ids {
            let request = GetFileInfoRequest {
                file_id: file_id.clone(),
            };

            match client.get_file_info(tonic::Request::new(request)).await {
                Ok(response) => {
                    if let Some(info) = response.into_inner().info {
                        result.push(MediaAttachmentMetadata {
                            file_id: info.file_id,
                            file_name: info.file_name,
                            mime_type: info.mime_type,
                            size: info.size,
                            url: info.url,
                            cdn_url: info.cdn_url,
                        });
                    } else {
                        warn!(file_id = %file_id, "Media info missing in response");
                    }
                }
                Err(err) => {
                    warn!(error = ?err, file_id = %file_id, "Media service returned error");
                }
            }
        }

        Ok(result)
    }
}
