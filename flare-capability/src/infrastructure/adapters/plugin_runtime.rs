//! `PluginRuntime` over gRPC: `Inspect` needs no credential, `ApplyTenant` carries Core's ticket.
use std::time::Duration;

use flare_grpc_proto::capability::{
    GenericRequest, PluginRuntimeInspectRequest, PluginTenantState,
    extension_plugin_client::ExtensionPluginClient, plugin_runtime_client::PluginRuntimeClient,
};
use flare_im_capability_core::plugin::data::{DataRequest, OPERATION as DATA_OPERATION};
use flare_im_capability_core::plugin::invocation::PLUGIN_CALL_HEADER;
use serde_json::Value;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Status};

use crate::domain::plugin::reconcile::bounded_reason;
use crate::domain::plugin::{PluginDeploymentRecord, TenantDelivery};
use crate::domain::repository::plugin_runtime::{
    PluginRuntimeGateway, RuntimeAck, RuntimeFailure, RuntimeInspection,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const INSPECT_TIMEOUT: Duration = Duration::from_secs(4);
/// The host gives configuration validation 2 seconds of this.
const APPLY_TIMEOUT: Duration = Duration::from_secs(6);
/// An export or a snapshot writes its artifact before answering.
const DATA_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_ANSWER_BYTES: usize = 128 * 1024;

#[derive(Debug, Default, Clone, Copy)]
pub struct GrpcPluginRuntime;

/// A host answer: refusals keep the plugin's own message, unknown outcomes stay retryable.
fn failure(status: Status) -> RuntimeFailure {
    match status.code() {
        Code::InvalidArgument
        | Code::FailedPrecondition
        | Code::PermissionDenied
        | Code::Unauthenticated
        | Code::OutOfRange
        | Code::AlreadyExists
        | Code::Unimplemented
        | Code::Internal => RuntimeFailure::Refused(bounded_reason(&format!(
            "{:?}: {}",
            status.code(),
            status.message()
        ))),
        _ => RuntimeFailure::Unavailable,
    }
}

async fn channel(
    deployment: &PluginDeploymentRecord,
    timeout: Duration,
) -> Result<Channel, RuntimeFailure> {
    Endpoint::from_shared(deployment.grpc_authority.clone())
        .map_err(|_| RuntimeFailure::Refused("invalid plugin deployment address".into()))?
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(timeout)
        .connect()
        .await
        .map_err(|_| RuntimeFailure::Unavailable)
}

async fn client(
    deployment: &PluginDeploymentRecord,
    timeout: Duration,
) -> Result<PluginRuntimeClient<Channel>, RuntimeFailure> {
    Ok(
        PluginRuntimeClient::new(channel(deployment, timeout).await?)
            .max_decoding_message_size(MAX_ANSWER_BYTES),
    )
}

#[async_trait::async_trait]
impl PluginRuntimeGateway for GrpcPluginRuntime {
    async fn inspect(
        &self,
        deployment: &PluginDeploymentRecord,
    ) -> Result<RuntimeInspection, RuntimeFailure> {
        let call = async {
            let answer = client(deployment, INSPECT_TIMEOUT)
                .await?
                .inspect(PluginRuntimeInspectRequest {})
                .await
                .map_err(failure)?
                .into_inner();
            Ok(RuntimeInspection {
                deployment_id: answer.deployment_id,
                plugin_id: answer.plugin_id,
                plugin_version: answer.plugin_version,
                manifest_digest: answer.manifest_digest,
                artifact_digest: answer.artifact_digest,
                ready: answer.ready,
                draining: answer.draining,
                protocols: answer.protocols,
            })
        };
        tokio::time::timeout(INSPECT_TIMEOUT, call)
            .await
            .map_err(|_| RuntimeFailure::Unavailable)?
    }

    async fn apply(
        &self,
        deployment: &PluginDeploymentRecord,
        delivery: &TenantDelivery,
        ticket: &str,
    ) -> Result<RuntimeAck, RuntimeFailure> {
        let mut request = Request::new(PluginTenantState {
            organization_id: delivery.organization_id.clone(),
            deployment_id: delivery.deployment_id.clone(),
            app_id: delivery.app_id.clone(),
            grant_id: delivery.grant_id.clone(),
            revision: delivery.revision,
            manifest_digest: delivery.manifest_digest.clone(),
            configuration_json: serde_json::to_vec(&delivery.configuration)
                .map_err(|_| RuntimeFailure::Unavailable)?,
            configuration_digest: delivery.configuration_digest.clone(),
            enabled: delivery.enabled,
            valid_until_ms: delivery.valid_until_ms,
            validate_configuration: delivery.validate_configuration,
            approved_operations: delivery.approved_operations.clone(),
            permissions: delivery.permissions.clone(),
            data_scopes: delivery.data_scopes.clone(),
        });
        let mut credential =
            MetadataValue::try_from(ticket).map_err(|_| RuntimeFailure::Unavailable)?;
        credential.set_sensitive(true);
        request
            .metadata_mut()
            .insert(PLUGIN_CALL_HEADER, credential);
        let call = async {
            let answer = client(deployment, APPLY_TIMEOUT)
                .await?
                .apply_tenant(request)
                .await
                .map_err(failure)?
                .into_inner();
            Ok(RuntimeAck {
                revision: answer.revision,
                configuration_digest: answer.configuration_digest,
                enabled: answer.enabled,
            })
        };
        tokio::time::timeout(APPLY_TIMEOUT, call)
            .await
            .map_err(|_| RuntimeFailure::Unavailable)?
    }

    async fn data(
        &self,
        deployment: &PluginDeploymentRecord,
        request: &DataRequest,
        ticket: &str,
    ) -> Result<Value, RuntimeFailure> {
        let mut call = Request::new(GenericRequest {
            operation: DATA_OPERATION.to_owned(),
            request_id: request.job_id.clone(),
            payload: Some(prost_types::Any {
                type_url: "type.googleapis.com/flare.capability.v1.PayloadJson".to_owned(),
                value: serde_json::to_vec(request).map_err(|_| RuntimeFailure::Unavailable)?,
            }),
            ..Default::default()
        });
        let mut credential =
            MetadataValue::try_from(ticket).map_err(|_| RuntimeFailure::Unavailable)?;
        credential.set_sensitive(true);
        call.metadata_mut().insert(PLUGIN_CALL_HEADER, credential);
        let exchange = async {
            let answer = ExtensionPluginClient::new(channel(deployment, DATA_TIMEOUT).await?)
                .max_decoding_message_size(MAX_ANSWER_BYTES)
                .call(call)
                .await
                .map_err(failure)?
                .into_inner();
            if !answer.ok || answer.request_id != request.job_id {
                return Err(RuntimeFailure::Refused(bounded_reason(&answer.error_code)));
            }
            serde_json::from_slice(&answer.payload.ok_or(RuntimeFailure::Unavailable)?.value)
                .map_err(|_| RuntimeFailure::Unavailable)
        };
        tokio::time::timeout(DATA_TIMEOUT, exchange)
            .await
            .map_err(|_| RuntimeFailure::Unavailable)?
    }
}
