//! Browser/native capability ingress. Only the verified bearer principal is
//! signed downstream; the JSON body cannot choose a tenant or actor.
use super::MessageContextSigner;
use axum::{Json, extract::Extension};
use flare_grpc_proto::capability::{
    DispatchCapabilityRequest, capability_service_client::CapabilityServiceClient,
};
use flare_server_core::{
    AuthenticatedPrincipal, SignedActorType, SignedContextIdentity,
    http::{ApiResponse, HttpApiError},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

#[derive(Clone)]
pub(crate) struct CapabilityUserClient(pub Option<Channel>);
impl CapabilityUserClient {
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let channel = std::env::var("FLARE_API_GATEWAY_GRPC_CAPABILITY_SERVICE_URL")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(|url| {
                Endpoint::from_shared(url).map(|endpoint| {
                    endpoint
                        .connect_timeout(Duration::from_secs(2))
                        .timeout(Duration::from_secs(15))
                        .connect_lazy()
                })
            })
            .transpose()?;
        Ok(Self(channel))
    }
}
#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DispatchRequest {
    pub capability_id: String,
    pub request_id: String,
    /// Optimistic client scope assertion, compared with the verified bearer.
    pub expected_tenant_id: String,
    pub expected_user_id: String,
    #[serde(default)]
    pub conversation_id: Option<String>,
    #[serde(default)]
    pub payload: Value,
}
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DispatchResponse {
    pub request_id: String,
    pub success: bool,
    pub plugin_id: String,
    pub capability_id: String,
    pub data: Value,
    pub error: Option<String>,
}
#[utoipa::path(post,path="/api/v1/capabilities/dispatch",tag="Capabilities",request_body=DispatchRequest,responses((status=200,body=ApiResponse<DispatchResponse>),(status=403,description="Capability denied"),(status=503,description="Unknown command outcome: reuse business command ID")))]
pub async fn dispatch(
    Extension(client): Extension<CapabilityUserClient>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Extension(signer): Extension<MessageContextSigner>,
    Json(input): Json<DispatchRequest>,
) -> Result<Json<ApiResponse<DispatchResponse>>, HttpApiError> {
    let (tenant, user) = user_binding(&principal)?;
    if input.expected_tenant_id != tenant || input.expected_user_id != user {
        return Err(forbidden());
    }
    if input.request_id.is_empty()
        || input.request_id.len() > 128
        || !input.request_id.bytes().all(|b| b.is_ascii_graphic())
        || input.capability_id.len() > 256
        || input.capability_id.is_empty()
        || input
            .conversation_id
            .as_ref()
            .is_some_and(|s| s.len() > 256)
    {
        return Err(HttpApiError::bad_request(
            "INVALID_CAPABILITY_REQUEST",
            "invalid capability request",
        ));
    }
    let payload = serde_json::to_string(&input.payload).map_err(|_| {
        HttpApiError::bad_request("INVALID_CAPABILITY_REQUEST", "invalid capability payload")
    })?;
    if payload.len() > 65536 {
        return Err(HttpApiError::bad_request(
            "INVALID_CAPABILITY_REQUEST",
            "capability payload limit",
        ));
    }
    let channel = client.0.ok_or_else(|| {
        HttpApiError::internal(
            "CAPABILITY_UNAVAILABLE",
            "capability endpoint not configured",
        )
    })?;
    let signer = signer.0.ok_or_else(|| {
        HttpApiError::internal("CAPABILITY_UNAVAILABLE", "identity signer not configured")
    })?;
    let token = signer
        .sign(&SignedContextIdentity::new(
            SignedActorType::User,
            user,
            tenant,
            &input.request_id,
        ))
        .map_err(|_| HttpApiError::internal("CAPABILITY_UNAVAILABLE", "identity signing failed"))?;
    let mut request = tonic::Request::new(DispatchCapabilityRequest {
        tenant_id: tenant.into(),
        user_id: user.into(),
        capability_id: input.capability_id,
        request_id: input.request_id,
        conversation_id: input.conversation_id.unwrap_or_default(),
        payload_json: payload,
    });
    let mut header: tonic::metadata::MetadataValue<_> = token.parse().map_err(|_| {
        HttpApiError::internal("CAPABILITY_UNAVAILABLE", "identity encoding failed")
    })?;
    header.set_sensitive(true);
    request.metadata_mut().insert("x-flare-ctx", header);
    let mut client =
        CapabilityServiceClient::new(channel).max_decoding_message_size(2 * 1024 * 1024);
    let response = client
        .dispatch(request)
        .await
        .map_err(|error| match error.code() {
            tonic::Code::PermissionDenied | tonic::Code::Unauthenticated => forbidden(),
            tonic::Code::InvalidArgument => HttpApiError::bad_request(
                "INVALID_CAPABILITY_REQUEST",
                "invalid capability request",
            ),
            _ => HttpApiError(
                flare_server_core::error::ErrorBuilder::new(
                    flare_server_core::error::ErrorCode::HttpServiceUnavailable,
                    "CAPABILITY_OUTCOME_UNKNOWN",
                )
                .build_error(),
            ),
        })?
        .into_inner()
        .result
        .ok_or_else(|| {
            HttpApiError::internal("CAPABILITY_UNAVAILABLE", "empty capability response")
        })?;
    let data = serde_json::from_str(&response.result_json).map_err(|_| {
        HttpApiError::internal("CAPABILITY_UNAVAILABLE", "invalid capability response")
    })?;
    Ok(Json(ApiResponse::success(DispatchResponse {
        request_id: response.request_id,
        success: response.success,
        plugin_id: response.plugin_id,
        capability_id: response.capability_id,
        data,
        error: if response.error_message.is_empty() {
            None
        } else {
            Some(response.error_message)
        },
    })))
}
fn user_binding(principal: &AuthenticatedPrincipal) -> Result<(&str, &str), HttpApiError> {
    let tenant = principal
        .tenant_id
        .as_deref()
        .filter(|t| flare_im_contracts::domain::tenant::is_valid_tenant_id(t))
        .ok_or_else(|| {
            HttpApiError::bad_request("TENANT_REQUIRED", "authenticated tenant required")
        })?;
    if principal.user_id.is_empty() || principal.has_gateway_admin_scope() {
        return Err(forbidden());
    }
    Ok((tenant, &principal.user_id))
}
fn forbidden() -> HttpApiError {
    HttpApiError(
        flare_server_core::error::ErrorBuilder::new(
            flare_server_core::error::ErrorCode::HttpForbidden,
            "CAPABILITY_DENIED",
        )
        .build_error(),
    )
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn body_cannot_supply_actor_or_tenant() {
        for key in ["tenantId", "userId", "tenant_id", "user_id", "x-flare-ctx"] {
            let mut value = serde_json::json!({"capabilityId":"flare.redpacket.create","requestId":"one","expectedTenantId":"201","expectedUserId":"u1","payload":{}});
            value[key] = Value::String("forged".into());
            assert!(serde_json::from_value::<DispatchRequest>(value).is_err());
        }
    }
}
