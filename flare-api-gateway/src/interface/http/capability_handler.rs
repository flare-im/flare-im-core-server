//! Browser/native capability ingress. Only the verified bearer principal is
//! signed downstream; the JSON body cannot choose a tenant or actor.
use super::MessageContextSigner;
use axum::{Json, extract::Extension};
use flare_grpc_proto::capability::{
    DispatchCapabilityRequest, capability_service_client::CapabilityServiceClient,
};
use flare_server_core::error::ErrorCode;
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
    /// The stored message (server id) a plugin card action or detail comes from. Core accepts a
    /// call naming a card's entity only from the card bound to it.
    #[serde(default)]
    pub message_id: Option<String>,
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
#[utoipa::path(post,path="/api/v1/capabilities/dispatch",tag="Capabilities",request_body=DispatchRequest,responses((status=200,body=ApiResponse<DispatchResponse>),(status=403,description="Capability denied, or PLUGIN_DISABLED / PLUGIN_PERMISSION_DENIED / PLUGIN_UNSUPPORTED_PLATFORM"),(status=404,description="PLUGIN_NOT_INSTALLED"),(status=409,description="PLUGIN_CARD_UNBOUND (a copy of a card) or PLUGIN_MESSAGE_RECALLED"),(status=422,description="The plugin refused the command: the reason is its own error code"),(status=429,description="CAPABILITY_RATE_LIMITED"),(status=502,description="PLUGIN_CARD_NOT_POSTED: the plugin was told and rolled its entity back"),(status=503,description="Unknown command outcome: reuse business command ID")))]
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
        || input
            .message_id
            .as_ref()
            .is_some_and(|s| s.len() > 256 || s.chars().any(char::is_control))
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
        message_id: input.message_id.unwrap_or_default(),
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
        .map_err(|error| dispatch_error(&error))?
        .into_inner()
        .result
        .ok_or_else(|| {
            HttpApiError::internal("CAPABILITY_UNAVAILABLE", "empty capability response")
        })?;
    let data: Value = serde_json::from_str(&response.result_json).map_err(|_| {
        HttpApiError::internal("CAPABILITY_UNAVAILABLE", "invalid capability response")
    })?;
    if !response.success
        && let Some(rejection) = plugin_rejected(&data, &response.error_message)
    {
        return Err(rejection);
    }
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
fn http_error(code: ErrorCode, reason: &str, details: &str) -> HttpApiError {
    let builder = flare_server_core::error::ErrorBuilder::new(code, reason);
    HttpApiError(if details.is_empty() {
        builder.build_error()
    } else {
        builder.details(details).build_error()
    })
}

fn http_code(status: u16) -> ErrorCode {
    match status {
        404 => ErrorCode::HttpNotFound,
        409 => ErrorCode::HttpConflict,
        422 => ErrorCode::HttpUnprocessableEntity,
        429 => ErrorCode::HttpTooManyRequests,
        502 => ErrorCode::HttpBadGateway,
        _ => ErrorCode::HttpForbidden,
    }
}

/// Capability's refusal or failure (plugin platform v2 §4). A platform code in the status
/// (`ErrorDetail.reason`: `PLUGIN_DISABLED`, `PLUGIN_CARD_UNBOUND`, ...) is answered with its own
/// HTTP status; other identity refusals stay `CAPABILITY_DENIED`, invalid input 400, the rate
/// limit 429. Only what leaves the outcome open (timeout, transport, internal) is 503
/// `CAPABILITY_OUTCOME_UNKNOWN`, which tells the client to retry with the same command id.
fn dispatch_error(error: &tonic::Status) -> HttpApiError {
    use flare_im_service_kit::plugin_platform::{
        CAPABILITY_OUTCOME_UNKNOWN, PLATFORM_DENIAL_CODES,
    };
    if let Some(detail) = flare_server_core::error::grpc::decode_error_detail(error)
        && let Some((code, status)) = PLATFORM_DENIAL_CODES
            .iter()
            .find(|(code, _)| *code == detail.reason)
    {
        return http_error(http_code(*status), code, &detail.message);
    }
    match error.code() {
        tonic::Code::PermissionDenied | tonic::Code::Unauthenticated => forbidden(),
        tonic::Code::InvalidArgument => {
            HttpApiError::bad_request("INVALID_CAPABILITY_REQUEST", "invalid capability request")
        }
        tonic::Code::ResourceExhausted => http_error(
            ErrorCode::HttpTooManyRequests,
            "CAPABILITY_RATE_LIMITED",
            "",
        ),
        _ => http_error(
            ErrorCode::HttpServiceUnavailable,
            CAPABILITY_OUTCOME_UNKNOWN,
            "",
        ),
    }
}

/// A plugin that answered and refused: 422 with its own error code (`data.errorCode`, which Core
/// sets on every plugin rejection), the command did not run. A refusal of another capability
/// without a code keeps its `success: false` answer.
fn plugin_rejected(data: &Value, message: &str) -> Option<HttpApiError> {
    use flare_im_service_kit::plugin_platform::is_error_code;
    let code = data
        .get("errorCode")
        .and_then(Value::as_str)
        .filter(|code| is_error_code(code))?;
    Some(http_error(
        ErrorCode::HttpUnprocessableEntity,
        code,
        message,
    ))
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

    fn reason(error: &HttpApiError) -> (Option<ErrorCode>, String) {
        match &error.0 {
            flare_server_core::error::FlareError::Localized { code, reason, .. } => {
                (Some(*code), reason.clone())
            }
            other => (other.code(), String::new()),
        }
    }

    fn plugin_status(code: &str) -> tonic::Status {
        tonic::Status::from(
            flare_server_core::error::ErrorBuilder::new(ErrorCode::PermissionDenied, code)
                .details("why")
                .build_error(),
        )
    }

    #[test]
    fn platform_codes_keep_their_reason_and_status() {
        for (code, status) in [
            ("PLUGIN_NOT_INSTALLED", ErrorCode::HttpNotFound),
            ("PLUGIN_DISABLED", ErrorCode::HttpForbidden),
            ("PLUGIN_PERMISSION_DENIED", ErrorCode::HttpForbidden),
            ("PLUGIN_UNSUPPORTED_PLATFORM", ErrorCode::HttpForbidden),
            ("PLUGIN_CARD_UNBOUND", ErrorCode::HttpConflict),
            ("PLUGIN_MESSAGE_RECALLED", ErrorCode::HttpConflict),
            ("PLUGIN_CARD_NOT_POSTED", ErrorCode::HttpBadGateway),
        ] {
            assert_eq!(
                reason(&dispatch_error(&plugin_status(code))),
                (Some(status), code.to_owned()),
                "{code}"
            );
        }
        // An unknown reason is not echoed: a plain permission refusal stays CAPABILITY_DENIED.
        assert_eq!(
            reason(&dispatch_error(&plugin_status("SOMETHING_ELSE"))).1,
            "CAPABILITY_DENIED"
        );
    }

    #[test]
    fn only_an_open_outcome_answers_outcome_unknown() {
        for code in [
            tonic::Code::DeadlineExceeded,
            tonic::Code::Unavailable,
            tonic::Code::Internal,
            tonic::Code::Unknown,
        ] {
            assert_eq!(
                reason(&dispatch_error(&tonic::Status::new(code, "x"))),
                (
                    Some(ErrorCode::HttpServiceUnavailable),
                    "CAPABILITY_OUTCOME_UNKNOWN".to_owned()
                )
            );
        }
        assert_eq!(
            reason(&dispatch_error(&tonic::Status::resource_exhausted("x"))).1,
            "CAPABILITY_RATE_LIMITED"
        );
        assert_eq!(
            reason(&dispatch_error(&tonic::Status::invalid_argument("x"))).1,
            "INVALID_CAPABILITY_REQUEST"
        );
    }

    #[test]
    fn a_plugin_rejection_passes_its_own_code_as_422() {
        let error = plugin_rejected(&serde_json::json!({"errorCode": "POLL_CLOSED"}), "closed")
            .expect("a plugin rejection");
        assert_eq!(
            reason(&error),
            (
                Some(ErrorCode::HttpUnprocessableEntity),
                "POLL_CLOSED".to_owned()
            )
        );
        // Not a plugin's coded rejection: the `success: false` answer stands.
        for data in [
            serde_json::json!({}),
            serde_json::json!(null),
            serde_json::json!({"errorCode": "lower_case"}),
        ] {
            assert!(plugin_rejected(&data, "").is_none(), "{data}");
        }
    }

    #[test]
    fn message_id_is_optional_and_bounded() {
        let mut value = serde_json::json!({"capabilityId":"example.poll.vote","requestId":"one","expectedTenantId":"201","expectedUserId":"u1","messageId":"m-1","payload":{}});
        let parsed: DispatchRequest = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(parsed.message_id.as_deref(), Some("m-1"));
        value.as_object_mut().unwrap().remove("messageId");
        let parsed: DispatchRequest = serde_json::from_value(value).unwrap();
        assert!(parsed.message_id.is_none());
    }
    #[test]
    fn body_cannot_supply_actor_or_tenant() {
        for key in ["tenantId", "userId", "tenant_id", "user_id", "x-flare-ctx"] {
            let mut value = serde_json::json!({"capabilityId":"flare.redpacket.create","requestId":"one","expectedTenantId":"201","expectedUserId":"u1","payload":{}});
            value[key] = Value::String("forged".into());
            assert!(serde_json::from_value::<DispatchRequest>(value).is_err());
        }
    }
}
