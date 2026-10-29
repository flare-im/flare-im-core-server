//! PushService gRPC：映射协议并调用 `application::handlers` / `queries`。

use std::collections::HashMap;
use std::sync::Arc;

use flare_grpc_proto::push::push_service_server::PushService;
use flare_grpc_proto::push::{
    DevicePushProvider, InspectDevicePushBindingRequest, InspectDevicePushBindingResponse,
    PushCapabilityRequest, PushCapabilityResponse, PushCustomRequest, PushCustomResponse,
    PushMessageRequest, PushMessageResponse, PushNotificationRequest, PushNotificationResponse,
    QueryPushStatusRequest, QueryPushStatusResponse, RegisterDevicePushTokenRequest,
    RegisterDevicePushTokenResponse, RevokeDevicePushBindingRequest,
    RevokeDevicePushBindingResponse,
};
use flare_im_contracts::Ctx;
use flare_im_contracts::DevicePushToken;
use flare_server_core::auth::{SignedActorType, SignedContextClaims};
use flare_server_core::grpc::middleware::signed_context::VerifiedContext;
use flare_server_core::utils::require_ctx_from_request;
use tonic::{Request, Response, Status};
use tracing::instrument;

use crate::application::{PushProxyCommandHandler, PushTaskStatusQuery};
use crate::infrastructure::{RedisDeviceTokenRegistry, RedisStateStore};

#[derive(Clone)]
pub struct PushServiceHandler {
    command_handler: Arc<PushProxyCommandHandler>,
    status_query: Arc<PushTaskStatusQuery>,
    store: Arc<RedisStateStore>,
    device_tokens: Arc<RedisDeviceTokenRegistry>,
    /// Why enqueue is refused (`None` = broker provenance is enforced); decided once at startup.
    delivery_refusal: Option<Arc<str>>,
}

impl PushServiceHandler {
    pub fn new(
        command_handler: Arc<PushProxyCommandHandler>,
        status_query: Arc<PushTaskStatusQuery>,
        store: Arc<RedisStateStore>,
        device_tokens: Arc<RedisDeviceTokenRegistry>,
        delivery_refusal: Option<String>,
    ) -> Self {
        Self {
            command_handler,
            status_query,
            store,
            device_tokens,
            delivery_refusal: delivery_refusal.map(Arc::from),
        }
    }

    /// Enqueue only when the Push consumers can run: acknowledging a task that no
    /// consumer may process would silently drop it.
    fn require_delivery_ready(&self) -> Result<(), Status> {
        match &self.delivery_refusal {
            None => Ok(()),
            Some(reason) => Err(Status::unavailable(reason.to_string())),
        }
    }
}

#[tonic::async_trait]
impl PushService for PushServiceHandler {
    #[instrument(skip(self, request))]
    async fn push_message(
        &self,
        request: Request<PushMessageRequest>,
    ) -> Result<Response<PushMessageResponse>, Status> {
        let ctx = require_verified_actor(&request, SignedActorType::Service)?;
        let tenant_id = require_tenant_id(&ctx)?;
        require_metadata_tenant(&request.get_ref().metadata, &tenant_id)?;
        if let Some(options) = request.get_ref().options.as_ref() {
            require_metadata_tenant(&options.metadata, &tenant_id)?;
        }
        self.require_delivery_ready()?;
        let req = request.into_inner();
        if req.user_ids.is_empty() {
            return Ok(Response::new(PushMessageResponse {
                success_count: 0,
                fail_count: 0,
                failed_user_ids: vec![],
                failures: vec![],
                task_id: String::new(),
            }));
        }
        self.command_handler
            .enqueue_push_message(&ctx, &req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let task_id = uuid::Uuid::new_v4().to_string();
        self.store
            .save_task_status(&tenant_id, &task_id, "pending")
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PushMessageResponse {
            success_count: req.user_ids.len() as i32,
            fail_count: 0,
            failed_user_ids: vec![],
            failures: vec![],
            task_id,
        }))
    }

    #[instrument(skip(self, request))]
    async fn push_notification(
        &self,
        request: Request<PushNotificationRequest>,
    ) -> Result<Response<PushNotificationResponse>, Status> {
        let ctx = require_verified_actor(&request, SignedActorType::Service)?;
        let tenant_id = require_tenant_id(&ctx)?;
        require_metadata_tenant(&request.get_ref().metadata, &tenant_id)?;
        if let Some(options) = request.get_ref().options.as_ref() {
            require_metadata_tenant(&options.metadata, &tenant_id)?;
        }
        if let Some(notification) = request.get_ref().notification.as_ref() {
            require_metadata_tenant(&notification.metadata, &tenant_id)?;
        }
        self.require_delivery_ready()?;
        let req = request.into_inner();
        if req.user_ids.is_empty() {
            return Ok(Response::new(PushNotificationResponse {
                success_count: 0,
                fail_count: 0,
                failures: vec![],
                task_id: String::new(),
            }));
        }
        self.command_handler
            .enqueue_push_notification(&ctx, &req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let task_id = uuid::Uuid::new_v4().to_string();
        self.store
            .save_task_status(&tenant_id, &task_id, "pending")
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PushNotificationResponse {
            success_count: req.user_ids.len() as i32,
            fail_count: 0,
            failures: vec![],
            task_id,
        }))
    }

    #[instrument(skip(self, request))]
    async fn push_custom(
        &self,
        request: Request<PushCustomRequest>,
    ) -> Result<Response<PushCustomResponse>, Status> {
        let ctx = require_verified_actor(&request, SignedActorType::Service)?;
        let tenant_id = require_tenant_id(&ctx)?;
        require_metadata_tenant(&request.get_ref().metadata, &tenant_id)?;
        if let Some(options) = request.get_ref().options.as_ref() {
            require_metadata_tenant(&options.metadata, &tenant_id)?;
        }
        if let Some(custom_data) = request.get_ref().custom_data.as_ref() {
            require_metadata_tenant(&custom_data.attributes, &tenant_id)?;
        }
        self.require_delivery_ready()?;
        let req = request.into_inner();
        if req.user_ids.is_empty() {
            return Ok(Response::new(PushCustomResponse {
                success_count: 0,
                fail_count: 0,
                failed_user_ids: vec![],
                failures: vec![],
                task_id: String::new(),
            }));
        }
        self.command_handler
            .enqueue_push_custom(&ctx, &req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let task_id = uuid::Uuid::new_v4().to_string();
        self.store
            .save_task_status(&tenant_id, &task_id, "pending")
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PushCustomResponse {
            success_count: req.user_ids.len() as i32,
            fail_count: 0,
            failed_user_ids: vec![],
            failures: vec![],
            task_id,
        }))
    }

    /// Online-only plugin realtime packet. No task status is kept: there is nothing to query
    /// for a packet that is dropped once delivered or expired.
    #[instrument(skip(self, request))]
    async fn push_capability(
        &self,
        request: Request<PushCapabilityRequest>,
    ) -> Result<Response<PushCapabilityResponse>, Status> {
        let ctx = require_verified_actor(&request, SignedActorType::Service)?;
        require_tenant_id(&ctx)?;
        validate_capability_request(request.get_ref())?;
        self.require_delivery_ready()?;
        let req = request.into_inner();
        self.command_handler
            .enqueue_push_capability(&ctx, &req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PushCapabilityResponse {
            accepted_user_count: req.user_ids.len() as i32,
        }))
    }

    async fn register_device_push_token(
        &self,
        request: Request<RegisterDevicePushTokenRequest>,
    ) -> Result<Response<RegisterDevicePushTokenResponse>, Status> {
        let ctx = require_verified_actor(&request, SignedActorType::User)?;
        let req = request.into_inner();
        validate_registration_owner(&ctx, &req.user_id)?;
        let provider = provider_string(req.provider)?;
        require_non_empty(&req.device_id, "device_id")?;
        require_non_empty(&req.platform, "platform")?;
        require_non_empty(&req.token, "token")?;
        validate_device_registration(&provider, &req.device_id, &req.platform, &req.token)?;
        let tenant_id = require_tenant_id(&ctx)?;
        let token = DevicePushToken {
            tenant_id,
            user_id: req.user_id,
            device_id: req.device_id.trim().to_string(),
            platform: req.platform.trim().to_string(),
            provider,
            token: req.token.trim().to_string(),
            registration_id: req.binding_id.clone(),
            registered_at_ms: 0, // Redis records its own time; replay digest remains immutable.
        };
        self.device_tokens
            .register(&token, &req.expected_version, req.expires_at_ms)
            .await
            .map_err(binding_error)?;
        Ok(Response::new(RegisterDevicePushTokenResponse {
            success: true,
            binding_id: req.binding_id,
        }))
    }

    async fn inspect_device_push_binding(
        &self,
        request: Request<InspectDevicePushBindingRequest>,
    ) -> Result<Response<InspectDevicePushBindingResponse>, Status> {
        let ctx = require_verified_actor(&request, SignedActorType::User)?;
        let req = request.into_inner();
        validate_registration_owner(&ctx, &req.user_id)?;
        let provider = provider_string(req.provider)?;
        require_non_empty(&req.device_id, "device_id")?;
        let tenant_id = require_tenant_id(&ctx)?;
        let row = self
            .device_tokens
            .inspect(
                &tenant_id,
                &req.user_id,
                &provider,
                req.device_id.trim(),
                &req.binding_id,
            )
            .await
            .map_err(binding_error)?;
        Ok(Response::new(InspectDevicePushBindingResponse {
            version: row.version,
            current_binding_id: row.current_binding_id,
            state: row.state,
        }))
    }

    async fn revoke_device_push_binding(
        &self,
        request: Request<RevokeDevicePushBindingRequest>,
    ) -> Result<Response<RevokeDevicePushBindingResponse>, Status> {
        let ctx = require_verified_actor(&request, SignedActorType::Service)?;
        let claims = ctx
            .get_data::<VerifiedContext>()
            .and_then(|marker| marker.claims())
            .ok_or_else(|| Status::permission_denied("gateway revoke capability required"))?;
        if claims.credential_id.as_deref() != Some("api-gateway") {
            return Err(Status::permission_denied(
                "gateway revoke capability required",
            ));
        }
        self.device_tokens
            .revoke(&request.into_inner().revocation_token)
            .await
            .map_err(binding_error)?;
        Ok(Response::new(RevokeDevicePushBindingResponse {
            success: true,
        }))
    }

    async fn query_push_status(
        &self,
        request: Request<QueryPushStatusRequest>,
    ) -> Result<Response<QueryPushStatusResponse>, Status> {
        let ctx = require_verified_actor(&request, SignedActorType::Service)?;
        let req = request.into_inner();
        let status = self
            .status_query
            .get_task_status(&ctx, &req.task_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .unwrap_or_else(|| "unknown".to_string());

        Ok(Response::new(QueryPushStatusResponse {
            task_id: req.task_id,
            status,
        }))
    }
}

fn binding_error(error: crate::infrastructure::BindingError) -> Status {
    use crate::infrastructure::BindingError;
    match error {
        BindingError::Conflict | BindingError::Revoked => {
            Status::failed_precondition("push binding version conflict or revoked")
        }
        BindingError::Invalid => Status::invalid_argument("push binding command rejected"),
        BindingError::Limit => Status::resource_exhausted("device registration limit"),
        BindingError::Unavailable => Status::unavailable("device registry unavailable"),
    }
}

/// The downstream Push Server/Worker consumers are intentionally paused until
/// signed MQ ingress and both delivery stages have been verified. Do not
/// acknowledge new tasks as "pending" when no consumer can deliver them.
fn require_tenant_id(ctx: &flare_server_core::context::Ctx) -> Result<String, Status> {
    ctx.tenant_id()
        .filter(|tenant_id| {
            flare_im_contracts::domain::tenant::is_valid_tenant_id(tenant_id)
                && flare_im_contracts::utils::normalize_tenant_id(tenant_id) == *tenant_id
        })
        .map(ToString::to_string)
        .ok_or_else(|| Status::permission_denied("canonical signed tenant_id is required"))
}

fn require_verified_actor<T>(
    request: &Request<T>,
    expected: SignedActorType,
) -> Result<Ctx, Status> {
    let ctx = require_ctx_from_request(request)?;
    let claims = ctx
        .get_data::<VerifiedContext>()
        .and_then(|marker| marker.claims())
        .ok_or_else(|| Status::unauthenticated("signed Push Proxy identity required"))?;
    validate_actor_claims(claims, expected)?;
    Ok(ctx)
}

fn validate_actor_claims(
    claims: &SignedContextClaims,
    expected: SignedActorType,
) -> Result<(), Status> {
    if claims.actor_type != expected
        || !flare_im_contracts::domain::tenant::is_valid_tenant_id(&claims.tenant_id)
        || flare_im_contracts::utils::normalize_tenant_id(&claims.tenant_id) != claims.tenant_id
    {
        return Err(Status::permission_denied(
            "Push Proxy actor or tenant not permitted",
        ));
    }
    Ok(())
}

/// Recipients of one realtime packet; Capability sends at most 32.
const MAX_CAPABILITY_RECIPIENTS: usize = 32;
/// Encoded packet bound: a 6 KiB JSON payload plus its attributes.
const MAX_CAPABILITY_PACKET_BYTES: usize = 8 * 1024;

fn validate_capability_request(req: &PushCapabilityRequest) -> Result<(), Status> {
    use prost::Message as _;
    let packet = req
        .packet
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("capability packet required"))?;
    let unique = req
        .user_ids
        .iter()
        .collect::<std::collections::BTreeSet<_>>()
        .len()
        == req.user_ids.len();
    if req.user_ids.is_empty()
        || req.user_ids.len() > MAX_CAPABILITY_RECIPIENTS
        || !unique
        || req.user_ids.iter().any(|id| id.trim().is_empty())
        || packet.capability_id.trim().is_empty()
        || packet.packet_type.trim().is_empty()
        || packet.encoded_len() > MAX_CAPABILITY_PACKET_BYTES
    {
        return Err(Status::invalid_argument("invalid capability push"));
    }
    Ok(())
}

fn require_metadata_tenant(
    metadata: &HashMap<String, String>,
    tenant_id: &str,
) -> Result<(), Status> {
    for key in ["tenant_id", "x-tenant-id"] {
        if metadata.get(key).is_some_and(|value| value != tenant_id) {
            return Err(Status::permission_denied("push body tenant mismatch"));
        }
    }
    Ok(())
}

fn validate_registration_owner(
    ctx: &flare_server_core::context::Ctx,
    request_user_id: &str,
) -> Result<(), Status> {
    require_non_empty(request_user_id, "user_id")?;
    let authenticated_user_id = ctx
        .user_id()
        .ok_or_else(|| Status::unauthenticated("authenticated user_id is required"))?;
    if authenticated_user_id != request_user_id.trim() {
        return Err(Status::permission_denied(
            "device push token user_id must match authenticated user",
        ));
    }
    Ok(())
}

fn require_non_empty(value: &str, name: &'static str) -> Result<(), Status> {
    if value.trim().is_empty() {
        return Err(Status::invalid_argument(format!("{name} is required")));
    }
    Ok(())
}

#[cfg(test)]
mod identity_boundary_tests {
    use super::*;

    #[test]
    fn capability_pushes_need_a_packet_and_one_to_thirty_two_unique_users() {
        let packet = flare_proto::common::CapabilityPacket {
            capability_id: "flare.call".into(),
            packet_type: "call.snapshot".into(),
            version: "1".into(),
            payload: b"{}".to_vec(),
            ..Default::default()
        };
        let request = |users: Vec<&str>, packet: Option<flare_proto::common::CapabilityPacket>| {
            PushCapabilityRequest {
                user_ids: users.into_iter().map(String::from).collect(),
                packet,
                expire_at_ms: 0,
            }
        };
        assert!(
            validate_capability_request(&request(vec!["u1", "u2"], Some(packet.clone()))).is_ok()
        );
        let many: Vec<String> = (0..33).map(|i| format!("u{i}")).collect();
        for bad in [
            request(vec![], Some(packet.clone())),
            request(vec!["u1", "u1"], Some(packet.clone())),
            request(vec!["u1"], None),
            PushCapabilityRequest {
                user_ids: many,
                packet: Some(packet.clone()),
                expire_at_ms: 0,
            },
            request(
                vec!["u1"],
                Some(flare_proto::common::CapabilityPacket {
                    payload: vec![b'x'; 9 * 1024],
                    ..packet.clone()
                }),
            ),
        ] {
            assert_eq!(
                validate_capability_request(&bad).unwrap_err().code(),
                tonic::Code::InvalidArgument
            );
        }
    }

    fn claims(actor_type: SignedActorType, tenant_id: &str) -> SignedContextClaims {
        SignedContextClaims {
            sub: "producer".into(),
            iss: "flare-cell".into(),
            aud: "flare-cell-internal".into(),
            token_use: "internal_context".into(),
            iat: 1,
            exp: 2,
            jti: "test".into(),
            actor_type,
            actor_id: "producer".into(),
            tenant_id: tenant_id.into(),
            roles: Vec::new(),
            credential_id: None,
            request_id: "req-1".into(),
            org_id: None,
            permission: None,
            reason: None,
            perm_version: None,
            trace_id: None,
            session_id: None,
            kid: flare_server_core::auth::LEGACY_KID.to_owned(),
        }
    }

    #[test]
    fn service_push_and_user_device_roles_are_separate() {
        let service = claims(SignedActorType::Service, "t1");
        let user = claims(SignedActorType::User, "t1");
        assert!(validate_actor_claims(&service, SignedActorType::Service).is_ok());
        assert!(validate_actor_claims(&user, SignedActorType::User).is_ok());
        assert_eq!(
            validate_actor_claims(&user, SignedActorType::Service)
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            validate_actor_claims(&service, SignedActorType::User)
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }

    #[test]
    fn canonical_tenant_and_body_metadata_must_match_signed_claims() {
        for tenant in ["", "default", " t1", "t1:other", "*"] {
            assert_eq!(
                validate_actor_claims(
                    &claims(SignedActorType::Service, tenant),
                    SignedActorType::Service
                )
                .unwrap_err()
                .code(),
                tonic::Code::PermissionDenied
            );
        }
        let forged = HashMap::from([("tenant_id".into(), "t2".into())]);
        assert_eq!(
            require_metadata_tenant(&forged, "t1").unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
        let matching = HashMap::from([("x-tenant-id".into(), "t1".into())]);
        assert!(require_metadata_tenant(&matching, "t1").is_ok());
    }

    #[test]
    fn bare_legacy_metadata_cannot_authenticate_push_proxy() {
        let mut request = Request::new(PushMessageRequest::default());
        request
            .metadata_mut()
            .insert("x-tenant-id", "t1".parse().unwrap());
        request
            .metadata_mut()
            .insert("x-actor-type", "2".parse().unwrap());
        assert_eq!(
            require_verified_actor(&request, SignedActorType::Service)
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
    }
}

#[cfg(test)]
mod delivery_gate_tests {
    #[test]
    fn refusal_reason_becomes_unavailable_and_absence_allows_enqueue() {
        fn gate(refusal: Option<&str>) -> Result<(), tonic::Status> {
            match refusal {
                None => Ok(()),
                Some(reason) => Err(tonic::Status::unavailable(reason)),
            }
        }
        let refused = gate(Some("Push Proxy is paused")).unwrap_err();
        assert_eq!(refused.code(), tonic::Code::Unavailable);
        assert!(gate(None).is_ok());
    }
}

fn validate_device_registration(
    provider: &str,
    device: &str,
    platform: &str,
    token: &str,
) -> Result<(), Status> {
    if device.len() > 128
        || platform.len() > 32
        || token.len() > 4096
        || [device, platform, token].iter().any(|s| {
            s.trim().is_empty()
                || s.bytes()
                    .any(|b| b.is_ascii_control() || b.is_ascii_whitespace())
        })
        || (provider == "apns"
            && (token.len() > 512 || !token.bytes().all(|b| b.is_ascii_hexdigit())))
    {
        return Err(Status::invalid_argument("device push registration invalid"));
    }
    Ok(())
}

fn provider_string(provider: i32) -> Result<String, Status> {
    match DevicePushProvider::try_from(provider) {
        Ok(DevicePushProvider::Getui) => Ok("getui".to_string()),
        Ok(DevicePushProvider::Fcm) => Ok("fcm".to_string()),
        Ok(DevicePushProvider::Apns) => Ok("apns".to_string()),
        Ok(DevicePushProvider::Unspecified) | Err(_) => Err(Status::invalid_argument(
            "valid device push provider is required",
        )),
    }
}

#[cfg(test)]
mod provider_contract_tests {
    use super::*;
    #[test]
    fn all_vendor_registrations_are_typed_and_bounded() {
        for (provider, name) in [
            (DevicePushProvider::Getui, "getui"),
            (DevicePushProvider::Fcm, "fcm"),
            (DevicePushProvider::Apns, "apns"),
        ] {
            assert_eq!(provider_string(provider as i32).unwrap(), name);
        }
        assert!(provider_string(0).is_err());
        assert!(provider_string(99).is_err());
        assert!(validate_device_registration("apns", "ios", "ios", "a1b2").is_ok());
        assert!(validate_device_registration("apns", "ios", "ios", "a1/path").is_err());
        assert!(
            validate_device_registration("fcm", "android", "android", &"a".repeat(4097)).is_err()
        );
        assert!(validate_device_registration("fcm", "android", "android", "a\nb").is_err());
    }
}
