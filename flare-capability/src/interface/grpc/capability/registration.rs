//! Delegated credentials are accepted only here. The public protocol fixes
//! their purpose and binds the exact RPC body; no Cell identity is synthesized.
use super::service::{CapabilityGrpcServer, PluginRouteActor};
use flare_grpc_proto::capability::{
    DeregisterPluginEndpointRequest, RegisterPluginEndpointRequest,
};
use flare_im_capability_core::plugin::workload::*;
use tonic::{Request, Status};

pub(super) fn register_intent(r: &RegisterPluginEndpointRequest) -> RegistrationRequest {
    RegistrationRequest {
        plugin_id: r.plugin_id.clone(),
        tenant_id: r.tenant_id.clone(),
        request_id: r.request_id.clone(),
        action: RegistrationOperation::Register(EndpointDeclaration {
            capability_id: r.capability_id.clone(),
            grpc_authority: r.grpc_authority.clone(),
            plugin_version: r.plugin_version.clone(),
            api_version: r.api_version.clone(),
            manifest_sha256: r.manifest_sha256.clone(),
            declared_operations: r.declared_operations.clone(),
            labels: r.labels.clone().into_iter().collect(),
        }),
    }
}
pub(super) fn deregister_intent(r: &DeregisterPluginEndpointRequest) -> RegistrationRequest {
    RegistrationRequest {
        plugin_id: r.plugin_id.clone(),
        tenant_id: r.tenant_id.clone(),
        request_id: r.request_id.clone(),
        action: RegistrationOperation::Deregister,
    }
}
impl CapabilityGrpcServer {
    pub fn with_registration_verifier(mut self, verifier: flare_server_core::TokenService) -> Self {
        self.registration_verifier = Some(verifier);
        self
    }
    pub(super) fn registration_actor<T>(
        &self,
        request: &Request<T>,
        intent: &RegistrationRequest,
    ) -> Result<PluginRouteActor, Status> {
        if !request.metadata().contains_key(REGISTRATION_HEADER) {
            return self.plugin_route_actor(
                request,
                flare_server_core::auth::core_admin::permission::PLUGINS_WRITE,
                &intent.tenant_id,
            );
        }
        if request.metadata().contains_key("x-flare-ctx")
            || request.metadata().contains_key("authorization")
        {
            return Err(Status::unauthenticated(
                "ambiguous registration credentials",
            ));
        }
        let verifier = self
            .registration_verifier
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("registration delegation unavailable"))?;
        let mut values = request.metadata().get_all(REGISTRATION_HEADER).iter();
        let token = values
            .next()
            .and_then(|v| v.to_str().ok())
            .filter(|v| v.len() <= 8192)
            .ok_or_else(|| Status::unauthenticated("invalid registration credential"))?;
        if values.next().is_some() {
            return Err(Status::unauthenticated(
                "one registration credential required",
            ));
        }
        let audience = intent
            .audience()
            .map_err(|_| Status::invalid_argument("invalid registration request"))?;
        let claims = verifier
            .validate_ed25519_with_use(token, REGISTRATION_TOKEN_USE, &audience)
            .map_err(|_| Status::unauthenticated("invalid registration credential"))?;
        if claims.sub != intent.plugin_id
            || claims.tenant_id.as_deref() != Some(intent.tenant_id.as_str())
            || claims.exp <= claims.iat
            || claims.exp.saturating_sub(claims.iat) > REGISTRATION_TTL_SECS as usize
        {
            return Err(Status::permission_denied(
                "registration credential scope mismatch",
            ));
        }
        Ok(PluginRouteActor::Service {
            plugin_id: intent.plugin_id.clone(),
            request_id: intent.request_id.clone(),
        })
    }
}
