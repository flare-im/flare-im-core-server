//! gRPC 侧通用辅助：上下文提取、领域错误映射、管理面鉴权。

use std::sync::Arc;

use tonic::metadata::MetadataMap;
use tonic::{Request, Status};

use crate::domain::capability::CapabilityError;
use flare_im_service_kit::control_auth::{
    ConsoleControlVerifier, ControlActorKind, VerifiedControlActor, require_grpc_control,
};

/// 从 metadata 取 `Ctx`；缺失时生成带新 `request_id` 的上下文（与 `ContextLayer::allow_missing` 配合）。
pub fn ctx_allow_missing<T>(req: &Request<T>) -> flare_im_contracts::Ctx {
    flare_server_core::middleware::get_context(req)
        .cloned()
        .unwrap_or_else(|| {
            Arc::new(flare_server_core::context::Context::with_request_id(
                uuid::Uuid::new_v4().to_string(),
            ))
        })
}

pub fn capability_error_to_status(e: CapabilityError) -> Status {
    let msg = e.to_string();
    match &e {
        CapabilityError::PolicyDenied(_) | CapabilityError::Rejected(_) => {
            Status::permission_denied(msg)
        }
        CapabilityError::NotRegistered(_) => Status::not_found(msg),
        CapabilityError::NotSupported(_) => Status::unimplemented(msg),
        CapabilityError::Timeout(_) => Status::deadline_exceeded(msg),
        _ => Status::internal(msg),
    }
}

pub fn metadata_ascii<'a>(m: &'a MetadataMap, key: &str) -> Option<&'a str> {
    m.get(key).and_then(|v| v.to_str().ok())
}

pub fn trace_id_from_request<T>(req: &Request<T>) -> Option<String> {
    metadata_ascii(req.metadata(), "x-trace-id").map(str::to_string)
}

/// Capability control calls require a Console-issued employee token. Legacy
/// metadata and the former shared secret are never authorization sources.
pub fn require_capability_policy_admin<T>(
    req: &Request<T>,
    verifier: Option<&ConsoleControlVerifier>,
) -> Result<VerifiedControlActor, Status> {
    let actor = require_grpc_control(req, verifier)?;
    actor.require_kind(ControlActorKind::Employee)?;
    Ok(actor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use flare_im_service_kit::control_auth::{
        CONSOLE_ISSUER, CORE_CONTROL_AUDIENCE, PROJECTION_WORKER_SUBJECT,
    };

    fn signed_request(subject: &str, audience: &str) -> (ConsoleControlVerifier, Request<()>) {
        let key = SigningKey::from_bytes(&[91; 32]);
        let private = key.to_pkcs8_pem(Default::default()).unwrap();
        let public = key
            .verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap();
        let signer =
            flare_server_core::TokenService::from_ed25519_private_key(&private, CONSOLE_ISSUER, 60)
                .unwrap();
        let token = signer
            .generate_with_ed25519(subject, Some("tenant-a"), "control", audience)
            .unwrap();
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        (
            ConsoleControlVerifier::from_public_key(&public).unwrap(),
            request,
        )
    }

    #[test]
    fn capability_admin_rejects_legacy_headers_and_non_employee_assertions() {
        let (verifier, mut request) = signed_request("employee-1", CORE_CONTROL_AUDIENCE);
        assert_eq!(
            require_capability_policy_admin(&request, Some(&verifier))
                .unwrap()
                .subject,
            "employee-1"
        );
        request.metadata_mut().remove("authorization");
        request
            .metadata_mut()
            .insert("x-capability-admin-secret", "old-secret".parse().unwrap());
        request
            .metadata_mut()
            .insert("x-actor-id", "employee-1".parse().unwrap());
        assert_eq!(
            require_capability_policy_admin(&request, Some(&verifier))
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
        assert_eq!(
            require_capability_policy_admin(&request, None)
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );

        let (_, worker) = signed_request(PROJECTION_WORKER_SUBJECT, CORE_CONTROL_AUDIENCE);
        assert_eq!(
            require_capability_policy_admin(&worker, Some(&verifier))
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        let (_, wrong_audience) = signed_request("employee-1", "flare-social-control");
        assert_eq!(
            require_capability_policy_admin(&wrong_audience, Some(&verifier))
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
    }
}
