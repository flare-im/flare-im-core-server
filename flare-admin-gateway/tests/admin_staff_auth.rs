//! 管理边界的身份负向用例（v4 R3-2）：唯一身份是网关 admin 分区按 `kid=gateway` 签的 Staff
//! `x-flare-ctx`；平台签的 admin 断言（`authorization: Bearer`）、hook 钥签的 Staff 上下文、
//! 非 Staff 的签名上下文都是 401，权限码不符与组织员工调平台专属路由是 403；单钥部署下只认
//! `kid=gateway` 的上下文（没有 kid 的上下文按 `legacy` 钥名被拒）。
mod common;

use std::sync::Arc;

use axum::http::StatusCode;
use common::*;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
use flare_admin_gateway::interface::http::create_admin_router;
use flare_im_service_kit::audit_effects::AuditEffects;
use flare_im_service_kit::control_auth::{CORE_PROJECTION_AUDIENCE, PLATFORM_ISSUER};
use flare_im_service_kit::staff_context::admin_staff_verifier_from_sources;
use flare_server_core::auth::kid;
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, TokenService,
};
use tonic::transport::Channel;

fn reason(body: &serde_json::Value) -> &str {
    body["reason"].as_str().unwrap_or_default()
}

const READ: (&str, &str) = ("GET", "/api/v1/admin/conversations");
const WRITE: (&str, &str) = ("POST", "/api/v1/admin/users/u1/kick");

/// A platform-signed assertion of the retired admin shape (and of the live control shape): the
/// gateway holds no platform key and reads no bearer, so both are 401 before any route logic.
#[tokio::test]
async fn platform_assertions_in_authorization_are_not_an_identity() {
    let h = harness(true).await;
    let key = SigningKey::from_bytes(&[91; 32]);
    let private = key.to_pkcs8_pem(Default::default()).unwrap();
    let platform = TokenService::from_ed25519_private_key(&private, PLATFORM_ISSUER, 60).unwrap();
    let admin = platform
        .generate_with_ed25519(EMPLOYEE, Some(TENANT), "admin", "flare-core-admin")
        .unwrap();
    let control = platform
        .generate_with_ed25519(EMPLOYEE, Some(TENANT), "control", CORE_PROJECTION_AUDIENCE)
        .unwrap();
    for token in [admin, control] {
        for (method, path) in [READ, WRITE] {
            let mut extra = vec![("x-tenant-id", TENANT), ("x-actor-id", EMPLOYEE)];
            if is_write(method, path) {
                extra.extend(WRITE_HEADERS.iter().copied());
            }
            let bearer = format!("Bearer {token}");
            extra.push(("authorization", bearer.as_str()));
            let (status, body) = send(&h.router, request(method, path, None, &extra)).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {path}: {body}");
            assert_eq!(reason(&body), "UNAUTHORIZED", "{method} {path}");
        }
    }
    assert!(h.recorder.calls().is_empty());
}

#[tokio::test]
async fn a_staff_context_signed_by_the_hook_key_is_refused() {
    let h = harness(true).await;
    for (method, path) in [READ, WRITE] {
        let route = flare_server_core::auth::core_admin::resolve(
            method,
            path.strip_prefix("/api/v1/admin").unwrap(),
        )
        .unwrap();
        let forged = h.gateway.hook_signed_staff_token(TENANT, route.permission);
        let extra = if route.write { WRITE_HEADERS } else { &[] };
        let (status, body) = send(&h.router, request(method, path, Some(&forged), extra)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {path}: {body}");
        assert_eq!(
            reason(&body),
            "ADMIN_STAFF_CONTEXT_INVALID",
            "{method} {path}"
        );
    }
    assert!(h.recorder.calls().is_empty());
}

#[tokio::test]
async fn non_staff_duplicate_and_platform_scoped_contexts_are_refused() {
    let h = harness(true).await;
    // A Service context signed by the gateway key verifies, but is not staff.
    let service = h.gateway.service_token(TENANT);
    let (status, body) = send(&h.router, request(READ.0, READ.1, Some(&service), &[])).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    assert_eq!(reason(&body), "ADMIN_STAFF_CONTEXT_INVALID");

    // Two contexts, even identical ones.
    let token = h.gateway.token_for(READ.0, READ.1);
    let (status, body) = send(
        &h.router,
        request(
            READ.0,
            READ.1,
            Some(&token),
            &[("x-flare-ctx", token.as_str())],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    assert_eq!(reason(&body), "ADMIN_STAFF_CONTEXT_INVALID");

    // Platform-scoped staff work (empty application) has no Core counterpart.
    let platform_scoped = h
        .gateway
        .platform_staff_token(EMPLOYEE, "", "core:conversations:read");
    let (status, body) = send(
        &h.router,
        request(READ.0, READ.1, Some(&platform_scoped), &[]),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(reason(&body), "ADMIN_IDENTITY_REQUIRED");
    assert!(h.recorder.calls().is_empty());
}

#[tokio::test]
async fn permission_and_scope_mismatches_are_403_with_typed_reasons() {
    let h = harness(true).await;
    // The codes of other routes.
    for permission in ["core:users:read", "core:messages:read"] {
        let token = h.gateway.staff_token(EMPLOYEE, TENANT, permission);
        let (status, body) = send(&h.router, request(READ.0, READ.1, Some(&token), &[])).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{permission}: {body}");
        assert_eq!(reason(&body), "ADMIN_PERMISSION_DENIED", "{permission}");
    }
    // A verified staff context that carries no permission claim at all, on a read and on a
    // write with every audit header: no route's code matches it.
    let codeless = h.gateway.staff_token_without_permission(EMPLOYEE, TENANT);
    for (method, path) in [READ, WRITE] {
        let extra = if is_write(method, path) {
            WRITE_HEADERS
        } else {
            &[]
        };
        let (status, body) = send(&h.router, request(method, path, Some(&codeless), extra)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} {path}: {body}");
        assert_eq!(reason(&body), "ADMIN_PERMISSION_DENIED", "{method} {path}");
    }
    // Organization staff on a platform-only route, with the right code.
    let org = h
        .gateway
        .staff_token(EMPLOYEE, TENANT, "core:plugins:write");
    let (status, body) = send(
        &h.router,
        request_with_body(
            "POST",
            "/api/v1/admin/plugins/register",
            Some(&org),
            WRITE_HEADERS,
            Some(r#"{"plugin_id":"p","capability_id":"c","grpc_authority":"http://p:9","plugin_version":"1.0.0","api_version":"1.0","manifest_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","declared_operations":["c"]}"#),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(reason(&body), "ADMIN_SCOPE_DENIED");
    assert!(h.recorder.calls().is_empty());
}

/// Until per-component keys exist a deployment holds one Cell key, registered as `legacy`:
/// a staff context naming `kid=gateway` resolves to that key and passes; a context without a
/// kid is `legacy`, which the Staff pin does not accept, so the admin partition must set the
/// gateway kid on every deployment.
#[tokio::test]
async fn a_single_key_deployment_verifies_the_gateway_kid_with_its_only_key() {
    let key = SigningKey::from_bytes(&[92; 32]);
    let private = key.to_pkcs8_pem(Default::default()).unwrap();
    let public = key
        .verifying_key()
        .to_public_key_pem(Default::default())
        .unwrap();
    // The production constructor on the single legacy key.
    let verifier =
        admin_staff_verifier_from_sources("admin-gateway", None, Some(public.as_str())).unwrap();
    assert!(verifier.key_set().is_single_key());
    // The downstreams hold the same single Cell key.
    let (address, recorder) = spawn_fakes(verifier.clone()).await;
    let channel = Channel::from_shared(format!("http://{address}"))
        .unwrap()
        .connect_lazy();
    let unused = Channel::from_static("http://127.0.0.1:9").connect_lazy();
    let router = create_admin_router(Arc::new(clients(channel, unused.clone(), unused)), None)
        .layer(axum::Extension(Arc::new(verifier)))
        .layer(axum::Extension(Arc::new(AuditEffects::disabled(
            "admin-gateway",
        ))));
    let identity = SignedContextIdentity::new(SignedActorType::Staff, EMPLOYEE, TENANT, "req-1")
        .with_staff_grant(None, Some("core:conversations:read".to_string()), 1);

    let with_kid = SignedContextSigner::from_private_key(&private)
        .unwrap()
        .with_kid(kid::GATEWAY)
        .unwrap()
        .sign(&identity)
        .unwrap();
    let (status, body) = send(&router, request(READ.0, READ.1, Some(&with_kid), &[])).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(recorder.calls().len(), 1);

    let kid_less = SignedContextSigner::from_private_key(&private)
        .unwrap()
        .sign(&identity)
        .unwrap();
    let (status, body) = send(&router, request(READ.0, READ.1, Some(&kid_less), &[])).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    assert_eq!(reason(&body), "ADMIN_STAFF_CONTEXT_INVALID");
    assert_eq!(recorder.calls().len(), 1);
}

#[tokio::test]
async fn auth_check_echoes_the_signed_staff_grant() {
    let h = harness(true).await;
    let token = h.gateway.token_for("GET", "/api/v1/admin/auth/check");
    let (status, body) = send(
        &h.router,
        request(
            "GET",
            "/api/v1/admin/auth/check",
            Some(&token),
            &[("x-tenant-id", OTHER_TENANT), ("x-actor-id", "attacker")],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let data = &body["data"];
    assert_eq!(data["user_id"], EMPLOYEE);
    assert_eq!(data["actor_id"], EMPLOYEE);
    assert_eq!(data["tenant_id"], TENANT);
    assert_eq!(data["org_id"], "org-1");
    assert_eq!(data["permission"], "core:gateway:read");
    assert_eq!(data["request_id"], "req-staff-1");
    assert_eq!(data["trace_id"], "trace-staff-1");
    assert_eq!(data["scopes"], serde_json::json!(["admin_gateway:admin"]));
}
