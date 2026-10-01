//! `GET /api/v1/admin/gateway/config` 的认证一节：如实报告 Admin Gateway 只用 Console Ed25519
//! 公钥验签，不转述随网关设置一起解析、但本服务从不使用的 `FLARE_ADMIN_GATEWAY_AUTH_*`。
//!
//! Control 原样转发这份快照，并在 flare-social 的管理契约里登记了它的结构，所以这里同时钉住
//! 响应里 `auth` 的字段集与 OpenAPI 的 `AdminAuthConfigSnapshot` 字段集。
mod common;

use std::collections::HashMap;

use axum::http::StatusCode;
use common::*;
use flare_im_service_kit::gateway::{GatewayEnvScope, GatewaySettings};
use serde_json::json;

const HOOK_URL: &str = "https://admin-auth.example.com/validate";
const HOOK_SECRET_MARKER: &str = "hook-secret-marker-not-a-real-key";

/// 部署方把 API Gateway 那套认证变量误配到 Admin 作用域：http_hook 带 URL 与 hook 密钥。
fn settings_with_unused_hook() -> GatewaySettings {
    let env = HashMap::from([
        ("FLARE_ADMIN_GATEWAY_AUTH_MODE", "http_hook"),
        ("FLARE_ADMIN_GATEWAY_AUTH_HOOK_URL", HOOK_URL),
        ("FLARE_ADMIN_GATEWAY_AUTH_HOOK_SECRET", HOOK_SECRET_MARKER),
        (
            "FLARE_ADMIN_GATEWAY_AUTH_HOOK_SECRET_HEADER",
            "x-custom-hook",
        ),
    ]);
    GatewaySettings::from_env_source(GatewayEnvScope::Admin, |key| {
        env.get(key).map(|value| value.to_string())
    })
    .expect("admin gateway settings")
}

#[tokio::test]
async fn gateway_config_reports_console_ed25519_and_no_hook() {
    let h = harness(true).await;
    let router = h
        .router
        .clone()
        .layer(axum::Extension(settings_with_unused_hook()));

    let (status, body) = send(
        &router,
        admin_request("GET", "/api/v1/admin/gateway/config", &h.token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let data = &body["data"];
    assert_eq!(
        keys(data),
        sorted(&[
            "server",
            "grpc",
            "auth",
            "rate_limit",
            "tracing",
            "redacted_fields"
        ])
    );
    assert_eq!(
        data["auth"],
        json!({
            "mode": "console_ed25519",
            "hook_url": null,
            "hook_timeout_ms": 0,
            "hook_secret_header": "",
            "hook_secret_configured": null,
        })
    );
    // 快照里没有密钥材料，也就没有打码字段。
    assert_eq!(data["redacted_fields"], json!([]));
    let rendered = body.to_string();
    for unused in [
        HOOK_URL,
        HOOK_SECRET_MARKER,
        "x-custom-hook",
        "http_hook",
        "core_jwt",
    ] {
        assert!(!rendered.contains(unused), "{unused} leaked: {rendered}");
    }

    // 未经 admin 认证仍是 401：快照本身也只对已验过的 admin 断言开放。
    let (status, _) = send(
        &router,
        request(
            "GET",
            "/api/v1/admin/gateway/config",
            None,
            Some(TENANT),
            &[],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn openapi_pins_the_auth_snapshot_fields() {
    let h = harness(true).await;
    let (status, doc) = send(
        &h.router,
        request("GET", "/api-doc/openapi.json", None, None, &[]),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let schema = &doc["components"]["schemas"]["AdminAuthConfigSnapshot"];
    assert_eq!(
        keys(&schema["properties"]),
        sorted(&[
            "mode",
            "hook_url",
            "hook_timeout_ms",
            "hook_secret_header",
            "hook_secret_configured",
        ]),
        "{schema}"
    );
    let description = schema["properties"]["mode"]["description"]
        .as_str()
        .unwrap_or_default();
    assert!(description.contains("console_ed25519"), "{schema}");
}
