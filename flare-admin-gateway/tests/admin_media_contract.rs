//! SEC-05 迁入的媒资管理路由契约，与 ADM-01 路由同口径：缺令牌 401、跨租户 403、写操作审计头
//! 403，经回环替身 Media 验证成功响应形状、下游收到的请求与签名身份，以及下游拒绝的类型化
//! 映射（不回显下游文案）。
mod common;

use axum::http::StatusCode;
use common::media::*;
use common::*;
use flare_server_core::SignedActorType;

fn reason(body: &serde_json::Value) -> &str {
    body["reason"].as_str().unwrap_or_default()
}

fn write_extra(method: &str) -> &'static [(&'static str, &'static str)] {
    if method == "POST" { WRITE_HEADERS } else { &[] }
}

#[tokio::test]
async fn every_media_route_requires_an_admin_token() {
    let h = media_harness(true, None).await;
    let forged = Console::new_with_seed(11).admin_token(EMPLOYEE, TENANT);
    for (method, path, body) in media_routes() {
        let extra = write_extra(method);
        let (status, response) = send(
            &h.router,
            media_request(method, &path, None, Some(TENANT), extra, body.as_deref()),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {path}");
        assert_eq!(reason(&response), "UNAUTHORIZED", "{method} {path}");

        // 格式不对的令牌与别的 Console 密钥签的令牌同样是 401。
        for token in ["not-a-jwt", forged.as_str()] {
            let (status, _) = send(
                &h.router,
                media_request(
                    method,
                    &path,
                    Some(token),
                    Some(TENANT),
                    extra,
                    body.as_deref(),
                ),
            )
            .await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {path}");
        }
    }
    assert!(h.media.calls().is_empty(), "{:?}", h.media.calls());
}

#[tokio::test]
async fn every_media_route_rejects_a_foreign_or_missing_tenant_header() {
    let h = media_harness(true, None).await;
    // 另一租户员工的令牌配本租户的头，同样 403。
    let other = h.console.admin_token(EMPLOYEE, OTHER_TENANT);
    for (method, path, body) in media_routes() {
        let extra = write_extra(method);
        for tenant in [Some(OTHER_TENANT), None, Some("*")] {
            let (status, response) = send(
                &h.router,
                media_request(
                    method,
                    &path,
                    Some(&h.token),
                    tenant,
                    extra,
                    body.as_deref(),
                ),
            )
            .await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{method} {path} {tenant:?}");
            assert_eq!(reason(&response), "FORBIDDEN", "{method} {path} {tenant:?}");
        }
        let (status, _) = send(
            &h.router,
            media_request(
                method,
                &path,
                Some(&other),
                Some(TENANT),
                extra,
                body.as_deref(),
            ),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "{method} {path} foreign token"
        );
    }
    assert!(h.media.calls().is_empty(), "{:?}", h.media.calls());
}

#[tokio::test]
async fn media_write_routes_require_actor_audit_reason_and_idempotency() {
    let h = media_harness(true, None).await;
    // 写标记与能力契约一致：两个 POST 是写，两个 GET 不是。
    let contract = flare_admin_gateway::interface::http::build_admin_capabilities();
    for (method, path, _) in media_routes() {
        let bare = path.split('?').next().unwrap();
        let endpoint = contract
            .endpoints
            .iter()
            .find(|endpoint| endpoint.path == bare)
            .unwrap_or_else(|| panic!("{bare} missing from the contract"));
        assert_eq!(endpoint.method, method, "{bare}");
        assert_eq!(endpoint.write, method == "POST", "{bare}");
    }

    let writes: Vec<_> = media_routes()
        .into_iter()
        .filter(|(method, _, _)| *method == "POST")
        .collect();
    assert_eq!(writes.len(), 2);
    let without = |dropped: &[&str]| -> Vec<(&str, &str)> {
        WRITE_HEADERS
            .iter()
            .copied()
            .filter(|(name, _)| !dropped.contains(name))
            .collect()
    };
    for (method, path, body) in &writes {
        for dropped in [
            &["x-audit-reason"][..],
            &["idempotency-key"][..],
            &["x-actor-id"][..],
        ] {
            let headers = without(dropped);
            let (status, response) = send(
                &h.router,
                media_request(
                    method,
                    path,
                    Some(&h.token),
                    Some(TENANT),
                    &headers,
                    body.as_deref(),
                ),
            )
            .await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{path} without {dropped:?}");
            assert_eq!(reason(&response), "FORBIDDEN", "{path} without {dropped:?}");
        }
        let mut spoofed = without(&["x-actor-id"]);
        spoofed.push(("x-actor-id", "someone-else"));
        let (status, _) = send(
            &h.router,
            media_request(
                method,
                path,
                Some(&h.token),
                Some(TENANT),
                &spoofed,
                body.as_deref(),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{path} spoofed actor");
    }
    assert!(h.media.calls().is_empty(), "{:?}", h.media.calls());

    // idempotency-key 与 x-request-id 任一即可；只读的两条 GET 不需要审计头。
    for (method, path, body) in &writes {
        let mut with_request_id = without(&["idempotency-key"]);
        with_request_id.push(("x-request-id", "req-7"));
        let (status, response) = send(
            &h.router,
            media_request(
                method,
                path,
                Some(&h.token),
                Some(TENANT),
                &with_request_id,
                body.as_deref(),
            ),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{path} with x-request-id: {response}"
        );
        let (status, response) = send(
            &h.router,
            media_admin_request(method, path, &h.token, body.as_deref()),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{path} with idempotency-key: {response}"
        );
    }
    for (method, path, _) in media_routes().into_iter().filter(|(m, _, _)| *m == "GET") {
        let (status, response) = send(
            &h.router,
            media_request(method, &path, Some(&h.token), Some(TENANT), &[], None),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{path}: {response}");
    }
}

#[tokio::test]
async fn media_calls_carry_only_the_signed_tenant_admin_of_the_token() {
    let h = media_harness(true, None).await;
    let other = h.console.admin_token(EMPLOYEE, OTHER_TENANT);
    let forged_values = ["victim-user", "system", "forged.context.token", "tenant-z"];
    for (token, tenant) in [(&h.token, TENANT), (&other, OTHER_TENANT)] {
        for (method, path, body) in media_routes() {
            h.media.clear();
            let mut extra: Vec<(&str, &str)> = write_extra(method).to_vec();
            extra.retain(|(name, _)| *name != "idempotency-key");
            extra.extend([
                ("x-request-id", "req-media-1"),
                // 伪造的身份头：签名身份与转发给 Media 的元数据都不能受它们影响。
                ("x-user-id", "victim-user"),
                ("x-actor-type", "system"),
                ("x-flare-ctx", "forged.context.token"),
                ("x-device-id", "tenant-z"),
            ]);
            let (status, response) = send(
                &h.router,
                media_request(
                    method,
                    &path,
                    Some(token),
                    Some(tenant),
                    &extra,
                    body.as_deref(),
                ),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{method} {path}: {response}");

            let calls = h.media.calls();
            assert_eq!(calls.len(), 1, "{path}");
            let call = &calls[0];
            assert_eq!(call.signed_context.len(), 1, "{}", call.method);
            let claims = h
                .cell
                .verify(&call.signed_context[0])
                .unwrap_or_else(|_| panic!("{} context does not verify", call.method));
            assert_eq!(
                claims.actor_type,
                SignedActorType::TenantAdmin,
                "{}",
                call.method
            );
            assert_eq!(claims.tenant_id, tenant, "{}", call.method);
            assert_eq!(claims.actor_id, EMPLOYEE, "{}", call.method);
            assert_eq!(claims.request_id, "req-media-1", "{}", call.method);
            // admin 令牌不交给 Media：Media 只认 Cell 签名上下文。
            assert!(call.authorization.is_empty(), "{}", call.method);
            for (key, value) in &call.x_metadata {
                assert!(
                    !forged_values.contains(&value.as_str()),
                    "{} received forged {key}={value}",
                    call.method
                );
            }
        }
    }
}

#[tokio::test]
async fn cleanup_shape_and_request() {
    let h = media_harness(true, None).await;
    let (status, body) = send(
        &h.router,
        media_admin_request("POST", CLEANUP, &h.token, Some(&cleanup_body(50))),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["code"], 0);
    let data = &body["data"];
    assert_eq!(
        keys(data),
        sorted(&["file_ids", "success", "error_message", "scanned"])
    );
    assert_eq!(
        data["file_ids"],
        serde_json::json!(["f-orphan-1", "f-orphan-2"])
    );
    assert_eq!(data["scanned"], 2);
    assert_eq!(data["success"], true);
    let calls = h.media.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].method, "CleanupOrphanedAssets");
    assert_eq!(calls[0].request, "limit=50 dry_run=false");

    // limit 原样交给 Media（0 表示 Media 的缺省 100，上限 1000 由 Media 截断）。
    h.media.clear();
    let (status, _) = send(
        &h.router,
        media_admin_request("POST", CLEANUP, &h.token, Some(&cleanup_body(0))),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(h.media.calls()[0].request, "limit=0 dry_run=false");

    // Media 不支持演练，dry_run=true 会照常删除：网关直接 400，不调用 Media。
    h.media.clear();
    let (status, body) = send(
        &h.router,
        media_admin_request(
            "POST",
            CLEANUP,
            &h.token,
            Some(r#"{"limit":10,"dry_run":true}"#),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(reason(&body), "ADMIN_MEDIA_DRY_RUN_UNSUPPORTED");
    // 空体或缺字段不会触发一次清理。
    for invalid in ["{}", r#"{"limit":10}"#, "not json"] {
        let (status, _) = send(
            &h.router,
            media_admin_request("POST", CLEANUP, &h.token, Some(invalid)),
        )
        .await;
        assert!(status.is_client_error(), "{invalid}: {status}");
    }
    assert!(h.media.calls().is_empty(), "{:?}", h.media.calls());
}

#[tokio::test]
async fn object_acl_shape_and_request() {
    let h = media_harness(true, None).await;
    let (status, body) = send(
        &h.router,
        media_admin_request("POST", OBJECT_ACL, &h.token, Some(&acl_body("f-1"))),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["code"], 0);
    assert_eq!(body["data"], true);
    let calls = h.media.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].method, "SetObjectAcl");
    assert_eq!(
        calls[0].request,
        "file_id=f-1 entries=role:auditor:read,user:u-owner:read+write"
    );

    // 缺 entries 或不是 JSON 都在网关拒绝，不调用 Media。
    h.media.clear();
    for invalid in [r#"{"file_id":"f-1"}"#, "not json"] {
        let (status, _) = send(
            &h.router,
            media_admin_request("POST", OBJECT_ACL, &h.token, Some(invalid)),
        )
        .await;
        assert!(status.is_client_error(), "{invalid}: {status}");
    }
    assert!(h.media.calls().is_empty(), "{:?}", h.media.calls());
}

#[tokio::test]
async fn list_objects_shape_and_request() {
    let h = media_harness(true, None).await;
    let path = format!("{OBJECTS}?bucket=media&prefix=2026/");
    let (status, body) = send(&h.router, media_admin_request("GET", &path, &h.token, None)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let data = &body["data"];
    assert_eq!(keys(data), sorted(&["files"]));
    let files = data["files"].as_array().unwrap();
    assert_eq!(files.len(), 2);
    // 只给五个字段：签名 URL、CDN 地址、租户、桶与扩展元数据都不出网关。
    assert_eq!(
        keys(&files[0]),
        sorted(&["file_id", "file_name", "mime_type", "size", "object_key"])
    );
    assert_eq!(files[0]["file_id"], "f-1");
    assert_eq!(files[0]["size"], 2048);
    assert_eq!(files[0]["object_key"], format!("{TENANT}/2026/f-1.png"));
    assert!(!body.to_string().contains("X-Amz-Signature"), "{body}");
    let calls = h.media.calls();
    assert_eq!(calls[0].method, "ListObjects");
    assert_eq!(
        calls[0].request,
        "bucket=media prefix=2026/ pagination=false filters=0 sort=0"
    );

    // prefix 可省略；bucket 缺失是 400，不调用 Media。
    h.media.clear();
    let (status, _) = send(
        &h.router,
        media_admin_request("GET", &format!("{OBJECTS}?bucket=media"), &h.token, None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        h.media.calls()[0].request,
        "bucket=media prefix= pagination=false filters=0 sort=0"
    );
    h.media.clear();
    let (status, _) = send(
        &h.router,
        media_admin_request("GET", OBJECTS, &h.token, None),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(h.media.calls().is_empty());
}

#[tokio::test]
async fn bucket_shape_and_request() {
    let h = media_harness(true, None).await;
    let (status, body) = send(
        &h.router,
        media_admin_request("GET", &format!("{BUCKET}?bucket=media"), &h.token, None),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        keys(&body["data"]),
        sorted(&[
            "bucket",
            "region",
            "storage_class",
            "versioning_enabled",
            "metadata"
        ])
    );
    assert_eq!(
        body["data"]["metadata"]["tenant_prefix"],
        format!("{TENANT}/")
    );
    assert_eq!(h.media.calls()[0].request, "bucket=media");

    // bucket 缺失是 400，不调用 Media。
    h.media.clear();
    let (status, _) = send(
        &h.router,
        media_admin_request("GET", BUCKET, &h.token, None),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(h.media.calls().is_empty(), "{:?}", h.media.calls());
}

#[tokio::test]
async fn media_rejections_are_typed_and_never_echo_media_text() {
    let h = media_harness(true, None).await;
    // (失败选择键, 期望状态, 期望 reason)；500 的 reason 按路由固定。
    let cases = [
        ("denied", StatusCode::FORBIDDEN, "FORBIDDEN"),
        (
            "down",
            StatusCode::SERVICE_UNAVAILABLE,
            "ADMIN_UPSTREAM_UNAVAILABLE",
        ),
        (
            "rejected",
            StatusCode::BAD_GATEWAY,
            "ADMIN_UPSTREAM_REJECTED_ASSERTION",
        ),
        ("unsupported", StatusCode::INTERNAL_SERVER_ERROR, ""),
    ];
    let limit_of = |key: &str| match key {
        "denied" => 403,
        "down" => 503,
        "rejected" => 401,
        _ => 500,
    };
    for (key, expected, expected_reason) in cases {
        let requests = [
            (
                "POST",
                CLEANUP.to_string(),
                Some(cleanup_body(limit_of(key))),
                "ADMIN_MEDIA_CLEANUP_FAILED",
            ),
            (
                "POST",
                OBJECT_ACL.to_string(),
                Some(acl_body(key)),
                "ADMIN_MEDIA_OBJECT_ACL_FAILED",
            ),
            (
                "GET",
                format!("{OBJECTS}?bucket={key}"),
                None,
                "ADMIN_MEDIA_LIST_OBJECTS_FAILED",
            ),
            (
                "GET",
                format!("{BUCKET}?bucket={key}"),
                None,
                "ADMIN_MEDIA_BUCKET_FAILED",
            ),
        ];
        for (method, path, body, failure_reason) in requests {
            let (status, response) = send(
                &h.router,
                media_admin_request(method, &path, &h.token, body.as_deref()),
            )
            .await;
            assert_eq!(status, expected, "{key} {path}: {response}");
            let want = if expected_reason.is_empty() {
                failure_reason
            } else {
                expected_reason
            };
            assert_eq!(reason(&response), want, "{key} {path}");
            assert!(
                !response.to_string().contains(SECRET_MARKER),
                "{key} {path} echoed Media text: {response}"
            );
        }
    }

    // 文件不在本租户：404，同样不回显。
    let (status, response) = send(
        &h.router,
        media_admin_request("POST", OBJECT_ACL, &h.token, Some(&acl_body("missing"))),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{response}");
    assert_eq!(reason(&response), "NOT_FOUND");
    assert!(!response.to_string().contains(SECRET_MARKER));
}

#[tokio::test]
async fn media_routes_without_a_signer_never_call_media() {
    let h = media_harness(false, None).await;
    for (method, path, body) in media_routes() {
        let (status, response) = send(
            &h.router,
            media_admin_request(method, &path, &h.token, body.as_deref()),
        )
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{path}");
        assert_eq!(reason(&response), "MEDIA_SIGNER_UNAVAILABLE", "{path}");
    }
    assert!(h.media.calls().is_empty(), "{:?}", h.media.calls());
}

#[tokio::test]
async fn media_signed_context_layer_accepts_the_gateway_and_rejects_another_cell_key() {
    // Media 实际使用的 SignedContextLayer，配与网关私钥成对的公钥：四条路由都通过。
    let (_, cell) = cell_keys(62);
    let h = media_harness(true, Some(cell)).await;
    for (method, path, body) in media_routes() {
        let (status, response) = send(
            &h.router,
            media_admin_request(method, &path, &h.token, body.as_deref()),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{path}: {response}");
    }
    assert_eq!(h.media.calls().len(), 4);

    // 公钥换成另一个 Cell 的：层内 UNAUTHENTICATED，handler 不执行，网关报 502。
    let (_, foreign) = cell_keys(63);
    let h = media_harness(true, Some(foreign)).await;
    for (method, path, body) in media_routes() {
        let (status, response) = send(
            &h.router,
            media_admin_request(method, &path, &h.token, body.as_deref()),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_GATEWAY, "{path}: {response}");
        assert_eq!(
            reason(&response),
            "ADMIN_UPSTREAM_REJECTED_ASSERTION",
            "{path}"
        );
    }
    assert!(h.media.calls().is_empty(), "{:?}", h.media.calls());
}
