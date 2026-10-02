//! ADM-01 路由契约：每条路由的缺令牌 401、跨租户 403、写操作审计头 403，以及经回环
//! 替身 tonic 服务验证的成功响应形状与下游请求。
mod common;

use axum::http::StatusCode;
use common::*;

fn reason(body: &serde_json::Value) -> &str {
    body["reason"].as_str().unwrap_or_default()
}

#[tokio::test]
async fn every_ops_route_requires_an_admin_token() {
    let h = harness(true).await;
    for (method, path) in OPS_ROUTES {
        let extra = if *method == "POST" {
            WRITE_HEADERS
        } else {
            &[]
        };
        let (status, body) =
            send(&h.router, request(method, path, None, Some(TENANT), extra)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {path}");
        assert_eq!(reason(&body), "UNAUTHORIZED", "{method} {path}");

        // 格式不对的令牌与别的签名方签的令牌同样是 401。
        let forged = Console::new_with_seed(11).admin_token(EMPLOYEE, TENANT);
        for token in ["not-a-jwt", forged.as_str()] {
            let (status, _) = send(
                &h.router,
                request(method, path, Some(token), Some(TENANT), extra),
            )
            .await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {path}");
        }
    }
    assert!(h.recorder.calls().is_empty(), "{:?}", h.recorder.calls());
}

#[tokio::test]
async fn every_ops_route_rejects_a_foreign_or_missing_tenant_header() {
    let h = harness(true).await;
    for (method, path) in OPS_ROUTES {
        let extra = if *method == "POST" {
            WRITE_HEADERS
        } else {
            &[]
        };
        for tenant in [Some(OTHER_TENANT), None, Some("*")] {
            let (status, body) = send(
                &h.router,
                request(method, path, Some(&h.token), tenant, extra),
            )
            .await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{method} {path} {tenant:?}");
            assert_eq!(reason(&body), "FORBIDDEN", "{method} {path} {tenant:?}");
        }
    }
    assert!(h.recorder.calls().is_empty(), "{:?}", h.recorder.calls());
}

#[tokio::test]
async fn write_routes_require_actor_audit_reason_and_idempotency() {
    let h = harness(true).await;
    let writes: Vec<_> = OPS_ROUTES
        .iter()
        .filter(|(method, _)| *method == "POST")
        .collect();
    assert_eq!(writes.len(), 3);
    for (method, path) in &writes {
        let without = |dropped: &[&str]| -> Vec<(&str, &str)> {
            WRITE_HEADERS
                .iter()
                .copied()
                .filter(|(name, _)| !dropped.contains(name))
                .collect()
        };
        for dropped in [
            &["x-audit-reason"][..],
            &["idempotency-key"][..],
            &["x-actor-id"][..],
        ] {
            let headers = without(dropped);
            let (status, _) = send(
                &h.router,
                request(method, path, Some(&h.token), Some(TENANT), &headers),
            )
            .await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{path} without {dropped:?}");
        }
        // x-actor-id 与令牌主体不一致。
        let mut spoofed = without(&["x-actor-id"]);
        spoofed.push(("x-actor-id", "someone-else"));
        let (status, _) = send(
            &h.router,
            request(method, path, Some(&h.token), Some(TENANT), &spoofed),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{path} spoofed actor");
    }
    assert!(h.recorder.calls().is_empty(), "{:?}", h.recorder.calls());

    // idempotency-key 与 x-request-id 任一即可。
    for (method, path) in &writes {
        let mut with_request_id: Vec<_> = WRITE_HEADERS
            .iter()
            .copied()
            .filter(|(name, _)| *name != "idempotency-key")
            .collect();
        with_request_id.push(("x-request-id", "req-7"));
        let (status, body) = send(
            &h.router,
            request(method, path, Some(&h.token), Some(TENANT), &with_request_id),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{path} with x-request-id: {body}");
        let (status, body) = send(&h.router, admin_request(method, path, &h.token)).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{path} with idempotency-key: {body}"
        );
    }
}

#[tokio::test]
async fn read_routes_need_no_audit_headers() {
    let h = harness(true).await;
    for (method, path) in OPS_ROUTES.iter().filter(|(method, _)| *method == "GET") {
        let (status, body) = send(
            &h.router,
            request(method, path, Some(&h.token), Some(TENANT), &[]),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
    }
}

#[tokio::test]
async fn forged_identity_headers_never_reach_downstream() {
    let h = harness(true).await;
    for (method, path) in OPS_ROUTES {
        let mut extra: Vec<(&str, &str)> = if *method == "POST" {
            WRITE_HEADERS.to_vec()
        } else {
            Vec::new()
        };
        extra.extend([
            ("x-user-id", "victim-user"),
            ("x-actor-type", "system"),
            ("x-flare-ctx", "forged.context.token"),
        ]);
        let (status, body) = send(
            &h.router,
            request(method, path, Some(&h.token), Some(TENANT), &extra),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{method} {path}: {body}");
    }
    let calls = h.recorder.calls();
    assert!(calls.len() >= OPS_ROUTES.len());
    for call in &calls {
        assert_forwarded_only_the_assertion(call, &h.token);
    }
}

#[tokio::test]
async fn list_conversations_shape_and_paging() {
    let h = harness(true).await;
    let (status, body) = send(
        &h.router,
        admin_request(
            "GET",
            "/api/v1/admin/conversations?cursor=c1:prev&limit=20&include_deleted=true",
            &h.token,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let data = &body["data"];
    assert_eq!(
        keys(data),
        sorted(&["conversations", "next_cursor", "has_more", "limit"])
    );
    assert_eq!(data["limit"], 20);
    assert_eq!(data["has_more"], true);
    assert_eq!(data["next_cursor"], "c1:next");
    let conversation = &data["conversations"][0];
    assert_eq!(
        keys(conversation),
        sorted(&[
            "conversation_id",
            "conversation_type",
            "channel_id",
            "display_name",
            "avatar_url",
            "description",
            "visibility",
            "lifecycle_state",
            "member_count",
            "created_at",
            "updated_at",
            "attributes",
        ])
    );
    assert_eq!(conversation["visibility"], "tenant");
    assert_eq!(conversation["lifecycle_state"], "active");
    assert_eq!(conversation["updated_at"], 1_700_000_100_000_i64);
    let calls = h.recorder.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].method, "AdminListConversations");
    assert_eq!(
        calls[0].request,
        "cursor=c1:prev limit=20 include_deleted=true"
    );
    assert_forwarded_only_the_assertion(&calls[0], &h.token);

    // 缺省页大小 50，超过上限截到 200，非正数 400 且不调用下游。
    h.recorder.clear();
    for (query, limit) in [("", 50), ("?limit=9999", 200)] {
        let path = format!("/api/v1/admin/conversations{query}");
        let (status, body) = send(&h.router, admin_request("GET", &path, &h.token)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["data"]["limit"], limit);
    }
    let requests: Vec<_> = h.recorder.calls().into_iter().map(|c| c.request).collect();
    assert_eq!(
        requests,
        [
            "cursor= limit=50 include_deleted=false",
            "cursor= limit=200 include_deleted=false"
        ]
    );
    h.recorder.clear();
    let (status, body) = send(
        &h.router,
        admin_request("GET", "/api/v1/admin/conversations?limit=0", &h.token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(reason(&body), "ADMIN_PAGE_LIMIT_INVALID");
    assert!(h.recorder.calls().is_empty());

    // 类型不对的查询参数：网关自己的 400 信封 `ADMIN_INVALID_QUERY`，不回显取值，不调用下游
    // （原先是 axum 的纯文本 400，带着解析器说明与取值）。
    for query in ["limit=leak-marker-77", "include_deleted=leak-marker-77"] {
        let path = format!("/api/v1/admin/conversations?{query}");
        let (status, body) = send(&h.router, admin_request("GET", &path, &h.token)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}: {body}");
        assert_eq!(body["code"], 400, "{query}: {body}");
        assert_eq!(reason(&body), "ADMIN_INVALID_QUERY", "{query}: {body}");
        assert!(!body.to_string().contains("leak-marker-77"), "{body}");
    }
    assert!(h.recorder.calls().is_empty());
}

#[tokio::test]
async fn get_conversation_shape_and_downstream_rejections() {
    let h = harness(true).await;
    let (status, body) = send(
        &h.router,
        admin_request("GET", "/api/v1/admin/conversations/c1", &h.token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(keys(&body["data"]), sorted(&["conversation"]));
    assert_eq!(body["data"]["conversation"]["conversation_id"], "c1");
    assert_eq!(body["data"]["conversation"]["member_count"], 3);
    assert_eq!(h.recorder.calls()[0].request, "conversation_id=c1");

    for (id, expected, expected_reason) in [
        ("missing", StatusCode::NOT_FOUND, "NOT_FOUND"),
        ("denied", StatusCode::FORBIDDEN, "FORBIDDEN"),
        (
            "down",
            StatusCode::SERVICE_UNAVAILABLE,
            "ADMIN_UPSTREAM_UNAVAILABLE",
        ),
        (
            "slow",
            StatusCode::GATEWAY_TIMEOUT,
            "ADMIN_UPSTREAM_TIMEOUT",
        ),
    ] {
        let path = format!("/api/v1/admin/conversations/{id}");
        let (status, body) = send(&h.router, admin_request("GET", &path, &h.token)).await;
        assert_eq!(status, expected, "{id}");
        assert_eq!(reason(&body), expected_reason, "{id}");
        // 下游文案（可能含别的标识）不回显。
        assert!(!body.to_string().contains("id-secret-1"), "{body}");
    }
}

#[tokio::test]
async fn list_participants_shape() {
    let h = harness(true).await;
    let (status, body) = send(
        &h.router,
        admin_request(
            "GET",
            "/api/v1/admin/conversations/c1/participants?include_removed=true&limit=1000",
            &h.token,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let data = &body["data"];
    assert_eq!(
        keys(data),
        sorted(&[
            "conversation_id",
            "participants",
            "next_cursor",
            "has_more",
            "member_count",
            "limit",
        ])
    );
    assert_eq!(data["limit"], 500);
    assert_eq!(data["next_cursor"], serde_json::Value::Null);
    assert_eq!(
        keys(&data["participants"][0]),
        sorted(&[
            "user_id",
            "roles",
            "muted",
            "pinned",
            "mention_only",
            "visible_from_seq",
            "joined_at",
            "attributes",
        ])
    );
    assert_eq!(data["participants"][0]["visible_from_seq"], 12);
    assert_eq!(
        h.recorder.calls()[0].request,
        "conversation_id=c1 cursor= limit=500 include_removed=true"
    );
    let (status, _) = send(
        &h.router,
        admin_request(
            "GET",
            "/api/v1/admin/conversations/missing/participants",
            &h.token,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

const DEVICE_FIELDS: &[&str] = &[
    "device_id",
    "platform",
    "model",
    "os_version",
    "priority",
    "token_version",
    "last_active_at",
    "gateway_id",
    "server_id",
];

#[tokio::test]
async fn list_user_devices_shape() {
    let h = harness(true).await;
    let (status, body) = send(
        &h.router,
        admin_request("GET", "/api/v1/admin/users/u1/devices", &h.token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let data = &body["data"];
    assert_eq!(keys(data), sorted(&["user_id", "devices"]));
    assert_eq!(data["user_id"], "u1");
    let device = &data["devices"][0];
    assert_eq!(keys(device), sorted(DEVICE_FIELDS));
    assert_eq!(device["priority"], "high");
    assert_eq!(device["last_active_at"], 1_700_000_000_250_i64);
    assert_eq!(h.recorder.calls()[0].method, "AdminListUserDevices");
    assert_eq!(h.recorder.calls()[0].request, "user_id=u1");
}

#[tokio::test]
async fn get_user_presence_shape() {
    let h = harness(true).await;
    let (status, body) = send(
        &h.router,
        admin_request("GET", "/api/v1/admin/users/u1/presence", &h.token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let data = &body["data"];
    assert_eq!(
        keys(data),
        sorted(&["user_id", "online", "last_seen_at", "devices"])
    );
    assert_eq!(data["online"], true);
    assert_eq!(data["last_seen_at"], 1_700_000_000_000_i64);
    assert_eq!(keys(&data["devices"][0]), sorted(DEVICE_FIELDS));

    // 从未上线：Online 不回 presence，读作离线。
    let (status, body) = send(
        &h.router,
        admin_request("GET", "/api/v1/admin/users/never/presence", &h.token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["online"], false);
    assert_eq!(body["data"]["last_seen_at"], serde_json::Value::Null);
}

#[tokio::test]
async fn kick_user_kicks_every_listed_device() {
    let h = harness(true).await;
    let (status, body) = send(
        &h.router,
        admin_request("POST", "/api/v1/admin/users/u1/kick", &h.token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let data = &body["data"];
    assert_eq!(
        keys(data),
        sorted(&["user_id", "kicked", "already_offline"])
    );
    assert_eq!(data["kicked"], serde_json::json!(["d1"]));
    assert_eq!(data["already_offline"], serde_json::json!(["d-gone"]));
    let calls = h.recorder.calls();
    let methods: Vec<_> = calls.iter().map(|c| c.method).collect();
    assert_eq!(
        methods,
        ["AdminListUserDevices", "KickDevice", "KickDevice"]
    );
    // 审计原因原样作为踢线原因交给 Online。
    assert_eq!(
        calls[1].request,
        "user_id=u1 device_id=d1 reason=incident-42 containment"
    );
    for call in &calls {
        assert_forwarded_only_the_assertion(call, &h.token);
    }

    // 某台设备的会话属于别的租户：整个请求 403，不吞掉。
    let (status, body) = send(
        &h.router,
        admin_request("POST", "/api/v1/admin/users/u-foreign/kick", &h.token),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");

    // 没有在线设备：200，两份列表都为空。
    let (status, body) = send(
        &h.router,
        admin_request("POST", "/api/v1/admin/users/nobody/kick", &h.token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["kicked"], serde_json::json!([]));
}

#[tokio::test]
async fn kick_device_shape_and_rejections() {
    let h = harness(true).await;
    let (status, body) = send(
        &h.router,
        admin_request("POST", "/api/v1/admin/users/u1/devices/d1/kick", &h.token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let data = &body["data"];
    assert_eq!(keys(data), sorted(&["user_id", "device_id", "kicked"]));
    assert_eq!(data["kicked"], true);
    assert_eq!(
        h.recorder.calls()[0].request,
        "user_id=u1 device_id=d1 reason=incident-42 containment"
    );
    for (device, expected) in [
        ("d-gone", StatusCode::NOT_FOUND),
        ("d-foreign", StatusCode::FORBIDDEN),
        ("d-down", StatusCode::SERVICE_UNAVAILABLE),
    ] {
        let path = format!("/api/v1/admin/users/u1/devices/{device}/kick");
        let (status, body) = send(&h.router, admin_request("POST", &path, &h.token)).await;
        assert_eq!(status, expected, "{device}: {body}");
    }
}

#[tokio::test]
async fn kick_all_targets_only_the_token_tenant() {
    let h = harness(true).await;
    let (status, body) = send(
        &h.router,
        admin_request("POST", "/api/v1/admin/tenant/kick-all", &h.token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let data = &body["data"];
    assert_eq!(keys(data), sorted(&["tenant_id", "kicked"]));
    assert_eq!(data["tenant_id"], TENANT);
    assert_eq!(data["kicked"], 3);
    let calls = h.recorder.calls();
    assert_eq!(calls[0].method, "KickTenant");
    assert_eq!(
        calls[0].request,
        format!("tenant_id={TENANT} reason=incident-42 containment")
    );

    // 另一租户的员工只能踢自己的租户。
    h.recorder.clear();
    let other = h.console.admin_token(EMPLOYEE, OTHER_TENANT);
    let mut extra = WRITE_HEADERS.to_vec();
    extra.push(("x-request-id", "req-b"));
    let (status, body) = send(
        &h.router,
        request(
            "POST",
            "/api/v1/admin/tenant/kick-all",
            Some(&other),
            Some(OTHER_TENANT),
            &extra,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["tenant_id"], OTHER_TENANT);
    assert_eq!(
        h.recorder.calls()[0].request,
        format!("tenant_id={OTHER_TENANT} reason=incident-42 containment")
    );
    assert_forwarded_only_the_assertion(&h.recorder.calls()[0], &other);
}

#[tokio::test]
async fn tenant_runtime_shape() {
    let h = harness(true).await;
    let (status, body) = send(
        &h.router,
        admin_request("GET", "/api/v1/admin/tenant/runtime", &h.token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let data = &body["data"];
    assert_eq!(
        keys(data),
        sorted(&[
            "tenant_id",
            "projected",
            "name",
            "status",
            "quota",
            "settings_json",
            "version",
            "projected_at",
            "updated_at",
        ])
    );
    assert_eq!(data["status"], "suspended");
    assert_eq!(data["version"], 9);
    assert_eq!(data["settings_json"], r#"{"locale":"zh-CN"}"#);
    assert_eq!(
        keys(&data["quota"]),
        sorted(&[
            "max_users",
            "max_groups",
            "max_group_members",
            "send_qps",
            "sync_pull_qps",
            "max_online_devices_per_user",
        ])
    );
    assert_eq!(data["quota"]["max_online_devices_per_user"], 3);
    assert_eq!(data["projected_at"], 1_700_000_000_000_i64);
    assert_eq!(h.recorder.calls()[0].method, "GetTenantRuntime");
}

#[tokio::test]
async fn tenant_capabilities_shape() {
    let h = harness(true).await;
    let (status, body) = send(
        &h.router,
        admin_request("GET", "/api/v1/admin/capabilities/tenant", &h.token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let data = &body["data"];
    assert_eq!(keys(data), sorted(&["tenant_id", "switches"]));
    assert_eq!(
        keys(&data["switches"][0]),
        sorted(&["capability_id", "enabled", "updated_at"])
    );
    assert_eq!(data["switches"][1]["enabled"], false);
    assert_eq!(data["switches"][1]["updated_at"], serde_json::Value::Null);
    assert_eq!(h.recorder.calls()[0].method, "ListTenantCapabilitySwitches");
}

#[tokio::test]
async fn plugins_shape_and_filter() {
    let h = harness(true).await;
    let (status, body) = send(
        &h.router,
        admin_request(
            "GET",
            "/api/v1/admin/plugins?capability_id=rtc.call",
            &h.token,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let data = &body["data"];
    assert_eq!(keys(data), sorted(&["tenant_id", "plugins"]));
    assert_eq!(data["tenant_id"], TENANT);
    let ids: Vec<_> = data["plugins"]
        .as_array()
        .unwrap()
        .iter()
        .map(|plugin| plugin["plugin_id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids, ["plugin-a", "plugin-b"]);
    assert_eq!(
        keys(&data["plugins"][0]),
        sorted(&[
            "plugin_id",
            "capability_id",
            "grpc_authority",
            "plugin_version",
            "api_version",
            "manifest_sha256",
            "declared_operations",
            "unverified",
            "seat_model",
            "labels",
        ])
    );
    // 请求里的租户取自令牌，不取自任何头或查询参数。
    let call = &h.recorder.calls()[0];
    assert_eq!(call.method, "ListRegisteredPlugins");
    assert_eq!(
        call.request,
        format!("tenant_id={TENANT} capability_id=rtc.call")
    );
    assert_forwarded_only_the_assertion(call, &h.token);
}

#[tokio::test]
async fn capability_routes_answer_503_when_the_downstream_is_disabled() {
    let h = harness(false).await;
    for path in [
        "/api/v1/admin/tenant/runtime",
        "/api/v1/admin/capabilities/tenant",
        "/api/v1/admin/plugins",
    ] {
        let (status, body) = send(&h.router, admin_request("GET", path, &h.token)).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{path}");
        assert_eq!(reason(&body), "ADMIN_CAPABILITY_DISABLED", "{path}");
    }
    // 其他 ADM-01 路由不受影响。
    let (status, _) = send(
        &h.router,
        admin_request("GET", "/api/v1/admin/conversations", &h.token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(h.recorder.calls().len(), 1);
}

/// 进审计日志的三个请求头有长度上限：原因 512 字节，请求号与幂等键各 128 字节。超长回 400
/// `ADMIN_AUDIT_HEADER_TOO_LONG`，写与敏感读都一样，请求到不了下游；恰好在上限上的照常执行。
/// 修复前这些头不论多长都原样进审计日志。
#[tokio::test]
async fn over_long_audit_headers_are_400_and_never_reach_downstream() {
    let h = harness(false).await;
    let reason_at_limit = "r".repeat(512);
    let id_at_limit = "i".repeat(128);
    let cases = [
        ("POST", "/api/v1/admin/users/u1/kick", "x-audit-reason", 512),
        (
            "POST",
            "/api/v1/admin/users/u1/kick",
            "idempotency-key",
            128,
        ),
        ("POST", "/api/v1/admin/users/u1/kick", "x-request-id", 128),
        (
            "GET",
            "/api/v1/admin/users/u1/devices",
            "x-audit-reason",
            512,
        ),
        ("GET", "/api/v1/admin/users/u1/devices", "x-request-id", 128),
    ];
    for (method, path, name, max) in cases {
        let too_long = "x".repeat(max + 1);
        let mut extra: Vec<(&str, &str)> = if method == "POST" {
            WRITE_HEADERS
                .iter()
                .copied()
                .filter(|(header, _)| *header != name)
                .collect()
        } else {
            Vec::new()
        };
        extra.push((name, &too_long));
        let (status, body) = send(
            &h.router,
            request(method, path, Some(&h.token), Some(TENANT), &extra),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{method} {path} {name}: {body}"
        );
        assert_eq!(reason(&body), "ADMIN_AUDIT_HEADER_TOO_LONG", "{name}");
        assert!(h.recorder.calls().is_empty(), "{method} {path} {name}");
    }

    let at_limit = [
        ("x-actor-id", EMPLOYEE),
        ("x-audit-reason", reason_at_limit.as_str()),
        ("idempotency-key", id_at_limit.as_str()),
        ("x-request-id", id_at_limit.as_str()),
    ];
    let (status, body) = send(
        &h.router,
        request(
            "POST",
            "/api/v1/admin/users/u1/kick",
            Some(&h.token),
            Some(TENANT),
            &at_limit,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(!h.recorder.calls().is_empty());
}

/// 路径参数百分号解码后不是 UTF-8（`%FF`）：网关自己的 400 信封 `ADMIN_INVALID_PATH`，不回显取值，
/// 不调用下游。修复前是 axum 的纯文本 400（`Invalid URL: ...`，带着解码后的取值）。读写、单参数
/// 与双参数的路由都一样。
#[tokio::test]
async fn undecodable_path_parameters_are_typed_bad_requests_without_echo() {
    let h = harness(true).await;
    let cases = [
        ("GET", "/api/v1/admin/conversations/leak-marker-77%FF"),
        (
            "GET",
            "/api/v1/admin/conversations/leak-marker-77%FF/participants",
        ),
        ("GET", "/api/v1/admin/users/leak-marker-77%FF/devices"),
        ("GET", "/api/v1/admin/users/leak-marker-77%FF/presence"),
        ("POST", "/api/v1/admin/users/leak-marker-77%FF/kick"),
        (
            "POST",
            "/api/v1/admin/users/u1/devices/leak-marker-77%FF/kick",
        ),
    ];
    for (method, path) in cases {
        let (status, body) = send(&h.router, admin_request(method, path, &h.token)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{method} {path}: {body}");
        assert_eq!(body["code"], 400, "{method} {path}: {body}");
        assert_eq!(
            reason(&body),
            "ADMIN_INVALID_PATH",
            "{method} {path}: {body}"
        );
        assert!(!body.to_string().contains("leak-marker-77"), "{body}");
    }
    assert!(h.recorder.calls().is_empty());
}
