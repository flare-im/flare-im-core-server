//! ADM-01 路由契约：每条路由的缺上下文 401、权限码不符 403、写操作审计头 403，以及经回环
//! 替身 tonic 服务验证的成功响应形状与下游请求。
mod common;

use axum::http::StatusCode;
use common::*;

fn reason(body: &serde_json::Value) -> &str {
    body["reason"].as_str().unwrap_or_default()
}

fn write_extra(method: &str, path: &str) -> &'static [(&'static str, &'static str)] {
    if is_write(method, path) {
        WRITE_HEADERS
    } else {
        &[]
    }
}

#[tokio::test]
async fn every_ops_route_requires_a_staff_context() {
    let h = harness(true).await;
    for (method, path) in OPS_ROUTES {
        let extra = write_extra(method, path);
        let (status, body) = send(&h.router, request(method, path, None, extra)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {path}");
        assert_eq!(reason(&body), "UNAUTHORIZED", "{method} {path}");

        // 格式不对的上下文与别的 Cell 密钥签的上下文同样是 401。
        let foreign = Gateway::new_with_seed(11).token_for(method, path);
        for token in ["not-a-jwt", foreign.as_str()] {
            let (status, body) = send(&h.router, request(method, path, Some(token), extra)).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {path}");
            assert_eq!(
                reason(&body),
                "ADMIN_STAFF_CONTEXT_INVALID",
                "{method} {path}"
            );
        }
    }
    assert!(h.recorder.calls().is_empty(), "{:?}", h.recorder.calls());
}

#[tokio::test]
async fn every_ops_route_rejects_another_routes_permission_code() {
    let h = harness(true).await;
    for (method, path) in OPS_ROUTES {
        let extra = write_extra(method, path);
        // 另一条路由的权限码，以及没有权限码的 Staff 上下文。
        let other = h.gateway.staff_token(EMPLOYEE, TENANT, "core:gateway:read");
        let codeless = h.gateway.staff_token_without_permission(EMPLOYEE, TENANT);
        for token in [other, codeless] {
            let (status, body) = send(&h.router, request(method, path, Some(&token), extra)).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{method} {path}");
            assert_eq!(reason(&body), "ADMIN_PERMISSION_DENIED", "{method} {path}");
        }
    }
    assert!(h.recorder.calls().is_empty(), "{:?}", h.recorder.calls());
}

#[tokio::test]
async fn write_routes_require_audit_reason_and_idempotency() {
    let h = harness(true).await;
    let writes: Vec<_> = OPS_ROUTES
        .iter()
        .filter(|(method, path)| is_write(method, path))
        .collect();
    assert_eq!(writes.len(), 3);
    for (method, path) in &writes {
        let token = h.gateway.token_for(method, path);
        let without = |dropped: &[&str]| -> Vec<(&str, &str)> {
            WRITE_HEADERS
                .iter()
                .copied()
                .filter(|(name, _)| !dropped.contains(name))
                .collect()
        };
        for dropped in [&["x-audit-reason"][..], &["idempotency-key"][..]] {
            let headers = without(dropped);
            let (status, body) =
                send(&h.router, request(method, path, Some(&token), &headers)).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{path} without {dropped:?}");
            assert_eq!(reason(&body), "FORBIDDEN", "{path} without {dropped:?}");
        }
    }
    assert!(h.recorder.calls().is_empty(), "{:?}", h.recorder.calls());

    // idempotency-key 与 x-request-id 任一即可；x-actor-id / x-tenant-id 不再需要，带了也不看。
    for (method, path) in &writes {
        let token = h.gateway.token_for(method, path);
        let mut with_request_id: Vec<_> = WRITE_HEADERS
            .iter()
            .copied()
            .filter(|(name, _)| *name != "idempotency-key")
            .collect();
        with_request_id.push(("x-request-id", "req-7"));
        with_request_id.push(("x-actor-id", "someone-else"));
        with_request_id.push(("x-tenant-id", OTHER_TENANT));
        let (status, body) = send(
            &h.router,
            request(method, path, Some(&token), &with_request_id),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{path} with x-request-id: {body}");
        let (status, body) = send(&h.router, admin_request(method, path, &h.gateway)).await;
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
        let token = h.gateway.token_for(method, path);
        let (status, body) = send(&h.router, request(method, path, Some(&token), &[])).await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
    }
}

#[tokio::test]
async fn forged_identity_headers_never_reach_downstream() {
    let h = harness(true).await;
    for (method, path) in OPS_ROUTES {
        let mut extra: Vec<(&str, &str)> = write_extra(method, path).to_vec();
        extra.extend([
            ("x-user-id", "victim-user"),
            ("x-tenant-id", OTHER_TENANT),
            ("x-actor-id", "attacker"),
            ("x-actor-type", "system"),
            ("authorization", "Bearer forged.platform.assertion"),
        ]);
        let token = h.gateway.token_for(method, path);
        let (status, body) = send(&h.router, request(method, path, Some(&token), &extra)).await;
        assert_eq!(status, StatusCode::OK, "{method} {path}: {body}");
        for call in h.recorder.calls() {
            assert_eq!(call.signed_context, vec![token.clone()], "{method} {path}");
            assert_forwarded_for_route(&call, &h.gateway, method, path);
        }
        h.recorder.clear();
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
            &h.gateway,
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
    assert_forwarded_for_route(&calls[0], &h.gateway, "GET", "/api/v1/admin/conversations");

    // 缺省页大小 50，超过上限截到 200，非正数 400 且不调用下游。
    h.recorder.clear();
    for (query, limit) in [("", 50), ("?limit=9999", 200)] {
        let path = format!("/api/v1/admin/conversations{query}");
        let (status, body) = send(&h.router, admin_request("GET", &path, &h.gateway)).await;
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
        admin_request("GET", "/api/v1/admin/conversations?limit=0", &h.gateway),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(reason(&body), "ADMIN_PAGE_LIMIT_INVALID");
    assert!(h.recorder.calls().is_empty());

    // 类型不对的查询参数：网关自己的 400 信封 `ADMIN_INVALID_QUERY`，不回显取值，不调用下游。
    for query in ["limit=leak-marker-77", "include_deleted=leak-marker-77"] {
        let path = format!("/api/v1/admin/conversations?{query}");
        let (status, body) = send(&h.router, admin_request("GET", &path, &h.gateway)).await;
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
        admin_request("GET", "/api/v1/admin/conversations/c1", &h.gateway),
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
        let (status, body) = send(&h.router, admin_request("GET", &path, &h.gateway)).await;
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
            &h.gateway,
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
            &h.gateway,
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
        admin_request("GET", "/api/v1/admin/users/u1/devices", &h.gateway),
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
        admin_request("GET", "/api/v1/admin/users/u1/presence", &h.gateway),
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
        admin_request("GET", "/api/v1/admin/users/never/presence", &h.gateway),
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
        admin_request("POST", "/api/v1/admin/users/u1/kick", &h.gateway),
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
        assert_forwarded_for_route(call, &h.gateway, "POST", "/api/v1/admin/users/u1/kick");
    }

    // 某台设备的会话属于别的租户：整个请求 403，不吞掉。
    let (status, body) = send(
        &h.router,
        admin_request("POST", "/api/v1/admin/users/u-foreign/kick", &h.gateway),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");

    // 没有在线设备：200，两份列表都为空。
    let (status, body) = send(
        &h.router,
        admin_request("POST", "/api/v1/admin/users/nobody/kick", &h.gateway),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["kicked"], serde_json::json!([]));
}

/// 整用户踢线经替身走完真实的码规则：网关转发的是路由码 `core:users:kick`，替身 Online 的
/// `AdminListUserDevices` 与真实 Online 一样认读码与踢线码，`KickDevice` 只认踢线码；替身确实
/// 把关（同一个踢线上下文查在线状态就是 PERMISSION_DENIED），所以两边的码再对不上会在这里失败。
#[tokio::test]
async fn whole_user_kick_passes_the_downstream_code_gates() {
    let h = harness(true).await;
    let path = "/api/v1/admin/users/u1/kick";
    let token = h.gateway.token_for("POST", path);
    let (status, body) = send(
        &h.router,
        request("POST", path, Some(&token), WRITE_HEADERS),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["kicked"], serde_json::json!(["d1"]));
    let calls = h.recorder.calls();
    let methods: Vec<_> = calls.iter().map(|c| c.method).collect();
    assert_eq!(
        methods,
        ["AdminListUserDevices", "KickDevice", "KickDevice"]
    );
    for call in &calls {
        assert_eq!(call.signed_context, vec![token.clone()], "{}", call.method);
        assert_forwarded_only_the_staff_context(call, &h.gateway, TENANT, "core:users:kick");
    }

    // The stand-in Online gates like the real one: the kick context does not read presence,
    // and a read context does not kick.
    let channel = tonic::transport::Channel::from_shared(format!("http://{}", h.downstream))
        .unwrap()
        .connect_lazy();
    let mut online =
        flare_im_service_kit::clients::OnlineServiceClientWrapper::from_channel(channel);
    let presence = online
        .admin_get_user_presence(
            &token,
            flare_grpc_proto::signaling::online::AdminGetUserPresenceRequest {
                user_id: "u1".to_string(),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(
        presence.code(),
        Some(flare_server_core::error::ErrorCode::PermissionDenied),
        "{presence}"
    );
    let read = h.gateway.staff_token(EMPLOYEE, TENANT, "core:users:read");
    let kick = online
        .kick_device(
            &read,
            flare_grpc_proto::signaling::online::KickDeviceRequest {
                user_id: "u1".to_string(),
                device_id: "d1".to_string(),
                reason: "probe".to_string(),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(
        kick.code(),
        Some(flare_server_core::error::ErrorCode::PermissionDenied),
        "{kick}"
    );
}

#[tokio::test]
async fn kick_device_shape_and_rejections() {
    let h = harness(true).await;
    let (status, body) = send(
        &h.router,
        admin_request("POST", "/api/v1/admin/users/u1/devices/d1/kick", &h.gateway),
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
        let (status, body) = send(&h.router, admin_request("POST", &path, &h.gateway)).await;
        assert_eq!(status, expected, "{device}: {body}");
    }
}

#[tokio::test]
async fn kick_all_targets_only_the_signed_application() {
    let h = harness(true).await;
    let (status, body) = send(
        &h.router,
        admin_request("POST", "/api/v1/admin/tenant/kick-all", &h.gateway),
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

    // 另一应用的员工只能踢自己的应用：租户只取自签名。
    h.recorder.clear();
    let other = h
        .gateway
        .token_for_tenant("POST", "/api/v1/admin/tenant/kick-all", OTHER_TENANT);
    let mut extra = WRITE_HEADERS.to_vec();
    extra.push(("x-request-id", "req-b"));
    extra.push(("x-tenant-id", TENANT));
    let (status, body) = send(
        &h.router,
        request(
            "POST",
            "/api/v1/admin/tenant/kick-all",
            Some(&other),
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
    assert_eq!(h.recorder.calls()[0].signed_context, vec![other.clone()]);
    assert_forwarded_only_the_staff_context(
        &h.recorder.calls()[0],
        &h.gateway,
        OTHER_TENANT,
        "core:tenant:kick",
    );
}

#[tokio::test]
async fn tenant_runtime_shape() {
    let h = harness(true).await;
    let (status, body) = send(
        &h.router,
        admin_request("GET", "/api/v1/admin/tenant/runtime", &h.gateway),
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
        admin_request("GET", "/api/v1/admin/capabilities/tenant", &h.gateway),
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
            &h.gateway,
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
    // 请求里的租户取自签名，不取自任何头或查询参数。
    let call = &h.recorder.calls()[0];
    assert_eq!(call.method, "ListRegisteredPlugins");
    assert_eq!(
        call.request,
        format!("tenant_id={TENANT} capability_id=rtc.call")
    );
    assert_forwarded_for_route(call, &h.gateway, "GET", "/api/v1/admin/plugins");
}

#[tokio::test]
async fn capability_routes_answer_503_when_the_downstream_is_disabled() {
    let h = harness(false).await;
    for path in [
        "/api/v1/admin/tenant/runtime",
        "/api/v1/admin/capabilities/tenant",
        "/api/v1/admin/capabilities/catalog",
        "/api/v1/admin/plugins",
        "/api/v1/admin/hooks",
    ] {
        let (status, body) = send(&h.router, admin_request("GET", path, &h.gateway)).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{path}");
        assert_eq!(reason(&body), "ADMIN_CAPABILITY_DISABLED", "{path}");
    }
    // 其他 ADM-01 路由不受影响。
    let (status, _) = send(
        &h.router,
        admin_request("GET", "/api/v1/admin/conversations", &h.gateway),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(h.recorder.calls().len(), 1);
}

/// 进审计日志的三个请求头有长度上限：原因解码后 512 字节（原始头值 1536 字节），请求号与幂等键
/// 各 128 字节。超长回 400 `ADMIN_AUDIT_HEADER_TOO_LONG`，写与敏感读都一样，请求到不了下游；
/// 恰好在上限上的照常执行。
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
        let token = h.gateway.token_for(method, path);
        let (status, body) = send(&h.router, request(method, path, Some(&token), &extra)).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{method} {path} {name}: {body}"
        );
        assert_eq!(reason(&body), "ADMIN_AUDIT_HEADER_TOO_LONG", "{name}");
        assert!(h.recorder.calls().is_empty(), "{method} {path} {name}");
    }

    let at_limit = [
        ("x-audit-reason", reason_at_limit.as_str()),
        ("idempotency-key", id_at_limit.as_str()),
        ("x-request-id", id_at_limit.as_str()),
    ];
    let token = h.gateway.token_for("POST", "/api/v1/admin/users/u1/kick");
    let (status, body) = send(
        &h.router,
        request(
            "POST",
            "/api/v1/admin/users/u1/kick",
            Some(&token),
            &at_limit,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(!h.recorder.calls().is_empty());
}

/// `x-audit-reason` 是百分号编码的 UTF-8（控制台用 `encodeURIComponent`）：中文原因解码后交给
/// Online 作踢线原因；坏的编码、解码后不是 UTF-8、重复的头回 400 `ADMIN_AUDIT_REASON_INVALID`，
/// 解码后超过 512 字节或原始值超过 1536 字节回 400 `ADMIN_AUDIT_HEADER_TOO_LONG`；读写都一样，
/// 被拒的请求到不了下游。
#[tokio::test]
async fn the_audit_reason_is_percent_encoded_utf8() {
    let h = harness(false).await;
    let kick = "/api/v1/admin/users/u1/devices/d1/kick";
    let devices = "/api/v1/admin/users/u1/devices";
    let with_reason = |reason: &str| -> Vec<(String, String)> {
        vec![
            ("x-audit-reason".to_string(), reason.to_string()),
            ("idempotency-key".to_string(), "idem-9".to_string()),
        ]
    };
    let send_with = |method: &'static str, path: &'static str, headers: Vec<(String, String)>| {
        let token = h.gateway.token_for(method, path);
        let router = h.router.clone();
        async move {
            let extra: Vec<(&str, &str)> = headers
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_str()))
                .collect();
            send(&router, request(method, path, Some(&token), &extra)).await
        }
    };

    // encodeURIComponent("清理违规 spam")
    let encoded = "%E6%B8%85%E7%90%86%E8%BF%9D%E8%A7%84%20spam";
    let (status, body) = send_with("POST", kick, with_reason(encoded)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        h.recorder.calls()[0].request,
        "user_id=u1 device_id=d1 reason=清理违规 spam"
    );
    h.recorder.clear();

    // 512 bytes once decoded pass; 513 do not, though the raw value is within 1536 bytes.
    let at_limit = "%E4%B8%AD".repeat(170) + "ab";
    let (status, body) = send_with("POST", kick, with_reason(&at_limit)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        h.recorder.calls()[0]
            .request
            .ends_with(&format!("{}ab", "中".repeat(170)))
    );
    h.recorder.clear();

    let over_decoded = "%E4%B8%AD".repeat(170) + "abc";
    let over_raw = "%41".repeat(512) + "A";
    assert!(over_decoded.len() <= 1536 && over_raw.len() > 1536);
    for (method, path) in [("POST", kick), ("GET", devices)] {
        for value in [over_decoded.as_str(), over_raw.as_str()] {
            let (status, body) = send_with(method, path, with_reason(value)).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{method} {path}: {body}");
            assert_eq!(
                reason(&body),
                "ADMIN_AUDIT_HEADER_TOO_LONG",
                "{method} {path}"
            );
        }
        for malformed in ["%ZZ", "50%", "%E6%B8", "%FF%FE"] {
            let (status, body) = send_with(method, path, with_reason(malformed)).await;
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "{method} {path} {malformed}: {body}"
            );
            assert_eq!(
                reason(&body),
                "ADMIN_AUDIT_REASON_INVALID",
                "{method} {path} {malformed}"
            );
        }
        let mut repeated = with_reason("first");
        repeated.push(("x-audit-reason".to_string(), "second".to_string()));
        let (status, body) = send_with(method, path, repeated).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{method} {path}: {body}");
        assert_eq!(reason(&body), "ADMIN_AUDIT_REASON_INVALID");
    }
    assert!(h.recorder.calls().is_empty(), "{:?}", h.recorder.calls());

    // Blank once decoded is no reason: a write is refused as without one.
    let (status, body) = send_with("POST", kick, with_reason("%20%20")).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert!(h.recorder.calls().is_empty());
}

/// 路径参数由共享路由表 `core_admin::resolve` 决定：百分号编码、空格、超过 128 字节或带 `/` 的
/// 参数不是管理路由的参数，回类型化 404 `NOT_FOUND`，不回显取值，不调用下游。网关 admin 分区
/// 用同一张表解析，这些请求在那一侧就已经是 404。
#[tokio::test]
async fn invalid_path_parameters_are_typed_not_found_without_echo() {
    let h = harness(true).await;
    let long = "a".repeat(129);
    let cases = [
        (
            "GET",
            "/api/v1/admin/conversations/leak-marker-77%FF".to_string(),
        ),
        (
            "GET",
            "/api/v1/admin/conversations/leak-marker-77%FF/participants".to_string(),
        ),
        (
            "GET",
            "/api/v1/admin/users/leak-marker-77%FF/devices".to_string(),
        ),
        (
            "GET",
            "/api/v1/admin/users/leak-marker-77%20x/presence".to_string(),
        ),
        (
            "POST",
            "/api/v1/admin/users/leak-marker-77%FF/kick".to_string(),
        ),
        (
            "POST",
            "/api/v1/admin/users/u1/devices/leak-marker-77%FF/kick".to_string(),
        ),
        ("GET", format!("/api/v1/admin/users/{long}/presence")),
    ];
    for (method, path) in cases {
        // The permission is the route's; only the parameter is invalid.
        let pattern = path
            .replace("leak-marker-77%FF", "x")
            .replace("leak-marker-77%20x", "x");
        let pattern = pattern.replace(&long, "x");
        let token = h.gateway.token_for(method, &pattern);
        let extra = if method == "POST" { WRITE_HEADERS } else { &[] };
        let (status, body) = send(&h.router, request(method, &path, Some(&token), extra)).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{method} {path}: {body}");
        assert_eq!(body["code"], 404, "{method} {path}: {body}");
        assert_eq!(reason(&body), "NOT_FOUND", "{method} {path}: {body}");
        assert!(!body.to_string().contains("leak-marker-77"), "{body}");
    }
    assert!(h.recorder.calls().is_empty());
}
