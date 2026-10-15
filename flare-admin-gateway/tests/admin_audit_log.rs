//! `admin_audit` 事件：写操作与三类敏感读（会话成员、用户设备、在线状态）各记一条，其余管理读
//! 不记。效果事件（`flare.audit.effect.v1`）走 NATS，这里的审计效果汇是禁用的；它的形状由
//! 服务套件单测钉住。
//!
//! 单独一个测试二进制、只有一个测试：这里装全局 tracing 订阅者收集事件，不能和别的测试并行
//! 共享进程。
mod common;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use axum::http::StatusCode;
use common::media::*;
use common::*;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

type AuditEvent = BTreeMap<String, String>;

/// 只收 `target: "admin_audit"` 的事件，把字段记成字符串。`pending` 由断言逐段取走，
/// `history` 保留全部事件做最后的整体检查。
#[derive(Clone, Default)]
struct AuditCapture {
    pending: Arc<Mutex<Vec<AuditEvent>>>,
    history: Arc<Mutex<Vec<AuditEvent>>>,
}

struct Fields<'a>(&'a mut AuditEvent);

impl Visit for Fields<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_string(), value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }
}

impl Subscriber for AuditCapture {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.is_event() && metadata.target() == "admin_audit"
    }

    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut fields = AuditEvent::new();
        event.record(&mut Fields(&mut fields));
        self.history.lock().unwrap().push(fields.clone());
        self.pending.lock().unwrap().push(fields);
    }

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}

impl AuditCapture {
    fn take(&self) -> Vec<AuditEvent> {
        std::mem::take(&mut *self.pending.lock().unwrap())
    }

    fn history(&self) -> Vec<AuditEvent> {
        self.history.lock().unwrap().clone()
    }
}

fn field<'a>(event: &'a AuditEvent, name: &str) -> &'a str {
    event
        .get(name)
        .map(String::as_str)
        .unwrap_or_else(|| panic!("admin_audit event without {name}: {event:?}"))
}

#[tokio::test]
async fn sensitive_reads_and_writes_are_audited_other_reads_are_not() {
    let capture = AuditCapture::default();
    tracing::subscriber::set_global_default(capture.clone()).expect("first subscriber");

    let h = harness(true).await;
    // 敏感读：每条一条 access=read 事件，目标是被查看的会话或用户；带了原因就一并记下。
    let sensitive = [
        (
            "/api/v1/admin/conversations/c1/participants",
            "conversations.participants",
            "c1",
        ),
        ("/api/v1/admin/users/u1/devices", "users.devices", "u1"),
        ("/api/v1/admin/users/u1/presence", "users.presence", "u1"),
    ];
    for (path, route, target) in sensitive {
        capture.take();
        let token = h.gateway.token_for("GET", path);
        let (status, body) = send(
            &h.router,
            request(
                "GET",
                path,
                Some(&token),
                &[
                    ("x-request-id", "req-read-1"),
                    ("x-audit-reason", "ticket-9"),
                ],
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
        let events = capture.take();
        assert_eq!(events.len(), 1, "{path}: {events:?}");
        let event = &events[0];
        assert_eq!(field(event, "access"), "read", "{path}");
        assert_eq!(field(event, "route"), route, "{path}");
        assert_eq!(field(event, "actor_id"), EMPLOYEE, "{path}");
        assert_eq!(field(event, "tenant_id"), TENANT, "{path}");
        assert_eq!(field(event, "target"), target, "{path}");
        assert_eq!(field(event, "request_id"), "req-read-1", "{path}");
        assert_eq!(field(event, "audit_reason"), "ticket-9", "{path}");
    }

    // 敏感读不要求审计原因：没带就记空串，请求照常成功。
    capture.take();
    let (status, _) = send(
        &h.router,
        admin_request("GET", "/api/v1/admin/users/u1/devices", &h.gateway),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let events = capture.take();
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(field(&events[0], "audit_reason"), "");

    // 其余管理读不记 admin_audit。
    for path in [
        "/api/v1/admin/conversations",
        "/api/v1/admin/conversations/c1",
        "/api/v1/admin/tenant/runtime",
        "/api/v1/admin/capabilities/catalog",
        "/api/v1/admin/capabilities/tenant",
        "/api/v1/admin/plugins",
        "/api/v1/admin/hooks",
        "/api/v1/admin/hooks/7",
        "/api/v1/admin/hooks/7/statistics",
        "/api/v1/admin/hooks/7/executions",
    ] {
        let (status, body) = send(&h.router, admin_request("GET", path, &h.gateway)).await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
    }
    let events = capture.take();
    assert!(events.is_empty(), "{events:?}");

    // 写操作：access=write，带原因、请求号与幂等键。
    let (status, _) = send(
        &h.router,
        admin_request("POST", "/api/v1/admin/users/u1/devices/d1/kick", &h.gateway),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let events = capture.take();
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(field(&events[0], "access"), "write");
    assert_eq!(field(&events[0], "route"), "devices.kick");
    assert_eq!(field(&events[0], "target"), "u1/d1");
    assert_eq!(field(&events[0], "audit_reason"), "incident-42 containment");
    assert_eq!(field(&events[0], "idempotency_key"), "idem-1");

    // `x-audit-reason` 是百分号编码的 UTF-8：中文原因解码后记下；解码出的控制字符（换行）
    // 转义后记下，伪造不了日志行。
    let kick = "/api/v1/admin/users/u1/devices/d1/kick";
    let token = h.gateway.token_for("POST", kick);
    let (status, body) = send(
        &h.router,
        request(
            "POST",
            kick,
            Some(&token),
            &[
                (
                    "x-audit-reason",
                    "%E6%B8%85%E7%90%86%E8%BF%9D%E8%A7%84%20spam",
                ),
                ("idempotency-key", "idem-2"),
            ],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let events = capture.take();
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(field(&events[0], "audit_reason"), "清理违规 spam");
    let devices = "/api/v1/admin/users/u1/devices";
    let token = h.gateway.token_for("GET", devices);
    let (status, body) = send(
        &h.router,
        request(
            "GET",
            devices,
            Some(&token),
            &[(
                "x-audit-reason",
                "%E5%B7%A5%E5%8D%95%0AINFO%20admin_audit%20access=write",
            )],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let events = capture.take();
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(field(&events[0], "access"), "read");
    assert_eq!(
        field(&events[0], "audit_reason"),
        r"工单\nINFO admin_audit access=write"
    );
    // A malformed reason is refused before anything is logged.
    let (status, _) = send(
        &h.router,
        request(
            "POST",
            kick,
            Some(&h.gateway.token_for("POST", kick)),
            &[("x-audit-reason", "%ZZ"), ("idempotency-key", "idem-3")],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(capture.take().is_empty());

    // R3-2 新增的写路由同样各记一条：能力开关、插件登记与注销、Hook 创建/更新/删除/状态。
    let capability_writes = [
        (
            "PUT",
            "/api/v1/admin/capabilities/tenant/rtc.call",
            Some(r#"{"enabled":false}"#),
            "capabilities.tenant_switch",
            "rtc.call",
        ),
        (
            "POST",
            "/api/v1/admin/plugins/register",
            Some(
                r#"{"plugin_id":"plugin-c","capability_id":"rtc.call","grpc_authority":"http://sfu:9"}"#,
            ),
            "plugins.register",
            "plugin-c",
        ),
        (
            "POST",
            "/api/v1/admin/plugins/deregister",
            Some(r#"{"plugin_id":"plugin-c"}"#),
            "plugins.deregister",
            "plugin-c",
        ),
        (
            "POST",
            "/api/v1/admin/hooks",
            Some(r#"{"name":"dlp","hook_type":"message_pre_send"}"#),
            "hooks.create",
            "dlp",
        ),
        (
            "PUT",
            "/api/v1/admin/hooks/7",
            Some(r#"{"name":"dlp-2"}"#),
            "hooks.update",
            "7",
        ),
        ("DELETE", "/api/v1/admin/hooks/7", None, "hooks.delete", "7"),
        (
            "PUT",
            "/api/v1/admin/hooks/message_pre_send:dlp/status",
            Some(r#"{"enabled":true}"#),
            "hooks.set_status",
            "message_pre_send%3Adlp",
        ),
    ];
    for (method, path, body, route, target) in capability_writes {
        let (status, response) = send(
            &h.router,
            admin_request_with_body(method, path, &h.gateway, body),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{method} {path}: {response}");
        let events = capture.take();
        assert_eq!(events.len(), 1, "{method} {path}: {events:?}");
        assert_eq!(field(&events[0], "access"), "write", "{path}");
        assert_eq!(field(&events[0], "route"), route, "{path}");
        assert_eq!(field(&events[0], "target"), target, "{path}");
        assert_eq!(field(&events[0], "actor_id"), EMPLOYEE, "{path}");
        assert_eq!(field(&events[0], "tenant_id"), TENANT, "{path}");
    }

    // 媒资写同样记一条；媒资读不记。被网关拒绝的写（dry_run）不记。
    let m = media_harness(None).await;
    let writes = [
        (
            CLEANUP,
            cleanup_body(10),
            "medias.cleanup_orphaned_assets",
            TENANT,
        ),
        (OBJECT_ACL, acl_body("f-1"), "medias.object_acl", "f-1"),
    ];
    for (path, body, route, target) in writes {
        let (status, response) = send(
            &m.router,
            media_admin_request("POST", path, &m.gateway, Some(&body)),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{path}: {response}");
        let events = capture.take();
        assert_eq!(events.len(), 1, "{path}: {events:?}");
        assert_eq!(field(&events[0], "access"), "write", "{path}");
        assert_eq!(field(&events[0], "route"), route, "{path}");
        assert_eq!(field(&events[0], "actor_id"), EMPLOYEE, "{path}");
        assert_eq!(field(&events[0], "tenant_id"), TENANT, "{path}");
        assert_eq!(field(&events[0], "target"), target, "{path}");
    }
    for path in [
        format!("{OBJECTS}?bucket=media"),
        format!("{BUCKET}?bucket=media"),
    ] {
        let (status, _) = send(
            &m.router,
            media_admin_request("GET", &path, &m.gateway, None),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{path}");
    }
    let (status, _) = send(
        &m.router,
        media_admin_request(
            "POST",
            CLEANUP,
            &m.gateway,
            Some(r#"{"limit":10,"dry_run":true}"#),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let events = capture.take();
    assert!(events.is_empty(), "{events:?}");

    // 路径参数由共享路由表限定为标识符字符（字母、数字、`_-.:`，至多 128 字节）：带换行、
    // 控制字符、`/` 或超长的参数在中间件就是 404，不产生任何审计事件。取自请求体的目标
    // （ACL 的 file_id、Hook 的 name）仍可以是任意 UTF-8，一律转义后才进事件：换行与控制
    // 字符不能伪造日志行，`/` 只作复合目标的分隔符，超长输入截断并标明原长。
    let long_user = "a".repeat(1_000);
    let refused = [
        (
            "GET",
            "/api/v1/admin/users/u1%0AINFO%20admin_audit%20access=write/devices".to_string(),
            "/api/v1/admin/users/x/devices",
        ),
        (
            "GET",
            "/api/v1/admin/conversations/c%00%09%1B%5B2J%7F/participants".to_string(),
            "/api/v1/admin/conversations/x/participants",
        ),
        (
            "POST",
            "/api/v1/admin/users/u1%2Fd1/devices/x/kick".to_string(),
            "/api/v1/admin/users/x/devices/x/kick",
        ),
        (
            "GET",
            format!("/api/v1/admin/users/{long_user}/presence"),
            "/api/v1/admin/users/x/presence",
        ),
    ];
    for (method, path, pattern) in &refused {
        let token = h.gateway.token_for(method, pattern);
        let extra = if *method == "POST" {
            WRITE_HEADERS
        } else {
            &[]
        };
        let (status, _) = send(&h.router, request(method, path, Some(&token), extra)).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
        let events = capture.take();
        assert!(events.is_empty(), "{path}: {events:?}");
    }
    send(
        &m.router,
        media_admin_request("POST", OBJECT_ACL, &m.gateway, Some(&acl_body(r"f\n1/x"))),
    )
    .await;
    let events = capture.take();
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(field(&events[0], "route"), "medias.object_acl");
    assert_eq!(field(&events[0], "target"), "f%0A1%2Fx");
    let long_name = "n".repeat(1_000);
    let body = format!(r#"{{"name":"{long_name}","hook_type":"message_pre_send"}}"#);
    send(
        &h.router,
        admin_request_with_body("POST", "/api/v1/admin/hooks", &h.gateway, Some(&body)),
    )
    .await;
    let events = capture.take();
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(field(&events[0], "route"), "hooks.create");
    assert_eq!(
        field(&events[0], "target"),
        format!("{}!truncated:1000", "n".repeat(256))
    );

    // 共 3+1 条敏感读、1 条踢线、2 条编码原因、7 条 Capability 写、2 条媒资写，加上 2 条转义用例；
    // 任何事件里都没有上下文令牌，任何目标都只含 unreserved 字符、`%XX`、分隔符 `/` 与截断标记。
    let history = capture.history();
    assert_eq!(history.len(), 18, "{history:?}");
    let all = format!("{history:?}");
    for token in [
        h.gateway.token_for("GET", "/api/v1/admin/users/u1/devices"),
        h.gateway
            .token_for("POST", "/api/v1/admin/users/u1/devices/d1/kick"),
        m.gateway.token_for("POST", OBJECT_ACL),
    ] {
        assert!(!all.contains(&token));
    }
    for event in &history {
        let target = field(event, "target");
        assert!(
            target
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-._~%/!:".contains(&b)),
            "{target:?}"
        );
    }
}
