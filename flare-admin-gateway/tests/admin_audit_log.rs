//! `admin_audit` 事件：写操作与三类敏感读（会话成员、用户设备、在线状态）各记一条，其余管理读
//! 不记。
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
        let (status, body) = send(
            &h.router,
            request(
                "GET",
                path,
                Some(&h.token),
                Some(TENANT),
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
        admin_request("GET", "/api/v1/admin/users/u1/devices", &h.token),
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
        "/api/v1/admin/capabilities/tenant",
        "/api/v1/admin/plugins",
    ] {
        let (status, body) = send(&h.router, admin_request("GET", path, &h.token)).await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
    }
    let events = capture.take();
    assert!(events.is_empty(), "{events:?}");

    // 写操作：access=write，带原因、请求号与幂等键。
    let (status, _) = send(
        &h.router,
        admin_request("POST", "/api/v1/admin/users/u1/devices/d1/kick", &h.token),
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

    // 媒资写同样记一条；媒资读不记。被网关拒绝的写（dry_run）不记。
    let m = media_harness(true, None).await;
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
            media_admin_request("POST", path, &m.token, Some(&body)),
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
        let (status, _) = send(&m.router, media_admin_request("GET", &path, &m.token, None)).await;
        assert_eq!(status, StatusCode::OK, "{path}");
    }
    let (status, _) = send(
        &m.router,
        media_admin_request(
            "POST",
            CLEANUP,
            &m.token,
            Some(r#"{"limit":10,"dry_run":true}"#),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let events = capture.take();
    assert!(events.is_empty(), "{events:?}");

    // 共 3+1 条敏感读、1 条踢线、2 条媒资写；任何事件里都没有令牌。
    let history = capture.history();
    assert_eq!(history.len(), 7, "{history:?}");
    let all = format!("{history:?}");
    assert!(!all.contains(&h.token) && !all.contains(&m.token));
}
