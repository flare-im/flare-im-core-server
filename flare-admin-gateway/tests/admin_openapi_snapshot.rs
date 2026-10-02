//! `AdminApiDoc` 的操作集合与各操作应答的快照，以及它与能力契约、路由清单、实际路由、文档
//! 四者的一致性。
//!
//! 快照文件 `tests/snapshots/admin_openapi_operations.txt` 每行一个 `METHOD path`，已排序；
//! `tests/snapshots/admin_openapi_responses.txt` 每行一个 `METHOD path STATUS 说明`，逐条列出
//! 每个操作文档里的应答与类型化原因。有意改动接口后用 `FLARE_UPDATE_SNAPSHOTS=1 cargo test
//! -p flare-admin-gateway --test admin_openapi_snapshot` 重写，再把差异作为接口变更一起评审。
mod common;

use std::collections::BTreeSet;
use std::path::PathBuf;

use axum::http::StatusCode;
use common::*;

const SNAPSHOT: &str = "tests/snapshots/admin_openapi_operations.txt";
const RESPONSES_SNAPSHOT: &str = "tests/snapshots/admin_openapi_responses.txt";

async fn openapi_document() -> serde_json::Value {
    let h = harness(true).await;
    let (status, doc) = send(
        &h.router,
        request("GET", "/api-doc/openapi.json", None, None, &[]),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    doc
}

/// 文档里的每个管理操作：(`METHOD path`, 操作对象)。
fn admin_operations(doc: &serde_json::Value) -> Vec<(String, serde_json::Value)> {
    let mut operations = Vec::new();
    for (path, item) in doc["paths"].as_object().expect("paths object") {
        if !path.starts_with("/api/v1/admin/") {
            continue;
        }
        for (method, operation) in item.as_object().expect("path item") {
            operations.push((
                format!("{} {path}", method.to_ascii_uppercase()),
                operation.clone(),
            ));
        }
    }
    operations.sort_by(|a, b| a.0.cmp(&b.0));
    operations
}

fn response_description<'a>(operation: &'a serde_json::Value, status: &str) -> Option<&'a str> {
    operation["responses"][status]["description"].as_str()
}

/// 把快照与本次生成的内容比较；设了 `FLARE_UPDATE_SNAPSHOTS` 时先重写快照。
fn assert_snapshot(relative: &str, rendered: &str) {
    let snapshot = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative);
    if std::env::var_os("FLARE_UPDATE_SNAPSHOTS").is_some() {
        std::fs::write(&snapshot, rendered).unwrap();
    }
    let committed = std::fs::read_to_string(&snapshot).unwrap_or_default();
    assert_eq!(
        rendered, committed,
        "AdminApiDoc changed; review the diff of {relative} and rerun with FLARE_UPDATE_SNAPSHOTS=1"
    );
}

async fn documented_operations() -> BTreeSet<String> {
    let doc = openapi_document().await;
    let mut operations = BTreeSet::new();
    for (path, item) in doc["paths"].as_object().expect("paths object") {
        for method in item.as_object().expect("path item").keys() {
            operations.insert(format!("{} {path}", method.to_ascii_uppercase()));
        }
    }
    operations
}

#[tokio::test]
async fn openapi_operations_match_the_committed_snapshot() {
    let operations = documented_operations().await;
    let rendered: String = operations.iter().map(|op| format!("{op}\n")).collect();
    assert_snapshot(SNAPSHOT, &rendered);
    // 生成两次结果相同：路径集合与顺序不依赖散列表迭代顺序。
    assert_eq!(documented_operations().await, operations);
}

/// 逐条列出每个管理操作文档里的应答：状态码与说明（含类型化原因）。改了任何一条错误映射或
/// 说明都会让快照不符，差异随接口变更一起评审。
#[tokio::test]
async fn openapi_responses_match_the_committed_snapshot() {
    let doc = openapi_document().await;
    let mut rendered = String::new();
    for (operation_name, operation) in admin_operations(&doc) {
        let responses = operation["responses"].as_object().expect("responses");
        for (status, response) in responses {
            let description = response["description"].as_str().unwrap_or_default();
            assert!(
                !description.contains('\n'),
                "{operation_name} {status}: one line per response"
            );
            rendered.push_str(&format!("{operation_name} {status} {description}\n"));
        }
    }
    assert_snapshot(RESPONSES_SNAPSHOT, &rendered);
    assert_eq!(openapi_document().await, doc);
}

/// 在调用下游之前就会回的 400 逐条写进每个操作：审计头超长（中间件，读写都查）、请求体解析
/// 失败（带请求体的 5 条）、查询串解析失败（带查询参数的 6 条）。网关不再回 422，文档里也
/// 不再有 422。
#[tokio::test]
async fn every_admin_operation_documents_its_request_rejections() {
    let doc = openapi_document().await;
    let operations = admin_operations(&doc);
    assert_eq!(operations.len(), 26);
    let (mut with_body, mut with_query) = (0, 0);
    for (operation_name, operation) in &operations {
        let bad_request = response_description(operation, "400")
            .unwrap_or_else(|| panic!("{operation_name} does not document 400"));
        assert!(
            bad_request.contains("ADMIN_AUDIT_HEADER_TOO_LONG"),
            "{operation_name}: {bad_request}"
        );
        if operation.get("requestBody").is_some() {
            with_body += 1;
            for reason in ["ADMIN_INVALID_JSON_BODY", "ADMIN_REQUEST_BODY_TOO_LARGE"] {
                assert!(
                    bad_request.contains(reason),
                    "{operation_name}: {bad_request}"
                );
            }
        }
        let query = operation["parameters"]
            .as_array()
            .is_some_and(|parameters| parameters.iter().any(|p| p["in"] == "query"));
        if query {
            with_query += 1;
            assert!(
                bad_request.contains("ADMIN_INVALID_QUERY"),
                "{operation_name}: {bad_request}"
            );
        }
        assert!(
            response_description(operation, "422").is_none(),
            "{operation_name} still documents 422"
        );
    }
    assert_eq!((with_body, with_query), (5, 6));
}

#[tokio::test]
async fn contract_inventory_and_openapi_list_the_same_operations() {
    let documented = documented_operations().await;

    let contract = flare_admin_gateway::interface::http::build_admin_capabilities();
    let described: BTreeSet<String> = contract
        .endpoints
        .iter()
        .map(|endpoint| format!("{} {}", endpoint.method, endpoint.path))
        .collect();
    assert_eq!(
        described.len(),
        contract.endpoints.len(),
        "duplicate endpoint"
    );
    assert_eq!(
        described, documented,
        "admin_contract endpoints vs AdminApiDoc"
    );

    let h = harness(true).await;
    let (status, body) = send(
        &h.router,
        admin_request("GET", "/api/v1/admin/gateway/routes", &h.token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let routes = body["data"]["routes"].as_array().unwrap();
    let inventory: BTreeSet<String> = routes
        .iter()
        .map(|route| {
            format!(
                "{} {}",
                route["method"].as_str().unwrap(),
                route["path"].as_str().unwrap()
            )
        })
        .collect();
    assert_eq!(inventory.len(), routes.len(), "duplicate route");
    assert_eq!(
        inventory, documented,
        "gateway route inventory vs AdminApiDoc"
    );

    // 写标记在两处一致；POST 除两个只读查询外都是写操作。
    for endpoint in &contract.endpoints {
        let route = routes
            .iter()
            .find(|route| route["method"] == endpoint.method && route["path"] == endpoint.path)
            .unwrap();
        assert_eq!(route["write"], endpoint.write, "{}", endpoint.path);
        let read_only_query = matches!(
            endpoint.path.as_str(),
            "/api/v1/admin/messages/query" | "/api/v1/admin/messages/write-ledger/query"
        );
        assert_eq!(
            endpoint.write,
            endpoint.method == "POST" && !read_only_query,
            "{} {}",
            endpoint.method,
            endpoint.path
        );
    }
}

#[tokio::test]
async fn every_documented_operation_is_routed_behind_admin_auth() {
    let documented = documented_operations().await;
    let h = harness(true).await;
    for operation in &documented {
        let (method, path) = operation.split_once(' ').unwrap();
        // 路径参数换成占位值；不带令牌必须是 401，而不是 404/405（未挂路由）。
        let concrete: String = path
            .split('/')
            .map(|segment| {
                if segment.starts_with('{') {
                    "x"
                } else {
                    segment
                }
            })
            .collect::<Vec<_>>()
            .join("/");
        let (status, _) = send(
            &h.router,
            request(method, &concrete, None, Some(TENANT), &[]),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{operation}");
    }
    assert!(h.recorder.calls().is_empty());
}

/// ADM-01、SEC-05 媒资路由与消息检索的 5 条路由都经 `admin_rpc_error` 映射下游失败：文档逐条
/// 列出它的每种答复及类型化原因——下游拒绝参数 400 `BAD_REQUEST`、拒绝租户 403 `FORBIDDEN`、
/// 路由固定原因的 500、拒绝已验过的令牌 502、不可用 503、超时 504（与 API Gateway 的 Media
/// 路由同一口径）。
#[tokio::test]
async fn downstream_routes_document_every_typed_downstream_failure() {
    let doc = openapi_document().await;
    let prefixes = [
        "/api/v1/admin/conversations",
        "/api/v1/admin/users/",
        "/api/v1/admin/tenant/",
        "/api/v1/admin/capabilities/tenant",
        "/api/v1/admin/plugins",
        "/api/v1/admin/medias/",
        "/api/v1/admin/messages/",
    ];
    let mut checked = 0;
    for (path, item) in doc["paths"].as_object().expect("paths object") {
        if !prefixes.iter().any(|prefix| path.starts_with(prefix)) {
            continue;
        }
        for (method, operation) in item.as_object().expect("path item") {
            for (code, reason) in [
                ("400", "(BAD_REQUEST)"),
                ("403", "(FORBIDDEN)"),
                ("500", "_FAILED"),
                ("502", "(ADMIN_UPSTREAM_REJECTED_ASSERTION)"),
                ("503", "(ADMIN_UPSTREAM_UNAVAILABLE)"),
                ("504", "(ADMIN_UPSTREAM_TIMEOUT)"),
            ] {
                let description = response_description(operation, code)
                    .unwrap_or_else(|| panic!("{method} {path} does not document {code}"));
                assert!(
                    description.contains(reason),
                    "{method} {path} {code} does not name {reason}: {description}"
                );
            }
            if let Some(not_found) = response_description(operation, "404") {
                assert!(not_found.contains("NOT_FOUND"), "{method} {path}");
            }
            checked += 1;
        }
    }
    assert_eq!(checked, 20);
}

const HTTP_METHODS: &[&str] = &["GET", "POST", "PUT", "PATCH", "DELETE"];

/// 文档里列出的操作：README 入口清单的 `METHOD /api/v1/admin/...` 行。
fn readme_operations(text: &str) -> BTreeSet<String> {
    text.lines()
        .filter_map(|line| {
            let mut parts = line.split_whitespace();
            let (method, path) = (parts.next()?, parts.next()?);
            (HTTP_METHODS.contains(&method)
                && path.starts_with("/api/v1/admin/")
                && parts.next().is_none())
            .then(|| format!("{method} {path}"))
        })
        .collect()
}

/// docs/admin-api.md 路由表首列的 `` `METHOD /path?query` ``，路径相对 `/api/v1/admin`。
fn guide_table_operations(text: &str) -> BTreeSet<String> {
    text.lines()
        .filter_map(|line| line.strip_prefix("| `")?.split('`').next())
        .filter_map(|cell| {
            let (method, path) = cell.split_once(' ')?;
            let path = path.split('?').next()?;
            (HTTP_METHODS.contains(&method) && path.starts_with('/'))
                .then(|| format!("{method} /api/v1/admin{path}"))
        })
        .collect()
}

#[tokio::test]
async fn readmes_and_admin_api_guide_list_the_routed_operations() {
    let documented = documented_operations().await;
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    // 两份 README 的入口清单列全部操作，不多不少。
    for readme in ["README.md", "README.zh-CN.md"] {
        let text = std::fs::read_to_string(root.join(readme)).unwrap();
        assert_eq!(
            readme_operations(&text),
            documented,
            "{readme} endpoint list vs AdminApiDoc"
        );
    }

    // docs/admin-api.md 的路由表只列真实操作，且 ADM-01 与 SEC-05 媒资路由一条不缺。
    let guide = std::fs::read_to_string(root.join("../docs/admin-api.md")).unwrap();
    let tabled = guide_table_operations(&guide);
    let stale: Vec<_> = tabled.difference(&documented).collect();
    assert!(
        stale.is_empty(),
        "docs/admin-api.md lists unrouted {stale:?}"
    );
    let described_prefixes = [
        "/api/v1/admin/conversations",
        "/api/v1/admin/users/",
        "/api/v1/admin/tenant/",
        "/api/v1/admin/capabilities/tenant",
        "/api/v1/admin/plugins",
        "/api/v1/admin/medias/",
    ];
    let required: BTreeSet<String> = documented
        .iter()
        .filter(|op| {
            let path = op.split_once(' ').unwrap().1;
            described_prefixes
                .iter()
                .any(|prefix| path.starts_with(prefix))
        })
        .cloned()
        .collect();
    assert_eq!(required.len(), 15, "{required:?}");
    let missing: Vec<_> = required.difference(&tabled).collect();
    assert!(missing.is_empty(), "docs/admin-api.md misses {missing:?}");
}
