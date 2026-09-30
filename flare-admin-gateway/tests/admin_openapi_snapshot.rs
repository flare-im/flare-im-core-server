//! `AdminApiDoc` 的操作集合快照，以及它与能力契约、路由清单、实际路由三者的一致性。
//!
//! 快照文件 `tests/snapshots/admin_openapi_operations.txt` 每行一个 `METHOD path`，已排序。
//! 有意改动接口后用 `FLARE_UPDATE_SNAPSHOTS=1 cargo test -p flare-admin-gateway --test
//! admin_openapi_snapshot` 重写，再把差异作为接口变更一起评审。
mod common;

use std::collections::BTreeSet;
use std::path::PathBuf;

use axum::http::StatusCode;
use common::*;

const SNAPSHOT: &str = "tests/snapshots/admin_openapi_operations.txt";

async fn documented_operations() -> BTreeSet<String> {
    let h = harness(true).await;
    let (status, doc) = send(
        &h.router,
        request("GET", "/api-doc/openapi.json", None, None, &[]),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
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
    let snapshot = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(SNAPSHOT);
    if std::env::var_os("FLARE_UPDATE_SNAPSHOTS").is_some() {
        std::fs::write(&snapshot, &rendered).unwrap();
    }
    let committed = std::fs::read_to_string(&snapshot).unwrap_or_default();
    assert_eq!(
        rendered, committed,
        "AdminApiDoc changed; review the diff and rerun with FLARE_UPDATE_SNAPSHOTS=1"
    );
    // 生成两次结果相同：路径集合与顺序不依赖散列表迭代顺序。
    assert_eq!(documented_operations().await, operations);
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
