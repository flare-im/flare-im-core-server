//! IM 运行时健康检查辅助模块
//!
//! 提供跨服务复用的依赖可达性健康检查（TCP）。

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use flare_core_runtime::error::HealthError;
use flare_core_runtime::{HealthCheck, ServiceRuntime};

/// 为运行时附加默认依赖健康检查。
///
/// 通过环境变量 `<SERVICE>_HEALTH_TARGETS` 配置检查目标：
/// - `SERVICE` 由 service_name 规范化得到（去掉 `flare-` 前缀并转大写，下划线分隔）
/// - 示例：`SIGNALING_ROUTE_HEALTH_TARGETS=redis=127.0.0.1:6379,jetstream=127.0.0.1:9092`
pub fn attach_runtime_health_checks(
    mut runtime: ServiceRuntime,
    service_name: &str,
) -> ServiceRuntime {
    for check in dependency_checks_from_env(service_name) {
        runtime = runtime.add_health_check(check);
    }
    runtime
}

/// `<SERVICE>_HEALTH_TARGETS` 声明的依赖检查；运行时健康检查与 `/readyz` 共用同一份目标。
fn dependency_checks_from_env(service_name: &str) -> Vec<Arc<dyn HealthCheck>> {
    let env_key = format!("{}_HEALTH_TARGETS", normalize_service_key(service_name));
    let raw = match std::env::var(&env_key) {
        Ok(v) if !v.trim().is_empty() => v,
        _ => return Vec::new(),
    };

    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|target| {
            let (name, endpoint) = parse_named_target(target);
            let check_name = format!("tcp-{}", name);
            Arc::new(TcpDependencyHealthCheck::new(check_name, endpoint)) as Arc<dyn HealthCheck>
        })
        .collect()
}

const PHASE_STARTING: u8 = 0;
const PHASE_READY: u8 = 1;
const PHASE_DRAINING: u8 = 2;

/// 就绪状态，`/readyz` 的依据：启动装配完成且依赖检查通过才就绪；停机一开始就转为不就绪，
/// 负载均衡据此先摘流量，再让进程关连接。
///
/// 与 `/health`（进程活着）分开：停机排空期间进程仍然健康，但不该再接新流量。
pub struct Readiness {
    phase: AtomicU8,
    checks: Vec<Arc<dyn HealthCheck>>,
}

impl Default for Readiness {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

impl Readiness {
    pub fn new(checks: Vec<Arc<dyn HealthCheck>>) -> Self {
        Self {
            phase: AtomicU8::new(PHASE_STARTING),
            checks,
        }
    }

    /// 依赖检查取 `<SERVICE>_HEALTH_TARGETS`（与运行时健康检查同一份）。
    pub fn from_env(service_name: &str) -> Self {
        Self::new(dependency_checks_from_env(service_name))
    }

    /// 启动装配完成。停机开始后再调用不会把状态拨回就绪。
    pub fn mark_ready(&self) {
        let _ = self.phase.compare_exchange(
            PHASE_STARTING,
            PHASE_READY,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    /// 停机开始：此后 `/readyz` 一直回 503。
    pub fn mark_draining(&self) {
        self.phase.store(PHASE_DRAINING, Ordering::SeqCst);
    }

    pub fn is_draining(&self) -> bool {
        self.phase.load(Ordering::SeqCst) == PHASE_DRAINING
    }

    /// 就绪返回 `Ok`；否则给出不就绪的原因（进 503 应答体，便于排查）。
    pub async fn check(&self) -> Result<(), String> {
        match self.phase.load(Ordering::SeqCst) {
            PHASE_STARTING => return Err("starting".to_string()),
            PHASE_DRAINING => return Err("draining".to_string()),
            _ => {}
        }
        let results =
            futures::future::join_all(self.checks.iter().map(|check| check.check())).await;
        let failures: Vec<String> = results
            .into_iter()
            .filter_map(|result| result.err().map(|error| error.to_string()))
            .collect();
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }

    /// `/readyz` 应答：就绪 200 `ready`，否则 503 带原因。
    pub async fn response(&self) -> axum::response::Response {
        use axum::http::StatusCode;
        use axum::response::IntoResponse;
        match self.check().await {
            Ok(()) => (StatusCode::OK, "ready").into_response(),
            Err(reason) => (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("not ready: {reason}"),
            )
                .into_response(),
        }
    }
}

/// 只含 `GET /readyz` 的路由，合并进服务已有的 HTTP 出口。
pub fn readyz_router(readiness: Arc<Readiness>) -> axum::Router {
    axum::Router::new().route(
        "/readyz",
        axum::routing::get(move || {
            let readiness = readiness.clone();
            async move { readiness.response().await }
        }),
    )
}

fn normalize_service_key(service_name: &str) -> String {
    service_name
        .trim()
        .to_ascii_uppercase()
        .trim_start_matches("FLARE_")
        .trim_start_matches("FLARE-")
        .replace('-', "_")
}

fn parse_named_target(input: &str) -> (String, String) {
    if let Some((name, endpoint)) = input.split_once('=') {
        return (name.trim().to_string(), endpoint.trim().to_string());
    }
    (input.trim().to_string(), input.trim().to_string())
}

struct TcpDependencyHealthCheck {
    name: String,
    endpoint: String,
    timeout: Duration,
}

impl TcpDependencyHealthCheck {
    fn new(name: String, endpoint: String) -> Self {
        Self {
            name,
            endpoint,
            timeout: Duration::from_secs(2),
        }
    }
}

impl HealthCheck for TcpDependencyHealthCheck {
    fn check(&self) -> Pin<Box<dyn Future<Output = Result<(), HealthError>> + Send + '_>> {
        let endpoint = self.endpoint.clone();
        let timeout = self.timeout;
        let name = self.name.clone();

        Box::pin(async move {
            let host_port = endpoint
                .strip_prefix("http://")
                .or_else(|| endpoint.strip_prefix("https://"))
                .unwrap_or(&endpoint)
                .to_string();

            match tokio::time::timeout(timeout, tokio::net::TcpStream::connect(&host_port)).await {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(e)) => Err(HealthError::CheckFailed {
                    name,
                    reason: format!("connect {} failed: {}", host_port, e),
                }),
                Err(_) => Err(HealthError::CheckFailed {
                    name,
                    reason: format!("connect {} timeout after {:?}", host_port, timeout),
                }),
            }
        })
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod readiness_tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    async fn readyz(readiness: &Arc<Readiness>) -> (StatusCode, String) {
        let response = readyz_router(readiness.clone())
            .oneshot(Request::get("/readyz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    /// 启动完成前 503，就绪后 200，停机一开始转 503，之后再标就绪也不会拨回去。
    #[tokio::test]
    async fn readyz_follows_startup_and_turns_503_when_draining() {
        let readiness = Arc::new(Readiness::default());
        assert_eq!(readyz(&readiness).await.0, StatusCode::SERVICE_UNAVAILABLE);

        readiness.mark_ready();
        assert_eq!(
            readyz(&readiness).await,
            (StatusCode::OK, "ready".to_string())
        );

        readiness.mark_draining();
        let (status, body) = readyz(&readiness).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.contains("draining"), "{body}");
        readiness.mark_ready();
        assert!(readiness.is_draining());
        assert_eq!(readyz(&readiness).await.0, StatusCode::SERVICE_UNAVAILABLE);
    }

    /// 依赖不可达时不就绪。
    #[tokio::test]
    async fn an_unreachable_dependency_keeps_readyz_503() {
        let closed = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap()
        };
        let readiness = Arc::new(Readiness::new(vec![Arc::new(
            TcpDependencyHealthCheck::new("tcp-redis".to_string(), closed.to_string()),
        )]));
        readiness.mark_ready();
        let (status, body) = readyz(&readiness).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.contains("tcp-redis"), "{body}");
    }
}
