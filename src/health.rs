//! IM 运行时健康检查辅助模块
//!
//! 提供跨服务复用的依赖可达性健康检查（TCP）。

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use flare_core_runtime::error::HealthError;
use flare_core_runtime::{HealthCheck, ServiceRuntime};

/// 为运行时附加默认依赖健康检查。
///
/// 通过环境变量 `<SERVICE>_HEALTH_TARGETS` 配置检查目标：
/// - `SERVICE` 由 service_name 规范化得到（去掉 `flare-` 前缀并转大写，下划线分隔）
/// - 示例：`SIGNALING_ROUTE_HEALTH_TARGETS=redis=127.0.0.1:6379,kafka=127.0.0.1:9092`
pub fn attach_runtime_health_checks(
    mut runtime: ServiceRuntime,
    service_name: &str,
) -> ServiceRuntime {
    let env_key = format!("{}_HEALTH_TARGETS", normalize_service_key(service_name));
    let raw = match std::env::var(&env_key) {
        Ok(v) if !v.trim().is_empty() => v,
        _ => return runtime,
    };

    for target in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (name, endpoint) = parse_named_target(target);
        let check_name = format!("tcp-{}", name);
        runtime = runtime.add_health_check(Arc::new(TcpDependencyHealthCheck::new(
            check_name, endpoint,
        )));
    }

    runtime
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
