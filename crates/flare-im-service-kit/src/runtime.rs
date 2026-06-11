//! Shared runtime assembly helpers for IM service binaries.
//!
//! Service crates should own their business wiring, while this module owns the
//! repeated runtime shell: config path resolution, endpoint selection, default
//! health checks, and graceful health-failure behavior.

use std::env;
use std::net::SocketAddr;

use flare_core_runtime::{HealthFailureAction, ServiceRuntime};
use flare_server_core::error::{AnyhowContext, Result};

use crate::config::{FlareAppConfig, ServiceEndpointConfig, ServiceRuntimeConfig};

/// Shutdown signal list accepted by embedded service runners.
pub type RuntimeShutdownSignals = Vec<Box<dyn flare_core_runtime::signal::ShutdownSignal>>;

/// Resolved runtime startup contract for one service process.
#[derive(Debug, Clone)]
pub struct ImServiceRuntimePlan {
    pub service_name: String,
    pub address: SocketAddr,
}

impl ImServiceRuntimePlan {
    /// Build a `ServiceRuntime` with Flare IM's default operational behavior.
    pub fn service_runtime(&self) -> ServiceRuntime {
        crate::health::attach_runtime_health_checks(
            ServiceRuntime::new(&self.service_name)
                .with_address(self.address)
                .with_health_failure_action(HealthFailureAction::GracefulShutdown),
            &self.service_name,
        )
    }
}

/// Build a named background runtime for MQ consumers and workers.
///
/// These services do not listen on a socket or register an address, but they
/// still need a stable runtime name, health checks, and graceful failure policy.
pub fn background_service_runtime(service_name: impl Into<String>) -> ServiceRuntime {
    let service_name = service_name.into();
    crate::health::attach_runtime_health_checks(
        ServiceRuntime::new(service_name.clone())
            .with_health_failure_action(HealthFailureAction::GracefulShutdown),
        &service_name,
    )
}

/// Build a background runtime from typed service config.
pub fn build_background_service_runtime(
    app_config: &FlareAppConfig,
    runtime: &ServiceRuntimeConfig,
    fallback_service_name: &str,
) -> ServiceRuntime {
    let service_config = app_config.compose_service_config(runtime, fallback_service_name);
    background_service_runtime(service_config.service.name)
}

/// Resolve the app config directory used by service binaries.
pub fn resolve_config_path() -> String {
    env::var("FLARE_CONFIG_PATH")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "config".to_string())
}

/// Load the global app config using `FLARE_CONFIG_PATH` or the standard config directory.
pub fn load_app_config_from_env() -> &'static FlareAppConfig {
    let config_path = resolve_config_path();
    crate::config::load_config(Some(&config_path))
}

/// Resolve a service runtime plan from typed config plus legacy env overrides.
///
/// Precedence:
/// 1. `{env_prefix}_LISTEN` as `host:port`
/// 2. `{env_prefix}_HOST` and `{env_prefix}_PORT`
/// 3. The service's typed `[services.<name>.server]` section
/// 4. Core server address plus the service-specific `default_port`
pub fn build_service_runtime_plan(
    app_config: &FlareAppConfig,
    runtime: &ServiceRuntimeConfig,
    fallback_service_name: &str,
    env_prefix: &str,
    default_port: u16,
) -> Result<ImServiceRuntimePlan> {
    let service_config = app_config.compose_service_config(runtime, fallback_service_name);
    let address = resolve_service_address(
        &app_config.core.server.address,
        app_config.core.server.port,
        runtime.server.as_ref(),
        env_prefix,
        default_port,
        |key| env::var(key).ok(),
    )?;

    Ok(ImServiceRuntimePlan {
        service_name: service_config.service.name,
        address,
    })
}

fn resolve_service_address<F>(
    core_address: &str,
    core_port: u16,
    runtime_server: Option<&ServiceEndpointConfig>,
    env_prefix: &str,
    default_port: u16,
    env_value: F,
) -> Result<SocketAddr>
where
    F: Fn(&str) -> Option<String>,
{
    let host_key = format!("{env_prefix}_HOST");
    let listen_key = format!("{env_prefix}_LISTEN");
    let port_key = format!("{env_prefix}_PORT");

    if let Some(listen) = env_value(&listen_key)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        return listen
            .parse()
            .with_context(|| format!("invalid {listen_key}: {listen}"));
    }

    let configured_host = runtime_server
        .and_then(|server| server.address.as_ref())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let configured_port = runtime_server.and_then(|server| server.port);

    let host = env_value(&host_key)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or(configured_host)
        .unwrap_or_else(|| core_address.to_string());

    let port = match env_value(&port_key)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        Some(value) => value
            .parse::<u16>()
            .with_context(|| format!("invalid {port_key}: {value}"))?,
        None => configured_port.unwrap_or_else(|| {
            if default_port == 0 {
                core_port
            } else {
                default_port
            }
        }),
    };

    format!("{host}:{port}")
        .parse()
        .with_context(|| format!("invalid service address: {host}:{port}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_service_endpoint_overrides_core_default_address() {
        let runtime_server = ServiceEndpointConfig {
            address: Some("127.0.0.1".to_string()),
            port: Some(60084),
        };

        let address = resolve_service_address(
            "0.0.0.0",
            50051,
            Some(&runtime_server),
            "SYNC_ORCHESTRATOR",
            60084,
            |_| None,
        )
        .expect("address should resolve");

        assert_eq!(address.to_string(), "127.0.0.1:60084");
    }

    #[test]
    fn env_endpoint_overrides_typed_service_endpoint() {
        let runtime_server = ServiceEndpointConfig {
            address: Some("127.0.0.1".to_string()),
            port: Some(60084),
        };

        let address = resolve_service_address(
            "0.0.0.0",
            50051,
            Some(&runtime_server),
            "SYNC_ORCHESTRATOR",
            60084,
            |key| match key {
                "SYNC_ORCHESTRATOR_HOST" => Some("0.0.0.0".to_string()),
                "SYNC_ORCHESTRATOR_PORT" => Some("61084".to_string()),
                _ => None,
            },
        )
        .expect("address should resolve");

        assert_eq!(address.to_string(), "0.0.0.0:61084");
    }

    #[test]
    fn missing_service_port_uses_service_default_not_core_port() {
        let address =
            resolve_service_address("0.0.0.0", 50051, None, "SYNC_ORCHESTRATOR", 60084, |_| None)
                .expect("address should resolve");

        assert_eq!(address.to_string(), "0.0.0.0:60084");
    }

    #[test]
    fn invalid_env_port_is_rejected() {
        let error =
            resolve_service_address("0.0.0.0", 50051, None, "SYNC_ORCHESTRATOR", 60084, |key| {
                (key == "SYNC_ORCHESTRATOR_PORT").then(|| "nope".to_string())
            })
            .expect_err("invalid env port should fail");

        assert!(
            error.to_string().contains("invalid SYNC_ORCHESTRATOR_PORT"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn listen_env_overrides_host_port_and_typed_service_endpoint() {
        let runtime_server = ServiceEndpointConfig {
            address: Some("127.0.0.1".to_string()),
            port: Some(50090),
        };

        let address = resolve_service_address(
            "0.0.0.0",
            50051,
            Some(&runtime_server),
            "PUSH_PROXY",
            50090,
            |key| match key {
                "PUSH_PROXY_LISTEN" => Some("0.0.0.0:61090".to_string()),
                "PUSH_PROXY_HOST" => Some("127.0.0.1".to_string()),
                "PUSH_PROXY_PORT" => Some("50090".to_string()),
                _ => None,
            },
        )
        .expect("listen address should resolve");

        assert_eq!(address.to_string(), "0.0.0.0:61090");
    }
}
