//! 运行时适配装配入口（仅暴露协议级装配函数，不暴露具体后端实现类型）。

use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;

use super::bootstrap::PluginContext;

type MediaBackendEntry = (String, String, String, String);

#[async_trait]
pub trait MediaControlBackendFactory: Send + Sync {
    async fn wire(
        &self,
        ctx: &PluginContext,
        tenant_id: &str,
        plugin_id: &str,
        capability_id: &str,
        grpc_endpoint: &str,
    ) -> Result<()>;
}

#[derive(Default)]
pub struct DefaultMediaControlBackendFactory;

#[async_trait]
impl MediaControlBackendFactory for DefaultMediaControlBackendFactory {
    async fn wire(
        &self,
        ctx: &PluginContext,
        tenant_id: &str,
        plugin_id: &str,
        capability_id: &str,
        grpc_endpoint: &str,
    ) -> Result<()> {
        crate::infrastructure::capability::wire_media_control_backend(
            &ctx.registry,
            &ctx.plugin_routes,
            tenant_id,
            plugin_id,
            capability_id,
            grpc_endpoint,
        )
        .await
    }
}

fn collect_media_backends(
    ctx: &PluginContext,
    env_endpoint: Option<String>,
) -> Vec<MediaBackendEntry> {
    let mut out = Vec::new();
    if let Some(endpoint) = env_endpoint
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        out.push((
            "default".to_string(),
            "media-control".to_string(),
            "rtc.media.control".to_string(),
            endpoint,
        ));
    }
    for ep in ctx.runtime.media_control_endpoints() {
        out.push((
            ep.tenant_id.clone(),
            ep.plugin_id.clone(),
            ep.capability_id.clone(),
            ep.grpc_authority.clone(),
        ));
    }
    out.sort();
    out.dedup();
    out
}

/// 按配置装配运行时 RTC/Extension 适配：
/// - 可选远端控制面后端（feature: `backend-remote` + config endpoint）
///
/// 不会对外暴露任何具体后端类型；仅修改 `CapabilityExtensionRegistry` 与 `PluginRouteBook`。
pub async fn wire_runtime_adapters(ctx: PluginContext) -> Result<()> {
    let env_endpoint = std::env::var("FLARE_MEDIA_CONTROL_GRPC_ENDPOINT")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    wire_runtime_adapters_with_factory(
        ctx,
        Arc::new(DefaultMediaControlBackendFactory),
        env_endpoint,
    )
    .await
}

pub async fn wire_runtime_adapters_with_factory(
    ctx: PluginContext,
    factory: Arc<dyn MediaControlBackendFactory>,
    env_endpoint: Option<String>,
) -> Result<()> {
    let backends = collect_media_backends(&ctx, env_endpoint);
    if backends.is_empty() {
        tracing::warn!(
            "media control grpc endpoint missing; capability will start without RTC/media backend"
        );
        return Ok(());
    }
    for (tenant_id, plugin_id, capability_id, endpoint) in backends {
        if let Err(error) = factory
            .wire(
                &ctx,
                tenant_id.as_str(),
                plugin_id.as_str(),
                capability_id.as_str(),
                endpoint.as_str(),
            )
            .await
        {
            tracing::warn!(
                tenant_id = %tenant_id,
                plugin_id = %plugin_id,
                capability_id = %capability_id,
                endpoint = %endpoint,
                error = %error,
                "media control backend unavailable; skipping optional capability backend"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        MediaControlBackendFactory, collect_media_backends, wire_runtime_adapters_with_factory,
    };
    use crate::composition::bootstrap::PluginContext;
    use crate::infrastructure::capability::{CapabilityExtensionRegistry, PluginRouteBook};
    use crate::infrastructure::config::CapabilityRuntimeConfig;
    use crate::infrastructure::config::capability_runtime::PluginDiscoveryEndpoint;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;

    #[derive(Default)]
    struct FakeFactory {
        calls: Arc<Mutex<Vec<(String, String, String, String)>>>,
        fail: bool,
    }

    #[async_trait]
    impl MediaControlBackendFactory for FakeFactory {
        async fn wire(
            &self,
            _ctx: &PluginContext,
            tenant_id: &str,
            plugin_id: &str,
            capability_id: &str,
            grpc_endpoint: &str,
        ) -> anyhow::Result<()> {
            self.calls.lock().await.push((
                tenant_id.to_string(),
                plugin_id.to_string(),
                capability_id.to_string(),
                grpc_endpoint.to_string(),
            ));
            if self.fail {
                anyhow::bail!("backend unavailable");
            }
            Ok(())
        }
    }

    fn test_runtime() -> Arc<CapabilityRuntimeConfig> {
        Arc::new(CapabilityRuntimeConfig {
            max_payload_json_bytes: 1024,
            dispatch_timeout: Duration::from_secs(2),
            admin_secret: None,
            deny_policy_mutations_without_secret: false,
            dispatch_max_per_minute: None,
            plugin_discovery_endpoints: vec![
                PluginDiscoveryEndpoint {
                    tenant_id: "tenant-a".into(),
                    plugin_id: "vendor-a".into(),
                    capability_id: "rtc.media.control".into(),
                    grpc_authority: "http://a:50051".into(),
                    labels: HashMap::new(),
                },
                PluginDiscoveryEndpoint {
                    tenant_id: "tenant-b".into(),
                    plugin_id: "vendor-b".into(),
                    capability_id: "rtc.media.control".into(),
                    grpc_authority: "http://b:50051".into(),
                    labels: HashMap::new(),
                },
            ],
            plugin_health_check_interval: Duration::from_secs(10),
            plugin_call_timeout: Duration::from_secs(3),
        })
    }

    #[tokio::test]
    async fn collect_media_backends_merges_env_and_discovery() {
        let ctx = PluginContext {
            registry: CapabilityExtensionRegistry::new(),
            plugin_routes: Arc::new(PluginRouteBook::new()),
            runtime: test_runtime(),
        };
        let v = collect_media_backends(&ctx, Some("http://env:50051".into()));
        assert_eq!(v.len(), 3);
    }

    #[tokio::test]
    async fn wire_with_factory_calls_for_each_backend() {
        let factory = Arc::new(FakeFactory::default());
        let calls_ref = Arc::clone(&factory.calls);
        let ctx = PluginContext {
            registry: CapabilityExtensionRegistry::new(),
            plugin_routes: Arc::new(PluginRouteBook::new()),
            runtime: test_runtime(),
        };
        wire_runtime_adapters_with_factory(ctx, factory, None)
            .await
            .expect("wire should succeed");
        let calls = calls_ref.lock().await;
        assert_eq!(calls.len(), 2);
    }

    #[tokio::test]
    async fn wire_without_backend_is_not_fatal() {
        let ctx = PluginContext {
            registry: CapabilityExtensionRegistry::new(),
            plugin_routes: Arc::new(PluginRouteBook::new()),
            runtime: Arc::new(CapabilityRuntimeConfig {
                max_payload_json_bytes: 1024,
                dispatch_timeout: Duration::from_secs(2),
                admin_secret: None,
                deny_policy_mutations_without_secret: false,
                dispatch_max_per_minute: None,
                plugin_discovery_endpoints: vec![],
                plugin_health_check_interval: Duration::from_secs(10),
                plugin_call_timeout: Duration::from_secs(3),
            }),
        };
        wire_runtime_adapters_with_factory(ctx, Arc::new(FakeFactory::default()), None)
            .await
            .expect("missing optional backend should not stop capability");
    }

    #[tokio::test]
    async fn wire_backend_failure_is_not_fatal() {
        let factory = Arc::new(FakeFactory {
            fail: true,
            ..Default::default()
        });
        let calls_ref = Arc::clone(&factory.calls);
        let ctx = PluginContext {
            registry: CapabilityExtensionRegistry::new(),
            plugin_routes: Arc::new(PluginRouteBook::new()),
            runtime: test_runtime(),
        };
        wire_runtime_adapters_with_factory(ctx, factory, None)
            .await
            .expect("unavailable optional backend should not stop capability");
        assert_eq!(calls_ref.lock().await.len(), 2);
    }
}
