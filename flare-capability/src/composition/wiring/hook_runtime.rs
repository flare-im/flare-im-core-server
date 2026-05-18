//! **Hook 运行时装配**：监控、适配器工厂、领域编排、应用 `Handler`、读侧 `CoreHookRegistry`、治理 gRPC。

use std::sync::Arc;

use crate::application::handler::HookCommandHandler;
use crate::composition::hook_registry::CoreHookRegistry;
use crate::composition::wiring::config_sources::ConfigSourcesReady;
use crate::domain::service::HookOrchestrationService;
use crate::infrastructure::adapters::HookAdapterFactory;
use crate::infrastructure::monitoring::{ExecutionRecorder, MetricsCollector};
use crate::interface::grpc::HookServiceServer;

/// Hook 侧已装配的 gRPC 与治理服务。
pub(crate) struct HookRuntimeReady {
    pub command_handler: Arc<HookCommandHandler>,
    pub registry: Arc<CoreHookRegistry>,
    pub adapter_factory: Arc<HookAdapterFactory>,
    pub hook_governance: Option<Arc<HookServiceServer>>,
}

pub(crate) fn build_hook_runtime(sources: &ConfigSourcesReady) -> HookRuntimeReady {
    let metrics_collector = Arc::new(MetricsCollector::new());
    let execution_recorder = Arc::new(ExecutionRecorder::new());
    let adapter_factory = Arc::new(HookAdapterFactory::new());
    let orchestration_service = Arc::new(HookOrchestrationService);
    let command_handler = Arc::new(HookCommandHandler::new(orchestration_service));
    let registry = Arc::new(CoreHookRegistry::new(sources.watcher.clone()));

    let hook_governance = sources.hook_config_repository.as_ref().map(|repository| {
        Arc::new(
            HookServiceServer::new(repository.clone(), registry.clone())
                .with_monitoring(metrics_collector.clone(), execution_recorder.clone()),
        )
    });

    if hook_governance.is_none() {
        tracing::warn!("database_url not set: Hook governance (Administer) unavailable");
    }

    HookRuntimeReady {
        command_handler,
        registry,
        adapter_factory,
        hook_governance,
    }
}
