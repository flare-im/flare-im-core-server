//! **Hook 运行时装配**：执行守卫（熔断 + 执行证据）、适配器工厂、领域编排、应用 `Handler`、读侧
//! `CoreHookRegistry`、治理 gRPC。

use std::sync::Arc;

use crate::application::handler::{HookCommandHandler, HookQueryHandler};
use crate::composition::hook_registry::CoreHookRegistry;
use crate::composition::wiring::config_sources::ConfigSourcesReady;
use crate::domain::repository::HookExecutionRepository;
use crate::domain::service::{HookExecutionGuard, HookExecutionObserver, HookOrchestrationService};
use crate::infrastructure::adapters::HookAdapterFactory;
use crate::infrastructure::config::HookExecutionSettings;
use crate::infrastructure::hook_execution::{
    HookExecutionBuffer, HookExecutionMetrics, HookExecutionTasks, MetricsOnlyHookExecutions,
};
use crate::infrastructure::persistence::PostgresHookExecutionRepository;
use crate::interface::grpc::HookServiceServer;

/// 执行证据的后台任务所需（有库且开启记录时）：由启动器挂到进程运行时上。
pub(crate) struct HookExecutionBackground {
    pub buffer: Arc<HookExecutionBuffer>,
    pub repository: Arc<dyn HookExecutionRepository>,
    pub metrics: Arc<HookExecutionMetrics>,
    pub tasks: HookExecutionTasks,
}

/// Hook 侧已装配的 gRPC 与治理服务。
pub(crate) struct HookRuntimeReady {
    pub command_handler: Arc<HookCommandHandler>,
    pub registry: Arc<CoreHookRegistry>,
    pub adapter_factory: Arc<HookAdapterFactory>,
    pub hook_governance: Option<Arc<HookServiceServer>>,
    /// 每个执行计划都挂上：(租户, Hook) 熔断与执行记录。
    pub execution_guard: Arc<HookExecutionGuard>,
    pub execution_background: Option<HookExecutionBackground>,
}

pub(crate) fn build_hook_runtime(
    sources: &ConfigSourcesReady,
    settings: &HookExecutionSettings,
) -> HookRuntimeReady {
    let adapter_factory = Arc::new(HookAdapterFactory::new());
    let orchestration_service = Arc::new(HookOrchestrationService);
    let command_handler = Arc::new(HookCommandHandler::new(orchestration_service));
    let registry = Arc::new(CoreHookRegistry::new(sources.watcher.clone()));
    let metrics = HookExecutionMetrics::registered();

    let execution_repository: Option<Arc<dyn HookExecutionRepository>> =
        sources.hook_config_repository.as_ref().map(|repository| {
            Arc::new(PostgresHookExecutionRepository::new(
                repository.connection_pool(),
            )) as Arc<dyn HookExecutionRepository>
        });

    let execution_background = execution_repository
        .as_ref()
        .filter(|_| settings.executions.enabled)
        .map(|repository| HookExecutionBackground {
            buffer: Arc::new(HookExecutionBuffer::new(
                settings.executions.buffer_settings(),
                Arc::clone(&metrics),
            )),
            repository: Arc::clone(repository),
            metrics: Arc::clone(&metrics),
            tasks: settings.executions.tasks(),
        });
    let observer: Arc<dyn HookExecutionObserver> = match execution_background.as_ref() {
        Some(background) => background.buffer.clone(),
        None => {
            if execution_repository.is_some() {
                tracing::info!("hook execution recording disabled; circuit breakers stay on");
            }
            Arc::new(MetricsOnlyHookExecutions::new(Arc::clone(&metrics)))
        }
    };
    let execution_guard = Arc::new(HookExecutionGuard::new(settings.circuit, observer));

    let hook_governance = sources.hook_config_repository.as_ref().map(|repository| {
        let mut governance = HookServiceServer::new(repository.clone(), registry.clone());
        if let Some(executions) = execution_repository.as_ref() {
            governance =
                governance.with_executions(Arc::new(HookQueryHandler::new(Arc::clone(executions))));
        }
        Arc::new(match sources.invalidation.as_ref() {
            Some(invalidation) => governance.with_invalidation(Arc::clone(invalidation)),
            None => governance,
        })
    });

    if hook_governance.is_none() {
        tracing::warn!("database_url not set: Hook governance (Administer) unavailable");
    }

    HookRuntimeReady {
        command_handler,
        registry,
        adapter_factory,
        hook_governance,
        execution_guard,
        execution_background,
    }
}
