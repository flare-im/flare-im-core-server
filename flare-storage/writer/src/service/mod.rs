//! 服务模块 - 包含服务启动、注册和管理相关功能

use flare_im_contracts::service_names::STORAGE_WRITER;
use flare_server_core::error::Result;
use tracing::info;

mod wire;

pub use wire::ApplicationContext;

/// 应用启动器
pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    /// 运行应用的主入口点
    pub async fn run() -> Result<()> {
        let app_config = flare_im_service_kit::load_app_config_from_env();
        let service_config = app_config.storage_writer_service();
        let runtime = flare_im_service_kit::build_background_service_runtime(
            app_config,
            &service_config.runtime,
            STORAGE_WRITER,
        );

        // 使用 Wire 风格的依赖注入构建应用上下文
        let context = self::wire::initialize(app_config).await?;

        info!("ApplicationBootstrap created successfully");

        // 运行服务
        Self::run_with_runtime(context, runtime).await
    }

    /// 运行服务（带应用上下文）
    ///
    /// 使用 ServiceRuntime 管理消费者生命周期，支持优雅停机
    pub async fn run_with_context(context: ApplicationContext) -> Result<()> {
        Self::run_with_runtime(
            context,
            flare_im_service_kit::background_service_runtime(STORAGE_WRITER),
        )
        .await
    }

    async fn run_with_runtime(
        context: ApplicationContext,
        mut runtime: flare_core_runtime::ServiceRuntime,
    ) -> Result<()> {
        info!(backend = %context.config.mq_backend, "Starting Storage Writer (MQ consumer via ServiceRuntime)");

        if context.config.metrics.enabled {
            let metrics_config = context.config.metrics.clone();
            runtime = runtime.add_spawn_with_shutdown(
                "storage-writer-metrics",
                move |shutdown_rx| async move {
                    flare_im_service_kit::metrics::serve_prometheus_metrics(
                        metrics_config,
                        shutdown_rx,
                    )
                    .await
                },
            );
        }

        let mut tasks = match context.config.mq_backend.as_str() {
            "kafka" => {
                flare_server_core::mq::kafka::build_kafka_consumer_tasks_with_failure_publishers(
                    context.config.as_ref(),
                    context.consumer_config.clone(),
                    context.dispatcher.clone(),
                    "storage-kafka-consumer",
                    context.failure_publishers.clone(),
                )
                .map_err(|e| {
                    flare_server_core::error::FlareError::system(format!(
                        "create storage-writer kafka consumers: {}",
                        e
                    ))
                })?
            }
            "nats" => {
                flare_server_core::mq::nats::build_nats_consumer_tasks_with_failure_publishers(
                    context.config.as_ref(),
                    context.consumer_config.clone(),
                    context.dispatcher.clone(),
                    "storage-nats-consumer",
                    context.failure_publishers.clone(),
                )
                .await
                .map_err(|e| {
                    flare_server_core::error::FlareError::system(format!(
                        "create storage-writer nats consumers: {}",
                        e
                    ))
                })?
            }
            other => {
                return Err(flare_server_core::error::FlareError::system(format!(
                    "unsupported mq backend: {other}"
                )));
            }
        };

        if context.config.mq_backend.as_str() == "kafka"
            && let Some(dispatcher) = context.retry_forwarder_dispatcher.clone()
        {
            let retry_tasks =
                flare_server_core::mq::kafka::build_kafka_consumer_tasks_with_failure_publishers(
                    context.config.as_ref(),
                    context
                        .consumer_config
                        .clone()
                        .with_ordered(false)
                        .with_batch_size(1),
                    dispatcher,
                    "storage-retry-forwarder",
                    context.failure_publishers.clone(),
                )
                .map_err(|e| {
                    flare_server_core::error::FlareError::system(format!(
                        "create storage-writer kafka retry-forwarder consumers: {}",
                        e
                    ))
                })?;
            tasks.extend(retry_tasks);
        }

        for task in tasks {
            runtime = runtime.add_task(Box::new(task));
        }

        runtime
            .run()
            .await
            .map_err(flare_server_core::error::FlareError::from)
    }
}
