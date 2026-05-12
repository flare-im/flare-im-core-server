//! 服务模块 - 包含服务启动、注册和管理相关功能

use anyhow::Result;
use flare_im_core::service_names::STORAGE_WRITER;
use tracing::info;

use flare_core_runtime::ServiceRuntime;

mod wire;

pub use wire::ApplicationContext;

/// 应用启动器
pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    /// 运行应用的主入口点
    pub async fn run() -> Result<()> {
        use flare_im_core::load_config;

        // 加载应用配置
        let app_config = load_config(Some("config"));

        // 使用 Wire 风格的依赖注入构建应用上下文
        let context = self::wire::initialize(app_config).await?;

        info!("ApplicationBootstrap created successfully");

        // 运行服务
        Self::run_with_context(context).await
    }

    /// 运行服务（带应用上下文）
    ///
    /// 使用 ServiceRuntime 管理消费者生命周期，支持优雅停机
    pub async fn run_with_context(context: ApplicationContext) -> Result<()> {
        info!(backend = %context.config.mq_backend, "Starting Storage Writer (MQ consumer via ServiceRuntime)");

        let mut runtime = ServiceRuntime::mq_consumer()
            .with_health_failure_action(flare_core_runtime::HealthFailureAction::GracefulShutdown);

        let tasks = match context.config.mq_backend.as_str() {
            "kafka" => flare_server_core::mq::kafka::build_kafka_consumer_tasks(
                context.config.as_ref(),
                context.consumer_config,
                context.dispatcher.clone(),
                "storage-kafka-consumer",
            )
            .map_err(|e| anyhow::anyhow!("create storage-writer kafka consumers: {}", e))?,
            "nats" | "jetstream" => flare_server_core::mq::nats::build_nats_consumer_tasks(
                context.config.as_ref(),
                context.consumer_config,
                context.dispatcher.clone(),
                "storage-nats-consumer",
            )
            .await
            .map_err(|e| anyhow::anyhow!("create storage-writer nats consumers: {}", e))?,
            other => anyhow::bail!("unsupported mq backend: {}", other),
        };

        for task in tasks {
            runtime = runtime.add_task(Box::new(task));
        }

        flare_im_core::health::attach_runtime_health_checks(runtime, STORAGE_WRITER)
            .run()
            .await
    }
}
