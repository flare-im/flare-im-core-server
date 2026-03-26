//! 服务模块 - 包含服务启动、注册和管理相关功能

use anyhow::Result;
use flare_im_core::service_names::STORAGE_WRITER;
use tracing::info;

use flare_server_core::runtime::ServiceRuntime;

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
        use flare_server_core::kafka::KafkaMessageFetcher;
        use flare_server_core::mq::consumer::ConsumerRuntimeTask;

        info!("Starting Storage Writer (Kafka consumer via ServiceRuntime)");

        let topics = context.dispatcher.topics();
        if topics.is_empty() {
            anyhow::bail!("storage-writer: no Kafka topics registered on dispatcher");
        }

        let fetcher = KafkaMessageFetcher::new_with_consumer_group(
            context.config.as_ref(),
            topics,
            context.consumer_config.kafka_consumer_group_override.as_deref(),
        )
        .map_err(|e| anyhow::anyhow!("create kafka fetcher: {}", e))?;

        let task = ConsumerRuntimeTask::from_parts(
            context.consumer_config,
            context.dispatcher.clone(),
            fetcher,
        );

        ServiceRuntime::new_consumer_only(STORAGE_WRITER)
            .add_mq_consumer_runtime("storage-kafka-consumer", task)
            .run()
            .await
    }
}
