use flare_im_contracts::service_names::PUSH_SERVER;
use flare_server_core::error::Result;
use tracing::info;

use crate::service::wire::{self, ApplicationContext};

pub struct ApplicationBootstrap;

impl ApplicationBootstrap {
    pub async fn run() -> Result<()> {
        Self::run_with_shutdown_signals(Vec::new()).await
    }

    pub async fn run_with_shutdown_signals(
        signals: flare_im_service_kit::RuntimeShutdownSignals,
    ) -> Result<()> {
        let app_config = flare_im_service_kit::load_app_config_from_env();
        let service_config = app_config.push_server_service();
        let runtime = flare_im_service_kit::build_background_service_runtime(
            app_config,
            &service_config.runtime,
            PUSH_SERVER,
        );
        let ctx = wire::initialize(app_config).await?;
        Self::run_with_runtime(ctx, runtime, signals).await
    }

    pub async fn run_with_context(context: ApplicationContext) -> Result<()> {
        Self::run_with_runtime(
            context,
            flare_im_service_kit::background_service_runtime(PUSH_SERVER),
            Vec::new(),
        )
        .await
    }

    async fn run_with_runtime(
        context: ApplicationContext,
        mut runtime: flare_core_runtime::ServiceRuntime,
        signals: flare_im_service_kit::RuntimeShutdownSignals,
    ) -> Result<()> {
        info!(
            "Starting Push Server (push-request -> push-online/push-offline) via ServiceRuntime..."
        );

        let tasks = match context.config.mq_backend.as_str() {
            "kafka" => flare_server_core::mq::kafka::build_kafka_consumer_tasks(
                context.config.as_ref(),
                context.consumer_config,
                context.dispatcher.clone(),
                "push-server-consumer",
            )
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!(
                    "create push-server kafka consumers: {}",
                    e
                ))
            })?,
            "nats" | "jetstream" => flare_server_core::mq::nats::build_nats_consumer_tasks(
                context.config.as_ref(),
                context.consumer_config,
                context.dispatcher.clone(),
                "push-server-consumer",
            )
            .await
            .map_err(|e| {
                flare_server_core::error::FlareError::system(format!(
                    "create push-server nats consumers: {}",
                    e
                ))
            })?,
            other => {
                return Err(flare_server_core::error::FlareError::system(format!(
                    "unsupported mq backend: {other}"
                )));
            }
        };

        for task in tasks {
            runtime = runtime.add_task(Box::new(task));
        }

        runtime
            .run_with_signals(signals)
            .await
            .map_err(flare_server_core::error::FlareError::from)
    }
}
